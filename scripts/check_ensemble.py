"""Exercise quorum and session ownership against the local three-server ensemble."""

import json
import logging
import socket
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

ROOT = Path(__file__).resolve().parents[1]
SERVERS = {"zk1": 2181, "zk2": 2182, "zk3": 2183}
HOSTS = ",".join(f"127.0.0.1:{port}" for port in SERVERS.values())
OUT = ROOT / "logs/ensemble"


def eventually(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.5)
    raise TimeoutError("The expected ensemble state was not reached")


def status(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
            connection.sendall(b"srvr")
            chunks = []
            while data := connection.recv(4096):
                chunks.append(data)
            return b"".join(chunks).decode("utf-8", errors="replace")
    except OSError as error:
        return str(error)


def roles():
    found = {}
    for name, port in SERVERS.items():
        output = status(port)
        if "Mode: leader" in output:
            found[name] = "leader"
        elif "Mode: follower" in output:
            found[name] = "follower"
    return found if len(found) == 3 and list(found.values()).count("leader") == 1 else None


def compose(*args, check=True):
    command = ["docker", "compose", *args]
    completed = subprocess.run(command, cwd=ROOT, text=True, encoding="utf-8",
                               errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    with (OUT / "commands.log").open("a", encoding="utf-8") as stream:
        stream.write("$ " + " ".join(command) + "\n" + completed.stdout + "\n")
    if check and completed.returncode:
        raise RuntimeError(completed.stdout)
    return completed.stdout


def write_probe(path, value):
    client = KazooClient(hosts=HOSTS, timeout=10, read_only=False)
    try:
        client.start(timeout=8)
        client.ensure_path(path)
        client.set(path, value)
        assert client.get(path)[0] == value
    finally:
        client.stop()
        client.close()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc).isoformat()
    (OUT / "commands.log").write_text(f"Ensemble check: {started}\n", encoding="utf-8")
    logging.basicConfig(level=logging.WARNING, filename=OUT / "client.log", filemode="w")
    summary = {"recorded_at_utc": started, "image": "zookeeper:3.9.6-jre-17"}
    probe = f"/lab4/ensemble/probe-{time.time_ns()}"
    observer = None
    cli = None
    try:
        compose("up", "-d", "--wait", "zk1", "zk2", "zk3")
        initial = eventually(roles)
        summary["initial_roles"] = initial
        for name, port in SERVERS.items():
            (OUT / f"{name}-initial.txt").write_text(status(port), encoding="utf-8")
            compose("exec", "-T", name, "cat", "/data/myid")
        compose("exec", "-T", "zk1", "java", "-version")
        compose("images")
        write_probe(probe, b"three servers")
        followers = [name for name, role in initial.items() if role == "follower"]
        compose("stop", followers[0])
        write_probe(probe, b"two servers")
        summary["one_server_stopped"] = {"server": followers[0], "write_read_succeeded": True}
        compose("stop", followers[1])
        remaining = next(name for name, role in initial.items() if role == "leader")
        eventually(lambda: "Mode:" not in status(SERVERS[remaining]), timeout=20)
        (OUT / "no-quorum.txt").write_text(status(SERVERS[remaining]), encoding="utf-8")
        client = KazooClient(hosts=HOSTS, timeout=4, read_only=False)
        unavailable = False
        try:
            client.start(timeout=5)
        except KazooTimeoutError:
            unavailable = True
        finally:
            client.stop()
            client.close()
        assert unavailable, "A read-write session unexpectedly survived without a majority"
        summary["two_servers_stopped"] = {"servers": followers, "read_write_session_unavailable": True}
        compose("ps", "-a")
        compose("start", *followers)
        summary["restored_roles"] = eventually(roles)
        write_probe(probe, b"restored quorum")
        observer = KazooClient(hosts=HOSTS, timeout=10)
        observer.start(timeout=10)
        ephemeral = f"/lab4/ensemble/ephemeral-{time.time_ns()}"
        command = ["docker", "compose", "exec", "-T", "zk1", "zkCli.sh", "-server", "zk2:2181"]
        cli = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
        cli.stdin.write(f"create -e {ephemeral} session-owned\n")
        cli.stdin.flush()
        created = eventually(lambda: observer.exists(ephemeral), timeout=20)
        assert created.ephemeralOwner != 0
        output, _ = cli.communicate("quit\n", timeout=20)
        eventually(lambda: observer.exists(ephemeral) is None, timeout=15)
        (OUT / "zkcli-ephemeral.log").write_text(
            "$ " + " ".join(command) + f"\n> create -e {ephemeral} session-owned\n> quit\n" + output,
            encoding="utf-8")
        summary["ephemeral"] = {"path": ephemeral, "created_in_zkcli": True,
                                "owner": created.ephemeralOwner, "absent_after_quit": True}
        summary["all_passed"] = True
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        if cli and cli.poll() is None:
            cli.kill()
        if observer:
            observer.stop()
            observer.close()
        compose("start", "zk1", "zk2", "zk3", check=False)
        for name in SERVERS:
            output = compose("logs", "--no-color", "--tail", "150", name, check=False)
            (OUT / f"{name}.log").write_text(output, encoding="utf-8")
        (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
                                          encoding="utf-8")


if __name__ == "__main__":
    main()
