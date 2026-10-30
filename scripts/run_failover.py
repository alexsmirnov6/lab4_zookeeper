"""Pause the whole leader container and verify recovery and publication fencing."""

import argparse
import json
import os
import time
from datetime import datetime, timezone

from kazoo.client import KazooClient

from runtime import ROOT, compose, environment, read_events, until
from storage import digest, encode, get_document, s3_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fencing", type=int, choices=(0, 1), required=True)
    parser.add_argument("--pause-seconds", type=float, default=40)
    args = parser.parse_args()
    if args.pause_seconds < 40:
        parser.error("Use at least 40 seconds to reproduce the assignment")
    environment()
    run_id = f"fencing-{args.fencing}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{time.time_ns() % 1000000}"
    folder = ROOT / "logs" / run_id
    folder.mkdir(parents=True)
    os.environ.update(RUN_ID=run_id, FENCING=str(args.fencing), APP_LOG_DIR="./logs/" + run_id)
    root = "/lab4/runs/" + run_id
    source = json.loads((ROOT / ".state/input.json").read_text(encoding="utf-8"))
    reference = json.loads((ROOT / "reference/lab3.json").read_text(encoding="utf-8"))
    summary = {"run_id": run_id, "fencing": bool(args.fencing), "input": source,
               "requested_session_timeout_seconds": 10}
    log = folder / "commands.log"
    zk = KazooClient(hosts="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", timeout=10)
    s3 = None
    leader = None
    paused = False
    try:
        compose("up", "-d", "--wait", "zk1", "zk2", "zk3", "rustfs", log=log)
        zk.start(timeout=20)
        zk.ensure_path(root + "/election")
        zk.ensure_path(root + "/publications")
        for child, body in (("epoch", b"{}"), ("checkpoint", b"{}"), ("result", b"{}"), ("input", encode(source))):
            zk.create(root + "/" + child, body)
        compose("up", "-d", "--build", "--wait", "receiver", log=log)
        compose("up", "-d", "coordinator-a", "coordinator-b", log=log)

        def current_epoch():
            data = json.loads(zk.get(root + "/epoch")[0])
            return data if data.get("epoch") else None

        initial = until(current_epoch)
        leader = initial["coordinator"]
        backup = "coordinator-b" if leader == "coordinator-a" else "coordinator-a"
        until(lambda: json.loads(zk.get(root + "/checkpoint")[0]).get("completed", 0) >= 1)
        compose("pause", leader, log=log)
        paused = True
        pause_started = time.monotonic()
        pause_time = datetime.now(timezone.utc).isoformat()

        def replaced():
            value = current_epoch()
            return value if value and value["coordinator"] == backup else None

        promoted = until(replaced, timeout=30)
        elapsed = time.monotonic() - pause_started
        until(lambda: any(e["event"] == "записал результат" and e["coordinator"] == backup
                          for e in read_events(folder, "receiver")), timeout=40)
        load = next(e for e in read_events(folder, backup) if e["event"] == "checkpoint_loaded")
        assert load["completed"] > 0 and load["vacancies"] > 0
        chunk_count = (source["bytes"] + 65535) // 65536
        assert load["completed"] < chunk_count, "The backup did not have any remaining chunks"
        while time.monotonic() - pause_started < args.pause_seconds:
            time.sleep(0.1)
        compose("unpause", leader, log=log)
        paused = False
        until(lambda: any(e["event"] in ("publication_accepted", "publication_rejected")
                          for e in read_events(folder, leader)), timeout=70)
        events = read_events(folder, "receiver")
        accepted = [e for e in events if e["event"] == "записал результат"]
        rejected = [e for e in events if e["event"] == "publication_rejected"]
        pointer_data, pointer_stat = zk.get(root + "/result")
        pointer = json.loads(pointer_data)
        expected_count = 1 if args.fencing else 2
        assert len(accepted) == expected_count
        assert len(zk.get_children(root + "/publications")) == expected_count
        assert pointer_stat.version == expected_count
        assert pointer["coordinator"] == (backup if args.fencing else leader)
        assert len(rejected) == args.fencing
        if rejected:
            assert rejected[0]["error"] == "BadVersionError"
        s3 = s3_client("http://127.0.0.1:9100")
        hashes = {}
        for coordinator in (leader, backup):
            completed = next(e for e in read_events(folder, coordinator) if e["event"] == "calculation_complete")
            document = get_document(s3, f"{run_id}/results/{coordinator}-" +
                                    str(initial["epoch"] if coordinator == leader else promoted["epoch"]) + ".json",
                                    completed["sha256"])
            checksum = digest(encode(document))
            assert checksum == reference["pairs_canonical_sha256"]
            assert document["vacancies"] == reference["vacancies"]
            assert document["unique_combinations"] == reference["unique_pairs"]
            assert sum(row["count"] for row in document["counts"]) == reference["total_pairs"]
            hashes[coordinator] = checksum
        summary.update(
            leader=leader, backup=backup, pause_confirmed_at_utc=pause_time,
            leadership_change_seconds=round(elapsed, 3), measurement="monotonic host clock; observation polling 0.1 seconds",
            checkpoint_chunks_loaded=load["completed"], checkpoint_vacancies_loaded=load["vacancies"],
            accepted_publications=len(accepted), rejected_publications=len(rejected),
            pointer_version=pointer_stat.version, pointer=pointer, result_sha256=hashes, all_passed=True,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        if paused:
            compose("unpause", leader, log=log, check=False)
        compose("logs", "--no-color", "receiver", "coordinator-a", "coordinator-b", log=log, check=False)
        compose("stop", "coordinator-a", "coordinator-b", "receiver", log=log, check=False)
        zk.stop()
        zk.close()
        if s3:
            s3.close()
        (folder / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
