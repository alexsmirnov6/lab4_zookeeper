"""Local environment and Docker Compose helpers for the experiment scripts."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def environment():
    path = ROOT / ".env"
    if not path.exists():
        raise FileNotFoundError("Run scripts/prepare_data.py first")
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            name, value = line.split("=", 1)
            if name not in ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY"):
                raise ValueError(f"Unexpected setting: {name}")
            os.environ[name] = value
    os.environ.setdefault("RUN_ID", "setup")


def compose(*args, log=None, check=True):
    command = ["docker", "compose", "-f", "compose.yaml", "-f", "compose.app.yaml", *args]
    output = subprocess.run(command, cwd=ROOT, text=True, encoding="utf-8", errors="replace",
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if log:
        with Path(log).open("a", encoding="utf-8") as stream:
            stream.write("$ " + " ".join(command) + "\n" + output.stdout + "\n")
    if output.returncode and check:
        raise RuntimeError(output.stdout)
    return output.stdout


def until(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError("The experiment did not reach the expected state")


def read_events(folder, component):
    path = Path(folder) / f"{component}.jsonl"
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            # A writer may still be appending the last line.
            continue
    return events
