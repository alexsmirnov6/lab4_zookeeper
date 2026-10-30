"""Elect a coordinator, resume chunk reduction and publish the result."""

import json
import os
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

from kazoo.exceptions import BadVersionError, NoNodeError, SessionExpiredError

from calculation import checkpoint_document, map_chunk, restore, result_document
from storage import BUCKET, commit_checked, encode, event, get_document, put_document, run_path, s3_client, zk_client


IDENTITY = os.environ.get("COORDINATOR_ID", "coordinator")


def submit(path, document):
    request = urllib.request.Request(os.environ["RESULT_RECEIVER"] + path,
                                     data=encode(document), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        details = json.load(error)
        if error.code == 409 and details.get("error") == "BadVersionError":
            raise BadVersionError("The receiver rejected a stale epoch") from error
        raise


def become_leader(zk, root):
    candidate = None
    while True:
        if json.loads(zk.get(root + "/result")[0]):
            return None
        if candidate is None or zk.exists(candidate) is None:
            candidate = zk.create(root + "/election/candidate-", IDENTITY.encode(),
                                  ephemeral=True, sequence=True)
            event(IDENTITY, "candidate", path=candidate)
        name = candidate.rsplit("/", 1)[1]
        children = sorted(zk.get_children(root + "/election"))
        if name not in children:
            candidate = None
            continue
        position = children.index(name)
        if position == 0:
            _, previous = zk.get(root + "/epoch")
            epoch = previous.version + 1
            token = {"epoch": epoch, "coordinator": IDENTITY, "time_unix_ns": time.time_ns()}
            transaction = zk.transaction()
            transaction.check(candidate, version=0)
            transaction.set_data(root + "/epoch", encode(token), version=previous.version)
            commit_checked(transaction)
            event(IDENTITY, "became_leader", **token)
            return epoch
        wake = threading.Event()
        predecessor = root + "/election/" + children[position - 1]
        if zk.exists(predecessor, watch=lambda change, signal=wake: signal.set()):
            wake.wait(timeout=0.5)


def calculate(zk, root, epoch):
    s3 = s3_client()
    source = json.loads(zk.get(root + "/input")[0])
    chunk_bytes = int(os.environ.get("CHUNK_BYTES", "65536"))
    delay = float(os.environ.get("CHUNK_DELAY", "0"))
    size = s3.head_object(Bucket=BUCKET, Key=source["key"])["ContentLength"]
    tasks = [(i, source["key"], start, min(size - 1, start + chunk_bytes - 1), size)
             for i, start in enumerate(range(0, size, chunk_bytes))]
    checkpoint = json.loads(zk.get(root + "/checkpoint")[0])
    counter, rows, completed = Counter(), 0, set()
    if checkpoint:
        saved = get_document(s3, checkpoint["key"], checkpoint["sha256"])
        counter, rows, completed = restore(saved, source["sha256"], chunk_bytes, len(tasks))
        event(IDENTITY, "checkpoint_loaded", completed=len(completed), vacancies=rows,
              source_coordinator=checkpoint["coordinator"])
    remaining = [task for task in tasks if task[0] not in completed]
    writable = True
    with ProcessPoolExecutor(max_workers=2) as pool:
        pending = [pool.submit(map_chunk, task) for task in remaining]
        for future in as_completed(pending):
            index, count, values = future.result()
            if index in completed:
                raise RuntimeError("A chunk was reduced twice")
            counter.update(values)
            rows += count
            completed.add(index)
            if writable:
                key = f"{os.environ['RUN_ID']}/checkpoints/{IDENTITY}-{epoch}-{len(completed)}.json"
                saved = checkpoint_document(counter, rows, completed, source["sha256"], chunk_bytes)
                checksum = put_document(s3, key, saved)
                try:
                    submit("/checkpoint", {"coordinator": IDENTITY, "epoch": epoch,
                                           "key": key, "sha256": checksum, "completed": len(completed)})
                    event(IDENTITY, "checkpoint_committed", completed=len(completed), vacancies=rows)
                except BadVersionError:
                    writable = False
                    event(IDENTITY, "checkpoint_fenced", epoch=epoch)
            # Slow only the experiment so that the process can be paused mid-reduction.
            time.sleep(delay)
    document = result_document(counter, rows)
    key = f"{os.environ['RUN_ID']}/results/{IDENTITY}-{epoch}.json"
    checksum = put_document(s3, key, document)
    event(IDENTITY, "calculation_complete", completed=len(completed), vacancies=rows,
          unique=document["unique_combinations"], sha256=checksum)
    try:
        answer = submit("/publish", {"coordinator": IDENTITY, "epoch": epoch,
                                    "key": key, "sha256": checksum})
        event(IDENTITY, "publication_accepted", **answer)
    except BadVersionError:
        event(IDENTITY, "publication_rejected", epoch=epoch, error="BadVersionError")
    finally:
        s3.close()


def main():
    root = run_path()
    zk = zk_client()
    zk.add_listener(lambda state: event(IDENTITY, "session_state", state=state))
    try:
        zk.start(timeout=30)
        event(IDENTITY, "started", requested_session_timeout_seconds=10)
        epoch = become_leader(zk, root)
        if epoch is None:
            event(IDENTITY, "already_complete")
            return
        # A paused worker can finish an old computation; the receiver owns the final gate.
        calculate(zk, root, epoch)
    except (NoNodeError, SessionExpiredError, BadVersionError) as error:
        event(IDENTITY, "leadership_lost", error=type(error).__name__)
        raise
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
