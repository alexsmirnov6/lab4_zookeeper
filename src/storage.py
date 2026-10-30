"""Shared serialization, S3 access and structured event logging."""

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from kazoo.client import KazooClient

BUCKET = "hh-lake"


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def run_path():
    run_id = os.environ["RUN_ID"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("RUN_ID may contain letters, digits, underscores and hyphens")
    return "/lab4/runs/" + run_id


def s3_client(endpoint=None):
    return boto3.client(
        "s3", endpoint_url=endpoint or os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"], region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3},
                      s3={"addressing_style": "path"}),
    )


def zk_client():
    return KazooClient(hosts=os.environ["ZK_HOSTS"], timeout=10, read_only=False)


def commit_checked(transaction):
    results = transaction.commit()
    # Kazoo returns transaction failures as values rather than raising them.
    for result in results:
        if isinstance(result, Exception):
            raise result
    return results


def event(component, name, **fields):
    record = {"time": datetime.now(timezone.utc).isoformat(), "component": component,
              "event": name, **fields}
    line = json.dumps(record, ensure_ascii=False)
    folder = Path(os.environ.get("LOG_DIR", "logs"))
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / f"{component}.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")
    print(line, flush=True)


def put_document(s3, key, document):
    payload = encode(document)
    checksum = digest(payload)
    s3.put_object(Bucket=BUCKET, Key=key, Body=payload, ContentType="application/json",
                  Metadata={"sha256": checksum})
    return checksum


def get_document(s3, key, checksum):
    response = s3.get_object(Bucket=BUCKET, Key=key)
    with response["Body"] as stream:
        payload = stream.read()
    if digest(payload) != checksum:
        raise ValueError(f"SHA-256 mismatch: {key}")
    return json.loads(payload)
