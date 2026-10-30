"""Pair MapReduce adapted from lab3/src/mapreduce_s3.py (91d0773)."""

import itertools
import json
from collections import Counter

from storage import BUCKET, s3_client


def read_complete_lines(s3, bucket, key, start, end, size):
    """Return records whose first byte belongs to the inclusive byte range."""
    if size == 0:
        return b""
    if not 0 <= start <= end < size:
        raise ValueError("Expected 0 <= start <= end < object size")

    def fetch(left, right):
        response = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={left}-{right}")
        with response["Body"] as stream:
            return stream.read()

    body = bytearray(fetch(max(0, start - 1), end))
    position = end + 1
    while body[-1:] != b"\n" and position < size:
        block = fetch(position, min(size - 1, position + 65535))
        if not block:
            raise IOError(f"Unexpected end of S3 response at byte {position}")
        newline = block.find(b"\n")
        body.extend(block if newline < 0 else block[:newline + 1])
        position += len(block)
        if newline >= 0:
            break
    if start > 0:
        newline = body.find(b"\n")
        return bytes(body[newline + 1:]) if newline >= 0 else b""
    return bytes(body)


def map_chunk(task):
    index, key, start, end, size = task
    s3 = s3_client()
    try:
        body = read_complete_lines(s3, BUCKET, key, start, end, size)
    finally:
        s3.close()
    counter = Counter()
    rows = 0
    for line in body.splitlines():
        if not line.strip():
            continue
        skills = json.loads(line)["skills"]
        if not isinstance(skills, list) or any(not isinstance(skill, str) for skill in skills):
            raise ValueError(f"Invalid skills in chunk {index}")
        counter.update(itertools.combinations(sorted(set(skills)), 2))
        rows += 1
    return index, rows, counter


def records(counter):
    return [{"skills": list(skills), "count": count}
            for skills, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))]


def result_document(counter, rows):
    return {"variant": 11, "segment": "DevOps инженер", "stack_size": 2,
            "vacancies": rows, "unique_combinations": len(counter), "counts": records(counter)}


def checkpoint_document(counter, rows, completed, input_sha256, chunk_bytes):
    return {"schema": 1, "input_sha256": input_sha256, "chunk_bytes": chunk_bytes,
            "completed": sorted(completed), "vacancies": rows, "counts": records(counter)}


def restore(document, input_sha256, chunk_bytes, chunks):
    if (document["schema"] != 1 or document["input_sha256"] != input_sha256
            or document["chunk_bytes"] != chunk_bytes):
        raise ValueError("Checkpoint belongs to a different input or partition")
    completed = document["completed"]
    if len(set(completed)) != len(completed) or any(i not in range(chunks) for i in completed):
        raise ValueError("Checkpoint contains invalid chunk IDs")
    counter = Counter({tuple(item["skills"]): item["count"] for item in document["counts"]})
    return counter, document["vacancies"], set(completed)
