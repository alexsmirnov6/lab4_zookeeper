"""Upload the unchanged lab3 input into the experiment's RustFS instance."""

import argparse
import json
import secrets
from pathlib import Path

from runtime import ROOT, compose, environment
from storage import BUCKET, digest, s3_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path,
                        default=ROOT.parent / "lab3/data/prepared/devops_vacancies.ndjson")
    args = parser.parse_args()
    reference = json.loads((ROOT / "reference/lab3.json").read_text(encoding="utf-8"))
    data = args.source.read_bytes()
    checksum = digest(data)
    if checksum != reference["input_sha256"]:
        raise ValueError("The input SHA-256 differs from lab3")
    env_file = ROOT / ".env"
    if not env_file.exists():
        env_file.write_text("RUSTFS_ACCESS_KEY=lab4-local\nRUSTFS_SECRET_KEY=" +
                            secrets.token_hex(24) + "\n", encoding="utf-8")
    environment()
    compose("up", "-d", "--wait", "rustfs")
    s3 = s3_client("http://127.0.0.1:9100")
    try:
        if BUCKET not in {b["Name"] for b in s3.list_buckets()["Buckets"]}:
            s3.create_bucket(Bucket=BUCKET)
        key = f"inputs/{checksum}.ndjson"
        s3.put_object(Bucket=BUCKET, Key=key, Body=data, Metadata={"sha256": checksum})
        with s3.get_object(Bucket=BUCKET, Key=key)["Body"] as stream:
            assert digest(stream.read()) == checksum
        state = {"key": key, "sha256": checksum, "bytes": len(data)}
        (ROOT / ".state").mkdir(exist_ok=True)
        (ROOT / ".state/input.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(state, indent=2))
    finally:
        s3.close()


if __name__ == "__main__":
    main()
