"""Prove the pipeline's IAM user is least-privilege.

Uses the same credentials as the pipeline (from .env) and tries a few actions.
The allowed ones should succeed; everything else should fail with AccessDenied.

    python scripts/check_s3_permissions.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

from etl.config import get_settings  # noqa: E402


def attempt(label: str, expected: str, fn) -> bool:
    try:
        fn()
        result = "allowed"
    except ClientError as exc:
        result = "denied" if exc.response["Error"]["Code"] == "AccessDenied" else exc.response["Error"]["Code"]
    ok = result == expected
    print(f"  {'✅' if ok else '❌'} {label:<42} {result.upper():<8} (expected {expected})")
    return ok


def main() -> int:
    cfg = get_settings().s3
    if not cfg.bucket:
        print("S3_BUCKET is not set in .env")
        return 1

    s3 = boto3.client("s3", region_name=cfg.region)
    b, p = cfg.bucket, cfg.prefix
    print(f"Checking IAM permissions for s3://{b}/{p}/ ...")

    checks = [
        (f"list objects under {p}/", "allowed", lambda: s3.list_objects_v2(Bucket=b, Prefix=f"{p}/raw/")),
        (f"delete an object under {p}/", "denied", lambda: s3.delete_object(Bucket=b, Key=f"{p}/permission-test")),
        ("write outside the pipeline prefix", "denied", lambda: s3.put_object(Bucket=b, Key="other/x", Body=b"x")),
        ("list the whole bucket", "denied", lambda: s3.list_objects_v2(Bucket=b)),
        ("list all buckets in the account", "denied", lambda: s3.list_buckets()),
    ]
    results = [attempt(*c) for c in checks]
    print("Least privilege verified." if all(results) else "Some permissions are not as expected!")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
