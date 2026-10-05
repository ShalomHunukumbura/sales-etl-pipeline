from __future__ import annotations

import hashlib
import logging
from datetime import date
from pathlib import Path

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from etl.config import S3Config

log = logging.getLogger("s3")


class S3Error(RuntimeError):
    pass


class S3Store:
    def __init__(self, cfg: S3Config, run_id: str, ingest_date: date | None = None):
        if not cfg.bucket:
            raise S3Error("S3_ENABLED=true but S3_BUCKET is not set")
        self.cfg = cfg
        self.run_id = run_id
        self.ingest_date = (ingest_date or date.today()).isoformat()
        self.client = boto3.client(
            "s3",
            region_name=cfg.region,
            # standard mode = exponential backoff + retry on throttling / 5xx
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    def key_for(self, zone: str, filename: str) -> str:
        return f"{self.cfg.prefix}/{zone}/ingest_date={self.ingest_date}/run_id={self.run_id}/{filename}"

    def upload(self, local_path: Path, zone: str, filename: str | None = None) -> str:
        key = self.key_for(zone, filename or local_path.name)
        md5 = hashlib.md5(local_path.read_bytes()).hexdigest()
        try:
            self.client.upload_file(
                str(local_path),
                self.cfg.bucket,
                key,
                ExtraArgs={
                    "ServerSideEncryption": "AES256",
                    "Metadata": {"run-id": self.run_id, "source-md5": md5},
                },
            )
        except NoCredentialsError as exc:
            raise S3Error("No AWS credentials found. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env") from exc
        except (ClientError, BotoCoreError, S3UploadFailedError) as exc:
            raise S3Error(f"Upload of {local_path.name} to s3://{self.cfg.bucket}/{key} failed: {exc}") from exc

        uri = f"s3://{self.cfg.bucket}/{key}"
        log.info("Uploaded %-28s -> %s (%s KB)", local_path.name, uri, f"{local_path.stat().st_size / 1024:,.0f}")
        return uri

    def list_run_objects(self) -> list[tuple[str, int]]:
        """List every object written by this run (used to verify the uploads)."""
        objects = []
        for zone in ("raw", "processed", "rejected"):
            resp = self.client.list_objects_v2(Bucket=self.cfg.bucket, Prefix=self.key_for(zone, ""))
            objects += [(o["Key"], o["Size"]) for o in resp.get("Contents", [])]
        return objects
