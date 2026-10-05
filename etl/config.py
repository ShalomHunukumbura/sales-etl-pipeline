from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REJECTED_DIR = DATA_DIR / "rejected"
LOG_DIR = PROJECT_ROOT / "logs"

load_dotenv(PROJECT_ROOT / ".env")


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    def dsn(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True)
class S3Config:
    enabled: bool
    bucket: str | None
    prefix: str
    region: str | None


@dataclass(frozen=True)
class Settings:
    postgres: PostgresConfig
    s3: S3Config
    log_level: str


def get_settings() -> Settings:
    password = os.getenv("PG_PASSWORD")
    if not password:
        raise RuntimeError("PG_PASSWORD is not set. Copy .env.example to .env and fill it in.")

    return Settings(
        postgres=PostgresConfig(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5434")),
            database=os.getenv("PG_DATABASE", "sales_dw"),
            user=os.getenv("PG_USER", "etl_user"),
            password=password,
        ),
        s3=S3Config(
            enabled=_bool(os.getenv("S3_ENABLED"), default=False),
            bucket=os.getenv("S3_BUCKET") or None,
            prefix=os.getenv("S3_PREFIX", "sales-etl").strip("/"),
            region=os.getenv("AWS_REGION") or None,
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
