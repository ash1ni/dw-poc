"""Teacher Structure pipeline configuration (loads repo .env)."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_env_file(REPO_ROOT / ".env")
_load_env_file(PROJECT_DIR / ".env")


def _resolve_bronze_root() -> Path:
    default = REPO_ROOT / "udise_data" / "bronze"
    raw = (os.getenv("UDISE_BRONZE_ROOT") or "").strip()
    if not raw or "absolute/path" in raw.replace("\\", "/") or raw.startswith("/absolute/"):
        return default.resolve()
    return Path(raw).expanduser().resolve()


BRONZE_ROOT = _resolve_bronze_root()
DORIS_REPLICATION_NUM = int(os.getenv("DORIS_REPLICATION_NUM", "1"))

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-layer")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "udise").rstrip("/")

AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
