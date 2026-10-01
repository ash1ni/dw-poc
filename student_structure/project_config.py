"""Student Structure pipeline configuration (loads repo .env)."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_DIR.parent

_env_file = REPO_ROOT / ".env"
if _env_file.is_file():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)

BRONZE_ROOT = Path(os.getenv("UDISE_BRONZE_ROOT", str(REPO_ROOT / "udise_data" / "bronze"))).resolve()
DORIS_REPLICATION_NUM = int(os.getenv("DORIS_REPLICATION_NUM", "1"))

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-layer")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "udise").rstrip("/")

AWS_ENDPOINT_URL_S3 = os.getenv("AWS_ENDPOINT_URL_S3")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
