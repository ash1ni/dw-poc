"""Minimal Doris SQL + Stream Load helpers for Student Structure."""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Sequence

try:
    import pymysql
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install pymysql to use student_structure Doris I/O") from exc


def _settings() -> dict[str, Any]:
    host = os.getenv("DORIS_HOST", "127.0.0.1")
    port = int(os.getenv("DORIS_PORT", "9030"))
    user = os.getenv("DORIS_USER", "root")
    password = os.getenv("DORIS_PASSWORD", "")
    database = os.getenv("DORIS_DATABASE", "udise_silver")
    fe_http_port = int(os.getenv("DORIS_HTTP_PORT", "8030"))
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "fe_http_port": fe_http_port,
    }


def connect():
    cfg = _settings()
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def query(conn, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params or ())
        if cursor.description:
            return list(cursor.fetchall())
        return []


def healthy(conn) -> None:
    rows = query(conn, "SELECT 1 AS ok")
    if not rows or rows[0].get("ok") != 1:
        raise RuntimeError("Doris health check failed")
    backends = query(conn, "SHOW BACKENDS")
    if not backends:
        raise RuntimeError("Doris SHOW BACKENDS returned no rows")


def stream_file(
    database: str,
    table: str,
    path: Path,
    label: str,
    expected_rows: int,
    columns: Sequence[str],
    *,
    file_format: str = "parquet",
) -> None:
    cfg = _settings()
    url = (
        f"http://{cfg['host']}:{cfg['fe_http_port']}/api/"
        f"{database}/{table}/_stream_load"
    )
    auth = base64.b64encode(f"{cfg['user']}:{cfg['password']}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Expect": "100-continue",
        "label": label,
        "format": file_format,
        "columns": ",".join(columns),
        "max_filter_ratio": "0",
    }
    body = path.read_bytes()
    request = urllib.request.Request(url, data=body, method="PUT", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Stream load HTTP error for {database}.{table}: {detail}") from exc

    status = str(payload.get("Status", "")).upper()
    if status not in {"SUCCESS", "OK"}:
        raise RuntimeError(f"Stream load failed for {database}.{table}: {payload}")
    loaded = int(payload.get("NumberLoadedRows", expected_rows))
    if loaded != expected_rows:
        raise RuntimeError(
            f"Stream load row mismatch for {database}.{table}: "
            f"expected={expected_rows:,}, loaded={loaded:,}, response={payload}"
        )
