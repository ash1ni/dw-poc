"""Minimal Doris SQL + Stream Load helpers (same contract as student_structure)."""
from __future__ import annotations

import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Sequence

try:
    import pymysql
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install pymysql to use facility_structure Doris I/O") from exc


def _settings() -> dict[str, Any]:
    host = os.getenv("DORIS_HOST", "127.0.0.1")
    port = int(os.getenv("DORIS_PORT", "9030"))
    user = os.getenv("DORIS_USER", "root")
    password = os.getenv("DORIS_PASSWORD", "")
    database = os.getenv("DORIS_DATABASE", "udise_silver")
    # If query port is remapped (e.g. 19030), HTTP is often remapped similarly (18030).
    default_http = "18030" if port == 19030 else "8030"
    fe_http_port = int(os.getenv("DORIS_HTTP_PORT", default_http))
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "fe_http_port": fe_http_port,
    }


def connect(*, database: str | None = None, use_default_database: bool = False):
    """Connect to Doris FE MySQL protocol.

    By default does not select a schema (avoids Access denied when the user can
    log in but is not yet granted on ``udise_silver``). Pass
    ``use_default_database=True`` or an explicit ``database=`` when needed.
    Fully-qualified SQL (``db.table``) works without a default schema.
    """
    cfg = _settings()
    kwargs: dict[str, Any] = {
        "host": cfg["host"],
        "port": cfg["port"],
        "user": cfg["user"],
        "password": cfg["password"],
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor,
        "connect_timeout": int(os.getenv("DORIS_CONNECT_TIMEOUT", "10")),
    }
    if database is not None:
        if database:
            kwargs["database"] = database
    elif use_default_database and cfg["database"]:
        kwargs["database"] = cfg["database"]

    attempts = max(1, int(os.getenv("DORIS_CONNECT_RETRIES", "8")))
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return pymysql.connect(**kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            sleep_s = min(30, 2 ** (attempt - 1))
            print(
                f"Doris FE connect attempt {attempt}/{attempts} failed "
                f"({cfg['host']}:{cfg['port']}): {exc}; retrying in {sleep_s}s"
            )
            time.sleep(sleep_s)

    raise RuntimeError(
        f"Cannot connect to Doris FE MySQL at {cfg['host']}:{cfg['port']} "
        f"as user `{cfg['user']}`. Check that Doris FE is running and that "
        f"DORIS_HOST / DORIS_PORT in .env match the FE query port "
        f"(often 9030, or 19030 if Docker-mapped). Original error: {last_exc}"
    ) from last_exc



def query(conn, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        # Do not pass an empty params tuple: PyMySQL still %-formats the SQL and
        # breaks literals like LIKE '%foo%'.
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
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


def list_databases(conn) -> set[str]:
    rows = query(conn, "SHOW DATABASES")
    names: set[str] = set()
    for row in rows:
        value = None
        for key, candidate in row.items():
            if str(key).lower() in {"database", "schema", "field", "name"}:
                value = candidate
                break
        if value is None and row:
            value = next(iter(row.values()))
        if value is not None:
            names.add(str(value).lower())
    return names


def ensure_database(conn, database: str) -> None:
    """Ensure ``database`` exists and is visible to the current Doris user."""
    if database.lower() in list_databases(conn):
        return

    cfg = _settings()
    user = cfg["user"]
    try:
        query(conn, f"CREATE DATABASE IF NOT EXISTS `{database}`")
    except Exception as exc:
        raise RuntimeError(
            f"Doris database `{database}` is missing or inaccessible for user `{user}`. "
            "Ask a Doris admin to run:\n"
            f"  CREATE DATABASE IF NOT EXISTS `{database}`;\n"
            f"  GRANT ALL ON `{database}`.* TO '{user}';\n"
            f"Original error: {exc}"
        ) from exc

    if database.lower() not in list_databases(conn):
        raise RuntimeError(
            f"Created or found `{database}`, but user `{user}` still cannot see it. "
            f"Grant access: GRANT ALL ON `{database}`.* TO '{user}';"
        )


def resolve_http_port(conn=None) -> tuple[str, int]:
    """Return (host, http_port) for Stream Load.

    Prefers ``DORIS_HTTP_PORT``. If unset and a connection is provided, tries
    ``SHOW FRONTENDS`` HttpPort for the master FE.
    """
    cfg = _settings()
    if os.getenv("DORIS_HTTP_PORT"):
        return cfg["host"], cfg["fe_http_port"]

    if conn is not None:
        try:
            rows = query(conn, "SHOW FRONTENDS")
            for row in rows:
                lower = {str(k).lower(): v for k, v in row.items()}
                is_master = str(lower.get("ismaster", lower.get("is_master", ""))).lower() in {
                    "true",
                    "1",
                }
                http_port = lower.get("httpport") or lower.get("http_port")
                host = lower.get("ip") or lower.get("host") or cfg["host"]
                if http_port and (is_master or len(rows) == 1):
                    return str(host), int(http_port)
        except Exception:
            pass
    return cfg["host"], cfg["fe_http_port"]


def _parse_stream_load_response(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"Status": "FAIL", "Message": text}


def stream_file(
    database: str,
    table: str,
    path: Path,
    label: str,
    expected_rows: int,
    columns: Sequence[str],
    *,
    file_format: str = "parquet",
    conn=None,
    retries: int | None = None,
) -> None:
    """PUT a local file into Doris via FE ``/_stream_load``.

    Uses curl when available (handles Doris 307 FE->BE redirect correctly).
    Falls back to urllib without ``Expect: 100-continue``.
    Retries transient memory-pressure / timeout failures.
    """
    host, http_port = resolve_http_port(conn)
    url = f"http://{host}:{http_port}/api/{database}/{table}/_stream_load"
    cfg = _settings()
    auth = f"{cfg['user']}:{cfg['password']}"
    max_attempts = retries if retries is not None else int(os.getenv("DORIS_STREAM_LOAD_RETRIES", "5"))
    max_attempts = max(1, max_attempts)
    curl = _which_curl()
    last_payload: dict[str, Any] = {}

    for attempt in range(1, max_attempts + 1):
        attempt_label = label if attempt == 1 else f"{label}_r{attempt}"
        headers = {
            "label": attempt_label,
            "format": file_format,
            "columns": ",".join(columns),
            "max_filter_ratio": "0",
            "timeout": "600",
        }
        if curl:
            payload = _stream_load_curl(curl, url, auth, headers, path)
        else:
            payload = _stream_load_urllib(url, auth, headers, path)
        last_payload = payload

        status = str(payload.get("Status", "")).upper()
        if status in {"SUCCESS", "OK"}:
            loaded = int(payload.get("NumberLoadedRows", expected_rows))
            if loaded != expected_rows:
                raise RuntimeError(
                    f"Stream load row mismatch for {database}.{table}: "
                    f"expected={expected_rows:,}, loaded={loaded:,}, response={payload}"
                )
            if attempt > 1:
                print(f"Stream load succeeded on attempt {attempt} for {database}.{table}")
            return

        message = str(payload.get("Message", ""))
        retryable = any(
            token in message.upper()
            for token in (
                "MEM_LIMIT",
                "LOW WATER",
                "TIMEOUT",
                "TRY AGAIN",
                "MEMORY",
                "CANCEL TOP MEMORY",
                "AVAILABLE MEMORY",
            )
        )
        if not retryable or attempt >= max_attempts:
            break
        sleep_s = min(60, 2**attempt)
        print(
            f"Stream load attempt {attempt}/{max_attempts} failed for {database}.{table} "
            f"({message[:160]}...); retrying in {sleep_s}s"
        )
        time.sleep(sleep_s)

    raise RuntimeError(
        f"Stream load failed for {database}.{table} via {url}: {last_payload}. "
        f"If this is a connection/HTTP error, set DORIS_HTTP_PORT to the FE http_port "
        f"(SHOW FRONTENDS). Query port is {cfg['port']}; common mapped HTTP port is 18030."
    )


def _which_curl() -> str | None:
    from shutil import which

    return which("curl")


def _stream_load_curl(
    curl: str,
    url: str,
    auth: str,
    headers: dict[str, str],
    path: Path,
) -> dict[str, Any]:
    cmd = [
        curl,
        "-sS",
        "-X",
        "PUT",
        url,
        "-u",
        auth,
        "-H",
        "Expect: 100-continue",
        "-T",
        str(path),
        "--location-trusted",
        "--max-redirs",
        "5",
        "--max-time",
        "600",
    ]
    for key, value in headers.items():
        cmd.extend(["-H", f"{key}:{value}"])

    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Stream load curl failed for {url}: rc={completed.returncode} "
            f"stderr={completed.stderr!r} stdout={completed.stdout!r}"
        )
    return _parse_stream_load_response(completed.stdout)


def _stream_load_urllib(
    url: str,
    auth: str,
    headers: dict[str, str],
    path: Path,
) -> dict[str, Any]:
    body = path.read_bytes()
    auth_header = base64.b64encode(auth.encode()).decode()
    req_headers = {
        "Authorization": f"Basic {auth_header}",
        "Expect": "100-continue",
        **headers,
    }

    class _RedirectHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N803
            if code in {301, 302, 303, 307, 308} and req.get_method() == "PUT":
                return urllib.request.Request(
                    newurl,
                    data=req.data,
                    headers=dict(req.header_items()),
                    method="PUT",
                )
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    opener = urllib.request.build_opener(_RedirectHandler)
    request = urllib.request.Request(url, data=body, method="PUT", headers=req_headers)
    try:
        with opener.open(request, timeout=600) as response:
            return _parse_stream_load_response(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(
            f"Stream load HTTP error for {url}: {detail}. "
            "Check DORIS_HTTP_PORT (SHOW FRONTENDS -> HttpPort)."
        ) from exc
