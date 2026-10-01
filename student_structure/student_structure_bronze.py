#!/usr/bin/env python3
"""Student Structure - Bronze stage.

Sync canonical UDISE Bronze Parquet from S3-compatible object storage into the
local Bronze layout used by Spark, then validate required Student Structure tables.

Flow:
    S3 Bronze Parquet -> local {BRONZE_ROOT}/{year}/{table}/*.parquet -> validated inputs

S3 layout (per table, latest ingest_date partition):

    {BRONZE_PREFIX}/{source_db}/{source_schema}/{table}/ingest_date=YYYY-MM-DD/part-*.parquet

Use --skip-sync to validate existing local Bronze only.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

import project_config as c

try:
    import boto3
    from botocore.config import Config
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install boto3 for Student Structure Bronze S3 sync") from exc

YEARS = (
    "2020-21",
    "2021-22",
    "2022-23",
    "2023-24",
    "2024-25",
    "2025-26",
)

SOURCE_DATABASES = {
    "2020-21": "udise_2021",
    "2021-22": "udise_2122",
    "2022-23": "udise_2223",
    "2023-24": "udise_2324",
    "2024-25": "udise_2425",
    "2025-26": "udise_2526",
}

SOURCE_SCHEMAS = {
    "2020-21": "udiseschema_np_2021",
    "2021-22": "udiseschema_np_2122",
    "2022-23": "udiseschema_np_2223",
    "2023-24": "udiseschema_np_2324",
    "2024-25": "udiseschema_np_2425",
    "2025-26": "udiseschema_np",
}

REQUIRED_TABLES = (
    "mst_state",
    "mst_district",
    "mst_sch_category",
    "sch_enr_fresh",
)

SCHOOL_MASTER_CANDIDATES = (
    "school_master",
    "school_master_local",
    "sch_master",
    "sch_master_local",
)

PROJECT_DIR = Path(getattr(c, "PROJECT_DIR", Path(__file__).resolve().parent)).resolve()
BRONZE_ROOT = Path(
    getattr(c, "BRONZE_ROOT", PROJECT_DIR / "udise_data" / "bronze")
).resolve()
MANIFEST_PATH = Path(
    os.getenv(
        "STUDENT_STRUCTURE_BRONZE_MANIFEST",
        str(PROJECT_DIR / ".pipeline_state" / "student_structure_bronze_manifest.json"),
    )
).resolve()


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def configured_years() -> tuple[str, ...]:
    env_years = os.getenv("SILVER_YEARS", "")
    if env_years.strip():
        ordered = [part.strip() for part in env_years.split(",") if part.strip()]
        missing = [year for year in ordered if year not in SOURCE_DATABASES]
        if missing:
            raise RuntimeError(f"SILVER_YEARS contains unknown academic years: {missing}")
        return tuple(ordered)
    return YEARS


def s3_table_prefix(academic_year: str, table: str) -> str:
    source_db = SOURCE_DATABASES[academic_year]
    source_schema = SOURCE_SCHEMAS[academic_year]
    prefix = getattr(c, "BRONZE_PREFIX", os.getenv("BRONZE_PREFIX", "udise")).rstrip("/")
    return f"{prefix}/{source_db}/{source_schema}/{table}/"


def local_table_dir(academic_year: str, table: str) -> Path:
    return BRONZE_ROOT / academic_year / table


def s3_client():
    access_key = getattr(c, "AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID"))
    secret_key = getattr(c, "AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY"))
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are required for Bronze S3 sync")
    return boto3.client(
        "s3",
        endpoint_url=getattr(c, "AWS_ENDPOINT_URL_S3", os.getenv("AWS_ENDPOINT_URL_S3")) or None,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=getattr(c, "AWS_REGION", os.getenv("AWS_REGION", "us-east-2")),
        config=Config(signature_version="s3v4"),
    )


def latest_ingest_prefix(client, bucket: str, table_prefix: str) -> str:
    resp = client.list_objects_v2(Bucket=bucket, Prefix=table_prefix, Delimiter="/")
    prefixes = [
        item["Prefix"] for item in resp.get("CommonPrefixes", []) if "ingest_date=" in item["Prefix"]
    ]
    if not prefixes:
        raise RuntimeError(f"No ingest_date partitions under s3://{bucket}/{table_prefix}")
    return sorted(prefixes)[-1]


def list_parquet_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs: dict = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            key = item["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    if not keys:
        raise RuntimeError(f"No Parquet objects under s3://{bucket}/{prefix}")
    return sorted(keys)


def table_exists_on_s3(client, academic_year: str, table: str) -> bool:
    bucket = getattr(c, "BRONZE_BUCKET", os.getenv("BRONZE_BUCKET", "bronze-layer"))
    prefix = s3_table_prefix(academic_year, table)
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("KeyCount"))


def discover_school_master_s3(client, academic_year: str) -> str:
    found = [
        name for name in SCHOOL_MASTER_CANDIDATES if table_exists_on_s3(client, academic_year, name)
    ]
    if len(found) != 1:
        raise RuntimeError(
            f"{academic_year}: expected exactly one school-master table on S3 from "
            f"{SCHOOL_MASTER_CANDIDATES}; found={found}"
        )
    return found[0]


def sync_table(client, academic_year: str, table: str) -> dict:
    bucket = getattr(c, "BRONZE_BUCKET", os.getenv("BRONZE_BUCKET", "bronze-layer"))
    prefix = latest_ingest_prefix(client, bucket, s3_table_prefix(academic_year, table))
    keys = list_parquet_keys(client, bucket, prefix)
    target_dir = local_table_dir(academic_year, table)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    for index, key in enumerate(keys):
        dest = target_dir / f"part-{index:05d}.parquet"
        client.download_file(bucket, key, str(dest))
    rows = sum(int(pq.ParquetFile(path).metadata.num_rows) for path in target_dir.glob("*.parquet"))
    return {
        "s3_prefix": f"s3://{bucket}/{prefix}",
        "local_path": str(target_dir),
        "files": len(keys),
        "row_count": rows,
    }


def parquet_files(path: Path) -> list[Path]:
    files = sorted(p for p in path.rglob("*.parquet") if p.is_file())
    if not files:
        raise RuntimeError(f"No Parquet files found under {path}")
    return files


def parquet_row_count(path: Path) -> int:
    return sum(int(pq.ParquetFile(file).metadata.num_rows) for file in parquet_files(path))


def parquet_columns(path: Path) -> set[str]:
    files = parquet_files(path)
    schema = pq.ParquetFile(files[0]).schema_arrow
    return set(schema.names)


def bronze_year_root(year: str) -> Path:
    """Resolve one unambiguous Bronze directory for an academic year."""
    canonical = BRONZE_ROOT / year
    if canonical.is_dir():
        return canonical.resolve()

    runs_root = PROJECT_DIR / "udise_data" / "bronze_runs"
    candidates: list[Path] = []
    if runs_root.is_dir():
        for candidate in sorted(runs_root.glob(f"*/{year}")):
            if candidate.is_dir():
                candidates.append(candidate.resolve())

    complete: list[Path] = []
    for candidate in candidates:
        has_required = all((candidate / table).is_dir() for table in REQUIRED_TABLES)
        school_matches = [
            name for name in SCHOOL_MASTER_CANDIDATES if (candidate / name).is_dir()
        ]
        if has_required and len(school_matches) == 1:
            complete.append(candidate)

    if len(complete) == 1:
        return complete[0]
    if len(complete) > 1:
        raise RuntimeError(
            f"{year}: multiple complete Bronze runs found; refusing to guess:\n  - "
            + "\n  - ".join(str(path) for path in complete)
        )
    raise RuntimeError(
        f"{year}: no complete Bronze directory found in {canonical} or {runs_root}/<run>/<year>"
    )


def find_school_master_table(year: str, year_root: Path | None = None) -> str:
    year_root = year_root or bronze_year_root(year)
    found = [name for name in SCHOOL_MASTER_CANDIDATES if (year_root / name).is_dir()]
    if len(found) != 1:
        raise RuntimeError(
            f"{year}: expected exactly one school-master table from "
            f"{SCHOOL_MASTER_CANDIDATES}; found={found}"
        )
    return found[0]


def require_any(columns: set[str], candidates: Iterable[str], label: str) -> str:
    lower = {name.lower(): name for name in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    raise RuntimeError(
        f"Missing {label}; expected one of {tuple(candidates)}, available={sorted(columns)}"
    )


def validate_schema(year: str, year_root: Path, school_table: str) -> None:
    school_cols = parquet_columns(year_root / school_table)
    require_any(school_cols, ("udise_sch_code", "school_code"), "school code")
    require_any(school_cols, ("state_cd", "udise_state_code", "state_code"), "state code")
    require_any(
        school_cols,
        ("district_cd", "udise_district_code", "district_code", "udise_dist_code"),
        "district code",
    )
    require_any(
        school_cols,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
        "CENTER management id",
    )

    enr_cols = parquet_columns(year_root / "sch_enr_fresh")
    require_any(enr_cols, ("udise_sch_code", "school_code"), "enrollment school code")
    require_any(enr_cols, ("item_group", "item_group_id"), "item_group")
    require_any(enr_cols, ("item_id", "itemid"), "item_id")
    for required_metric in ("c1_b", "c1_g", "c12_b", "c12_g"):
        if required_metric not in enr_cols:
            raise RuntimeError(f"{year}/sch_enr_fresh missing required metric {required_metric}")


def build_manifest(*, sync: bool = True) -> dict:
    banner("STUDENT STRUCTURE BRONZE (S3 -> LOCAL)" if sync else "STUDENT STRUCTURE BRONZE VALIDATION")
    client = s3_client() if sync else None
    result: dict[str, dict] = {}

    for year in configured_years():
        tables_synced: dict[str, dict] = {}

        if sync:
            for table in REQUIRED_TABLES:
                tables_synced[table] = sync_table(client, year, table)
            school_table = discover_school_master_s3(client, year)
            tables_synced[school_table] = sync_table(client, year, school_table)
            year_root = BRONZE_ROOT / year
        else:
            year_root = bronze_year_root(year)
            school_table = find_school_master_table(year, year_root)

        for table in REQUIRED_TABLES:
            path = year_root / table
            if not path.is_dir():
                raise RuntimeError(f"{year}: missing Bronze table directory {path}")

        validate_schema(year, year_root, school_table)

        tables = list(REQUIRED_TABLES) + [school_table]
        if sync:
            counts = {table: tables_synced[table]["row_count"] for table in tables}
        else:
            counts = {table: parquet_row_count(year_root / table) for table in tables}

        result[year] = {
            "source_db": SOURCE_DATABASES[year],
            "source_schema": SOURCE_SCHEMAS[year],
            "bronze_root": str(year_root),
            "school_master_table": school_table,
            "row_counts": counts,
            "tables": tables_synced,
        }

        print(
            f"{year}: PASS | school={school_table} | "
            f"schools={counts[school_table]:,} | enrollment={counts['sch_enr_fresh']:,}"
        )

    # Bronze is S3 -> local Parquet only; Doris is checked in Silver/Gold.

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Manifest: {MANIFEST_PATH}")
    print("BRONZE: PASS")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Validate existing local Bronze only (do not download from S3)",
    )
    args = parser.parse_args()
    build_manifest(sync=not args.skip_sync)


if __name__ == "__main__":
    main()
