#!/usr/bin/env python3
"""Facility Structure - Bronze stage.

Sync canonical UDISE Bronze Parquet from S3-compatible object storage into the
local Bronze layout used by Spark:

    {BRONZE_ROOT}/{academic_year}/{table}/*.parquet

S3 layout (per table, latest ingest_date partition):

    {BRONZE_PREFIX}/{source_db}/{source_schema}/{table}/ingest_date=YYYY-MM-DD/part-*.parquet
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq

import project_config as c
from doris_io import connect, healthy
from shared import (
    OPTIONAL_BRONZE_TABLES,
    REQUIRED_BRONZE_TABLES,
    SCHOOL_MASTER_CANDIDATES,
    configured_years,
    local_table_dir,
    s3_table_prefix,
    year_context,
)

try:
    import boto3
    from botocore.config import Config
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install boto3 for Facility Structure Bronze S3 sync") from exc

PROJECT_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = Path(
    os.getenv(
        "FACILITY_STRUCTURE_BRONZE_MANIFEST",
        str(PROJECT_DIR / ".pipeline_state" / "facility_structure_bronze_manifest.json"),
    )
).resolve()


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def s3_client():
    if not c.AWS_ACCESS_KEY_ID or not c.AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are required for Bronze S3 sync")
    return boto3.client(
        "s3",
        endpoint_url=c.AWS_ENDPOINT_URL_S3 or None,
        aws_access_key_id=c.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY,
        region_name=c.AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def latest_ingest_prefix(client, bucket: str, table_prefix: str) -> str:
    resp = client.list_objects_v2(Bucket=bucket, Prefix=table_prefix, Delimiter="/")
    prefixes = [item["Prefix"] for item in resp.get("CommonPrefixes", []) if "ingest_date=" in item["Prefix"]]
    if not prefixes:
        raise RuntimeError(f"No ingest_date partitions under s3://{bucket}/{table_prefix}")
    dated = sorted(prefixes)[-1]
    return dated


def list_parquet_keys(client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
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
    bucket = c.BRONZE_BUCKET
    prefix = s3_table_prefix(academic_year, table)
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("KeyCount"))


def discover_school_master_s3(client, academic_year: str) -> str:
    found = [name for name in SCHOOL_MASTER_CANDIDATES if table_exists_on_s3(client, academic_year, name)]
    if len(found) != 1:
        raise RuntimeError(
            f"{academic_year}: expected exactly one school-master table on S3 from "
            f"{SCHOOL_MASTER_CANDIDATES}; found={found}"
        )
    return found[0]


def sync_table(client, academic_year: str, table: str) -> dict:
    bucket = c.BRONZE_BUCKET
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


def find_school_master_table(academic_year: str) -> str:
    found = [name for name in SCHOOL_MASTER_CANDIDATES if local_table_dir(academic_year, name).is_dir()]
    if len(found) != 1:
        raise RuntimeError(
            f"{academic_year}: expected exactly one school-master table from "
            f"{SCHOOL_MASTER_CANDIDATES}; found={found}"
        )
    return found[0]


def require_any(columns: set[str], candidates: tuple[str, ...], label: str) -> None:
    lower = {name.lower() for name in columns}
    if not any(candidate.lower() in lower for candidate in candidates):
        raise RuntimeError(f"Missing {label}; expected one of {candidates}, available={sorted(columns)}")


def parquet_columns(path: Path) -> set[str]:
    files = sorted(path.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"No Parquet files under {path}")
    return set(pq.ParquetFile(files[0]).schema_arrow.names)


def validate_year(academic_year: str, school_table: str) -> None:
    school_cols = parquet_columns(local_table_dir(academic_year, school_table))
    require_any(school_cols, ("udise_sch_code", "school_code"), "school code")
    require_any(school_cols, ("state_cd", "udise_state_code", "state_code"), "state code")
    require_any(
        school_cols,
        ("district_cd", "udise_district_code", "district_code", "udise_dist_code"),
        "district code",
    )
    require_any(school_cols, ("block_cd", "udise_block_code", "block_code"), "block code")
    require_any(school_cols, ("cluster_cd", "udise_cluster_code", "cluster_code"), "cluster code")
    require_any(
        school_cols,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
        "CENTER management id",
    )

    facility_cols = parquet_columns(local_table_dir(academic_year, "sch_facility"))
    require_any(facility_cols, ("udise_sch_code", "school_code"), "facility school code")


def build_manifest(*, sync: bool = True) -> dict:
    banner("FACILITY STRUCTURE BRONZE (S3 -> LOCAL)")
    client = s3_client() if sync else None
    result: dict[str, dict] = {}

    for academic_year in configured_years():
        ctx = year_context(academic_year)
        tables_synced: dict[str, dict] = {}

        if sync:
            for table in REQUIRED_BRONZE_TABLES:
                tables_synced[table] = sync_table(client, academic_year, table)
            for table in OPTIONAL_BRONZE_TABLES:
                if table_exists_on_s3(client, academic_year, table):
                    tables_synced[table] = sync_table(client, academic_year, table)
            school_table = discover_school_master_s3(client, academic_year)
            tables_synced[school_table] = sync_table(client, academic_year, school_table)
        else:
            school_table = find_school_master_table(academic_year)

        validate_year(academic_year, school_table)

        result[academic_year] = {
            "source_db": ctx.source_db,
            "source_schema": ctx.source_schema,
            "bronze_root": str(c.BRONZE_ROOT / academic_year),
            "school_master_table": school_table,
            "tables": tables_synced,
        }
        if sync:
            facility_rows = tables_synced["sch_facility"]["row_count"]
            school_rows = tables_synced[school_table]["row_count"]
        else:
            facility_rows = sum(
                int(pq.ParquetFile(p).metadata.num_rows)
                for p in local_table_dir(academic_year, "sch_facility").glob("*.parquet")
            )
            school_rows = sum(
                int(pq.ParquetFile(p).metadata.num_rows)
                for p in local_table_dir(academic_year, school_table).glob("*.parquet")
            )
        print(
            f"{academic_year}: PASS | school={school_table} | "
            f"schools={school_rows:,} | facilities={facility_rows:,}"
        )

    conn = connect()
    try:
        healthy(conn)
        print("Doris FE/BE health: PASS")
    finally:
        conn.close()

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Manifest: {MANIFEST_PATH}")
    print("BRONZE: PASS")
    return result


def main() -> None:
    import argparse

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
