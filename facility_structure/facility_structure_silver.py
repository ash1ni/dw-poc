#!/usr/bin/env python3
"""Build Facility Structure Silver: normalized masters, school snapshot, full facility rows."""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import Iterable, Sequence

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import project_config as c
from doris_io import connect, healthy, query, stream_file
from shared import (
    BRONZE_METADATA_COLUMNS,
    SCHOOL_MASTER_CANDIDATES,
    SOURCE_DATABASES,
    SOURCE_SCHEMAS,
    configured_years,
    local_table_dir,
)

try:
    from doris_tables import ident
except Exception:
    def ident(value: str) -> str:
        if not value or any(
            ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
            for ch in value
        ):
            raise ValueError(f"Unsafe SQL identifier: {value!r}")
        return f"`{value}`"


PROJECT_DIR = Path(__file__).resolve().parent
SILVER_STAGE_ROOT = Path(
    os.getenv(
        "FACILITY_STRUCTURE_SILVER_STAGE_ROOT",
        str(PROJECT_DIR / "udise_data" / "facility_structure" / "silver_stage"),
    )
).resolve()
SILVER_DB = os.getenv("UDISE_SILVER_DB", "udise_silver")
REPLICATION_NUM = int(getattr(c, "DORIS_REPLICATION_NUM", os.getenv("DORIS_REPLICATION_NUM", "1")))

FACILITY_HISTORY_TABLE = "silver_school_facility_history"


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def make_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "2g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def union_all(frames: Sequence[DataFrame]) -> DataFrame:
    if not frames:
        raise RuntimeError("No DataFrames to union")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def first_existing(df: DataFrame, candidates: Iterable[str], *, required: bool = True) -> str | None:
    lower_map = {name.lower(): name for name in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    if required:
        raise RuntimeError(f"None of required columns {list(candidates)} exist. Available={df.columns}")
    return None


def safe_string(column: F.Column) -> F.Column:
    return F.trim(column.cast("string"))


def bronze_year_root(year: str) -> Path:
    root = local_table_dir(year, "mst_state").parent
    if not (root / "sch_facility").is_dir():
        raise RuntimeError(f"{year}: local Bronze missing under {root}")
    return root


def find_school_master_table(year: str) -> str:
    root = bronze_year_root(year)
    found = [name for name in SCHOOL_MASTER_CANDIDATES if (root / name).is_dir()]
    if len(found) != 1:
        raise RuntimeError(f"{year}: expected one school-master table, found={found}")
    return found[0]


def read_bronze(year: str, table: str) -> DataFrame:
    path = bronze_year_root(year) / table
    if not path.is_dir():
        raise RuntimeError(f"Missing Bronze Parquet: {path}")
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")
    return spark.read.parquet(path.as_posix())


@dataclass(frozen=True)
class TableSpec:
    table: str
    columns: tuple[tuple[str, str, bool], ...]
    keys: tuple[str, ...]
    distribution: tuple[str, ...]
    partitions: int = 4

    @property
    def local_path(self) -> Path:
        return SILVER_STAGE_ROOT / self.table

    @property
    def ordered_columns(self) -> tuple[tuple[str, str, bool], ...]:
        by_name = {name: (name, sql_type, nullable) for name, sql_type, nullable in self.columns}
        missing = [name for name in self.keys if name not in by_name]
        if missing:
            raise RuntimeError(f"{self.table}: missing key columns {missing}")
        key_set = set(self.keys)
        return tuple(by_name[name] for name in self.keys) + tuple(
            column for column in self.columns if column[0] not in key_set
        )

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _, _ in self.ordered_columns)


MASTER_META = (
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

STATE_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("state_cd", "VARCHAR(10)", False),
    ("state_name", "VARCHAR(160)", True),
    *MASTER_META,
)

DISTRICT_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("district_cd", "VARCHAR(20)", False),
    ("state_cd", "VARCHAR(10)", False),
    ("district_name", "VARCHAR(180)", True),
    *MASTER_META,
)

BLOCK_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("block_cd", "VARCHAR(20)", False),
    ("district_cd", "VARCHAR(20)", False),
    ("state_cd", "VARCHAR(10)", False),
    ("block_name", "VARCHAR(180)", True),
    *MASTER_META,
)

CLUSTER_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("cluster_cd", "VARCHAR(20)", False),
    ("block_cd", "VARCHAR(20)", False),
    ("cluster_name", "VARCHAR(180)", True),
    *MASTER_META,
)

CATEGORY_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("sch_category_id", "INT", False),
    ("category_name", "VARCHAR(255)", True),
    *MASTER_META,
)

MANAGEMENT_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("management_center_id", "INT", False),
    ("sch_mgmt_id", "INT", True),
    *MASTER_META,
)

SCHOOL_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("udise_sch_code", "VARCHAR(32)", False),
    ("school_name", "VARCHAR(255)", True),
    ("state_cd", "VARCHAR(10)", False),
    ("district_cd", "VARCHAR(20)", False),
    ("block_cd", "VARCHAR(20)", False),
    ("cluster_cd", "VARCHAR(20)", False),
    ("sch_category_id", "INT", True),
    ("management_center_id", "INT", False),
    ("sch_mgmt_id", "INT", True),
    ("school_status", "INT", True),
    ("source_table", "VARCHAR(64)", False),
    *MASTER_META,
)

FIXED_SPECS: dict[str, TableSpec] = {
    "silver_state": TableSpec("silver_state", STATE_COLUMNS, ("academic_year", "state_cd"), ("academic_year", "state_cd"), 1),
    "silver_district": TableSpec(
        "silver_district", DISTRICT_COLUMNS, ("academic_year", "district_cd"), ("academic_year", "district_cd"), 2
    ),
    "silver_block": TableSpec("silver_block", BLOCK_COLUMNS, ("academic_year", "block_cd"), ("academic_year", "state_cd"), 4),
    "silver_cluster": TableSpec(
        "silver_cluster", CLUSTER_COLUMNS, ("academic_year", "cluster_cd"), ("academic_year", "block_cd"), 4
    ),
    "silver_school_category": TableSpec(
        "silver_school_category", CATEGORY_COLUMNS, ("academic_year", "sch_category_id"), ("academic_year",), 1
    ),
    "silver_management": TableSpec(
        "silver_management", MANAGEMENT_COLUMNS, ("academic_year", "management_center_id"), ("academic_year",), 1
    ),
    "silver_school_master": TableSpec(
        "silver_school_master", SCHOOL_COLUMNS, ("academic_year", "udise_sch_code"), ("academic_year", "state_cd"), 8
    ),
}


def create_table_sql(spec: TableSpec, table_name: str) -> str:
    lines = []
    for name, sql_type, nullable in spec.ordered_columns:
        lines.append(f"        {ident(name)} {sql_type} {'NULL' if nullable else 'NOT NULL'}")
    keys = ", ".join(ident(x) for x in spec.keys)
    dist = ", ".join(ident(x) for x in spec.distribution)
    buckets = max(1, min(16, spec.partitions))
    return f"""
    CREATE TABLE {ident(SILVER_DB)}.{ident(table_name)}
    (
{',\n'.join(lines)}
    )
    DUPLICATE KEY ({keys})
    DISTRIBUTED BY HASH({dist}) BUCKETS {buckets}
    PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
    """


def table_exists(conn, database: str, table: str) -> bool:
    return bool(query(conn, f"SHOW TABLES FROM {ident(database)} LIKE %s", (table,)))


def parquet_files(path: Path) -> list[Path]:
    files = sorted(p for p in path.rglob("*.parquet") if p.is_file())
    if not files:
        raise RuntimeError(f"No Parquet files under {path}")
    return files


def parquet_row_count(path: Path) -> int:
    return sum(int(pq.ParquetFile(file).metadata.num_rows) for file in parquet_files(path))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_stage(df: DataFrame, spec: TableSpec) -> int:
    if spec.local_path.exists():
        shutil.rmtree(spec.local_path)
    spec.local_path.parent.mkdir(parents=True, exist_ok=True)
    (
        df.select(*spec.column_names)
        .repartition(max(1, spec.partitions))
        .write.mode("overwrite")
        .parquet(spec.local_path.as_posix())
    )
    rows = parquet_row_count(spec.local_path)
    print(f"STAGE PARQUET: {spec.local_path} -> {rows:,} rows")
    return rows


def publish(spec: TableSpec, expected_rows: int) -> int:
    run_token = uuid.uuid4().hex[:12]
    stage_table = f"{spec.table}__stage_{run_token}"
    conn = connect()
    try:
        healthy(conn)
        query(conn, f"CREATE DATABASE IF NOT EXISTS {ident(SILVER_DB)}")
        if not table_exists(conn, SILVER_DB, spec.table):
            query(conn, create_table_sql(spec, spec.table))

        query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")
        query(conn, create_table_sql(spec, stage_table))

        loaded = 0
        for index, path in enumerate(parquet_files(spec.local_path)):
            rows = int(pq.ParquetFile(path).metadata.num_rows)
            label = f"fs_silver_{run_token}_{index}_{file_sha256(path)[:8]}"
            stream_file(
                SILVER_DB,
                stage_table,
                path,
                label,
                rows,
                spec.column_names,
                file_format="parquet",
            )
            loaded += rows

        stage_count = int(
            query(conn, f"SELECT COUNT(*) AS n FROM {ident(SILVER_DB)}.{ident(stage_table)}")[0]["n"]
        )
        if loaded != expected_rows or stage_count != expected_rows:
            raise RuntimeError(
                f"{spec.table}: load mismatch expected={expected_rows:,}, files={loaded:,}, stage={stage_count:,}"
            )

        query(
            conn,
            f"ALTER TABLE {ident(SILVER_DB)}.{ident(spec.table)} "
            f"REPLACE WITH TABLE {ident(stage_table)} PROPERTIES (\"swap\"=\"true\")",
        )
        query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")

        final_count = int(
            query(conn, f"SELECT COUNT(*) AS n FROM {ident(SILVER_DB)}.{ident(spec.table)}")[0]["n"]
        )
        if final_count != expected_rows:
            raise RuntimeError(f"{spec.table}: final={final_count:,}, expected={expected_rows:,}")
        print(f"DORIS PUBLISHED: {SILVER_DB}.{spec.table} -> {final_count:,} rows")
        return final_count
    except Exception:
        try:
            query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def duplicate_conflict_count(df: DataFrame, keys: Sequence[str], value_cols: Sequence[str]) -> int:
    signature = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in value_cols],
        ),
        256,
    )
    return (
        df.select(*keys, signature.alias("_sig"))
        .groupBy(*keys)
        .agg(F.countDistinct("_sig").alias("_variants"))
        .filter(F.col("_variants") > 1)
        .count()
    )


def is_metadata_column(name: str) -> bool:
    lowered = name.lower()
    return lowered in {col.lower() for col in BRONZE_METADATA_COLUMNS}


def discover_facility_attribute_columns() -> list[str]:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")
    discovered: set[str] = set()
    for year in configured_years():
        df = read_bronze(year, "sch_facility")
        for name in df.columns:
            if is_metadata_column(name):
                continue
            if name.lower() in {"year_id", "udise_sch_code", "school_code"}:
                continue
            discovered.add(name)
    return sorted(discovered)


def facility_table_spec(attribute_columns: Sequence[str]) -> TableSpec:
    dynamic = tuple((name, "VARCHAR(128)", True) for name in attribute_columns)
    columns = (
        ("academic_year", "VARCHAR(7)", False),
        ("udise_sch_code", "VARCHAR(32)", False),
        ("row_checksum", "VARCHAR(64)", False),
        *dynamic,
        *MASTER_META,
    )
    return TableSpec(
        "silver_school_facility",
        columns,
        ("academic_year", "udise_sch_code"),
        ("academic_year", "udise_sch_code"),
        12,
    )


def normalize_state(year: str) -> DataFrame:
    df = read_bronze(year, "mst_state")
    code = first_existing(df, ("udise_state_code", "state_cd", "state_code"))
    name = first_existing(df, ("state_name", "state_name_english", "state_name_eng"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(code)).alias("state_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("state_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "state_cd"])


def normalize_district(year: str) -> DataFrame:
    df = read_bronze(year, "mst_district")
    state = first_existing(df, ("udise_state_code", "state_cd", "state_code"))
    code = first_existing(df, ("udise_district_code", "district_cd", "district_code", "udise_dist_code"))
    name = first_existing(df, ("district_name", "district_name_english", "district_name_eng"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(code)).alias("district_cd"),
        safe_string(F.col(state)).alias("state_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("district_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "district_cd"])


def normalize_block(year: str) -> DataFrame:
    df = read_bronze(year, "mst_block")
    block = first_existing(df, ("udise_block_code", "block_cd", "block_code"))
    district = first_existing(df, ("udise_dist_code", "district_cd", "district_code"))
    state = first_existing(df, ("udise_state_code", "state_cd", "state_code"))
    name = first_existing(df, ("block_name", "block_name_english"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(block)).alias("block_cd"),
        safe_string(F.col(district)).alias("district_cd"),
        safe_string(F.col(state)).alias("state_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("block_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "block_cd"])


def normalize_cluster(year: str) -> DataFrame:
    df = read_bronze(year, "mst_cluster")
    cluster = first_existing(df, ("udise_cluster_code", "cluster_cd", "cluster_code"))
    block = first_existing(df, ("udise_block_code", "block_cd", "block_code"))
    name = first_existing(df, ("cluster_name", "cluster_name_english"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(cluster)).alias("cluster_cd"),
        safe_string(F.col(block)).alias("block_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("cluster_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "cluster_cd"])


def normalize_category(year: str) -> DataFrame:
    root = bronze_year_root(year)
    if not (root / "mst_sch_category").is_dir():
        raise RuntimeError(f"{year}: missing optional Bronze table mst_sch_category")
    df = read_bronze(year, "mst_sch_category")
    category_id = first_existing(df, ("sch_category_id", "school_category_id", "category_id"))
    name = first_existing(
        df,
        (
            "sch_category_type",
            "sch_category_name",
            "school_category_name",
            "category_name",
            "sch_category",
            "school_category",
            "category",
        ),
        required=True,
    )
    return df.select(
        F.lit(year).alias("academic_year"),
        F.col(category_id).cast("int").alias("sch_category_id"),
        safe_string(F.col(name)).alias("category_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "sch_category_id"])


def normalize_management(year: str) -> DataFrame:
    source_table = find_school_master_table(year)
    df = read_bronze(year, source_table)
    center = first_existing(
        df,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
    )
    mgmt = first_existing(df, ("sch_mgmt_id", "sch_mgmt_center_id", "management_id"), required=False)
    return (
        df.select(
            F.lit(year).alias("academic_year"),
            F.col(center).cast("int").alias("management_center_id"),
            (F.col(mgmt).cast("int") if mgmt else F.lit(None).cast("int")).alias("sch_mgmt_id"),
            F.lit(SOURCE_DATABASES[year]).alias("source_db"),
            F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
            F.current_timestamp().alias("processed_at"),
        )
        .filter(F.col("management_center_id").isNotNull())
        .dropDuplicates(["academic_year", "management_center_id"])
    )


def normalize_school(year: str) -> DataFrame:
    source_table = find_school_master_table(year)
    df = read_bronze(year, source_table)
    school_code = first_existing(df, ("udise_sch_code", "school_code"))
    school_name = first_existing(df, ("school_name", "sch_name"), required=False)
    state_cd = first_existing(df, ("state_cd", "udise_state_code", "state_code"))
    district_cd = first_existing(df, ("district_cd", "udise_district_code", "district_code", "udise_dist_code"))
    block_cd = first_existing(df, ("block_cd", "udise_block_code", "block_code"))
    cluster_cd = first_existing(df, ("cluster_cd", "udise_cluster_code", "cluster_code"))
    category_id = first_existing(df, ("sch_category_id", "school_category_id", "category_id"), required=False)
    management_center_id = first_existing(
        df,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
    )
    sch_mgmt_id = first_existing(df, ("sch_mgmt_id",), required=False)
    school_status = first_existing(df, ("school_status", "sch_status"), required=False)

    result = df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(school_code)).alias("udise_sch_code"),
        (safe_string(F.col(school_name)) if school_name else F.lit(None).cast("string")).alias("school_name"),
        safe_string(F.col(state_cd)).alias("state_cd"),
        safe_string(F.col(district_cd)).alias("district_cd"),
        safe_string(F.col(block_cd)).alias("block_cd"),
        safe_string(F.col(cluster_cd)).alias("cluster_cd"),
        (F.col(category_id).cast("int") if category_id else F.lit(None).cast("int")).alias("sch_category_id"),
        F.col(management_center_id).cast("int").alias("management_center_id"),
        (F.col(sch_mgmt_id).cast("int") if sch_mgmt_id else F.lit(None).cast("int")).alias("sch_mgmt_id"),
        (F.col(school_status).cast("int") if school_status else F.lit(None).cast("int")).alias("school_status"),
        F.lit(source_table).alias("source_table"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).filter(F.col("udise_sch_code").isNotNull() & (F.length("udise_sch_code") > 0))

    conflicts = duplicate_conflict_count(
        result,
        ["academic_year", "udise_sch_code"],
        [
            "school_name",
            "state_cd",
            "district_cd",
            "block_cd",
            "cluster_cd",
            "sch_category_id",
            "management_center_id",
            "sch_mgmt_id",
            "school_status",
        ],
    )
    if conflicts:
        raise RuntimeError(f"{year}/{source_table}: {conflicts} school codes have conflicting attributes")
    return result.dropDuplicates(["academic_year", "udise_sch_code"])


def normalize_facility(year: str, attribute_columns: Sequence[str]) -> DataFrame:
    df = read_bronze(year, "sch_facility")
    school_code = first_existing(df, ("udise_sch_code", "school_code"))
    attr_exprs = []
    for name in attribute_columns:
        if name in df.columns:
            attr_exprs.append(safe_string(F.col(name)).alias(name))
        else:
            attr_exprs.append(F.lit(None).cast("string").alias(name))

    selected = df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(school_code)).alias("udise_sch_code"),
        *attr_exprs,
    ).filter(F.col("udise_sch_code").isNotNull() & (F.length("udise_sch_code") > 0))

    checksum = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(name).cast("string"), F.lit("<NULL>"))
                for name in attribute_columns
            ],
        ),
        256,
    )
    result = selected.withColumn("row_checksum", checksum).select(
        "academic_year",
        "udise_sch_code",
        "row_checksum",
        *attribute_columns,
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    )

    conflicts = duplicate_conflict_count(result, ["academic_year", "udise_sch_code"], ["row_checksum"])
    if conflicts:
        raise RuntimeError(f"{year}/sch_facility: {conflicts} duplicate school keys with conflicting checksums")
    return result


def preflight() -> None:
    banner("FACILITY STRUCTURE SILVER PREFLIGHT")
    errors: list[str] = []
    for year in configured_years():
        try:
            root = bronze_year_root(year)
            school = find_school_master_table(year)
            print(f"{year}: bronze={root} | school={school}")
        except Exception as exc:
            errors.append(str(exc))
    if errors:
        raise RuntimeError("Preflight failed:\n  - " + "\n  - ".join(errors))

    conn = connect()
    try:
        healthy(conn)
        print("Doris FE/BE health: PASS")
    finally:
        conn.close()


def build_silver() -> dict[str, int]:
    banner("BUILD FACILITY STRUCTURE SILVER")
    spark = make_spark("UDISE_Facility_Structure_Silver")
    try:
        facility_attrs = discover_facility_attribute_columns()
        facility_spec = facility_table_spec(facility_attrs)
        print(f"sch_facility attribute columns: {len(facility_attrs)}")

        frames: dict[str, list[DataFrame]] = {name: [] for name in FIXED_SPECS}
        facility_frames: list[DataFrame] = []

        for year in configured_years():
            print(f"Normalize {year}")
            frames["silver_state"].append(normalize_state(year))
            frames["silver_district"].append(normalize_district(year))
            frames["silver_block"].append(normalize_block(year))
            frames["silver_cluster"].append(normalize_cluster(year))
            if (bronze_year_root(year) / "mst_sch_category").is_dir():
                frames["silver_school_category"].append(normalize_category(year))
            frames["silver_management"].append(normalize_management(year))
            frames["silver_school_master"].append(normalize_school(year))
            facility_frames.append(normalize_facility(year, facility_attrs))

        outputs = {name: union_all(parts) for name, parts in frames.items() if parts}
        outputs["silver_school_facility"] = union_all(facility_frames)

        unmatched = (
            outputs["silver_school_facility"]
            .select("academic_year", "udise_sch_code")
            .distinct()
            .join(
                outputs["silver_school_master"].select("academic_year", "udise_sch_code"),
                ["academic_year", "udise_sch_code"],
                "left_anti",
            )
            .count()
        )
        if unmatched:
            raise RuntimeError(
                f"Silver validation: {unmatched} facility school keys are missing from silver_school_master"
            )

        published: dict[str, int] = {}
        for name, df in outputs.items():
            spec = FIXED_SPECS.get(name, facility_spec)
            rows = write_stage(df, spec)
            if rows <= 0:
                raise RuntimeError(f"{name}: zero Silver rows")
            published[name] = publish(spec, rows)

        print("SILVER: PASS")
        return published
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("normalize", "all"), default="all")
    args = parser.parse_args()
    if args.stage == "all":
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--stage", "normalize"],
            check=True,
        )
        subprocess.run(
            [sys.executable, str(Path(__file__).with_name("facility_structure_model.py")), "--stage", "silver"],
            check=True,
        )
    else:
        preflight()
        build_silver()


if __name__ == "__main__":
    main()
