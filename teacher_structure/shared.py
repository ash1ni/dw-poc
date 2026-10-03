"""Shared year mapping, S3 bronze paths, and local bronze layout."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import project_config as c

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

SCHOOL_MASTER_CANDIDATES = (
    "school_master",
    "school_master_local",
    "sch_master",
    "sch_master_local",
)

REQUIRED_BRONZE_TABLES = (
    "mst_state",
    "mst_district",
    "mst_sch_category",
    "tch_summary",
)


@dataclass(frozen=True)
class YearContext:
    academic_year: str
    source_db: str
    source_schema: str


def year_context(academic_year: str) -> YearContext:
    return YearContext(
        academic_year=academic_year,
        source_db=SOURCE_DATABASES[academic_year],
        source_schema=SOURCE_SCHEMAS[academic_year],
    )


def s3_table_prefix(academic_year: str, table: str) -> str:
    ctx = year_context(academic_year)
    return f"{c.BRONZE_PREFIX}/{ctx.source_db}/{ctx.source_schema}/{table}/"


def local_table_dir(academic_year: str, table: str) -> Path:
    return c.BRONZE_ROOT / academic_year / table


def bronze_year_root(academic_year: str) -> Path:
    return c.BRONZE_ROOT / academic_year


def configured_years() -> tuple[str, ...]:
    import os

    env_years = os.getenv("SILVER_YEARS", "")
    if env_years.strip():
        ordered = [part.strip() for part in env_years.split(",") if part.strip()]
        missing = [year for year in ordered if year not in SOURCE_DATABASES]
        if missing:
            raise RuntimeError(f"SILVER_YEARS contains unknown academic years: {missing}")
        return tuple(ordered)
    return YEARS
