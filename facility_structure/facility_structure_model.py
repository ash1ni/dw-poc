"""Facility Structure Doris model: Silver star schema + Gold use-case summaries."""
from __future__ import annotations

import argparse
import os
import time
from typing import Iterable

from doris_io import connect, ensure_database, healthy, query

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

SILVER_DB = os.getenv("UDISE_SILVER_DB", "udise_silver")
DW_DB = SILVER_DB
GOLD_DB = os.getenv("UDISE_GOLD_DB", "udise_gold")
MANAGEMENT_SOURCE_DB = os.getenv("UDISE_MANAGEMENT_SOURCE_DB", "udise_gold")
MANAGEMENT_SOURCE_TABLE = os.getenv("UDISE_MANAGEMENT_SOURCE_TABLE", "dim_management")
FORCE_REFRESH_MANAGEMENT = os.getenv("UDISE_REFRESH_MANAGEMENT_DIM", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
REPLICATION_NUM = int(os.getenv("DORIS_REPLICATION_NUM", "1"))

FACILITY_HISTORY_TABLE = "silver_school_facility_history"
GOLD_MANAGEMENT_TABLE = "facility_structure_management"
GOLD_CATEGORY_TABLE = "facility_structure_category"
GOLD_REPORT_TABLES = (
    GOLD_MANAGEMENT_TABLE,
    GOLD_CATEGORY_TABLE,
)
GOLD_AMENITY_MEASURES = (
    "electricity_available",
    "electricity_functional",
    "drinking_water_available",
    "drinking_water_functional",
    "boys_toilet_available",
    "boys_toilet_functional",
    "girls_toilet_available",
    "girls_toilet_functional",
)
GOLD_MANAGEMENT_REQUIRED = {
    "ac_year",
    "state_ut",
    "management",
    "total",
    *GOLD_AMENITY_MEASURES,
}
GOLD_CATEGORY_REQUIRED = {
    "ac_year",
    "state_ut",
    "category",
    "total",
    "government",
    "government_aided",
    "private_unaided_recognized",
    "others",
    *GOLD_AMENITY_MEASURES,
}
GOLD_REPORT_REQUIRED = {
    GOLD_MANAGEMENT_TABLE: GOLD_MANAGEMENT_REQUIRED,
    GOLD_CATEGORY_TABLE: GOLD_CATEGORY_REQUIRED,
}
GOLD_NATIONAL_GEOGRAPHY = "Available Source Total"
GOLD_ALL_MANAGEMENT = "All Management"
MANAGEMENT_GROUPS = (
    "Government",
    "Government Aided",
    "Private Unaided Recognized",
    "Others",
)
# Legacy Gold tables from earlier facility report shapes; never touch dim_management.
LEGACY_GOLD_STAR_TABLES = (
    "dim_year",
    "dim_geography",
    "dim_school",
    "fact_school_facility",
    "facility_electricity_by_management",
    "facility_drinking_water_by_management",
    "facility_boys_toilet_by_management",
)

DRINKING_WATER_AVAIL_CANDIDATES = (
    "hand_pump_yn",
    "well_prot_yn",
    "tap_yn",
    "othsrc_yn",
    "well_unprot_yn",
    "pack_water_yn",
)
DRINKING_WATER_FUNC_CANDIDATES = (
    "hand_pump_fun_yn",
    "well_prot_fun_yn",
    "tap_fun_yn",
    "othsrc_fun_yn",
    "well_unprot_fun_yn",
    "pack_water_fun_yn",
)
ELECTRICITY_CANDIDATES = (
    "electricity_yn",
    "electricity_availability",
    "electricity_avail",
    "electricity",
)
BOYS_TOILET_AVAIL_CANDIDATES = (
    "toiletb",
    "total_boys_toilet",
    "boys_toilet",
    "toilet_boys",
)
BOYS_TOILET_FUNC_CANDIDATES = (
    "toiletb_fun",
    "total_boys_func_toilet",
    "boys_func_toilet",
    "toilet_boys_fun",
)
GIRLS_TOILET_AVAIL_CANDIDATES = (
    "toiletg",
    "total_girls_toilet",
    "girls_toilet",
    "toilet_girls",
)
GIRLS_TOILET_FUNC_CANDIDATES = (
    "toiletg_fun",
    "total_girls_func_toilet",
    "girls_func_toilet",
    "toilet_girls_fun",
)
DIM_MANAGEMENT_REQUIRED = {
    "management_sk",
    "management_center_id",
    "management",
    "management_group",
    "updated_at",
}
DIM_SCHOOL_REQUIRED = {
    "udise_sch_code",
    "valid_from",
    "school_sk",
    "version_no",
    "school_name",
    "geography_sk",
    "category_sk",
    "management_sk",
    "state_cd",
    "district_cd",
    "block_cd",
    "cluster_cd",
    "sch_category_id",
    "management_center_id",
    "school_status",
    "valid_to",
    "is_current",
    "row_hash",
    "change_type",
    "created_at",
}
DIM_SCHOOL_INSERT_COLUMNS = (
    "udise_sch_code",
    "valid_from",
    "school_sk",
    "version_no",
    "school_name",
    "geography_sk",
    "category_sk",
    "management_sk",
    "state_cd",
    "district_cd",
    "block_cd",
    "cluster_cd",
    "sch_category_id",
    "management_center_id",
    "school_status",
    "valid_to",
    "is_current",
    "row_hash",
    "change_type",
    "created_at",
)


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def qname(database: str, table: str) -> str:
    return f"{ident(database)}.{ident(table)}"


def table_exists(conn, database: str, table: str) -> bool:
    return bool(query(conn, f"SHOW TABLES FROM {ident(database)} LIKE %s", (table,)))


def count_rows(conn, database: str, table: str) -> int:
    return int(query(conn, f"SELECT COUNT(*) AS n FROM {qname(database, table)}")[0]["n"])


def table_columns(conn, database: str, table: str) -> set[str]:
    rows = query(conn, f"DESC {qname(database, table)}")
    result: set[str] = set()
    for row in rows:
        candidate = None
        for key, value in row.items():
            if str(key).lower() in {"field", "column", "column_name", "name"}:
                candidate = value
                break
        if candidate is None and row:
            candidate = next(iter(row.values()))
        if candidate is not None:
            result.add(str(candidate))
    return result


def first_column(columns: Iterable[str], candidates: Iterable[str], *, required: bool = True) -> str | None:
    lower = {name.lower(): name for name in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    if required:
        raise RuntimeError(f"Expected one of {tuple(candidates)}; available={sorted(columns)}")
    return None


def execute(conn, sql: str) -> None:
    query(conn, sql)


def dim_management_ddl() -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_management')} (
            management_sk INT NOT NULL,
            management_center_id INT NOT NULL,
            management VARCHAR(128) NOT NULL,
            management_detailed VARCHAR(255) NULL,
            management_group VARCHAR(64) NOT NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (management_sk)
        DISTRIBUTED BY HASH(management_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """


def dim_school_ddl() -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_school')} (
            udise_sch_code VARCHAR(32) NOT NULL,
            valid_from DATE NOT NULL,
            school_sk VARCHAR(32) NOT NULL,
            version_no INT NOT NULL,
            school_name VARCHAR(255) NULL,
            geography_sk VARCHAR(32) NOT NULL,
            category_sk INT NULL,
            management_sk INT NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            district_cd VARCHAR(20) NOT NULL,
            block_cd VARCHAR(20) NULL,
            cluster_cd VARCHAR(20) NULL,
            sch_category_id INT NULL,
            management_center_id INT NOT NULL,
            school_status INT NULL,
            valid_to DATE NOT NULL,
            is_current TINYINT NOT NULL,
            row_hash VARCHAR(32) NOT NULL,
            change_type VARCHAR(32) NOT NULL,
            created_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (udise_sch_code, valid_from)
        DISTRIBUTED BY HASH(udise_sch_code) BUCKETS 16
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """


def gold_management_ddl() -> str:
    amenity_cols = ",\n            ".join(f"{name} BIGINT NOT NULL" for name in GOLD_AMENITY_MEASURES)
    return f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, GOLD_MANAGEMENT_TABLE)} (
            ac_year VARCHAR(7) NOT NULL,
            state_ut VARCHAR(160) NOT NULL,
            management VARCHAR(64) NOT NULL,
            total BIGINT NOT NULL,
            {amenity_cols}
        )
        DUPLICATE KEY (ac_year, state_ut, management)
        DISTRIBUTED BY HASH(ac_year) BUCKETS 2
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """


def gold_category_ddl() -> str:
    amenity_cols = ",\n            ".join(f"{name} BIGINT NOT NULL" for name in GOLD_AMENITY_MEASURES)
    return f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, GOLD_CATEGORY_TABLE)} (
            ac_year VARCHAR(7) NOT NULL,
            state_ut VARCHAR(160) NOT NULL,
            category VARCHAR(128) NOT NULL,
            total BIGINT NOT NULL,
            government BIGINT NOT NULL,
            government_aided BIGINT NOT NULL,
            private_unaided_recognized BIGINT NOT NULL,
            others BIGINT NOT NULL,
            {amenity_cols}
        )
        DUPLICATE KEY (ac_year, state_ut, category)
        DISTRIBUTED BY HASH(ac_year) BUCKETS 2
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """


def recreate_table_if_schema_drift(
    conn,
    database: str,
    table: str,
    required_columns: set[str],
    ddl_sql: str,
) -> None:
    """Drop/recreate when CREATE IF NOT EXISTS left an older incompatible schema."""
    if not table_exists(conn, database, table):
        execute(conn, ddl_sql)
        return
    existing = {name.lower() for name in table_columns(conn, database, table)}
    required = {name.lower() for name in required_columns}
    missing = sorted(required - existing)
    extra = sorted(existing - required)
    if not missing and not extra:
        return
    print(
        f"{table} schema drift "
        f"(missing={missing or '[]'}, extra={extra or '[]'}); "
        f"dropping and recreating {database}.{table}"
    )
    execute(conn, f"DROP TABLE IF EXISTS {qname(database, table)}")
    execute(conn, ddl_sql)


def ensure_dim_management_schema(conn) -> None:
    """Recreate silver dim_management when CREATE IF NOT EXISTS left an older schema in place."""
    if not table_exists(conn, DW_DB, "dim_management"):
        execute(conn, dim_management_ddl())
        return
    existing_cols = table_columns(conn, DW_DB, "dim_management")
    existing_lower = {name.lower() for name in existing_cols}
    missing = sorted(DIM_MANAGEMENT_REQUIRED - existing_lower)
    if not missing:
        return

    print(
        f"dim_management schema drift (missing {missing}); "
        f"recreating {DW_DB}.dim_management"
    )
    preserved: list[dict] = []
    id_col = first_column(
        existing_cols,
        ("management_center_id", "sch_mgmt_center_id", "management_sk"),
        required=False,
    )
    management_col = first_column(
        existing_cols,
        ("management", "management_name", "management_group"),
        required=False,
    )
    if id_col and management_col:
        detail_col = first_column(
            existing_cols,
            ("management_detailed", "management_detail", "management_description"),
            required=False,
        )
        group_col = first_column(existing_cols, ("management_group",), required=False)
        detail_expr = f"CAST({ident(detail_col)} AS STRING)" if detail_col else "NULL"
        mgmt_expr = f"TRIM(CAST({ident(management_col)} AS STRING))"
        group_expr = (
            f"TRIM(CAST({ident(group_col)} AS STRING))"
            if group_col
            else "CAST(NULL AS STRING)"
        )
        preserved = query(
            conn,
            f"""
            SELECT
                CAST({ident(id_col)} AS INT) AS management_sk,
                CAST({ident(id_col)} AS INT) AS management_center_id,
                {mgmt_expr} AS management,
                {detail_expr} AS management_detailed,
                {group_expr} AS management_group
            FROM {qname(DW_DB, 'dim_management')}
            """,
        )
        for row in preserved:
            if row.get("management_group"):
                continue
            label = str(row.get("management") or "").lower()
            if "private" in label and "unaided" in label:
                row["management_group"] = "Private Unaided Recognized"
            elif "aided" in label:
                row["management_group"] = "Government Aided"
            elif "government" in label or "govt" in label:
                row["management_group"] = "Government"
            else:
                row["management_group"] = "Others"

    execute(conn, f"DROP TABLE IF EXISTS {qname(DW_DB, 'dim_management')}")
    execute(conn, dim_management_ddl())

    if preserved:
        insert_sql = f"""
            INSERT INTO {qname(DW_DB, 'dim_management')}
            (management_sk, management_center_id, management, management_detailed, management_group, updated_at)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(6))
            """
        with conn.cursor() as cursor:
            for row in preserved:
                cursor.execute(
                    insert_sql,
                    (
                        int(row["management_sk"]),
                        int(row["management_center_id"]),
                        str(row["management"]),
                        row.get("management_detailed"),
                        str(row["management_group"]),
                    ),
                )
        print(f"Restored {len(preserved)} management rows into repaired dim_management")


def management_source_usable(conn, database: str, table: str) -> bool:
    # Never seed a dimension from itself.
    if database == DW_DB and table == "dim_management":
        return False
    if not table_exists(conn, database, table):
        return False
    columns = table_columns(conn, database, table)
    try:
        first_column(columns, ("management_center_id", "sch_mgmt_center_id", "management_sk"))
        first_column(columns, ("management", "management_name", "management_group"))
    except RuntimeError:
        return False
    return True


def ensure_databases(conn) -> None:
    if SILVER_DB == GOLD_DB:
        raise ValueError("Silver and Gold must use different databases")
    ensure_database(conn, SILVER_DB)
    ensure_database(conn, GOLD_DB)


def ensure_silver_sources(conn) -> None:
    required = (
        "silver_state",
        "silver_district",
        "silver_block",
        "silver_cluster",
        "silver_school_master",
        "silver_school_facility",
        "silver_management",
    )
    missing = [table for table in required if not table_exists(conn, SILVER_DB, table)]
    if missing:
        raise RuntimeError(f"Missing Doris Silver tables in {SILVER_DB}: {missing}. Run facility_structure_silver.py first.")


def ensure_history_table(conn) -> None:
    execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(SILVER_DB, FACILITY_HISTORY_TABLE)} (
            udise_sch_code VARCHAR(32) NOT NULL,
            academic_year VARCHAR(7) NOT NULL,
            previous_academic_year VARCHAR(7) NULL,
            row_checksum VARCHAR(64) NOT NULL,
            previous_row_checksum VARCHAR(64) NULL,
            change_type VARCHAR(32) NOT NULL,
            detected_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (udise_sch_code, academic_year)
        DISTRIBUTED BY HASH(udise_sch_code) BUCKETS 8
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
    )


def build_facility_history(conn) -> None:
    banner("BUILD silver_school_facility_history (checksum)")
    ensure_history_table(conn)
    execute(conn, f"TRUNCATE TABLE {qname(SILVER_DB, FACILITY_HISTORY_TABLE)}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(SILVER_DB, FACILITY_HISTORY_TABLE)}
        WITH base AS (
            SELECT
                udise_sch_code,
                academic_year,
                row_checksum,
                CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE) AS snapshot_date
            FROM {qname(SILVER_DB, 'silver_school_facility')}
        ),
        marked AS (
            SELECT
                b.*,
                LAG(academic_year) OVER (
                    PARTITION BY udise_sch_code ORDER BY snapshot_date
                ) AS previous_academic_year,
                LAG(row_checksum) OVER (
                    PARTITION BY udise_sch_code ORDER BY snapshot_date
                ) AS previous_row_checksum
            FROM base b
        )
        SELECT
            udise_sch_code,
            academic_year,
            previous_academic_year,
            row_checksum,
            previous_row_checksum,
            CASE
                WHEN previous_row_checksum IS NULL THEN 'INITIAL'
                WHEN previous_row_checksum = row_checksum THEN 'UNCHANGED'
                ELSE 'CHANGED'
            END AS change_type,
            CURRENT_TIMESTAMP(6)
        FROM marked
        """,
    )
    print(f"{FACILITY_HISTORY_TABLE}: {count_rows(conn, SILVER_DB, FACILITY_HISTORY_TABLE):,} rows")


def ensure_dw_tables(conn) -> None:
    ddl = [
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_year')} (
            year_sk INT NOT NULL,
            academic_year VARCHAR(7) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            is_latest TINYINT NOT NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (year_sk)
        DISTRIBUTED BY HASH(year_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_geography')} (
            geography_sk VARCHAR(32) NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            district_cd VARCHAR(20) NOT NULL,
            block_cd VARCHAR(20) NULL,
            cluster_cd VARCHAR(20) NULL,
            state_name VARCHAR(160) NULL,
            district_name VARCHAR(180) NULL,
            block_name VARCHAR(180) NULL,
            cluster_name VARCHAR(180) NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (geography_sk)
        DISTRIBUTED BY HASH(geography_sk) BUCKETS 4
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        dim_management_ddl(),
        dim_school_ddl(),
    ]
    for statement in ddl:
        execute(conn, statement)
    ensure_dim_management_schema(conn)
    recreate_table_if_schema_drift(
        conn, DW_DB, "dim_school", DIM_SCHOOL_REQUIRED, dim_school_ddl()
    )


def ensure_gold_tables(conn) -> None:
    recreate_table_if_schema_drift(
        conn,
        GOLD_DB,
        GOLD_MANAGEMENT_TABLE,
        GOLD_MANAGEMENT_REQUIRED,
        gold_management_ddl(),
    )
    recreate_table_if_schema_drift(
        conn,
        GOLD_DB,
        GOLD_CATEGORY_TABLE,
        GOLD_CATEGORY_REQUIRED,
        gold_category_ddl(),
    )


def drop_legacy_gold_star_tables(conn) -> None:
    """Remove older facility Gold shapes (star copies and amenity-by-management reports)."""
    for table in LEGACY_GOLD_STAR_TABLES:
        if table_exists(conn, GOLD_DB, table):
            print(f"Dropping legacy Gold table {GOLD_DB}.{table}")
            execute(conn, f"DROP TABLE IF EXISTS {qname(GOLD_DB, table)}")


def seed_management_dimension(conn) -> None:
    ensure_dim_management_schema(conn)
    target_rows = count_rows(conn, DW_DB, "dim_management")
    if target_rows > 0 and not FORCE_REFRESH_MANAGEMENT:
        print(f"Management dimension already populated: {target_rows} rows; keeping Doris source of truth")
        return

    source_db = None
    source_table = None
    for database, table in (
        (MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE),
        (GOLD_DB, "dim_management"),
    ):
        if management_source_usable(conn, database, table):
            source_db, source_table = database, table
            break

    if source_db and source_table:
        source_columns = table_columns(conn, source_db, source_table)
        id_col = first_column(
            source_columns,
            ("management_center_id", "sch_mgmt_center_id", "management_sk"),
        )
        management_col = first_column(source_columns, ("management", "management_name", "management_group"))
        detail_col = first_column(
            source_columns,
            ("management_detailed", "management_detail", "management_description"),
            required=False,
        )
        detail_expr = f"CAST({ident(detail_col)} AS STRING)" if detail_col else "NULL"
        mgmt_expr = f"TRIM(CAST({ident(management_col)} AS STRING))"
        lower_expr = f"LOWER({mgmt_expr})"
        print(f"Seeding {DW_DB}.dim_management from {source_db}.{source_table}")
        execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_management')}")
        execute(
            conn,
            f"""
            INSERT INTO {qname(DW_DB, 'dim_management')}
            (
                management_sk, management_center_id, management, management_detailed,
                management_group, updated_at
            )
            SELECT
                CAST({ident(id_col)} AS INT),
                CAST({ident(id_col)} AS INT),
                {mgmt_expr},
                {detail_expr},
                CASE
                    WHEN {lower_expr} LIKE '%private%' AND {lower_expr} LIKE '%unaided%'
                        THEN 'Private Unaided Recognized'
                    WHEN {lower_expr} LIKE '%aided%'
                        THEN 'Government Aided'
                    WHEN {lower_expr} LIKE '%government%' OR {lower_expr} LIKE '%govt%'
                        THEN 'Government'
                    ELSE 'Others'
                END,
                CURRENT_TIMESTAMP(6)
            FROM {qname(source_db, source_table)}
            """,
        )
    else:
        print(
            f"No usable management source "
            f"(UDISE_MANAGEMENT_SOURCE={MANAGEMENT_SOURCE_DB}.{MANAGEMENT_SOURCE_TABLE}); "
            f"bootstrapping from {SILVER_DB}.silver_school_master"
        )
        execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_management')}")
        execute(
            conn,
            f"""
            INSERT INTO {qname(DW_DB, 'dim_management')}
            (management_sk, management_center_id, management, management_detailed, management_group, updated_at)
            SELECT
                management_center_id,
                management_center_id,
                CONCAT('CENTER-', CAST(management_center_id AS STRING)),
                NULL,
                'Others',
                CURRENT_TIMESTAMP(6)
            FROM (
                SELECT DISTINCT management_center_id
                FROM {qname(SILVER_DB, 'silver_school_master')}
            ) x
            """,
        )

    rows = count_rows(conn, DW_DB, "dim_management")
    if rows <= 0:
        raise RuntimeError("dim_management produced zero rows")
    print(f"dim_management: {rows:,} rows")


def build_dim_year(conn) -> None:
    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_year')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_year')}
        SELECT
            CAST(CONCAT(SUBSTR(academic_year, 1, 4), SUBSTR(academic_year, 6, 2)) AS INT),
            academic_year,
            CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE),
            DATE_SUB(
                DATE_ADD(CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE), INTERVAL 1 YEAR),
                INTERVAL 1 DAY
            ),
            CASE
                WHEN academic_year = (SELECT MAX(academic_year) FROM {qname(SILVER_DB, 'silver_school_master')})
                THEN 1 ELSE 0
            END,
            CURRENT_TIMESTAMP(6)
        FROM (SELECT DISTINCT academic_year FROM {qname(SILVER_DB, 'silver_school_master')}) y
        """,
    )


def build_dim_geography(conn) -> None:
    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_geography')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_geography')}
        SELECT
            MD5(CONCAT_WS('|',
                sm.state_cd,
                sm.district_cd,
                COALESCE(sm.block_cd, ''),
                COALESCE(sm.cluster_cd, '')
            )) AS geography_sk,
            sm.state_cd,
            sm.district_cd,
            sm.block_cd,
            sm.cluster_cd,
            MAX(st.state_name),
            MAX(di.district_name),
            MAX(bl.block_name),
            MAX(cl.cluster_name),
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'silver_school_master')} sm
        LEFT JOIN {qname(SILVER_DB, 'silver_state')} st
          ON st.academic_year = sm.academic_year AND st.state_cd = sm.state_cd
        LEFT JOIN {qname(SILVER_DB, 'silver_district')} di
          ON di.academic_year = sm.academic_year AND di.district_cd = sm.district_cd
        LEFT JOIN {qname(SILVER_DB, 'silver_block')} bl
          ON bl.academic_year = sm.academic_year AND bl.block_cd = sm.block_cd
        LEFT JOIN {qname(SILVER_DB, 'silver_cluster')} cl
          ON cl.academic_year = sm.academic_year AND cl.cluster_cd = sm.cluster_cd
        GROUP BY sm.state_cd, sm.district_cd, sm.block_cd, sm.cluster_cd
        """,
    )


def build_dim_school_scd2(conn) -> None:
    banner("BUILD dim_school (SCD2)")

    missing_mgmt = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT DISTINCT s.management_center_id
                FROM {qname(SILVER_DB, 'silver_school_master')} s
                LEFT JOIN {qname(DW_DB, 'dim_management')} m
                  ON m.management_center_id = s.management_center_id
                WHERE m.management_center_id IS NULL
            ) x
            """,
        )[0]["n"]
    )
    if missing_mgmt:
        raise RuntimeError(f"dim_management incomplete: {missing_mgmt} school management IDs missing")

    # Full rebuild: drop/recreate so drifted schemas cannot mismatch the INSERT.
    print(f"Recreating {DW_DB}.dim_school for SCD2 rebuild")
    execute(conn, f"DROP TABLE IF EXISTS {qname(DW_DB, 'dim_school')}")
    execute(conn, dim_school_ddl())
    insert_cols = ",\n            ".join(ident(col) for col in DIM_SCHOOL_INSERT_COLUMNS)
    # Doris requires WITH LABEL + column list when INSERT ... uses a CTE.
    insert_label = f"fs_dim_school_{os.getpid()}_{int(time.time())}"
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_school')}
        WITH LABEL `{insert_label}`
        (
            {insert_cols}
        )
        WITH base AS (
            SELECT
                academic_year,
                CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE) AS snapshot_date,
                udise_sch_code,
                school_name,
                state_cd,
                district_cd,
                block_cd,
                cluster_cd,
                sch_category_id,
                management_center_id,
                school_status,
                MD5(CONCAT_WS('|',
                    COALESCE(school_name, '<NULL>'),
                    COALESCE(state_cd, '<NULL>'),
                    COALESCE(district_cd, '<NULL>'),
                    COALESCE(block_cd, '<NULL>'),
                    COALESCE(cluster_cd, '<NULL>'),
                    COALESCE(CAST(sch_category_id AS STRING), '<NULL>'),
                    COALESCE(CAST(management_center_id AS STRING), '<NULL>'),
                    COALESCE(CAST(school_status AS STRING), '<NULL>')
                )) AS row_hash
            FROM {qname(SILVER_DB, 'silver_school_master')}
        ),
        marked AS (
            SELECT
                b.*,
                CASE
                    WHEN LAG(row_hash) OVER (PARTITION BY udise_sch_code ORDER BY snapshot_date) = row_hash
                    THEN 0 ELSE 1
                END AS new_version
            FROM base b
        ),
        grouped AS (
            SELECT
                m.*,
                SUM(new_version) OVER (
                    PARTITION BY udise_sch_code ORDER BY snapshot_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS version_group
            FROM marked m
        ),
        collapsed AS (
            SELECT
                udise_sch_code,
                version_group,
                MIN(snapshot_date) AS valid_from,
                MAX(school_name) AS school_name,
                MAX(state_cd) AS state_cd,
                MAX(district_cd) AS district_cd,
                MAX(block_cd) AS block_cd,
                MAX(cluster_cd) AS cluster_cd,
                MAX(sch_category_id) AS sch_category_id,
                MAX(management_center_id) AS management_center_id,
                MAX(school_status) AS school_status,
                MAX(row_hash) AS row_hash
            FROM grouped
            GROUP BY udise_sch_code, version_group
        ),
        ranged AS (
            SELECT
                c.*,
                COALESCE(
                    DATE_SUB(
                        LEAD(valid_from) OVER (PARTITION BY udise_sch_code ORDER BY valid_from),
                        INTERVAL 1 DAY
                    ),
                    CAST('9999-12-31' AS DATE)
                ) AS valid_to,
                ROW_NUMBER() OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS version_no
            FROM collapsed c
        )
        SELECT
            r.udise_sch_code,
            r.valid_from,
            MD5(CONCAT(r.udise_sch_code, '|', CAST(r.valid_from AS STRING))) AS school_sk,
            r.version_no,
            r.school_name,
            MD5(CONCAT_WS('|',
                r.state_cd,
                r.district_cd,
                COALESCE(r.block_cd, ''),
                COALESCE(r.cluster_cd, '')
            )) AS geography_sk,
            r.sch_category_id AS category_sk,
            r.management_center_id AS management_sk,
            r.state_cd,
            r.district_cd,
            r.block_cd,
            r.cluster_cd,
            r.sch_category_id,
            r.management_center_id,
            r.school_status,
            r.valid_to,
            CASE WHEN r.valid_to = CAST('9999-12-31' AS DATE) THEN 1 ELSE 0 END AS is_current,
            r.row_hash,
            CASE WHEN r.version_no = 1 THEN 'INITIAL' ELSE 'ATTRIBUTE_CHANGE' END AS change_type,
            CURRENT_TIMESTAMP(6) AS created_at
        FROM ranged r
        """,
    )

    bad_current = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT udise_sch_code, SUM(is_current) AS current_versions
                FROM {qname(DW_DB, 'dim_school')}
                GROUP BY udise_sch_code
                HAVING current_versions <> 1
            ) x
            """,
        )[0]["n"]
    )
    if bad_current:
        raise RuntimeError(f"SCD2 validation failed: {bad_current} schools without exactly one current row")
    print(f"dim_school: {count_rows(conn, DW_DB, 'dim_school'):,} rows")


def facility_measure_columns(conn) -> list[str]:
    cols = table_columns(conn, SILVER_DB, "silver_school_facility")
    excluded = {
        "academic_year",
        "udise_sch_code",
        "row_checksum",
        "source_db",
        "source_schema",
        "processed_at",
    }
    return sorted(name for name in cols if name not in excluded)


def ensure_fact_table(conn, measure_columns: list[str]) -> None:
    measure_defs = ",\n            ".join(f"{ident(name)} VARCHAR(128) NULL" for name in measure_columns)
    execute(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'fact_school_facility')} (
            year_sk INT NOT NULL,
            school_sk VARCHAR(32) NOT NULL,
            geography_sk VARCHAR(32) NOT NULL,
            management_sk INT NOT NULL,
            academic_year VARCHAR(7) NOT NULL,
            udise_sch_code VARCHAR(32) NOT NULL,
            row_checksum VARCHAR(64) NOT NULL,
            {measure_defs},
            processed_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (year_sk, school_sk)
        DISTRIBUTED BY HASH(year_sk, school_sk) BUCKETS 16
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
    )


def build_fact(conn) -> None:
    banner("BUILD fact_school_facility")
    measures = facility_measure_columns(conn)
    ensure_fact_table(conn, measures)
    measure_select = ",\n            ".join(f"f.{ident(name)}" for name in measures)
    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'fact_school_facility')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'fact_school_facility')}
        SELECT
            y.year_sk,
            s.school_sk,
            g.geography_sk,
            m.management_sk,
            f.academic_year,
            f.udise_sch_code,
            f.row_checksum,
            {measure_select},
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'silver_school_facility')} f
        JOIN {qname(DW_DB, 'dim_year')} y
          ON y.academic_year = f.academic_year
        JOIN {qname(SILVER_DB, 'silver_school_master')} sm
          ON sm.academic_year = f.academic_year AND sm.udise_sch_code = f.udise_sch_code
        JOIN {qname(DW_DB, 'dim_geography')} g
          ON g.state_cd = sm.state_cd
         AND g.district_cd = sm.district_cd
         AND COALESCE(g.block_cd, '') = COALESCE(sm.block_cd, '')
         AND COALESCE(g.cluster_cd, '') = COALESCE(sm.cluster_cd, '')
        JOIN {qname(DW_DB, 'dim_management')} m
          ON m.management_center_id = sm.management_center_id
        JOIN {qname(DW_DB, 'dim_school')} s
          ON s.udise_sch_code = f.udise_sch_code
         AND CAST(CONCAT(SUBSTR(f.academic_year, 1, 4), '-04-01') AS DATE)
             BETWEEN s.valid_from AND s.valid_to
        """,
    )

    source_rows = count_rows(conn, SILVER_DB, "silver_school_facility")
    fact_rows = count_rows(conn, DW_DB, "fact_school_facility")
    if fact_rows != source_rows:
        raise RuntimeError(
            f"Fact row-count mismatch: silver_school_facility={source_rows:,}, fact={fact_rows:,}"
        )
    print(f"fact_school_facility: {fact_rows:,} rows")


def build_silver_model(conn) -> None:
    build_facility_history(conn)
    ensure_dw_tables(conn)
    build_dim_year(conn)
    build_dim_geography(conn)
    seed_management_dimension(conn)
    build_dim_school_scd2(conn)
    build_fact(conn)
    print("FACILITY SILVER MODEL: PASS")


def _flag_yes(column_sql: str) -> str:
    """1/3 = available-style yes; used for coded availability fields."""
    return f"CASE WHEN CAST({column_sql} AS INT) IN (1, 3) THEN 1 ELSE 0 END"


def _flag_functional(column_sql: str) -> str:
    return f"CASE WHEN CAST({column_sql} AS INT) = 1 THEN 1 ELSE 0 END"


def _flag_positive_count(column_sql: str) -> str:
    return f"CASE WHEN CAST({column_sql} AS INT) > 0 THEN 1 ELSE 0 END"


def _any_flag(exprs: Iterable[str], *, values: str) -> str:
    parts = [f"(CAST({expr} AS INT) IN ({values}))" for expr in exprs]
    return "CASE WHEN " + " OR ".join(parts) + " THEN 1 ELSE 0 END"


def _facility_columns(conn) -> dict[str, str]:
    cols = table_columns(conn, SILVER_DB, "silver_school_facility")
    return {name.lower(): name for name in cols}


def _resolve_one_column(lower_cols: dict[str, str], candidates: Iterable[str], *, label: str) -> str:
    for candidate in candidates:
        actual = lower_cols.get(candidate.lower())
        if actual is not None:
            print(f"Gold column map [{label}]: {actual}")
            return actual
    related = sorted(
        name
        for name in lower_cols.values()
        if any(token in name.lower() for token in label.lower().split())
    )
    raise RuntimeError(
        f"silver_school_facility missing {label}; tried={tuple(candidates)}; "
        f"related_columns={related or '[]'}"
    )


def _resolve_optional_columns(lower_cols: dict[str, str], candidates: Iterable[str]) -> list[str]:
    found: list[str] = []
    for candidate in candidates:
        actual = lower_cols.get(candidate.lower())
        if actual is not None:
            found.append(actual)
    return found


def _management_dim_db(conn) -> str:
    if table_exists(conn, DW_DB, "dim_management") and count_rows(conn, DW_DB, "dim_management") > 0:
        return DW_DB
    if table_exists(conn, GOLD_DB, "dim_management") and count_rows(conn, GOLD_DB, "dim_management") > 0:
        return GOLD_DB
    raise RuntimeError(
        f"No populated dim_management in {DW_DB} or {GOLD_DB}; run facility silver model first"
    )


def _category_join_and_expr(conn) -> tuple[str, str]:
    """Return (JOIN SQL fragment, category label expression) for gold summaries."""
    if table_exists(conn, SILVER_DB, "silver_school_category"):
        join_sql = f"""
            LEFT JOIN {qname(SILVER_DB, 'silver_school_category')} cat
              ON cat.academic_year = sm.academic_year
             AND cat.sch_category_id = sm.sch_category_id
        """
        category_expr = """
            COALESCE(
                NULLIF(TRIM(cat.category_name), ''),
                CASE
                    WHEN sm.sch_category_id IS NULL THEN 'Unknown'
                    ELSE CONCAT('Category-', CAST(sm.sch_category_id AS STRING))
                END
            )
        """
        return join_sql, category_expr
    print("Gold category map: silver_school_category missing; using sch_category_id labels")
    category_expr = """
        CASE
            WHEN sm.sch_category_id IS NULL THEN 'Unknown'
            ELSE CONCAT('Category-', CAST(sm.sch_category_id AS STRING))
        END
    """
    return "", category_expr


def _resolve_amenity_flag_exprs(conn) -> dict[str, str]:
    """Map gold amenity measure names to school-level 0/1 SQL expressions."""
    lower_cols = _facility_columns(conn)

    elec_col = _resolve_one_column(lower_cols, ELECTRICITY_CANDIDATES, label="electricity")
    elec = f"f.{ident(elec_col)}"

    water_avail_cols = _resolve_optional_columns(lower_cols, DRINKING_WATER_AVAIL_CANDIDATES)
    if not water_avail_cols:
        raise RuntimeError(
            "silver_school_facility missing drinking-water availability columns; "
            f"tried={DRINKING_WATER_AVAIL_CANDIDATES}"
        )
    water_func_cols = _resolve_optional_columns(lower_cols, DRINKING_WATER_FUNC_CANDIDATES)
    if not water_func_cols:
        water_func_cols = water_avail_cols
        print(
            "Gold column map [drinking_water_functional]: "
            "using availability columns with value=1 (no separate *_fun_yn)"
        )
    else:
        print(f"Gold column map [drinking_water_functional]: {water_func_cols}")
    print(f"Gold column map [drinking_water_available]: {water_avail_cols}")

    boys_avail_col = _resolve_one_column(
        lower_cols, BOYS_TOILET_AVAIL_CANDIDATES, label="boys toilet available"
    )
    boys_func_col = _resolve_one_column(
        lower_cols, BOYS_TOILET_FUNC_CANDIDATES, label="boys toilet functional"
    )
    girls_avail_col = _resolve_one_column(
        lower_cols, GIRLS_TOILET_AVAIL_CANDIDATES, label="girls toilet available"
    )
    girls_func_col = _resolve_one_column(
        lower_cols, GIRLS_TOILET_FUNC_CANDIDATES, label="girls toilet functional"
    )

    return {
        "electricity_available": _flag_yes(elec),
        "electricity_functional": _flag_functional(elec),
        "drinking_water_available": _any_flag(
            (f"f.{ident(c)}" for c in water_avail_cols), values="1, 3"
        ),
        "drinking_water_functional": _any_flag(
            (f"f.{ident(c)}" for c in water_func_cols), values="1"
        ),
        "boys_toilet_available": _flag_positive_count(f"f.{ident(boys_avail_col)}"),
        "boys_toilet_functional": _flag_positive_count(f"f.{ident(boys_func_col)}"),
        "girls_toilet_available": _flag_positive_count(f"f.{ident(girls_avail_col)}"),
        "girls_toilet_functional": _flag_positive_count(f"f.{ident(girls_func_col)}"),
    }


def _school_flags_cte(conn, amenity_exprs: dict[str, str]) -> str:
    """Shared school-level CTE used by both Gold reports."""
    mgmt_db = _management_dim_db(conn)
    category_join, category_expr = _category_join_and_expr(conn)
    amenity_select = ",\n                ".join(
        f"{amenity_exprs[name]} AS {name}" for name in GOLD_AMENITY_MEASURES
    )
    return f"""
        school_flags AS (
            SELECT
                f.academic_year AS ac_year,
                COALESCE(
                    NULLIF(TRIM(st.state_name), ''),
                    NULLIF(TRIM(sm.state_cd), ''),
                    'Unknown'
                ) AS state_ut,
                {category_expr} AS category,
                COALESCE(NULLIF(TRIM(m.management_group), ''), 'Others') AS management,
                {amenity_select}
            FROM {qname(SILVER_DB, 'silver_school_facility')} f
            JOIN {qname(SILVER_DB, 'silver_school_master')} sm
              ON sm.academic_year = f.academic_year
             AND sm.udise_sch_code = f.udise_sch_code
            LEFT JOIN {qname(SILVER_DB, 'silver_state')} st
              ON st.academic_year = sm.academic_year
             AND st.state_cd = sm.state_cd
            {category_join}
            LEFT JOIN {qname(mgmt_db, 'dim_management')} m
              ON m.management_center_id = sm.management_center_id
        ),
        geographies AS (
            SELECT * FROM school_flags
            UNION ALL
            SELECT
                ac_year,
                '{GOLD_NATIONAL_GEOGRAPHY}' AS state_ut,
                category,
                management,
                {", ".join(GOLD_AMENITY_MEASURES)}
            FROM school_flags
        )
    """


def _amenity_sum_exprs(prefix: str = "") -> str:
    qualifier = f"{prefix}." if prefix else ""
    return ",\n            ".join(
        f"SUM({qualifier}{name}) AS {name}" for name in GOLD_AMENITY_MEASURES
    )


def _insert_management_report(conn, amenity_exprs: dict[str, str]) -> None:
    """Build state × management facility report (plus national / All Management rollups)."""
    table = GOLD_MANAGEMENT_TABLE
    insert_label = f"fs_gold_{table}_{os.getpid()}_{int(time.time())}"
    amenity_cols = ", ".join(GOLD_AMENITY_MEASURES)
    amenity_sums = _amenity_sum_exprs()
    cte = _school_flags_cte(conn, amenity_exprs)
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, table)}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, table)}
        WITH LABEL `{insert_label}`
        (ac_year, state_ut, management, total, {amenity_cols})
        WITH {cte},
        report AS (
            SELECT * FROM geographies
            UNION ALL
            SELECT
                ac_year,
                state_ut,
                category,
                '{GOLD_ALL_MANAGEMENT}' AS management,
                {amenity_cols}
            FROM geographies
        )
        SELECT
            ac_year,
            state_ut,
            management,
            COUNT(*) AS total,
            {amenity_sums}
        FROM report
        GROUP BY ac_year, state_ut, management
        """,
    )
    rows = count_rows(conn, GOLD_DB, table)
    if rows <= 0:
        raise RuntimeError(f"{table} produced zero rows")
    print(f"{GOLD_DB}.{table}: {rows:,} rows")


def _insert_category_report(conn, amenity_exprs: dict[str, str]) -> None:
    """Build state × category facility report with management pivots + amenity totals."""
    table = GOLD_CATEGORY_TABLE
    insert_label = f"fs_gold_{table}_{os.getpid()}_{int(time.time())}"
    amenity_cols = ", ".join(GOLD_AMENITY_MEASURES)
    amenity_sums = _amenity_sum_exprs()
    cte = _school_flags_cte(conn, amenity_exprs)
    mgmt_aliases = (
        "government",
        "government_aided",
        "private_unaided_recognized",
        "others",
    )
    mgmt_pivots = ",\n            ".join(
        f"SUM(CASE WHEN management = '{group}' THEN 1 ELSE 0 END) AS {alias}"
        for group, alias in zip(MANAGEMENT_GROUPS, mgmt_aliases)
    )
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, table)}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, table)}
        WITH LABEL `{insert_label}`
        (ac_year, state_ut, category, total,
         government, government_aided, private_unaided_recognized, others,
         {amenity_cols})
        WITH {cte}
        SELECT
            ac_year,
            state_ut,
            category,
            COUNT(*) AS total,
            {mgmt_pivots},
            {amenity_sums}
        FROM geographies
        GROUP BY ac_year, state_ut, category
        """,
    )
    rows = count_rows(conn, GOLD_DB, table)
    if rows <= 0:
        raise RuntimeError(f"{table} produced zero rows")
    print(f"{GOLD_DB}.{table}: {rows:,} rows")


def build_gold_reports(conn) -> None:
    banner("BUILD facility Gold use-case summaries")
    drop_legacy_gold_star_tables(conn)
    ensure_gold_tables(conn)
    amenity_exprs = _resolve_amenity_flag_exprs(conn)
    _insert_management_report(conn, amenity_exprs)
    _insert_category_report(conn, amenity_exprs)

    print("FACILITY GOLD REPORTS: PASS")
    print("Example queries:")
    print(
        f"  SELECT * FROM {GOLD_DB}.{GOLD_MANAGEMENT_TABLE} "
        f"WHERE ac_year='2025-26' AND state_ut='{GOLD_NATIONAL_GEOGRAPHY}' "
        f"AND management='{GOLD_ALL_MANAGEMENT}';"
    )
    print(
        f"  SELECT * FROM {GOLD_DB}.{GOLD_CATEGORY_TABLE} "
        f"WHERE ac_year='2025-26' AND state_ut='{GOLD_NATIONAL_GEOGRAPHY}';"
    )


def validate_silver(conn) -> None:
    banner("VALIDATE FACILITY SILVER MODEL")
    checks = [
        (SILVER_DB, "silver_school_facility"),
        (SILVER_DB, FACILITY_HISTORY_TABLE),
        (DW_DB, "dim_year"),
        (DW_DB, "dim_geography"),
        (DW_DB, "dim_management"),
        (DW_DB, "dim_school"),
        (DW_DB, "fact_school_facility"),
    ]
    for database, table in checks:
        if not table_exists(conn, database, table):
            raise RuntimeError(f"Missing {database}.{table}")
        rows = count_rows(conn, database, table)
        if rows <= 0:
            raise RuntimeError(f"{database}.{table} has zero rows")
        print(f"{database}.{table}: {rows:,}")
    print("SILVER VALIDATION: PASS")


def validate_gold(conn) -> None:
    banner("VALIDATE FACILITY GOLD REPORTS")
    for table in GOLD_REPORT_TABLES:
        if not table_exists(conn, GOLD_DB, table):
            raise RuntimeError(f"Missing {GOLD_DB}.{table}")
        existing = {name.lower() for name in table_columns(conn, GOLD_DB, table)}
        missing = sorted(GOLD_REPORT_REQUIRED[table] - existing)
        if missing:
            raise RuntimeError(
                f"{GOLD_DB}.{table} missing columns {missing}; re-run facility_structure_gold.py"
            )
        rows = count_rows(conn, GOLD_DB, table)
        if rows <= 0:
            raise RuntimeError(f"{GOLD_DB}.{table} has zero rows")
        print(f"{GOLD_DB}.{table}: {rows:,}")

    amenity_bounds = " OR ".join(
        f"{name} > total" for name in GOLD_AMENITY_MEASURES
    )
    bad = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM {qname(GOLD_DB, GOLD_MANAGEMENT_TABLE)}
            WHERE {amenity_bounds}
            """,
        )[0]["n"]
    )
    if bad:
        raise RuntimeError(
            f"{GOLD_MANAGEMENT_TABLE}: {bad} rows fail amenity <= total bounds"
        )
    bad = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM {qname(GOLD_DB, GOLD_CATEGORY_TABLE)}
            WHERE {amenity_bounds}
               OR total <> government + government_aided
                              + private_unaided_recognized + others
            """,
        )[0]["n"]
    )
    if bad:
        raise RuntimeError(
            f"{GOLD_CATEGORY_TABLE}: {bad} rows fail amenity/management reconciliation"
        )

    # National All Management must equal sum of management buckets.
    reconciliation = query(
        conn,
        f"""
        SELECT
            ac_year,
            MAX(CASE WHEN management = '{GOLD_ALL_MANAGEMENT}' THEN total END) AS all_management,
            SUM(CASE WHEN management <> '{GOLD_ALL_MANAGEMENT}' THEN total ELSE 0 END) AS bucket_total
        FROM {qname(GOLD_DB, GOLD_MANAGEMENT_TABLE)}
        WHERE state_ut = '{GOLD_NATIONAL_GEOGRAPHY}'
        GROUP BY ac_year
        ORDER BY ac_year
        """,
    )
    for row in reconciliation:
        if row["all_management"] is None:
            raise RuntimeError(
                f"{GOLD_MANAGEMENT_TABLE} missing national All Management row: {row}"
            )
        if int(row["all_management"]) != int(row["bucket_total"]):
            raise RuntimeError(
                f"{GOLD_MANAGEMENT_TABLE} management reconciliation failed: {row}"
            )

    # State All Management should sum to national.
    state_vs_national = query(
        conn,
        f"""
        SELECT
            ac_year,
            MAX(CASE WHEN state_ut = '{GOLD_NATIONAL_GEOGRAPHY}' THEN total END) AS national_total,
            SUM(CASE WHEN state_ut <> '{GOLD_NATIONAL_GEOGRAPHY}' THEN total ELSE 0 END) AS state_total
        FROM {qname(GOLD_DB, GOLD_MANAGEMENT_TABLE)}
        WHERE management = '{GOLD_ALL_MANAGEMENT}'
        GROUP BY ac_year
        ORDER BY ac_year
        """,
    )
    for row in state_vs_national:
        if int(row["national_total"]) != int(row["state_total"]):
            raise RuntimeError(
                f"{GOLD_MANAGEMENT_TABLE} state vs national reconciliation failed: {row}"
            )

    # Category totals must match management All Management totals.
    mismatch = int(
        query(
            conn,
            f"""
            WITH actual AS (
                SELECT ac_year, state_ut, SUM(total) AS total
                FROM {qname(GOLD_DB, GOLD_CATEGORY_TABLE)}
                GROUP BY ac_year, state_ut
            ), expected AS (
                SELECT ac_year, state_ut, total
                FROM {qname(GOLD_DB, GOLD_MANAGEMENT_TABLE)}
                WHERE management = '{GOLD_ALL_MANAGEMENT}'
            )
            SELECT COUNT(*) AS n
            FROM actual a
            FULL OUTER JOIN expected e
              ON a.ac_year = e.ac_year AND a.state_ut = e.state_ut
            WHERE a.ac_year IS NULL OR e.ac_year IS NULL OR a.total <> e.total
            """,
        )[0]["n"]
    )
    if mismatch:
        raise RuntimeError(
            "Category totals differ from management All Management report"
        )

    for table in LEGACY_GOLD_STAR_TABLES:
        if table_exists(conn, GOLD_DB, table):
            raise RuntimeError(
                f"Legacy Gold table still present: {GOLD_DB}.{table}; "
                "re-run facility_structure_gold.py"
            )
    print("GOLD VALIDATION: PASS")


def validate(conn) -> None:
    validate_silver(conn)
    validate_gold(conn)
    print("VALIDATION: PASS")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=("silver", "fact", "gold", "validate"),
        default="silver",
    )
    args = parser.parse_args()
    conn = connect()
    try:
        healthy(conn)
        ensure_databases(conn)
        if args.stage == "validate":
            validate(conn)
            return
        if args.stage == "gold":
            ensure_silver_sources(conn)
            build_gold_reports(conn)
            validate_gold(conn)
            return
        ensure_silver_sources(conn)
        if args.stage == "silver":
            build_silver_model(conn)
        elif args.stage == "fact":
            build_fact(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
