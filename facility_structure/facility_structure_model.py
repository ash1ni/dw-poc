"""Facility Structure Doris model: Silver history + Gold star schema (SCD2 school, fact)."""
from __future__ import annotations

import argparse
import os
from typing import Iterable

from doris_io import connect, healthy, query

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


def ensure_databases(conn) -> None:
    if SILVER_DB == GOLD_DB:
        raise ValueError("Silver and Gold must use different databases")
    execute(conn, f"CREATE DATABASE IF NOT EXISTS {ident(SILVER_DB)}")
    execute(conn, f"CREATE DATABASE IF NOT EXISTS {ident(GOLD_DB)}")


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


def ensure_gold_tables(conn) -> None:
    ddl = [
        f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'dim_year')} (
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
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'dim_geography')} (
            geography_sk VARCHAR(32) NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            district_cd VARCHAR(20) NOT NULL,
            block_cd VARCHAR(20) NOT NULL,
            cluster_cd VARCHAR(20) NOT NULL,
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
        f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'dim_management')} (
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
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'dim_school')} (
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
            block_cd VARCHAR(20) NOT NULL,
            cluster_cd VARCHAR(20) NOT NULL,
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
        """,
    ]
    for statement in ddl:
        execute(conn, statement)


def seed_management_dimension(conn) -> None:
    target_rows = count_rows(conn, GOLD_DB, "dim_management")
    if target_rows > 0 and not FORCE_REFRESH_MANAGEMENT:
        print(f"Management dimension already populated: {target_rows} rows; keeping Doris source of truth")
        return

    if table_exists(conn, MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE):
        source_columns = table_columns(conn, MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE)
        id_col = first_column(source_columns, ("management_center_id", "sch_mgmt_center_id"))
        management_col = first_column(source_columns, ("management", "management_name", "management_group"))
        detail_col = first_column(
            source_columns,
            ("management_detailed", "management_detail", "management_description"),
            required=False,
        )
        detail_expr = f"CAST({ident(detail_col)} AS STRING)" if detail_col else "NULL"
        mgmt_expr = f"TRIM(CAST({ident(management_col)} AS STRING))"
        lower_expr = f"LOWER({mgmt_expr})"
        execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'dim_management')}")
        execute(
            conn,
            f"""
            INSERT INTO {qname(GOLD_DB, 'dim_management')}
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
            FROM {qname(MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE)}
            """,
        )
    else:
        execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'dim_management')}")
        execute(
            conn,
            f"""
            INSERT INTO {qname(GOLD_DB, 'dim_management')}
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

    rows = count_rows(conn, GOLD_DB, "dim_management")
    if rows <= 0:
        raise RuntimeError("dim_management produced zero rows")


def build_dim_year(conn) -> None:
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'dim_year')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, 'dim_year')}
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
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'dim_geography')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, 'dim_geography')}
        SELECT
            MD5(CONCAT_WS('|', sm.state_cd, sm.district_cd, sm.block_cd, sm.cluster_cd)) AS geography_sk,
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
                LEFT JOIN {qname(GOLD_DB, 'dim_management')} m
                  ON m.management_center_id = s.management_center_id
                WHERE m.management_center_id IS NULL
            ) x
            """,
        )[0]["n"]
    )
    if missing_mgmt:
        raise RuntimeError(f"dim_management incomplete: {missing_mgmt} school management IDs missing")

    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'dim_school')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, 'dim_school')}
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
            MD5(CONCAT_WS('|', r.state_cd, r.district_cd, r.block_cd, r.cluster_cd)) AS geography_sk,
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
            CURRENT_TIMESTAMP(6)
        FROM ranged r
        """,
    )

    bad_current = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT udise_sch_code, SUM(is_current) AS current_versions
                FROM {qname(GOLD_DB, 'dim_school')}
                GROUP BY udise_sch_code
                HAVING current_versions <> 1
            ) x
            """,
        )[0]["n"]
    )
    if bad_current:
        raise RuntimeError(f"SCD2 validation failed: {bad_current} schools without exactly one current row")
    print(f"dim_school: {count_rows(conn, GOLD_DB, 'dim_school'):,} rows")


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
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'fact_school_facility')} (
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
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, 'fact_school_facility')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, 'fact_school_facility')}
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
        JOIN {qname(GOLD_DB, 'dim_year')} y
          ON y.academic_year = f.academic_year
        JOIN {qname(SILVER_DB, 'silver_school_master')} sm
          ON sm.academic_year = f.academic_year AND sm.udise_sch_code = f.udise_sch_code
        JOIN {qname(GOLD_DB, 'dim_geography')} g
          ON g.state_cd = sm.state_cd
         AND g.district_cd = sm.district_cd
         AND g.block_cd = sm.block_cd
         AND g.cluster_cd = sm.cluster_cd
        JOIN {qname(GOLD_DB, 'dim_management')} m
          ON m.management_center_id = sm.management_center_id
        JOIN {qname(GOLD_DB, 'dim_school')} s
          ON s.udise_sch_code = f.udise_sch_code
         AND CAST(CONCAT(SUBSTR(f.academic_year, 1, 4), '-04-01') AS DATE)
             BETWEEN s.valid_from AND s.valid_to
        """,
    )

    source_rows = count_rows(conn, SILVER_DB, "silver_school_facility")
    fact_rows = count_rows(conn, GOLD_DB, "fact_school_facility")
    if fact_rows != source_rows:
        raise RuntimeError(
            f"Fact row-count mismatch: silver_school_facility={source_rows:,}, fact={fact_rows:,}"
        )
    print(f"fact_school_facility: {fact_rows:,} rows")


def build_silver_model(conn) -> None:
    build_facility_history(conn)
    ensure_gold_tables(conn)
    build_dim_year(conn)
    build_dim_geography(conn)
    seed_management_dimension(conn)
    build_dim_school_scd2(conn)
    build_fact(conn)
    print("FACILITY MODEL: PASS")


def validate(conn) -> None:
    banner("VALIDATE FACILITY STRUCTURE")
    checks = [
        (SILVER_DB, "silver_school_facility"),
        (SILVER_DB, FACILITY_HISTORY_TABLE),
        (GOLD_DB, "dim_year"),
        (GOLD_DB, "dim_geography"),
        (GOLD_DB, "dim_management"),
        (GOLD_DB, "dim_school"),
        (GOLD_DB, "fact_school_facility"),
    ]
    for database, table in checks:
        if not table_exists(conn, database, table):
            raise RuntimeError(f"Missing {database}.{table}")
        rows = count_rows(conn, database, table)
        if rows <= 0:
            raise RuntimeError(f"{database}.{table} has zero rows")
        print(f"{database}.{table}: {rows:,}")
    print("VALIDATION: PASS")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("silver", "fact", "validate"), default="silver")
    args = parser.parse_args()
    conn = connect()
    try:
        healthy(conn)
        ensure_databases(conn)
        if args.stage == "validate":
            validate(conn)
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
