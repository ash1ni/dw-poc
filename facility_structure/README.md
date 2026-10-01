# Facility Structure pipeline

S3 Bronze Parquet → `udise_silver` normalized tables → Gold star schema in `udise_gold`.

## Bronze (S3 → local)

Syncs latest `ingest_date` partitions from:

`{BRONZE_PREFIX}/{source_db}/{source_schema}/{table}/ingest_date=YYYY-MM-DD/part-*.parquet`

into:

`{UDISE_BRONZE_ROOT}/{academic_year}/{table}/*.parquet`

Required tables: `school_master` (or alias), `sch_facility`, `mst_state`, `mst_district`, `mst_block`, `mst_cluster`, plus `mst_sch_category` when present on S3.

```bash
python facility_structure/facility_structure_bronze.py
python facility_structure/facility_structure_bronze.py --skip-sync   # validate local Bronze only
```

## Silver (Spark → Doris)

Publishes:

- `silver_state`, `silver_district`, `silver_block`, `silver_cluster`
- `silver_school_category`, `silver_management`
- `silver_school_master` (annual school snapshot)
- `silver_school_facility` (full `sch_facility` row + `row_checksum`)

Then runs the SQL model stage (checksum history + Gold dimensions/fact):

```bash
python facility_structure/facility_structure_silver.py
```

## Gold (`udise_gold`)

- `dim_year`, `dim_geography`, `dim_management`, `dim_school` (SCD2, April 1 snapshot boundaries)
- `fact_school_facility` (one row per school per year; joins SCD2 school version for that year)

Silver also stores `silver_school_facility_history` (checksum change tracking).

## Environment

Uses repo `.env` (see `project_config.py`): S3 credentials, `BRONZE_BUCKET`, `BRONZE_PREFIX`, `UDISE_BRONZE_ROOT`, Doris FE/MySQL + HTTP ports, `UDISE_SILVER_DB`, `UDISE_GOLD_DB`, optional `SILVER_YEARS`.

Management labels bootstrap from `UDISE_MANAGEMENT_SOURCE_DB` / `UDISE_MANAGEMENT_SOURCE_TABLE` when populated (same pattern as Student Structure).

## Airflow

`airflow/dags/facility_structure_to_gold_dag.py` — Bronze sync → Silver normalize → model → Gold validation.
