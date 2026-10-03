# Facility Structure pipeline

S3 Bronze Parquet → `udise_silver` normalized + star schema → Gold use-case summaries in `udise_gold`.

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

Then runs the SQL model stage (checksum history + Silver dimensions/fact):

- `silver_school_facility_history`
- `dim_year`, `dim_geography`, `dim_management`, `dim_school` (SCD2)
- `fact_school_facility`

```bash
python facility_structure/facility_structure_silver.py
# or model only:
python facility_structure/facility_structure_model.py --stage silver
```

## Gold (`udise_gold`)

Two report tables matching the student/teacher Gold pattern:

| Table | Grain | Meaning |
| --- | --- | --- |
| `facility_structure_management` | State × management | School totals + amenity available/functional counts |
| `facility_structure_category` | State × category | School totals by management columns + amenity totals |

### `facility_structure_management`

Columns: `ac_year`, `state_ut`, `management`, `total`,
`electricity_available`, `electricity_functional`,
`drinking_water_available`, `drinking_water_functional`,
`boys_toilet_available`, `boys_toilet_functional`,
`girls_toilet_available`, `girls_toilet_functional`.

### `facility_structure_category`

Columns: `ac_year`, `state_ut`, `category`, `total`,
`government`, `government_aided`, `private_unaided_recognized`, `others`,
plus the same amenity available/functional columns as the management report.

Grain includes rollups (same pattern as student/teacher gold):

- States plus `Available Source Total` for national
- Management groups plus `All Management` (management report only)

Source field mapping (resolved with aliases):

- Electricity: `electricity_yn` (1=Yes, 2=No, 3=Yes but not functional)
- Drinking water: any of `hand_pump_yn` / `well_prot_yn` / `tap_yn` / `othsrc_yn` / `well_unprot_yn` / `pack_water_yn` (same 1/2/3 coding; functional = value 1)
- Boys toilet: `toiletb` available seats, `toiletb_fun` functional seats
- Girls toilet: `toiletg` available seats, `toiletg_fun` functional seats

```bash
python facility_structure/facility_structure_gold.py --stage all
```

Example:

```sql
SELECT * FROM udise_gold.facility_structure_management
WHERE ac_year = '2025-26'
  AND state_ut = 'Available Source Total'
  AND management = 'All Management';

SELECT * FROM udise_gold.facility_structure_category
WHERE ac_year = '2025-26'
  AND state_ut = 'Available Source Total';
```

Gold also drops legacy facility tables previously written into `udise_gold`
(`dim_year`, `dim_geography`, `dim_school`, `fact_school_facility`, and the older
`facility_*_by_management` amenity reports). Shared `udise_gold.dim_management`
is left untouched.

## Environment

Uses repo `.env` (see `project_config.py`): S3 credentials, `BRONZE_BUCKET`, `BRONZE_PREFIX`, `UDISE_BRONZE_ROOT`, Doris FE/MySQL + HTTP ports, `UDISE_SILVER_DB`, `UDISE_GOLD_DB`, optional `SILVER_YEARS`.

Management labels bootstrap from `UDISE_MANAGEMENT_SOURCE_DB` / `UDISE_MANAGEMENT_SOURCE_TABLE` into Silver (same pattern as Student Structure). Gold management bootstrap source is never modified by this pipeline.

## Airflow

`airflow/dags/facility_structure_to_gold_dag.py` — Bronze sync → Silver normalize → Silver model → Gold use-case reports.
