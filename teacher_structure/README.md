# Teacher Structure — two Gold reports with Silver SCD2

Deploy under the repo root that also holds `student_structure/` / `facility_structure/`
(e.g. `/home/hello/dw-poc`), so paths look like `.../teacher_structure/*.py`.

Package-local `doris_io.py` is included (same hardened helpers as facility/student).
`project_config.py` loads the repo `.env` and a package-local `.env` for Bronze S3 and paths.
Teacher scripts are self-contained flat imports (Airflow runs them with
`cd` into `teacher_structure/`). They reuse completed Student Silver tables, not student enrollment.

Copy the DAG from `airflow/dags/teacher_structure_to_gold_dag.py` (or this folder’s copy) into the
Airflow `dags/` folder. Set `UDISE_PROJECT_DIR` to the **repo root** (not `.../teacher_structure`).

## Airflow env (worker)

| Variable | Typical value |
|---|---|
| `UDISE_PROJECT_DIR` | `/home/hello/dw-poc` |
| `UDISE_PYTHON_BIN` | `/home/hello/airflow-venv/bin/python` |
| `JAVA_HOME` | `/usr/lib/jvm/java-17-openjdk-amd64` |
| `UDISE_BRONZE_ROOT` | writable path (not a `/absolute/...` placeholder) |

The DAG unsets `SPARK_HOME`, `PYTHONPATH`, and `CLASSPATH`, resolves nested vs flat script paths
at runtime, and runs Bronze → Silver normalize → Silver model (fact) → Gold.

## Bronze (S3 → local)

Syncs latest `ingest_date` partitions into `{UDISE_BRONZE_ROOT}/{academic_year}/{table}/*.parquet`.
Required tables: `tch_summary`, `mst_state`, `mst_district`, `mst_sch_category`, and exactly one
school-master table (`school_master`, `sch_master`, or `*_local` alias).

```bash
cd teacher_structure
python teacher_structure_bronze.py
python teacher_structure_bronze.py --skip-sync   # validate local Bronze only
```

## Run (same layout Airflow uses)

```bash
cd /home/hello/dw-poc/teacher_structure
unset SPARK_HOME
unset PYTHONPATH
unset CLASSPATH
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
python teacher_structure_bronze.py &&
python teacher_structure_silver.py --stage normalize &&
python teacher_structure_model.py --stage silver &&
python teacher_structure_gold.py --stage all
```

If Teacher Silver snapshot is already built, rebuild mapped facts without Spark:

```bash
python teacher_structure_model.py --stage silver
python teacher_structure_gold.py --stage all
```

For a change to Student school/state/management mapping, rerun Student Silver
first, then Teacher Silver and Gold.

## Output tables

| Database | Table | Purpose |
|---|---|---|
| udise_silver | teacher_structure_snapshot | Year + school code, male/female/transgender counts |
| udise_silver | teacher_structure_snapshot_history | Persistent SCD2 revisions of the teacher counts |
| udise_silver | fact_teacher_structure | Teacher counts + total + year-specific school/state/category/management mapping |
| udise_silver | fact_teacher_structure_history | SCD2 revisions of counts and mapped school attributes |
| udise_gold | teacher_structure_management | State/year/management totals, three broad levels and ten grade-category columns |
| udise_gold | teacher_structure_category | State/year/category rows with four management total columns |

No teacher_structure_ptr or teacher_structure_summary table is created.
Gold has no history tables. Superset datasets using old table names need updating.

Management report includes ac_year and management so the same physical table
supports All Management, Government, Government Aided, Private Unaided Recognized
and Others across all six academic years. No gender columns are in Gold.

Category report has exactly:
ac_year, india_state_ut, category, total, government, government_aided,
private_unaided_recognized, others.
All counts are teachers, not students. Category labels are Foundational +
Preparatory School, Middle School and Secondary School.

## Category mapping

Total teachers = male_tch + female_tch + transgen_tch.
Schools join on BOTH academic_year and udise_sch_code. Category validity is
checked against school_category_master (from mst_sch_category). Management
labels come from dim_management; duplicate or missing mappings fail for review.

| Report column | Source school category IDs |
|---|---|
| Foundational + Preparatory / grades_1_5 | 1 and 12 |
| Middle / grades_1_8 | 2 |
| Middle / grades_6_8 | 4 |
| Secondary / grades_1_10 | 6 |
| Secondary / grades_6_10 | 7 |
| Secondary / grades_9_10 | 8 |
| Secondary / grades_1_12 | 3 |
| Secondary / grades_6_12 | 5 |
| Secondary / grades_9_12 | 10 |
| Secondary / grades_11_12 | 11 |

Category 12 is included in the first report column. This is grounded in the
numbers you supplied: Maharashtra 136154 + 36 = 136190; Himachal Pradesh
21101 + 63 = 21164. Thus Total equals the ten report columns and the three
broad groups. The school-category IDs remain unchanged in Silver and its history;
only teacher report grouping folds pre-primary into the first group. It does
not alter the student's category dimension.

Expected Maharashtra example (2025-26, All Management), if the source is the
same: 750272, 136190, 193891, 272, 154013, 29421, 4060, 188668, 14851,
1651, 27255. These values are not hardcoded. Broad totals are 136190, 194163,
419919. Available Source Total is not labelled India without national coverage.

## SCD2 meaning

Business key = academic_year + udise_sch_code. Each observation of a changed
teacher count or mapped attribute closes the previous version and opens another.
Metadata: version_no, valid_from, valid_to, is_current, row_hash, is_deleted,
change_type. valid_from/valid_to are UTC observation times, with valid_to
exclusive. An unchanged rerun adds no version. Missing keys create DELETE
versions; returning keys create REACTIVATED versions. Always use complete
six-year snapshots; a partial snapshot would be interpreted as deletions.

Different academic years are distinct business keys, not revisions of each
other. Existing Student school SCD2 continues to represent annual school changes.
Teacher history records corrections within a year. History starts from the first
baseline available to this implementation; it cannot recover earlier overwritten
values. Gold is a current projection of Silver, without duplicated SCD2 rows.

Run only one pipeline/test at a time. Snapshot history, fact history and the two
Gold reports publish independently; a failure can leave stages at different
refresh points. Correct the problem and rerun. No multi-table transaction is
claimed. Snapshot/fact history is recorded before current-table replacement;
a rare replacement failure can leave history newer than current until rerun.

## Test one real teacher row and see history

The test changes one teacher count in existing Bronze Parquet, retains original
bytes and any Hadoop checksum outside Bronze, and guards against overwriting
unrelated modifications. Inspect first; choose a school present in tch_summary
using --school-code if the default 02010100501 is absent.

```bash
cd teacher_structure
python test_real_teacher_row.py inspect --school-code 02010100501
python test_real_teacher_row.py history --school-code 02010100501
python test_real_teacher_row.py apply --school-code 02010100501 --column male_tch --delta 10
python teacher_structure_silver.py --stage normalize && python teacher_structure_model.py --stage silver && python teacher_structure_gold.py --stage all
python test_real_teacher_row.py history --school-code 02010100501
python test_real_teacher_row.py restore --school-code 02010100501
python teacher_structure_silver.py --stage normalize && python teacher_structure_model.py --stage silver && python teacher_structure_gold.py --stage all
python test_real_teacher_row.py history --school-code 02010100501
```

Use the same --year, --column and --backup-dir when restoring (defaults:
2025-26 and male_tch). With original male count 6, +10 creates 16, and restore
returns 6. History should show INITIAL → CHANGE → CHANGE, with exactly one
active nondeleted version. Gold totals rise by 10 in the school's state,
management, broad category and detailed category, then return after restoration.
A completed backup is retained; a new test needs a new --backup-dir.

## Validation

Five offline tests passed. Included tests run generated aggregation SQL and persistent-history SQL against
SQLite with compatibility functions and mock Doris DDL. They check source joins,
all categories, gender totals, all management groups, report reconciliation,
pre-primary inclusion, initial/change/unchanged/delete/reactivation history, and
safe single-cell Parquet apply/restore. Python compilation and ZIP integrity
are also checked. Live Doris, Stream Load, Spark and Airflow execution must be
verified in your environment; they are not available here.

Run the offline tests from the parent project folder:

```bash
python teacher_structure/tests/test_teacher_structure.py
```
