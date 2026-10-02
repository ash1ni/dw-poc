# Teacher Structure — two Gold reports with Silver SCD2

Extract teacher_structure into this existing parent project folder:

`/home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver`

The result must be `.../udise_pipeline_to_silver/teacher_structure/*.py`.
Parent folder must contain your working project_config.py and doris_io.py.
Teacher scripts are self-contained and do not import the student Python modules.
They reuse the completed Student Silver tables, not student enrollment.
Copy teacher_structure_to_gold_dag.py to `/home/shubham/airflow/dags/`.

## Run

```bash
source /home/shubham/udise_env/bin/activate
cd /home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver
unset SPARK_HOME
unset PYTHONPATH
python -m teacher_structure.teacher_structure_bronze &&
python -m teacher_structure.teacher_structure_silver &&
python -m teacher_structure.teacher_structure_gold --retire-legacy
```

If Teacher Silver is already built, run it once to initialize persistent history
and build the new teacher fact. Subsequent Gold-only refreshes can run with:

```bash
python -m teacher_structure.teacher_structure_gold
```

For a change to Student school/state/management mapping, rerun Student Silver
first, then Teacher Silver and Gold. Teacher Silver's `--stage fact` can rebuild
mapped teacher facts using the current teacher snapshot without Spark.

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
`--retire-legacy` renames any existing old reports to `__retired_<token>` after
both new reports pass. It preserves old data; retired tables are not refreshed.
Without this flag, old reports remain as stale legacy tables. Gold has no new
history tables. Superset datasets using the old table names need updating.

Management report includes ac_year and management so the same physical table
supports All Management, Government, Government Aided, Private Unaided Recognized
and Others across all six academic years. The supplied query projections show
only the requested report columns. No gender columns are in Gold.

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
python -m teacher_structure.test_real_teacher_row inspect --school-code 02010100501
python -m teacher_structure.test_real_teacher_row history --school-code 02010100501
python -m teacher_structure.test_real_teacher_row apply --school-code 02010100501 --column male_tch --delta 10
python -m teacher_structure.teacher_structure_silver && python -m teacher_structure.teacher_structure_gold
python -m teacher_structure.test_real_teacher_row history --school-code 02010100501
python -m teacher_structure.test_real_teacher_row restore --school-code 02010100501
python -m teacher_structure.teacher_structure_silver && python -m teacher_structure.teacher_structure_gold
python -m teacher_structure.test_real_teacher_row history --school-code 02010100501
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
