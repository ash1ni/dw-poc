"""Airflow DAG for UDISE+ Facility Structure: S3 Bronze -> Silver -> Gold star schema."""

import os
import shlex
from datetime import datetime, timedelta, timezone

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv(
    "UDISE_PROJECT_DIR",
    "/home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver",
)
PYTHON_BIN = os.getenv("UDISE_PYTHON_BIN", "/home/shubham/udise_env/bin/python")
JAVA_HOME = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

FACILITY_DIR = os.path.join(PROJECT_DIR, "facility_structure")

BRONZE_SCRIPT = os.getenv(
    "FACILITY_STRUCTURE_BRONZE_SCRIPT",
    os.path.join(FACILITY_DIR, "facility_structure_bronze.py"),
)
SILVER_SCRIPT = os.getenv(
    "FACILITY_STRUCTURE_SILVER_SCRIPT",
    os.path.join(FACILITY_DIR, "facility_structure_silver.py"),
)
MODEL_SCRIPT = os.getenv(
    "FACILITY_STRUCTURE_MODEL_SCRIPT",
    os.path.join(FACILITY_DIR, "facility_structure_model.py"),
)
GOLD_SCRIPT = os.getenv(
    "FACILITY_STRUCTURE_GOLD_SCRIPT",
    os.path.join(FACILITY_DIR, "facility_structure_gold.py"),
)


def python_task(task_id: str, script: str, args: str, timeout: timedelta) -> BashOperator:
    command = f"""
        set -euo pipefail
        cd {shlex.quote(PROJECT_DIR)}

        unset SPARK_HOME || true
        unset PYTHONPATH || true
        unset CLASSPATH || true

        export JAVA_HOME={shlex.quote(JAVA_HOME)}
        export PYSPARK_PYTHON={shlex.quote(PYTHON_BIN)}
        export PYSPARK_DRIVER_PYTHON={shlex.quote(PYTHON_BIN)}

        test -f {shlex.quote(script)}
        {shlex.quote(PYTHON_BIN)} {shlex.quote(script)} {args}
    """
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        execution_timeout=timeout,
    )


with DAG(
    dag_id="udise_facility_structure_to_gold",
    description=(
        "UDISE+ Facility Structure: S3 Bronze sync -> Silver Doris -> "
        "checksum history + Gold dim_school SCD2 + fact_school_facility"
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "shubham",
        "retries": 0,
    },
    tags=["udise", "facility-structure", "bronze", "silver", "gold", "scd2", "doris", "s3"],
) as dag:

    bronze = python_task(
        "sync_facility_structure_bronze",
        BRONZE_SCRIPT,
        "",
        timedelta(hours=2),
    )

    silver = python_task(
        "build_facility_structure_silver",
        SILVER_SCRIPT,
        "--stage normalize",
        timedelta(hours=4),
    )

    silver_model = python_task(
        "build_facility_structure_model",
        MODEL_SCRIPT,
        "--stage silver",
        timedelta(hours=3),
    )

    gold = python_task(
        "validate_facility_structure_gold",
        GOLD_SCRIPT,
        "--stage all",
        timedelta(hours=1),
    )

    bronze >> silver >> silver_model >> gold
