"""Airflow DAG for UDISE+ Facility Structure: S3 Bronze -> Silver -> Gold reports.

UDISE_PROJECT_DIR should be the repo root that contains ``facility_structure/``
(same convention as student_structure), e.g. ``/home/hello/dw-poc``.

Flat deploys (scripts directly under UDISE_PROJECT_DIR) are also supported.
Paths are resolved on the worker at task runtime.
"""

from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta, timezone

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv("UDISE_PROJECT_DIR", "/home/hello/dw-poc")
PYTHON_BIN = os.getenv("UDISE_PYTHON_BIN", "/home/hello/airflow-venv/bin/python")
JAVA_HOME = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

# Optional full-path overrides. Otherwise worker resolves nested vs flat layout.
BRONZE_SCRIPT = os.getenv("FACILITY_STRUCTURE_BRONZE_SCRIPT", "")
SILVER_SCRIPT = os.getenv("FACILITY_STRUCTURE_SILVER_SCRIPT", "")
MODEL_SCRIPT = os.getenv("FACILITY_STRUCTURE_MODEL_SCRIPT", "")
GOLD_SCRIPT = os.getenv("FACILITY_STRUCTURE_GOLD_SCRIPT", "")


def python_task(task_id: str, script_name: str, override: str, args: str, timeout: timedelta) -> BashOperator:
    override_q = shlex.quote(override) if override else "''"
    project_q = shlex.quote(PROJECT_DIR)
    python_q = shlex.quote(PYTHON_BIN)
    java_q = shlex.quote(JAVA_HOME)
    name_q = shlex.quote(script_name)
    args_part = f" {args}" if args else ""

    command = f"""
        set -euo pipefail

        unset SPARK_HOME || true
        unset PYTHONPATH || true
        unset CLASSPATH || true

        export JAVA_HOME={java_q}
        export PYSPARK_PYTHON={python_q}
        export PYSPARK_DRIVER_PYTHON={python_q}

        PROJECT_DIR={project_q}
        OVERRIDE={override_q}
        SCRIPT_NAME={name_q}

        if [[ -n "$OVERRIDE" ]]; then
          SCRIPT="$OVERRIDE"
        elif [[ -f "$PROJECT_DIR/facility_structure/$SCRIPT_NAME" ]]; then
          SCRIPT="$PROJECT_DIR/facility_structure/$SCRIPT_NAME"
        elif [[ -f "$PROJECT_DIR/$SCRIPT_NAME" ]]; then
          SCRIPT="$PROJECT_DIR/$SCRIPT_NAME"
        else
          echo "Missing script: $SCRIPT_NAME" >&2
          echo "UDISE_PROJECT_DIR=$PROJECT_DIR" >&2
          echo "Tried:" >&2
          echo "  $PROJECT_DIR/facility_structure/$SCRIPT_NAME" >&2
          echo "  $PROJECT_DIR/$SCRIPT_NAME" >&2
          echo "Listing UDISE_PROJECT_DIR:" >&2
          ls -la "$PROJECT_DIR" >&2 || true
          if [[ -d "$PROJECT_DIR/facility_structure" ]]; then
            echo "Listing facility_structure/:" >&2
            ls -la "$PROJECT_DIR/facility_structure" >&2 || true
          fi
          exit 1
        fi

        echo "Running: $SCRIPT{args_part}"
        cd "$(dirname "$SCRIPT")"
        {python_q} "$SCRIPT"{args_part}
    """
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        execution_timeout=timeout,
    )


with DAG(
    dag_id="udise_facility_structure_to_gold",
    description=(
        "UDISE+ Facility Structure: S3 Bronze sync -> Silver Doris model -> "
        "Gold use-case summaries (electricity / drinking water / boys toilet by management)"
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
        "facility_structure_bronze.py",
        BRONZE_SCRIPT,
        "",
        timedelta(hours=2),
    )

    silver = python_task(
        "build_facility_structure_silver",
        "facility_structure_silver.py",
        SILVER_SCRIPT,
        "--stage normalize",
        timedelta(hours=4),
    )

    silver_model = python_task(
        "build_facility_structure_model",
        "facility_structure_model.py",
        MODEL_SCRIPT,
        "--stage silver",
        timedelta(hours=3),
    )

    gold = python_task(
        "build_facility_structure_gold_reports",
        "facility_structure_gold.py",
        GOLD_SCRIPT,
        "--stage all",
        timedelta(hours=1),
    )

    bronze >> silver >> silver_model >> gold
