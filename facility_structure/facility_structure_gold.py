#!/usr/bin/env python3
"""Validate Facility Structure Gold star schema after Silver model build."""
from __future__ import annotations

import argparse

import facility_structure_model as model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("validate", "all"), default="all")
    args = parser.parse_args()
    conn = model.connect()
    try:
        model.healthy(conn)
        if args.stage == "all":
            model.ensure_databases(conn)
            model.ensure_silver_sources(conn)
            for table in ("dim_year", "dim_geography", "dim_management", "dim_school", "fact_school_facility"):
                if not model.table_exists(conn, model.GOLD_DB, table):
                    raise RuntimeError(f"Missing {model.GOLD_DB}.{table}; run facility_structure_silver.py --stage all")
        model.validate(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
