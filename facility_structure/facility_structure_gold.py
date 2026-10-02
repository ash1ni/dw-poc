#!/usr/bin/env python3
"""Build facility use-case summaries in udise_gold from Silver facility sources."""
from __future__ import annotations

import argparse

import facility_structure_model as model


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("summary", "validate", "all"), default="all")
    args = parser.parse_args()
    conn = model.connect()
    try:
        model.healthy(conn)
        if args.stage == "validate":
            model.validate_gold(conn)
            return

        model.banner("FACILITY GOLD: build use-case reports")
        model.ensure_databases(conn)
        for table in ("silver_school_facility", "silver_school_master"):
            if not model.table_exists(conn, model.SILVER_DB, table):
                raise RuntimeError(
                    f"Missing {model.SILVER_DB}.{table}; run facility_structure_silver.py first"
                )
        # Always create/refresh Gold report tables before validating them.
        model.build_gold_reports(conn)
        model.validate_gold(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
