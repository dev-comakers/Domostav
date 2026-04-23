#!/usr/bin/env python
"""One-shot migration from legacy SQLite databases into the Postgres instance.

Usage:
    # Make sure DATABASE_URL points to the target Postgres instance and that
    # `apply_schemas()` has been run at least once (which happens automatically
    # when webapp.py starts, or you can run `python -c "from db import apply_schemas; apply_schemas()"`).
    python scripts/migrate_sqlite_to_postgres.py \
        --spp data/app.db \
        --mzdovy mzdovy/data/payroll.db

Both arguments are optional: omit either one to skip that module.
The script is idempotent for a clean target -- it TRUNCATEs each destination
table before inserting, then resets identity sequences.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Ensure we can import the app's `db` package whether invoked from repo root
# or from the `scripts/` directory.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import apply_schemas, get_conn  # noqa: E402


# Canonical Postgres column order per table. Only columns listed here are
# migrated; anything exotic left in an old SQLite schema is ignored silently.
SPP_TABLES: dict[str, list[str]] = {
    "projects": ["id", "code", "name", "prompt", "created_at"],
    "sessions": [
        "id", "project_code", "period_month", "spp_path", "inventory_path",
        "nf45_path", "output_path", "stats_json", "created_at",
    ],
    "mappings": ["id", "project_code", "file_type", "mapping_json", "updated_at"],
    "overrides": [
        "id", "project_code", "article", "spp_rows_json", "reason", "updated_at",
    ],
    "scoped_overrides": [
        "id", "scope_type", "scope_value", "item_key", "spp_rows_json",
        "reason", "updated_at",
    ],
    "analysis_drafts": [
        "id", "project_code", "project_name", "period_month",
        "spp_path", "inventory_path", "nf45_path", "rules_path", "nomenclature_path",
        "project_prompt", "spp_mapping_json", "inventory_mapping_json",
        "created_at", "updated_at",
    ],
    "rules_registry": [
        "id", "rule_type", "scope_type", "scope_value", "rule_key",
        "rule_value_json", "reason", "priority", "enabled", "created_at", "updated_at",
    ],
    "rules_versions": ["id", "project_code", "label", "rules_json", "created_at"],
}

MZDOVY_TABLES: dict[str, list[str]] = {
    "payroll_imports": ["id", "period", "created_at"],
    "payroll_import_files": [
        "id", "import_id", "filename", "report_type", "company_name",
        "period", "parser_mode", "saved_path", "created_at",
    ],
    "payroll_parsed_rows": [
        "id", "import_id", "import_file_id", "report_type", "company_name",
        "period", "employee_name", "normalized_name", "person_code",
        "gross_wage", "social_employee", "social_employer",
        "health_employee", "health_employer", "tax_amount",
        "payout_amount", "settlement_amount", "srazky", "zaloha",
        "health_insurance_name", "source_row_index", "parser_mode", "raw_json",
    ],
    "payroll_employees": [
        "id", "full_name", "normalized_name", "project_name", "coordinator_name",
        "company_code", "company_name", "notes",
        "odvody_strhavame", "mesicni_mzda",
        "created_at", "updated_at",
    ],
    "payroll_preview_rows": [
        "id", "import_id", "period", "display_name", "normalized_name",
        "company_name", "employee_id", "project_name", "coordinator_name",
        "company_code", "gross_wage", "social_employee", "social_employer",
        "health_employee", "health_employer", "tax_amount",
        "odvody_platime", "odvody_strhavame", "mesicni_mzda",
        "control_sum_parsed", "control_sum_expected",
        "match_status", "warnings_json", "source_files_json",
    ],
    "payroll_export_runs": ["id", "import_id", "output_path", "created_at"],
    "payroll_employee_change_log": [
        "id", "employee_id", "action", "payload_json", "created_at",
    ],
}

# Booleans-as-integers in SQLite that need normalising for Postgres BOOLEAN.
BOOLEAN_COLUMNS: dict[tuple[str, str], str] = {
    ("rules_registry", "enabled"): "bool",
}


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def _coerce(value, kind: str):
    if value is None:
        return None
    if kind == "bool":
        return bool(int(value)) if not isinstance(value, bool) else value
    return value


def _migrate_tables(sqlite_path: Path, schema: str, table_map: dict[str, list[str]]) -> None:
    if not sqlite_path.exists():
        print(f"[skip] {sqlite_path} does not exist; nothing to migrate for schema '{schema}'.")
        return

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    try:
        print(f"\n>>> Migrating '{sqlite_path}' into schema '{schema}' ...")

        for table, pg_columns in table_map.items():
            sqlite_cols = _sqlite_columns(sqlite_conn, table)
            if not sqlite_cols:
                print(f"  [skip] table '{table}' missing in SQLite; leaving Postgres table empty.")
                continue

            usable_cols = [c for c in pg_columns if c in sqlite_cols]
            if not usable_cols:
                print(f"  [skip] table '{table}' has no overlapping columns.")
                continue

            select_clause = ", ".join(usable_cols)
            rows = sqlite_conn.execute(f"SELECT {select_clause} FROM {table}").fetchall()

            # Coerce legacy types (e.g. SQLite 0/1 -> Postgres BOOLEAN).
            payload: list[tuple] = []
            for r in rows:
                row_values = []
                for col in usable_cols:
                    kind = BOOLEAN_COLUMNS.get((table, col))
                    row_values.append(_coerce(r[col], kind) if kind else r[col])
                payload.append(tuple(row_values))

            with get_conn(schema=schema) as pg_conn, pg_conn.cursor() as cur:
                cur.execute(f'TRUNCATE "{schema}"."{table}" RESTART IDENTITY CASCADE')

                if payload:
                    cols_sql = ", ".join(f'"{c}"' for c in usable_cols)
                    placeholders = ", ".join(["%s"] * len(usable_cols))
                    override = " OVERRIDING SYSTEM VALUE" if "id" in usable_cols else ""
                    sql = (
                        f'INSERT INTO "{schema}"."{table}" ({cols_sql}){override} '
                        f"VALUES ({placeholders})"
                    )
                    cur.executemany(sql, payload)

                    if "id" in usable_cols:
                        # Only relevant for IDENTITY integer ids; text PKs skip this.
                        cur.execute(
                            """
                            SELECT data_type FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s AND column_name = 'id'
                            """,
                            (schema, table),
                        )
                        id_type_row = cur.fetchone()
                        id_type = id_type_row["data_type"] if id_type_row else ""
                        if id_type in {"bigint", "integer", "smallint"}:
                            cur.execute(
                                f'SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM "{schema}"."{table}"'
                            )
                            next_id = int(cur.fetchone()["next_id"])
                            cur.execute(
                                f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN id RESTART WITH {next_id}'
                            )

            print(f"  [ok]  {table}: {len(payload)} rows migrated")
    finally:
        sqlite_conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spp", type=Path, default=None, help="Path to SPP SQLite DB (data/app.db)")
    parser.add_argument("--mzdovy", type=Path, default=None, help="Path to Mzdovy SQLite DB (mzdovy/data/payroll.db)")
    parser.add_argument("--skip-apply-schemas", action="store_true", help="Skip DDL apply (use if already run)")
    args = parser.parse_args()

    if not args.skip_apply_schemas:
        print("Applying DDL to ensure target schemas/tables exist ...")
        apply_schemas()

    if args.spp:
        _migrate_tables(args.spp, "spp", SPP_TABLES)
    if args.mzdovy:
        _migrate_tables(args.mzdovy, "mzdovy", MZDOVY_TABLES)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
