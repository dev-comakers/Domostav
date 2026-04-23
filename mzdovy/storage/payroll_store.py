"""Postgres storage for the Mzdovy prehled module (schema: `mzdovy`).

Public API intentionally matches the previous SQLite-backed store so callers
(service layer, blueprint, seed utilities) do not require changes.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from db import get_conn

from ..payroll.html_utils import normalize_name
from ..payroll.models import EmployeeInput, ImportSummary, utc_now


SCHEMA = "mzdovy"


class PayrollStore:
    def __init__(self, db_path: Any = None):
        # Accepted for backwards compatibility; connection comes from DATABASE_URL.
        self._db_path = db_path
        self._migrate_to_current_matching_scheme()

    # ---------- Internal helpers ----------

    def _migrate_to_current_matching_scheme(self) -> None:
        from ..payroll.employee_seed import clean_seed_name

        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT id, full_name, normalized_name, notes FROM payroll_employees")
            employee_rows = cur.fetchall()

            planned: list[tuple[int, str, str, str | None, bool]] = []
            migration_needed_employees = False
            for row in employee_rows:
                cleaned, extra_notes = clean_seed_name(row["full_name"])
                cleaned = cleaned or row["full_name"]
                target_norm = normalize_name(cleaned)
                target_notes = row["notes"] or extra_notes
                changed = (
                    cleaned != row["full_name"]
                    or target_norm != row["normalized_name"]
                    or (target_notes or None) != (row["notes"] or None)
                )
                if changed:
                    migration_needed_employees = True
                planned.append((row["id"], cleaned, target_norm, target_notes, changed))

            if migration_needed_employees:
                # Park all normalized names on a temp value to free the unique index.
                cur.execute("UPDATE payroll_employees SET normalized_name = '__tmp_' || id")
                seen: dict[str, int] = {}
                for emp_id, cleaned, target_norm, target_notes, _ in planned:
                    if target_norm in seen:
                        cur.execute(
                            "UPDATE payroll_preview_rows SET employee_id = %s WHERE employee_id = %s",
                            (seen[target_norm], emp_id),
                        )
                        cur.execute("DELETE FROM payroll_employees WHERE id = %s", (emp_id,))
                        continue
                    seen[target_norm] = emp_id
                    cur.execute(
                        "UPDATE payroll_employees SET full_name = %s, normalized_name = %s, notes = %s WHERE id = %s",
                        (cleaned, target_norm, target_notes, emp_id),
                    )

    # ---------- Imports ----------

    def create_import(self, period: str) -> int:
        now = utc_now()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO payroll_imports(period, created_at) VALUES(%s, %s) RETURNING id",
                (period, now),
            )
            row = cur.fetchone()
        return int(row["id"]) if row else 0

    def update_import_period(self, import_id: int, period: str) -> None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE payroll_imports SET period = %s WHERE id = %s",
                (period, import_id),
            )

    def save_import_file(
        self,
        *,
        import_id: int,
        filename: str,
        report_type: str,
        company_name: str,
        period: str,
        parser_mode: str,
        saved_path: str,
    ) -> int:
        now = utc_now()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payroll_import_files(
                    import_id, filename, report_type, company_name, period, parser_mode, saved_path, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (import_id, filename, report_type, company_name, period, parser_mode, saved_path, now),
            )
            row = cur.fetchone()
        return int(row["id"]) if row else 0

    def save_parsed_rows(self, import_id: int, import_file_id: int, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            payload = [
                (
                    import_id,
                    import_file_id,
                    row["report_type"],
                    row["company_name"],
                    row["period"],
                    row["employee_name"],
                    row["normalized_name"],
                    row.get("person_code"),
                    row.get("gross_wage") or 0,
                    row.get("social_employee") or 0,
                    row.get("social_employer") or 0,
                    row.get("health_employee") or 0,
                    row.get("health_employer") or 0,
                    row.get("tax_amount") or 0,
                    row.get("payout_amount") or 0,
                    row.get("settlement_amount") or 0,
                    row.get("srazky") or 0,
                    row.get("zaloha") or 0,
                    row.get("health_insurance_name"),
                    row["source_row_index"],
                    row["parser_mode"],
                    json.dumps(row.get("raw_payload") or {}, ensure_ascii=False),
                )
                for row in rows
            ]
            cur.executemany(
                """
                INSERT INTO payroll_parsed_rows(
                    import_id, import_file_id, report_type, company_name, period, employee_name, normalized_name,
                    person_code, gross_wage, social_employee, social_employer, health_employee, health_employer,
                    tax_amount, payout_amount, settlement_amount, srazky, zaloha, health_insurance_name,
                    source_row_index, parser_mode, raw_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                payload,
            )

    # ---------- Employees ----------

    def list_employees(self) -> list[dict[str, Any]]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM payroll_employees ORDER BY full_name ASC")
            rows = cur.fetchall()
        return [self._normalize_employee_row(dict(row)) for row in rows]

    @staticmethod
    def _normalize_employee_row(row: dict[str, Any]) -> dict[str, Any]:
        row["odvody_strhavame"] = float(row.get("odvody_strhavame") or 0)
        row["mesicni_mzda"] = float(row.get("mesicni_mzda") or 0)
        row.pop("odvody_equal", None)
        return row

    def list_employee_metadata(self) -> dict[str, list[str]]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT project_name AS value, LOWER(project_name) AS sort_key "
                "FROM payroll_employees "
                "WHERE project_name IS NOT NULL AND TRIM(project_name) <> '' "
                "ORDER BY sort_key"
            )
            projects = [row["value"] for row in cur.fetchall()]
            cur.execute(
                "SELECT DISTINCT coordinator_name AS value, LOWER(coordinator_name) AS sort_key "
                "FROM payroll_employees "
                "WHERE coordinator_name IS NOT NULL AND TRIM(coordinator_name) <> '' "
                "ORDER BY sort_key"
            )
            coordinators = [row["value"] for row in cur.fetchall()]
            cur.execute(
                "SELECT DISTINCT company_name AS value, LOWER(company_name) AS sort_key "
                "FROM payroll_employees "
                "WHERE company_name IS NOT NULL AND TRIM(company_name) <> '' "
                "ORDER BY sort_key"
            )
            companies = [row["value"] for row in cur.fetchall()]
        return {"projects": projects, "coordinators": coordinators, "companies": companies}

    def create_employee(self, data: EmployeeInput) -> int:
        now = utc_now()
        normalized_name = normalize_name(data.full_name)
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM payroll_employees WHERE normalized_name = %s",
                (normalized_name,),
            )
            existing = cur.fetchone()
            if existing:
                return int(existing["id"])

            cur.execute(
                """
                INSERT INTO payroll_employees(
                    full_name, normalized_name, project_name, coordinator_name,
                    company_code, company_name,
                    odvody_strhavame, mesicni_mzda,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    data.full_name, normalized_name, data.project_name, data.coordinator_name,
                    data.company_code, data.company_name,
                    float(data.odvody_strhavame or 0), float(data.mesicni_mzda or 0),
                    now, now,
                ),
            )
            employee_id = int(cur.fetchone()["id"])
            cur.execute(
                "INSERT INTO payroll_employee_change_log(employee_id, action, payload_json, created_at) "
                "VALUES (%s, %s, %s, %s)",
                (employee_id, "created", json.dumps(data.model_dump(), ensure_ascii=False), now),
            )
        return employee_id

    def update_employee(self, employee_id: int, data: EmployeeInput) -> None:
        now = utc_now()
        new_normalized = normalize_name(data.full_name)
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM payroll_employees WHERE normalized_name = %s AND id <> %s",
                (new_normalized, employee_id),
            )
            if cur.fetchone():
                raise ValueError("Zam\u011bstnanec se stejn\u00fdm jm\u00e9nem u\u017e existuje")
            cur.execute(
                """
                UPDATE payroll_employees
                SET full_name = %s, normalized_name = %s, project_name = %s, coordinator_name = %s,
                    company_code = %s, company_name = %s,
                    odvody_strhavame = %s, mesicni_mzda = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    data.full_name, new_normalized, data.project_name, data.coordinator_name,
                    data.company_code, data.company_name,
                    float(data.odvody_strhavame or 0), float(data.mesicni_mzda or 0),
                    now, employee_id,
                ),
            )
            cur.execute(
                "INSERT INTO payroll_employee_change_log(employee_id, action, payload_json, created_at) "
                "VALUES (%s, %s, %s, %s)",
                (employee_id, "updated", json.dumps(data.model_dump(), ensure_ascii=False), now),
            )

    def delete_employee(self, employee_id: int) -> None:
        now = utc_now()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE payroll_preview_rows SET employee_id = NULL, project_name = NULL, "
                "coordinator_name = NULL, company_code = NULL, match_status = 'missing' "
                "WHERE employee_id = %s",
                (employee_id,),
            )
            cur.execute(
                "INSERT INTO payroll_employee_change_log(employee_id, action, payload_json, created_at) "
                "VALUES (%s, %s, %s, %s)",
                (employee_id, "deleted", "{}", now),
            )
            cur.execute("DELETE FROM payroll_employees WHERE id = %s", (employee_id,))

    def bulk_upsert_employees(self, items: list[dict[str, Any]]) -> dict[str, int]:
        now = utc_now()
        created = 0
        updated = 0
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            for item in items:
                full_name = (item.get("full_name") or "").strip()
                if not full_name:
                    continue
                normalized = normalize_name(full_name)
                project_name = (item.get("project_name") or None) or None
                coordinator_name = (item.get("coordinator_name") or None) or None
                company_code = (item.get("company_code") or None) or None
                company_name = (item.get("company_name") or None) or None
                notes = (item.get("notes") or None) or None
                odvody_strhavame = float(item.get("odvody_strhavame") or 0)
                mesicni_mzda = float(item.get("mesicni_mzda") or 0)

                cur.execute(
                    "SELECT id FROM payroll_employees WHERE normalized_name = %s",
                    (normalized,),
                )
                existing = cur.fetchone()
                if existing:
                    cur.execute(
                        """
                        UPDATE payroll_employees
                        SET full_name = %s,
                            project_name = COALESCE(%s, project_name),
                            coordinator_name = COALESCE(%s, coordinator_name),
                            company_code = COALESCE(%s, company_code),
                            company_name = COALESCE(%s, company_name),
                            notes = COALESCE(%s, notes),
                            odvody_strhavame = CASE WHEN %s > 0 THEN %s ELSE odvody_strhavame END,
                            mesicni_mzda = CASE WHEN %s > 0 THEN %s ELSE mesicni_mzda END,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            full_name, project_name, coordinator_name, company_code, company_name, notes,
                            odvody_strhavame, odvody_strhavame, mesicni_mzda, mesicni_mzda, now, existing["id"],
                        ),
                    )
                    updated += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO payroll_employees(
                            full_name, normalized_name, project_name, coordinator_name,
                            company_code, company_name, notes,
                            odvody_strhavame, mesicni_mzda,
                            created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            full_name, normalized, project_name, coordinator_name,
                            company_code, company_name, notes,
                            odvody_strhavame, mesicni_mzda, now, now,
                        ),
                    )
                    created += 1
        return {"created": created, "updated": updated}

    def clear_employees(self) -> int:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM payroll_employees")
            count = int(cur.fetchone()["n"])
            cur.execute(
                "UPDATE payroll_preview_rows SET employee_id = NULL, project_name = NULL, "
                "coordinator_name = NULL, company_code = NULL, match_status = 'missing'"
            )
            cur.execute("DELETE FROM payroll_employees")
            cur.execute("DELETE FROM payroll_employee_change_log")
        return count

    def attach_employee_to_preview_row(self, preview_row_id: int, employee_id: int) -> None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM payroll_employees WHERE id = %s", (employee_id,))
            employee = cur.fetchone()
            if not employee:
                raise ValueError("Employee not found")
            cur.execute(
                """
                UPDATE payroll_preview_rows
                SET employee_id = %s, project_name = %s, coordinator_name = %s, company_code = %s,
                    match_status = 'matched'
                WHERE id = %s
                """,
                (employee["id"], employee["project_name"], employee["coordinator_name"],
                 employee["company_code"], preview_row_id),
            )

    # ---------- Preview rebuild ----------

    def rebuild_preview_rows(self, import_id: int) -> None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pr.*, pf.filename
                FROM payroll_parsed_rows pr
                JOIN payroll_import_files pf ON pf.id = pr.import_file_id
                WHERE pr.import_id = %s
                ORDER BY pr.normalized_name, pr.report_type
                """,
                (import_id,),
            )
            parsed_rows = cur.fetchall()
            cur.execute("SELECT * FROM payroll_employees")
            employees = {row["normalized_name"]: dict(row) for row in cur.fetchall()}
            cur.execute("DELETE FROM payroll_preview_rows WHERE import_id = %s", (import_id,))

            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in parsed_rows:
                grouped[row["normalized_name"]].append(dict(row))

            for normalized_name, group_rows in grouped.items():
                first = group_rows[0]
                display_name = first["employee_name"]
                company_name = first["company_name"]
                period = first["period"]
                source_files = sorted({row["filename"] for row in group_rows})
                report_types = {row["report_type"] for row in group_rows}

                gross_wage = max(((row["gross_wage"] or 0) for row in group_rows), default=0)
                social_employee = max(
                    ((row["social_employee"] or 0) for row in group_rows
                     if row["report_type"] in {"socialka", "prehled_mezd"}),
                    default=0,
                )
                social_employer = max(
                    ((row["social_employer"] or 0) for row in group_rows
                     if row["report_type"] == "socialka"),
                    default=0,
                )
                health_employee = sum(
                    (row["health_employee"] or 0) for row in group_rows
                    if row["report_type"] == "zdravotka"
                )
                health_employer = sum(
                    (row["health_employer"] or 0) for row in group_rows
                    if row["report_type"] == "zdravotka"
                )
                tax_amount = max(
                    ((row["tax_amount"] or 0) for row in group_rows
                     if row["report_type"] == "prehled_mezd"),
                    default=0,
                )
                payout_amount = max(
                    ((row["payout_amount"] or 0) for row in group_rows
                     if row["report_type"] == "prehled_mezd"),
                    default=0,
                )
                settlement_amount = max(
                    ((row["settlement_amount"] or 0) for row in group_rows
                     if row["report_type"] == "prehled_mezd"),
                    default=0,
                )
                srazky = max(
                    ((row.get("srazky") or 0) for row in group_rows
                     if row["report_type"] == "prehled_mezd"),
                    default=0,
                )
                zaloha = max(
                    ((row.get("zaloha") or 0) for row in group_rows
                     if row["report_type"] == "prehled_mezd"),
                    default=0,
                )

                warnings: list[str] = []
                if "prehled_mezd" not in report_types:
                    warnings.append("Chyb\u00ed p\u0159ehled mezd")
                if "socialka" not in report_types:
                    warnings.append("Chyb\u00ed soci\u00e1lka")
                if "zdravotka" not in report_types:
                    warnings.append("Chyb\u00ed zdravotka")
                if gross_wage > 0 and tax_amount == 0 and ("socialka" not in report_types or "zdravotka" not in report_types):
                    warnings.append("Chyb\u00ed da\u0148")

                employee = employees.get(normalized_name)
                match_status = "matched" if employee else "missing"
                employee_id = employee["id"] if employee else None
                project_name = employee["project_name"] if employee else None
                coordinator_name = employee["coordinator_name"] if employee else None
                company_code = employee["company_code"] if employee else None
                mesicni_mzda = float(employee["mesicni_mzda"] or 0) if employee else 0.0

                odvody_platime = (
                    social_employee + social_employer
                    + health_employee + health_employer
                    + tax_amount
                )

                odvody_strhavame = float(employee["odvody_strhavame"] or 0) if employee else 0.0

                deductions = social_employee + health_employee + tax_amount + srazky + zaloha
                control_sum_parsed = gross_wage - deductions
                control_sum_expected = settlement_amount
                if (
                    control_sum_expected > 0
                    and control_sum_parsed > 0
                    and abs(control_sum_parsed - control_sum_expected) > 1.0
                ):
                    warnings.append("Rozd\u00edl v kontroln\u00edm sou\u010dtu")

                cur.execute(
                    """
                    INSERT INTO payroll_preview_rows(
                        import_id, period, display_name, normalized_name, company_name, employee_id,
                        project_name, coordinator_name, company_code, gross_wage, social_employee,
                        social_employer, health_employee, health_employer, tax_amount,
                        odvody_platime, odvody_strhavame, mesicni_mzda,
                        control_sum_parsed, control_sum_expected,
                        match_status, warnings_json, source_files_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        import_id, period, display_name, normalized_name, company_name, employee_id,
                        project_name, coordinator_name, company_code,
                        gross_wage, social_employee, social_employer, health_employee, health_employer,
                        tax_amount,
                        odvody_platime, odvody_strhavame, mesicni_mzda,
                        control_sum_parsed, control_sum_expected,
                        match_status,
                        json.dumps(warnings, ensure_ascii=False),
                        json.dumps(source_files, ensure_ascii=False),
                    ),
                )

    def list_preview_rows(self, import_id: int) -> list[dict[str, Any]]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM payroll_preview_rows WHERE import_id = %s ORDER BY display_name ASC",
                (import_id,),
            )
            rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["warnings"] = json.loads(item["warnings_json"])
            item["source_files"] = json.loads(item["source_files_json"])
            item.pop("warnings_json", None)
            item.pop("source_files_json", None)
            result.append(item)
        return result

    def get_preview_row(self, preview_row_id: int) -> dict[str, Any] | None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM payroll_preview_rows WHERE id = %s",
                (preview_row_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        item = dict(row)
        item["warnings"] = json.loads(item["warnings_json"])
        item["source_files"] = json.loads(item["source_files_json"])
        return item

    def save_export_run(self, import_id: int, output_path: str) -> None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO payroll_export_runs(import_id, output_path, created_at) VALUES (%s, %s, %s)",
                (import_id, output_path, utc_now()),
            )

    def list_imports(self) -> list[dict[str, Any]]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pi.id, pi.period, pi.created_at,
                       COUNT(DISTINCT pif.id) AS file_count,
                       COUNT(DISTINCT ppr.id) AS preview_rows
                FROM payroll_imports pi
                LEFT JOIN payroll_import_files pif ON pif.import_id = pi.id
                LEFT JOIN payroll_preview_rows ppr ON ppr.import_id = pi.id
                GROUP BY pi.id
                ORDER BY pi.id DESC
                """
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def get_import_summary(self, import_id: int) -> ImportSummary | None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pi.id AS import_id, pi.period, pi.created_at,
                       (SELECT COUNT(*) FROM payroll_import_files WHERE import_id = pi.id) AS file_count,
                       (SELECT COUNT(*) FROM payroll_parsed_rows WHERE import_id = pi.id) AS parsed_rows,
                       (SELECT COUNT(*) FROM payroll_preview_rows WHERE import_id = pi.id) AS preview_rows,
                       (SELECT COUNT(*) FROM payroll_preview_rows WHERE import_id = pi.id AND match_status = 'matched') AS matched_rows,
                       (SELECT COUNT(*) FROM payroll_preview_rows WHERE import_id = pi.id AND match_status = 'missing') AS missing_rows
                FROM payroll_imports pi
                WHERE pi.id = %s
                """,
                (import_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return ImportSummary(**dict(row))
