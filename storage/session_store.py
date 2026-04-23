"""Postgres storage for projects, sessions, mapping confirmations, and overrides.

Public API mirrors the old SQLite-backed store so callers (webapp.py, services)
do not need to change. Data lives in the `spp` schema.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from db import get_conn


SCHEMA = "spp"


class SessionStore:
    def __init__(self, db_path: Any = None):
        # `db_path` is accepted for backwards compatibility with the SQLite-era
        # constructor but is ignored; connection details come from DATABASE_URL.
        self._db_path = db_path

    # ---------- Projects ----------

    def ensure_project(self, code: str, name: str | None = None, prompt: str = "") -> None:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO projects(code, name, prompt, created_at)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(code) DO UPDATE SET
                    name = EXCLUDED.name,
                    prompt = CASE WHEN EXCLUDED.prompt <> '' THEN EXCLUDED.prompt ELSE projects.prompt END
                """,
                (code, name or code.title(), prompt, now),
            )

    def list_projects(self) -> list[dict]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT code, name, prompt, created_at FROM projects ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]

    # ---------- Mappings ----------

    def save_mapping(self, project_code: str, file_type: str, mapping: dict[str, Any]) -> None:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mappings(project_code, file_type, mapping_json, updated_at)
                VALUES(%s, %s, %s, %s)
                ON CONFLICT(project_code, file_type) DO UPDATE SET
                    mapping_json = EXCLUDED.mapping_json,
                    updated_at = EXCLUDED.updated_at
                """,
                (project_code, file_type, json.dumps(mapping, ensure_ascii=False), now),
            )

    def get_mapping(self, project_code: str, file_type: str) -> dict[str, Any] | None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT mapping_json FROM mappings WHERE project_code=%s AND file_type=%s",
                (project_code, file_type),
            )
            row = cur.fetchone()
        if not row:
            return None
        return json.loads(row["mapping_json"])

    # ---------- Overrides (legacy per-article) ----------

    def save_override(self, project_code: str, article: str, spp_rows: list[int], reason: str = "") -> None:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO overrides(project_code, article, spp_rows_json, reason, updated_at)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT(project_code, article) DO UPDATE SET
                    spp_rows_json = EXCLUDED.spp_rows_json,
                    reason = EXCLUDED.reason,
                    updated_at = EXCLUDED.updated_at
                """,
                (project_code, article.strip().upper(), json.dumps(spp_rows), reason, now),
            )

    def get_overrides(self, project_code: str) -> dict[str, dict]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT article, spp_rows_json, reason FROM overrides WHERE project_code=%s",
                (project_code,),
            )
            rows = cur.fetchall()
        result: dict[str, dict] = {}
        for row in rows:
            result[row["article"]] = {
                "spp_rows": json.loads(row["spp_rows_json"]),
                "reason": row["reason"],
            }
        return result

    # ---------- Rules registry ----------

    def upsert_rule(
        self,
        *,
        rule_type: str,
        scope_type: str,
        scope_value: str,
        rule_key: str,
        rule_value: dict[str, Any],
        reason: str = "",
        priority: int = 100,
        enabled: bool = True,
    ) -> int:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rules_registry(
                    rule_type, scope_type, scope_value, rule_key, rule_value_json,
                    reason, priority, enabled, created_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(rule_type, scope_type, scope_value, rule_key) DO UPDATE SET
                    rule_value_json = EXCLUDED.rule_value_json,
                    reason = EXCLUDED.reason,
                    priority = EXCLUDED.priority,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                RETURNING id
                """,
                (
                    rule_type.strip().lower(),
                    scope_type.strip().lower(),
                    scope_value.strip(),
                    rule_key.strip(),
                    json.dumps(rule_value, ensure_ascii=False),
                    reason,
                    int(priority),
                    bool(enabled),
                    now,
                    now,
                ),
            )
            row = cur.fetchone()
        return int(row["id"]) if row else 0

    def list_rules(
        self,
        *,
        project_code: str | None = None,
        rule_type: str | None = None,
        include_disabled: bool = False,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT id, rule_type, scope_type, scope_value, rule_key, rule_value_json,
                   reason, priority, enabled, created_at, updated_at
            FROM rules_registry
            WHERE 1=1
        """
        params: list[Any] = []
        if not include_disabled:
            query += " AND enabled = TRUE"
        if rule_type:
            query += " AND rule_type = %s"
            params.append(rule_type.strip().lower())
        if project_code:
            query += (
                " AND ((scope_type='system' AND scope_value='global')"
                " OR (scope_type='project' AND scope_value=%s))"
            )
            params.append(project_code)
        query += " ORDER BY priority ASC, updated_at DESC"

        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["rule_value"] = json.loads(item.pop("rule_value_json"))
            item["enabled"] = bool(item["enabled"])
            result.append(item)
        return result

    def delete_rule(self, rule_id: int) -> bool:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM rules_registry WHERE id=%s", (int(rule_id),))
            return cur.rowcount > 0

    def get_effective_rules(self, project_code: str, rule_type: str) -> list[dict[str, Any]]:
        return self.list_rules(project_code=project_code, rule_type=rule_type, include_disabled=False)

    def create_rules_snapshot(self, project_code: str, label: str) -> int:
        now = datetime.utcnow().isoformat()
        rules = self.list_rules(project_code=project_code, include_disabled=True)
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rules_versions(project_code, label, rules_json, created_at)
                VALUES(%s, %s, %s, %s)
                RETURNING id
                """,
                (project_code, label, json.dumps(rules, ensure_ascii=False), now),
            )
            row = cur.fetchone()
        return int(row["id"]) if row else 0

    def list_rules_snapshots(self, project_code: str) -> list[dict[str, Any]]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, project_code, label, rules_json, created_at
                FROM rules_versions
                WHERE project_code=%s
                ORDER BY created_at DESC
                """,
                (project_code,),
            )
            rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["rules"] = json.loads(item.pop("rules_json"))
            result.append(item)
        return result

    # ---------- Scoped overrides ----------

    def save_scoped_override(
        self,
        scope_type: str,
        scope_value: str,
        item_key: str,
        spp_rows: list[int],
        reason: str = "",
    ) -> None:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scoped_overrides(scope_type, scope_value, item_key, spp_rows_json, reason, updated_at)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT(scope_type, scope_value, item_key) DO UPDATE SET
                    spp_rows_json = EXCLUDED.spp_rows_json,
                    reason = EXCLUDED.reason,
                    updated_at = EXCLUDED.updated_at
                """,
                (scope_type, scope_value, item_key, json.dumps(spp_rows), reason, now),
            )

    def get_scoped_overrides(self, scope_type: str, scope_value: str) -> dict[str, dict]:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT item_key, spp_rows_json, reason
                FROM scoped_overrides
                WHERE scope_type=%s AND scope_value=%s
                """,
                (scope_type, scope_value),
            )
            rows = cur.fetchall()
        result: dict[str, dict] = {}
        for row in rows:
            result[row["item_key"]] = {
                "spp_rows": json.loads(row["spp_rows_json"]),
                "reason": row["reason"],
            }
        return result

    def get_effective_overrides(self, project_code: str) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for article, payload in self.get_overrides(project_code).items():
            result[f"ARTICLE:{article.strip().upper()}"] = payload
        result.update(self.get_scoped_overrides("system", "global"))
        result.update(self.get_scoped_overrides("project", project_code))
        return result

    def delete_scoped_override(self, scope_type: str, scope_value: str, item_key: str) -> bool:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM scoped_overrides
                WHERE scope_type=%s AND scope_value=%s AND item_key=%s
                """,
                (scope_type, scope_value, item_key),
            )
            return cur.rowcount > 0

    def get_override_status(self, project_code: str, item_key: str) -> dict[str, dict[str, Any]]:
        system_data = self.get_scoped_overrides("system", "global").get(item_key)
        project_data = self.get_scoped_overrides("project", project_code).get(item_key)
        return {
            "system": system_data or {},
            "project": project_data or {},
        }

    # ---------- Analysis drafts ----------

    def create_analysis_draft(
        self,
        *,
        draft_id: str,
        project_code: str,
        project_name: str,
        period_month: str,
        spp_path: str,
        inventory_path: str,
        nf45_path: str | None,
        rules_path: str | None,
        nomenclature_path: str | None,
        project_prompt: str,
        spp_mapping: dict[str, Any] | None,
        inventory_mapping: dict[str, Any] | None,
    ) -> str:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analysis_drafts(
                    id, project_code, project_name, period_month,
                    spp_path, inventory_path, nf45_path, rules_path, nomenclature_path,
                    project_prompt, spp_mapping_json, inventory_mapping_json,
                    created_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    project_code = EXCLUDED.project_code,
                    project_name = EXCLUDED.project_name,
                    period_month = EXCLUDED.period_month,
                    spp_path = EXCLUDED.spp_path,
                    inventory_path = EXCLUDED.inventory_path,
                    nf45_path = EXCLUDED.nf45_path,
                    rules_path = EXCLUDED.rules_path,
                    nomenclature_path = EXCLUDED.nomenclature_path,
                    project_prompt = EXCLUDED.project_prompt,
                    spp_mapping_json = EXCLUDED.spp_mapping_json,
                    inventory_mapping_json = EXCLUDED.inventory_mapping_json,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    draft_id,
                    project_code,
                    project_name,
                    period_month,
                    spp_path,
                    inventory_path,
                    nf45_path,
                    rules_path,
                    nomenclature_path,
                    project_prompt,
                    json.dumps(spp_mapping, ensure_ascii=False) if spp_mapping else None,
                    json.dumps(inventory_mapping, ensure_ascii=False) if inventory_mapping else None,
                    now,
                    now,
                ),
            )
        return draft_id

    def get_analysis_draft(self, draft_id: str) -> dict[str, Any] | None:
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, project_code, project_name, period_month,
                       spp_path, inventory_path, nf45_path, rules_path, nomenclature_path,
                       project_prompt, spp_mapping_json, inventory_mapping_json,
                       created_at, updated_at
                FROM analysis_drafts
                WHERE id=%s
                """,
                (draft_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        item = dict(row)
        item["spp_mapping"] = json.loads(item.pop("spp_mapping_json")) if item.get("spp_mapping_json") else None
        item["inventory_mapping"] = (
            json.loads(item.pop("inventory_mapping_json")) if item.get("inventory_mapping_json") else None
        )
        return item

    # ---------- Sessions ----------

    def save_session(
        self,
        project_code: str,
        period_month: str,
        spp_path: str,
        inventory_path: str,
        output_path: str,
        stats: dict[str, Any],
        nf45_path: str | None = None,
    ) -> int:
        now = datetime.utcnow().isoformat()
        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sessions(project_code, period_month, spp_path, inventory_path, nf45_path, output_path, stats_json, created_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    project_code,
                    period_month,
                    spp_path,
                    inventory_path,
                    nf45_path,
                    output_path,
                    json.dumps(stats, ensure_ascii=False),
                    now,
                ),
            )
            row = cur.fetchone()
        return int(row["id"]) if row else 0

    def list_sessions(self, project_code: str | None = None, period_month: str | None = None) -> list[dict]:
        query = "SELECT id, project_code, period_month, output_path, stats_json, created_at FROM sessions WHERE 1=1"
        params: list[Any] = []
        if project_code:
            query += " AND project_code=%s"
            params.append(project_code)
        if period_month:
            query += " AND period_month=%s"
            params.append(period_month)
        query += " ORDER BY created_at DESC"

        with get_conn(schema=SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        result = []
        for r in rows:
            item = dict(r)
            item["stats"] = json.loads(item.pop("stats_json"))
            result.append(item)
        return result
