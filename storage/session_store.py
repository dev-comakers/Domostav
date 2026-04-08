"""SQLite storage for projects, sessions, mapping confirmations, and overrides."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class SessionStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                prompt TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL,
                period_month TEXT NOT NULL,
                spp_path TEXT NOT NULL,
                inventory_path TEXT NOT NULL,
                nf45_path TEXT,
                output_path TEXT NOT NULL,
                stats_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL,
                file_type TEXT NOT NULL,
                mapping_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(project_code, file_type)
            );

            CREATE TABLE IF NOT EXISTS overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL,
                article TEXT NOT NULL,
                spp_rows_json TEXT NOT NULL,
                reason TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                UNIQUE(project_code, article)
            );

            CREATE TABLE IF NOT EXISTS scoped_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_type TEXT NOT NULL,
                scope_value TEXT NOT NULL,
                item_key TEXT NOT NULL,
                spp_rows_json TEXT NOT NULL,
                reason TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                UNIQUE(scope_type, scope_value, item_key)
            );

            CREATE TABLE IF NOT EXISTS analysis_drafts (
                id TEXT PRIMARY KEY,
                project_code TEXT NOT NULL,
                project_name TEXT NOT NULL,
                period_month TEXT NOT NULL,
                spp_path TEXT NOT NULL,
                inventory_path TEXT NOT NULL,
                nf45_path TEXT,
                rules_path TEXT,
                nomenclature_path TEXT,
                project_prompt TEXT DEFAULT '',
                spp_mapping_json TEXT,
                inventory_mapping_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rules_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_value TEXT NOT NULL,
                rule_key TEXT NOT NULL,
                rule_value_json TEXT NOT NULL,
                reason TEXT DEFAULT '',
                priority INTEGER NOT NULL DEFAULT 100,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(rule_type, scope_type, scope_value, rule_key)
            );

            CREATE TABLE IF NOT EXISTS rules_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_code TEXT NOT NULL,
                label TEXT NOT NULL,
                rules_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
        conn.close()

    def ensure_project(self, code: str, name: str | None = None, prompt: str = "") -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO projects(code, name, prompt, created_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                prompt=CASE WHEN excluded.prompt <> '' THEN excluded.prompt ELSE projects.prompt END
            """,
            (code, name or code.title(), prompt, now),
        )
        conn.commit()
        conn.close()

    def list_projects(self) -> list[dict]:
        conn = self._connect()
        rows = conn.execute("SELECT code, name, prompt, created_at FROM projects ORDER BY created_at DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def save_mapping(self, project_code: str, file_type: str, mapping: dict[str, Any]) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO mappings(project_code, file_type, mapping_json, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(project_code, file_type) DO UPDATE SET
                mapping_json=excluded.mapping_json,
                updated_at=excluded.updated_at
            """,
            (project_code, file_type, json.dumps(mapping, ensure_ascii=False), now),
        )
        conn.commit()
        conn.close()

    def get_mapping(self, project_code: str, file_type: str) -> dict[str, Any] | None:
        conn = self._connect()
        row = conn.execute(
            "SELECT mapping_json FROM mappings WHERE project_code=? AND file_type=?",
            (project_code, file_type),
        ).fetchone()
        conn.close()
        if not row:
            return None
        return json.loads(row["mapping_json"])

    def save_override(self, project_code: str, article: str, spp_rows: list[int], reason: str = "") -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO overrides(project_code, article, spp_rows_json, reason, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(project_code, article) DO UPDATE SET
                spp_rows_json=excluded.spp_rows_json,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (project_code, article.strip().upper(), json.dumps(spp_rows), reason, now),
        )
        conn.commit()
        conn.close()

    def get_overrides(self, project_code: str) -> dict[str, dict]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT article, spp_rows_json, reason FROM overrides WHERE project_code=?",
            (project_code,),
        ).fetchall()
        conn.close()
        result: dict[str, dict] = {}
        for row in rows:
            result[row["article"]] = {
                "spp_rows": json.loads(row["spp_rows_json"]),
                "reason": row["reason"],
            }
        return result

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
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO rules_registry(
                rule_type, scope_type, scope_value, rule_key, rule_value_json,
                reason, priority, enabled, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_type, scope_type, scope_value, rule_key) DO UPDATE SET
                rule_value_json=excluded.rule_value_json,
                reason=excluded.reason,
                priority=excluded.priority,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at
            """,
            (
                rule_type.strip().lower(),
                scope_type.strip().lower(),
                scope_value.strip(),
                rule_key.strip(),
                json.dumps(rule_value, ensure_ascii=False),
                reason,
                int(priority),
                1 if enabled else 0,
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT id FROM rules_registry
            WHERE rule_type=? AND scope_type=? AND scope_value=? AND rule_key=?
            """,
            (
                rule_type.strip().lower(),
                scope_type.strip().lower(),
                scope_value.strip(),
                rule_key.strip(),
            ),
        ).fetchone()
        conn.close()
        return int(row["id"]) if row else 0

    def list_rules(
        self,
        *,
        project_code: str | None = None,
        rule_type: str | None = None,
        include_disabled: bool = False,
    ) -> list[dict[str, Any]]:
        conn = self._connect()
        query = """
            SELECT id, rule_type, scope_type, scope_value, rule_key, rule_value_json,
                   reason, priority, enabled, created_at, updated_at
            FROM rules_registry
            WHERE 1=1
        """
        params: list[Any] = []
        if not include_disabled:
            query += " AND enabled=1"
        if rule_type:
            query += " AND rule_type=?"
            params.append(rule_type.strip().lower())
        if project_code:
            query += " AND ((scope_type='system' AND scope_value='global') OR (scope_type='project' AND scope_value=?))"
            params.append(project_code)
        query += " ORDER BY priority ASC, updated_at DESC"
        rows = conn.execute(query, params).fetchall()
        conn.close()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["rule_value"] = json.loads(item.pop("rule_value_json"))
            item["enabled"] = bool(item["enabled"])
            result.append(item)
        return result

    def delete_rule(self, rule_id: int) -> bool:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM rules_registry WHERE id=?", (int(rule_id),))
        deleted = cur.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def get_effective_rules(self, project_code: str, rule_type: str) -> list[dict[str, Any]]:
        return self.list_rules(project_code=project_code, rule_type=rule_type, include_disabled=False)

    def create_rules_snapshot(self, project_code: str, label: str) -> int:
        now = datetime.utcnow().isoformat()
        rules = self.list_rules(project_code=project_code, include_disabled=True)
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO rules_versions(project_code, label, rules_json, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (project_code, label, json.dumps(rules, ensure_ascii=False), now),
        )
        snapshot_id = int(cur.lastrowid)
        conn.commit()
        conn.close()
        return snapshot_id

    def list_rules_snapshots(self, project_code: str) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT id, project_code, label, rules_json, created_at
            FROM rules_versions
            WHERE project_code=?
            ORDER BY created_at DESC
            """,
            (project_code,),
        ).fetchall()
        conn.close()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["rules"] = json.loads(item.pop("rules_json"))
            result.append(item)
        return result

    def save_scoped_override(
        self,
        scope_type: str,
        scope_value: str,
        item_key: str,
        spp_rows: list[int],
        reason: str = "",
    ) -> None:
        now = datetime.utcnow().isoformat()
        conn = self._connect()
        conn.execute(
            """
            INSERT INTO scoped_overrides(scope_type, scope_value, item_key, spp_rows_json, reason, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope_type, scope_value, item_key) DO UPDATE SET
                spp_rows_json=excluded.spp_rows_json,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (scope_type, scope_value, item_key, json.dumps(spp_rows), reason, now),
        )
        conn.commit()
        conn.close()

    def get_scoped_overrides(self, scope_type: str, scope_value: str) -> dict[str, dict]:
        conn = self._connect()
        rows = conn.execute(
            """
            SELECT item_key, spp_rows_json, reason
            FROM scoped_overrides
            WHERE scope_type=? AND scope_value=?
            """,
            (scope_type, scope_value),
        ).fetchall()
        conn.close()
        result: dict[str, dict] = {}
        for row in rows:
            result[row["item_key"]] = {
                "spp_rows": json.loads(row["spp_rows_json"]),
                "reason": row["reason"],
            }
        return result

    def get_effective_overrides(self, project_code: str) -> dict[str, dict]:
        result: dict[str, dict] = {}

        # Legacy project-level overrides keyed by article.
        for article, payload in self.get_overrides(project_code).items():
            result[f"ARTICLE:{article.strip().upper()}"] = payload

        # System-level overrides apply first, then project-specific overrides win.
        result.update(self.get_scoped_overrides("system", "global"))
        result.update(self.get_scoped_overrides("project", project_code))
        return result

    def delete_scoped_override(self, scope_type: str, scope_value: str, item_key: str) -> bool:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM scoped_overrides
            WHERE scope_type=? AND scope_value=? AND item_key=?
            """,
            (scope_type, scope_value, item_key),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def get_override_status(self, project_code: str, item_key: str) -> dict[str, dict[str, Any]]:
        system_data = self.get_scoped_overrides("system", "global").get(item_key)
        project_data = self.get_scoped_overrides("project", project_code).get(item_key)
        return {
            "system": system_data or {},
            "project": project_data or {},
        }

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
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO analysis_drafts(
                id, project_code, project_name, period_month,
                spp_path, inventory_path, nf45_path, rules_path, nomenclature_path,
                project_prompt, spp_mapping_json, inventory_mapping_json,
                created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        conn.commit()
        conn.close()
        return draft_id

    def get_analysis_draft(self, draft_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        row = conn.execute(
            """
            SELECT id, project_code, project_name, period_month,
                   spp_path, inventory_path, nf45_path, rules_path, nomenclature_path,
                   project_prompt, spp_mapping_json, inventory_mapping_json,
                   created_at, updated_at
            FROM analysis_drafts
            WHERE id=?
            """,
            (draft_id,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        item = dict(row)
        item["spp_mapping"] = json.loads(item.pop("spp_mapping_json")) if item.get("spp_mapping_json") else None
        item["inventory_mapping"] = (
            json.loads(item.pop("inventory_mapping_json")) if item.get("inventory_mapping_json") else None
        )
        return item

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
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sessions(project_code, period_month, spp_path, inventory_path, nf45_path, output_path, stats_json, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
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
        session_id = cur.lastrowid
        conn.commit()
        conn.close()
        return int(session_id)

    def list_sessions(self, project_code: str | None = None, period_month: str | None = None) -> list[dict]:
        conn = self._connect()
        query = "SELECT id, project_code, period_month, output_path, stats_json, created_at FROM sessions WHERE 1=1"
        params: list[Any] = []
        if project_code:
            query += " AND project_code=?"
            params.append(project_code)
        if period_month:
            query += " AND period_month=?"
            params.append(period_month)
        query += " ORDER BY created_at DESC"
        rows = conn.execute(query, params).fetchall()
        conn.close()
        result = []
        for r in rows:
            item = dict(r)
            item["stats"] = json.loads(item.pop("stats_json"))
            result.append(item)
        return result
