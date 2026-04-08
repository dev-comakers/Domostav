"""Localhost web app for Domostav AI demo."""

from __future__ import annotations

import importlib
import json
import os
from datetime import datetime
from pathlib import Path
import uuid

import openpyxl
from flask import Flask, jsonify, request, send_from_directory

from config import settings
from config.settings import DATA_DIR, OUTPUT_DIR, PROJECT_ROOT
from llm.client import ClaudeClient
from models import ColumnMapping, WriteoffRecommendation
from parsers.mapping_engine import auto_detect_mapping
from services.pipeline_service import mapping_to_dict
from storage.session_store import SessionStore

app = Flask(__name__, static_folder="design", static_url_path="")
app.config["JSON_AS_ASCII"] = False

UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "app.db"
store = SessionStore(DB_PATH)


def _ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.lower() in ("1", "true", "yes", "y", "on")


def _save_uploaded_file(file_obj, session_dir: Path) -> str:
    safe_name = file_obj.filename or f"upload_{uuid.uuid4().hex}"
    path = session_dir / safe_name
    file_obj.save(path)
    return str(path)


def _override_item_key(article: str | None, inventory_row: int | None) -> str | None:
    normalized = (article or "").strip().upper()
    if normalized:
        return f"ARTICLE:{normalized}"
    if inventory_row is not None:
        return f"ROW:{int(inventory_row)}"
    return None


def _load_pipeline_module():
    import output.excel_generator as excel_gen
    import services.pipeline_service as pipeline_mod

    importlib.reload(excel_gen)
    importlib.reload(pipeline_mod)
    return pipeline_mod


def _run_pipeline(
    *,
    project_code: str,
    spp_path: str,
    inventory_path: str,
    no_ai: bool,
    auto_map: bool,
    spp_mapping_override: dict | None,
    inv_mapping_override: dict | None,
    project_prompt: str,
    rules_path: str | None,
    nomenclature_path: str | None,
    nf45_path: str | None,
    generate_excel: bool,
):
    alias_rules = store.get_effective_rules(project_code, "alias")
    category_rules = store.get_effective_rules(project_code, "category")
    alias_map: dict[str, str] = {}
    for rule in alias_rules:
        val = rule.get("rule_value") or {}
        alias = (val.get("alias") or rule.get("rule_key") or "").strip()
        canonical = (val.get("canonical") or "").strip()
        if alias and canonical:
            alias_map[alias] = canonical

    pipeline_mod = _load_pipeline_module()
    return pipeline_mod.run_analysis_pipeline(
        project=project_code,
        spp_path=spp_path,
        inventory_path=inventory_path,
        api_key=settings.OPENAI_API_KEY or settings.ANTHROPIC_API_KEY,
        no_ai=no_ai,
        auto_map=auto_map,
        spp_mapping_override=spp_mapping_override,
        inv_mapping_override=inv_mapping_override,
        project_prompt_override=project_prompt,
        rules_xlsm_path=rules_path,
        nomenclature_path=nomenclature_path,
        nf45_path=nf45_path,
        overrides=store.get_effective_overrides(project_code),
        alias_map=alias_map,
        category_rules=category_rules,
        generate_excel=generate_excel,
        include_export_artifacts=not generate_excel,
    )


def _draft_artifacts_path(spp_path: str) -> Path:
    return Path(spp_path).parent / "analysis_artifacts.json"


def _write_draft_artifacts(spp_path: str, result: dict) -> None:
    payload = {
        "result_snapshot": {k: v for k, v in result.items() if k != "export_artifacts"},
        "export_artifacts": result.get("export_artifacts") or {},
    }
    _draft_artifacts_path(spp_path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_draft_artifacts(spp_path: str) -> dict | None:
    path = _draft_artifacts_path(spp_path)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _strip_export_artifacts(result: dict) -> dict:
    return {k: v for k, v in result.items() if k != "export_artifacts"}


def _finalize_from_cached_artifacts(draft: dict) -> dict[str, object]:
    cached = _read_draft_artifacts(draft["spp_path"])
    if not cached:
        raise FileNotFoundError("Cached analysis artifacts not found")

    export_artifacts = cached.get("export_artifacts") or {}
    recommendations = [
        WriteoffRecommendation(**item)
        for item in (export_artifacts.get("recommendations") or [])
    ]
    output_path = str(OUTPUT_DIR / f"analysis_{draft['project_code']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

    import output.excel_generator as excel_gen

    excel_gen.generate_output(
        source_path=draft["inventory_path"],
        output_path=output_path,
        recommendations=recommendations,
        data_start_row=int(export_artifacts.get("data_start_row") or 12),
        summary=export_artifacts.get("summary") or {},
        spp_coverage=export_artifacts.get("spp_coverage") or [],
    )
    result = dict(cached.get("result_snapshot") or {})
    result["output_path"] = output_path
    return result


def _validate_upload_slots(spp, inventory, nf45, rules_file) -> str | None:
    """Basic filename validation to prevent users from mixing upload slots."""
    spp_name = (spp.filename or "").lower()
    inv_name = (inventory.filename or "").lower()
    nf45_name = (nf45.filename or "").lower() if nf45 else ""
    rules_name = (rules_file.filename or "").lower() if rules_file else ""

    if "spp" not in spp_name:
        return "Slot SPP: vyberte soubor SPP (napr. 'SPP Chirana ...xlsm')."
    if "invent" in spp_name or "nf-30" in spp_name or "номенклат" in spp_name:
        return "Slot SPP: nahran spatny soubor. Vyberte SPP, ne Inventuru/Nomenklaturu."

    inv_ok = (
        ("invent" in inv_name)
        or ("nf-30" in inv_name)
        or ("fakturace" in inv_name)
        or ("soupis" in inv_name)
    )
    if not inv_ok:
        return "Slot Inventura: vyberte inventarizaci NF-30 nebo soupis/fakturaci s polozkami."
    if "spp" in inv_name or "правил" in inv_name or "topeni+kanalizace" in inv_name:
        return "Slot Inventura: nahran spatny soubor. Vyberte Inventuru NF-30."

    if nf45 and ("nf-45" not in nf45_name) and ("списан" not in nf45_name):
        return "Slot NF-45: vyberte soubor NF-45 (fakticke списание), nebo nechte prazdne."

    if rules_file and ("правил" not in rules_name) and ("topeni+kanalizace" not in rules_name) and ("rules" not in rules_name):
        return "Slot Rules: vyberte soubor pravidel (SPP_Chirana_...pravila...)."

    return None


def _default_mapping(file_type: str) -> ColumnMapping:
    """Fallback mapping for known Chirana templates when AI is unavailable."""
    if file_type == "spp":
        return ColumnMapping(
            name="I",
            unit="K",
            quantity="L",
            price="M",
            total="N",
            percent_month="R",
            total_month="S",
            header_row=5,
            data_start_row=6,
        )
    return ColumnMapping(
        number="B",
        article="D",
        name="F",
        deviation="K",
        quantity="N",
        quantity_accounting="Q",
        unit="T",
        price="V",
        header_row=11,
        data_start_row=12,
    )


def _find_default_rules_file() -> str | None:
    workspace = PROJECT_ROOT.parent.parent
    candidates = list(workspace.glob("SPP_Chirana_02_26_*Topeni+Kanalizace.xlsm"))
    return str(candidates[0]) if candidates else None


def _find_default_nomenclature_file() -> str | None:
    workspace = PROJECT_ROOT.parent.parent
    candidates = list(workspace.glob("*номенклатури*.xlsx"))
    return str(candidates[0]) if candidates else None


def _detect_training_file_kind(path: Path, filename: str) -> str:
    low = filename.lower()
    if any(k in low for k in ["spp", "rozpo", "fakturace sod", "soupis prac", "výkaz výměr", "vykaz vymer"]):
        return "spp"
    if any(k in low for k in ["invent", "nf-30", "sklad", "zásob", "zasob", "fakturace_"]):
        return "inventory"
    if any(k in low for k in ["nf-45", "spisani", "списан"]):
        return "writeoff"
    if any(k in low for k in ["pravid", "rules", "topeni+kanalizace"]):
        return "rules"
    if any(k in low for k in ["nomen", "номенклат"]):
        return "nomenclature"

    # Lightweight header-based fallback for xlsx/xlsm.
    if path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
        return "unknown"
    try:
        wb = openpyxl.load_workbook(str(path), data_only=True, read_only=True)
        ws = wb.active
        text_parts: list[str] = []
        max_col = min(ws.max_column, 40)
        max_row = min(ws.max_row, 40)
        for r in range(1, max_row + 1):
            vals = [ws.cell(r, c).value for c in range(1, max_col + 1)]
            text_parts.extend(str(v) for v in vals if v not in (None, ""))
        wb.close()
        header_blob = " | ".join(text_parts).lower()
        if "název položky" in header_blob or "nazev polozky" in header_blob:
            return "spp"
        if "invent" in header_blob or "odchyl" in header_blob or "deviation" in header_blob:
            return "inventory"
        if "soupis prac" in header_blob and "pč" in header_blob and "kód" in header_blob:
            return "inventory"
        if "nomenkl" in header_blob:
            return "nomenclature"
    except Exception:
        return "unknown"
    return "unknown"


@app.get("/")
def root():
    return send_from_directory("design", "dashboard.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "domostav-ai-web"})


@app.get("/api/version")
def api_version():
    """Expose active runtime components for quick verification."""
    import output.excel_generator as excel_gen
    return jsonify(
        {
            "ok": True,
            "excel_generator_file": str(Path(excel_gen.__file__).resolve()),
            "ai_columns": [name for name, _ in excel_gen.AI_COLUMNS],
            "supports_spp_coverage": hasattr(excel_gen, "_add_spp_coverage_sheet"),
        }
    )


@app.get("/api/projects")
def list_projects():
    return jsonify({"projects": store.list_projects()})


@app.post("/api/projects")
def create_project():
    payload = request.get_json(silent=True) or {}
    code = (payload.get("code") or "").strip().lower()
    name = (payload.get("name") or "").strip()
    prompt = payload.get("prompt") or ""
    if not code:
        return jsonify({"error": "code is required"}), 400
    store.ensure_project(code, name or code.title(), prompt)
    return jsonify({"ok": True})


@app.get("/api/sessions")
def list_sessions():
    project = request.args.get("project")
    month = request.args.get("month")
    sessions = store.list_sessions(project_code=project, period_month=month)
    for s in sessions:
        output_path = s.get("output_path") or ""
        p = Path(output_path)
        s["output_exists"] = p.exists()
        s["output_filename"] = p.name if p.name else None
    return jsonify({"sessions": sessions})


@app.get("/api/dashboard")
def dashboard_stats():
    project = request.args.get("project", "chirana")
    month = request.args.get("month")
    sessions = store.list_sessions(project_code=project, period_month=month)
    latest = sessions[0] if sessions else None
    monthly = {}
    for s in sessions:
        key = s["period_month"]
        monthly.setdefault(
            key,
            {
                "count": 0,
                "ok": 0,
                "warning": 0,
                "red_flag": 0,
            },
        )
        monthly[key]["count"] += 1
        kpi = s["stats"].get("kpis", {})
        monthly[key]["ok"] += int(kpi.get("ok", 0))
        monthly[key]["warning"] += int(kpi.get("warning", 0))
        monthly[key]["red_flag"] += int(kpi.get("red_flag", 0))

    return jsonify(
        {
            "project": project,
            "latest": latest,
            "monthly": monthly,
            "sessions_count": len(sessions),
        }
    )


@app.post("/api/overrides")
def save_override():
    payload = request.get_json(silent=True) or {}
    project = payload.get("project_code", "chirana")
    scope = (payload.get("scope") or "project").strip().lower()
    action = (payload.get("action") or "save").strip().lower()
    article = (payload.get("article") or "").strip().upper()
    inventory_row = payload.get("inventory_row")
    item_key = (payload.get("item_key") or "").strip()
    rows = payload.get("spp_rows") or []
    reason = payload.get("reason") or ""
    if not item_key:
        item_key = _override_item_key(article, inventory_row)
    if action == "delete":
        removed: list[str] = []
        if scope in {"system", "both"}:
            if store.delete_scoped_override("system", "global", item_key):
                removed.append("system")
        if scope in {"project", "both"}:
            if store.delete_scoped_override("project", project, item_key):
                removed.append("project")
        status = store.get_override_status(project, item_key)
        return jsonify({"ok": True, "action": "delete", "removed_scopes": removed, "status": status})

    if not item_key or not isinstance(rows, list):
        return jsonify({"error": "item_key/article and spp_rows are required"}), 400

    normalized_rows = [int(x) for x in rows]
    saved: list[str] = []
    if scope in {"system", "both"}:
        store.save_scoped_override("system", "global", item_key, normalized_rows, reason)
        saved.append("system")
    if scope in {"project", "both"}:
        store.save_scoped_override("project", project, item_key, normalized_rows, reason)
        saved.append("project")
    status = store.get_override_status(project, item_key)
    return jsonify({"ok": True, "action": "save", "saved_scopes": saved, "status": status})


@app.post("/api/overrides/bulk")
def save_overrides_bulk():
    payload = request.get_json(silent=True) or {}
    project = (payload.get("project_code") or "chirana").strip().lower()
    scope = (payload.get("scope") or "project").strip().lower()
    rows = payload.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return jsonify({"error": "rows array is required"}), 400

    saved_count = 0
    failed: list[dict] = []
    for idx, row in enumerate(rows):
        try:
            article = (row.get("article") or "").strip().upper()
            inventory_row = row.get("inventory_row")
            item_key = (row.get("item_key") or "").strip() or _override_item_key(article, inventory_row)
            spp_row = int(row.get("spp_row") or 0)
            reason = row.get("reason") or ""
            if not item_key or spp_row <= 0:
                raise ValueError("item_key/article and spp_row are required")
            if scope in {"system", "both"}:
                store.save_scoped_override("system", "global", item_key, [spp_row], reason)
            if scope in {"project", "both"}:
                store.save_scoped_override("project", project, item_key, [spp_row], reason)
            saved_count += 1
        except Exception as exc:
            failed.append({"index": idx, "error": str(exc)})
    return jsonify({"ok": True, "saved_count": saved_count, "failed": failed})


@app.get("/api/rules")
def list_rules():
    project = request.args.get("project")
    rule_type = request.args.get("type")
    include_disabled = (request.args.get("include_disabled") or "").lower() in {"1", "true", "yes"}
    rules = store.list_rules(
        project_code=project,
        rule_type=rule_type,
        include_disabled=include_disabled,
    )
    return jsonify({"ok": True, "rules": rules})


@app.post("/api/rules")
def upsert_rule():
    payload = request.get_json(silent=True) or {}
    project = (payload.get("project_code") or "chirana").strip().lower()
    rule_type = (payload.get("rule_type") or "").strip().lower()
    scope = (payload.get("scope") or "project").strip().lower()
    rule_key = (payload.get("rule_key") or "").strip()
    rule_value = payload.get("rule_value") or {}
    reason = payload.get("reason") or ""
    priority = int(payload.get("priority") or 100)
    enabled = bool(payload.get("enabled", True))

    if rule_type not in {"alias", "category", "mapping"}:
        return jsonify({"error": "rule_type must be alias/category/mapping"}), 400
    if scope not in {"project", "system"}:
        return jsonify({"error": "scope must be project/system"}), 400
    if not rule_key:
        return jsonify({"error": "rule_key is required"}), 400
    if not isinstance(rule_value, dict):
        return jsonify({"error": "rule_value must be object"}), 400

    scope_value = "global" if scope == "system" else project
    rule_id = store.upsert_rule(
        rule_type=rule_type,
        scope_type=scope,
        scope_value=scope_value,
        rule_key=rule_key,
        rule_value=rule_value,
        reason=reason,
        priority=priority,
        enabled=enabled,
    )
    return jsonify({"ok": True, "rule_id": rule_id})


@app.delete("/api/rules/<int:rule_id>")
def delete_rule(rule_id: int):
    deleted = store.delete_rule(rule_id)
    return jsonify({"ok": True, "deleted": deleted})


@app.post("/api/rules/snapshot")
def create_rules_snapshot():
    payload = request.get_json(silent=True) or {}
    project = (payload.get("project_code") or "chirana").strip().lower()
    label = (payload.get("label") or f"snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}").strip()
    snapshot_id = store.create_rules_snapshot(project, label)
    return jsonify({"ok": True, "snapshot_id": snapshot_id, "label": label})


@app.get("/api/rules/snapshots")
def list_rules_snapshots():
    project = (request.args.get("project") or "chirana").strip().lower()
    snapshots = store.list_rules_snapshots(project)
    return jsonify({"ok": True, "snapshots": snapshots})


@app.get("/api/rules/export")
def export_rules():
    project = (request.args.get("project") or "chirana").strip().lower()
    rules = store.list_rules(project_code=project, include_disabled=True)
    return jsonify({"ok": True, "project_code": project, "rules": rules})


@app.post("/api/rules/import")
def import_rules():
    payload = request.get_json(silent=True) or {}
    project = (payload.get("project_code") or "chirana").strip().lower()
    scope_default = (payload.get("scope_default") or "project").strip().lower()
    rules = payload.get("rules") or []
    if not isinstance(rules, list):
        return jsonify({"error": "rules must be array"}), 400
    imported = 0
    failed: list[dict] = []
    for idx, item in enumerate(rules):
        try:
            rule_type = (item.get("rule_type") or "").strip().lower()
            scope = (item.get("scope_type") or item.get("scope") or scope_default).strip().lower()
            rule_key = (item.get("rule_key") or "").strip()
            rule_value = item.get("rule_value") or {}
            if rule_type not in {"alias", "category", "mapping"}:
                raise ValueError("invalid rule_type")
            if scope not in {"project", "system"}:
                raise ValueError("invalid scope")
            if not rule_key:
                raise ValueError("missing rule_key")
            if not isinstance(rule_value, dict):
                raise ValueError("rule_value must be object")
            scope_value = "global" if scope == "system" else project
            store.upsert_rule(
                rule_type=rule_type,
                scope_type=scope,
                scope_value=scope_value,
                rule_key=rule_key,
                rule_value=rule_value,
                reason=item.get("reason") or "",
                priority=int(item.get("priority") or 100),
                enabled=bool(item.get("enabled", True)),
            )
            imported += 1
        except Exception as exc:
            failed.append({"index": idx, "error": str(exc)})
    return jsonify({"ok": True, "imported": imported, "failed": failed})


@app.post("/api/training/files/preview")
def preview_training_files():
    _ensure_dirs()
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "At least one file is required"}), 400

    session_dir = UPLOAD_DIR / f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    session_dir.mkdir(parents=True, exist_ok=True)
    allowed_ext = {".xlsx", ".xlsm", ".xls", ".csv"}

    payload: list[dict[str, object]] = []
    for f in files:
        name = (f.filename or "").strip()
        if not name:
            continue
        ext = Path(name).suffix.lower()
        if ext not in allowed_ext:
            payload.append(
                {
                    "filename": name,
                    "accepted": False,
                    "error": f"Unsupported extension: {ext}. Allowed: {', '.join(sorted(allowed_ext))}",
                }
            )
            continue
        saved_path = Path(_save_uploaded_file(f, session_dir))
        kind = _detect_training_file_kind(saved_path, name)
        payload.append(
            {
                "filename": name,
                "accepted": True,
                "detected_kind": kind,
                "saved_path": str(saved_path),
            }
        )
    return jsonify(
        {
            "ok": True,
            "files": payload,
            "supported_kinds": ["spp", "inventory", "writeoff", "rules", "nomenclature"],
            "help": {
                "spp": "SPP / vykaz praci za mesic",
                "inventory": "Inventura NF-30 nebo soupis/fakturace s polozkami",
                "writeoff": "NF-45 skutecne odpisy (volitelne)",
                "rules": "Excel s pravidly/aliasy",
                "nomenclature": "Referencni seznam materialu",
            },
        }
    )


@app.post("/api/analyze")
def analyze():
    _ensure_dirs()
    form = request.form
    files = request.files

    project_code = (form.get("project_code") or "chirana").strip().lower()
    project_name = (form.get("project_name") or project_code.title()).strip()
    period_month = (form.get("period_month") or datetime.now().strftime("%Y-%m")).strip()
    project_prompt = form.get("project_prompt") or ""
    no_ai = _bool(form.get("no_ai"))
    if no_ai:
        return jsonify({"error": "AI mode is required. Vypnete 'Rezim bez AI' a spustte znovu."}), 400
    auto_map = not _bool(form.get("disable_auto_map"))

    spp = files.get("spp")
    inventory = files.get("inventory")
    nf45 = files.get("nf45")
    rules_file = files.get("rules")
    if spp is None or inventory is None:
        return jsonify({"error": "spp and inventory files are required"}), 400
    slot_error = _validate_upload_slots(spp, inventory, nf45, rules_file)
    if slot_error:
        return jsonify({"error": slot_error}), 400

    session_dir = UPLOAD_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    session_dir.mkdir(parents=True, exist_ok=True)

    spp_path = _save_uploaded_file(spp, session_dir)
    inventory_path = _save_uploaded_file(inventory, session_dir)
    nf45_path = _save_uploaded_file(nf45, session_dir) if nf45 else None
    rules_path = _save_uploaded_file(rules_file, session_dir) if rules_file else _find_default_rules_file()
    nomenclature_path = _find_default_nomenclature_file()

    try:
        spp_mapping_override = json.loads(form.get("spp_mapping") or "null")
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid spp_mapping JSON"}), 400
    try:
        inv_mapping_override = json.loads(form.get("inventory_mapping") or "null")
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid inventory_mapping JSON"}), 400

    store.ensure_project(project_code, project_name, project_prompt)
    if spp_mapping_override is None:
        spp_mapping_override = store.get_mapping(project_code, "spp")
    if inv_mapping_override is None:
        inv_mapping_override = store.get_mapping(project_code, "inventory")

    try:
        result = _run_pipeline(
            project_code=project_code,
            spp_path=spp_path,
            inventory_path=inventory_path,
            no_ai=no_ai,
            auto_map=auto_map,
            spp_mapping_override=spp_mapping_override,
            inv_mapping_override=inv_mapping_override,
            project_prompt=project_prompt,
            rules_path=rules_path,
            nomenclature_path=nomenclature_path,
            nf45_path=nf45_path,
            generate_excel=False,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    if result["mappings"]["spp"]:
        store.save_mapping(project_code, "spp", result["mappings"]["spp"])
    if result["mappings"]["inventory"]:
        store.save_mapping(project_code, "inventory", result["mappings"]["inventory"])
    _write_draft_artifacts(spp_path, result)

    draft_id = uuid.uuid4().hex
    store.create_analysis_draft(
        draft_id=draft_id,
        project_code=project_code,
        project_name=project_name,
        period_month=period_month,
        spp_path=spp_path,
        inventory_path=inventory_path,
        nf45_path=nf45_path,
        rules_path=rules_path,
        nomenclature_path=nomenclature_path,
        project_prompt=project_prompt,
        spp_mapping=result["mappings"]["spp"],
        inventory_mapping=result["mappings"]["inventory"],
    )

    return jsonify(
        {
            "ok": True,
            "draft_id": draft_id,
            "result": _strip_export_artifacts(result),
        }
    )


@app.post("/api/review/recalculate")
def review_recalculate():
    payload = request.get_json(silent=True) or {}
    draft_id = (payload.get("draft_id") or "").strip()
    if not draft_id:
        return jsonify({"error": "draft_id is required"}), 400

    draft = store.get_analysis_draft(draft_id)
    if not draft:
        return jsonify({"error": "Draft not found"}), 404

    try:
        result = _run_pipeline(
            project_code=draft["project_code"],
            spp_path=draft["spp_path"],
            inventory_path=draft["inventory_path"],
            no_ai=False,
            auto_map=True,
            spp_mapping_override=draft.get("spp_mapping"),
            inv_mapping_override=draft.get("inventory_mapping"),
            project_prompt=draft.get("project_prompt") or "",
            rules_path=draft.get("rules_path"),
            nomenclature_path=draft.get("nomenclature_path"),
            nf45_path=draft.get("nf45_path"),
            generate_excel=False,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _write_draft_artifacts(draft["spp_path"], result)
    return jsonify({"ok": True, "draft_id": draft_id, "result": _strip_export_artifacts(result)})


@app.post("/api/review/finalize")
def review_finalize():
    payload = request.get_json(silent=True) or {}
    draft_id = (payload.get("draft_id") or "").strip()
    if not draft_id:
        return jsonify({"error": "draft_id is required"}), 400

    draft = store.get_analysis_draft(draft_id)
    if not draft:
        return jsonify({"error": "Draft not found"}), 404

    try:
        result = _finalize_from_cached_artifacts(draft)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    session_id = store.save_session(
        project_code=draft["project_code"],
        period_month=draft["period_month"],
        spp_path=draft["spp_path"],
        inventory_path=draft["inventory_path"],
        nf45_path=draft.get("nf45_path"),
        output_path=result["output_path"],
        stats=result,
    )
    return jsonify({"ok": True, "session_id": session_id, "result": result})


@app.post("/api/detect-mapping")
def detect_mapping():
    files = request.files
    spp = files.get("spp")
    inventory = files.get("inventory")
    if spp is None and inventory is None:
        return jsonify({"error": "spp or inventory file is required"}), 400

    _ensure_dirs()
    session_dir = UPLOAD_DIR / f"mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    session_dir.mkdir(parents=True, exist_ok=True)

    api_key = (
        settings.OPENAI_API_KEY
        or os.environ.get("OPENAI_API_KEY")
        or settings.ANTHROPIC_API_KEY
        or os.environ.get("ANTHROPIC_API_KEY")
        or ""
    ).strip()
    client = ClaudeClient(api_key=api_key) if api_key else None

    result = {}
    if spp:
        spp_path = Path(_save_uploaded_file(spp, session_dir))
        mapping = _default_mapping("spp")
        if client:
            try:
                mapping = auto_detect_mapping(spp_path, client, "spp")
            except Exception:
                pass
        result["spp_mapping"] = mapping_to_dict(mapping)
    if inventory:
        inv_path = Path(_save_uploaded_file(inventory, session_dir))
        mapping = _default_mapping("inventory")
        if client:
            try:
                mapping = auto_detect_mapping(inv_path, client, "inventory")
            except Exception:
                pass
        result["inventory_mapping"] = mapping_to_dict(mapping)

    return jsonify({"ok": True, **result})


@app.get("/output/<path:filename>")
def get_output(filename: str):
    return send_from_directory(str(OUTPUT_DIR), filename, as_attachment=True)


if __name__ == "__main__":
    _ensure_dirs()
    store.ensure_project("chirana", "Chirana")
    app.run(host="127.0.0.1", port=8000, debug=False, threaded=True)
