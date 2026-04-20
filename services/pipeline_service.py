"""Shared pipeline service for CLI and web app."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re
import time
from typing import Any
from urllib import request

import yaml
from rapidfuzz import fuzz, process

from analysis.anomaly_detector import analyze_all, get_summary
from analysis.nf45_validator import validate_against_nf45
from config.settings import CONFIG_DIR, DEFAULT_INVENTORY_DATA_START, OUTPUT_DIR
from llm.client import ClaudeClient
from matching.material_matcher import match_all
from models import AnomalyStatus, ColumnMapping, MatchMethod, MatchResult, MaterialCategory
from output.excel_generator import generate_output
from parsers.inventory_parser import parse_inventory
from parsers.mapping_engine import auto_detect_mapping
from parsers.nomenclature_parser import build_nomenclature_index, parse_nomenclature
from parsers.rules_parser import build_runtime_rules, parse_rules_catalog
from parsers.spp_parser import filter_spp_by_month, parse_spp
from parsers.writeoff_parser import parse_writeoff


_DEBUG_LOG_PATH = "/Users/dmytriivezerian/Desktop/Domostav x Fajnwork/.cursor/debug-f07731.log"


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": "f07731",
        "runId": "initial",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    try:
        req = request.Request(
            "http://127.0.0.1:7897/ingest/d0e90649-22bc-4799-98cf-38260af08d14",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Debug-Session-Id": "f07731",
            },
            method="POST",
        )
        request.urlopen(req, timeout=2).read()
    except Exception:
        pass


def load_project_config(project: str) -> dict:
    config_path = CONFIG_DIR / "projects" / f"{project}.yaml"
    if not config_path.exists():
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_system_prompt() -> str:
    prompt_path = CONFIG_DIR / "system_prompt.txt"
    if not prompt_path.exists():
        return ""
    return prompt_path.read_text(encoding="utf-8")


def build_column_mapping(config: dict, file_type: str) -> ColumnMapping | None:
    section = config.get(file_type)
    if not section or "columns" not in section:
        return None
    cols = section["columns"]
    return ColumnMapping(
        row_number=cols.get("number"),
        article=cols.get("article"),
        name=cols.get("name"),
        unit=cols.get("unit"),
        quantity=cols.get("quantity", cols.get("quantity_fact")),
        quantity_accounting=cols.get("quantity_accounting"),
        deviation=cols.get("deviation"),
        price=cols.get("price", cols.get("price_per_unit")),
        total=cols.get("total"),
        percent_month=cols.get("percent_month"),
        total_month=cols.get("total_month"),
        header_row=section.get("header_row", 1),
        data_start_row=section.get("data_start_row", 2),
    )


def mapping_to_dict(mapping: ColumnMapping) -> dict[str, Any]:
    return {
        "row_number": mapping.row_number,
        "article": mapping.article,
        "name": mapping.name,
        "unit": mapping.unit,
        "quantity": mapping.quantity,
        "quantity_accounting": mapping.quantity_accounting,
        "deviation": mapping.deviation,
        "price": mapping.price,
        "total": mapping.total,
        "percent_month": mapping.percent_month,
        "total_month": mapping.total_month,
        "sheet_name": mapping.sheet_name,
        "header_row": mapping.header_row,
        "data_start_row": mapping.data_start_row,
    }


def mapping_from_dict(payload: dict[str, Any] | None) -> ColumnMapping | None:
    if not payload:
        return None
    return ColumnMapping(**payload)


def _category_from_group(group_name: str) -> MaterialCategory:
    low = group_name.lower()
    if "trub" in low:
        return MaterialCategory.PIPE
    if "tvar" in low or "kolen" in low or "reduk" in low:
        return MaterialCategory.FITTING
    if "izol" in low:
        return MaterialCategory.INSULATION
    if "kohout" in low or "ventil" in low:
        return MaterialCategory.VALVE
    if "spotreb" in low or "ostatni" in low:
        return MaterialCategory.CONSUMABLE
    return MaterialCategory.OTHER


def apply_rules_catalog_to_inventory(inventory_items: list, runtime_rules: dict | None) -> int:
    """Apply parsed XLSM rules as runtime normalization/category hints.

    Returns count of rows updated.
    """
    if not runtime_rules:
        return 0
    catalog = runtime_rules.get("catalog", {})
    flat: list[tuple[str, MaterialCategory]] = []
    for group_name, rows in catalog.items():
        category = _category_from_group(group_name)
        for r in rows:
            flat.append((r.get("name_norm", ""), category))

    updated = 0
    for item in inventory_items:
        inv_norm = _norm(item.name)
        best_score = 0
        best_category = None
        for norm_name, cat in flat:
            if not norm_name:
                continue
            score = fuzz.ratio(inv_norm, norm_name)
            if score > best_score:
                best_score = score
                best_category = cat
        if best_category and best_score >= 88:
            if item.category != best_category:
                item.category = best_category
                updated += 1
    return updated


def apply_nomenclature_normalization(inventory_items: list, nomenclature_path: str | Path | None) -> int:
    if not nomenclature_path or not Path(nomenclature_path).exists():
        return 0
    items = parse_nomenclature(nomenclature_path)
    idx = build_nomenclature_index(items)
    choices = list(idx.keys())
    if not choices:
        return 0

    changed = 0
    for inv in inventory_items:
        norm = _norm(inv.name)
        match = process.extractOne(norm, choices, scorer=fuzz.WRatio)
        if not match:
            continue
        best_name, score, _ = match
        if score < 90:
            continue
        ref = idx[best_name][0]
        if ref.name and ref.name != inv.name:
            inv.name = ref.name
            changed += 1
    return changed


def apply_overrides_to_matches(matches: dict[int, MatchResult], inventory_items: list, overrides: dict[str, dict]) -> int:
    changed = 0
    key_to_row = {}
    for inv in inventory_items:
        if inv.article:
            key_to_row[f"ARTICLE:{inv.article.strip().upper()}"] = inv.row
        key_to_row[f"ROW:{inv.row}"] = inv.row
    for item_key, override in overrides.items():
        inv_row = key_to_row.get(item_key)
        if inv_row is None:
            continue
        matches[inv_row] = MatchResult(
            inventory_row=inv_row,
            matched_spp_rows=list(override.get("spp_rows", [])),
            match_method=MatchMethod.MANUAL,
            confidence=1.0,
            match_reason=override.get("reason", "manual override"),
        )
        changed += 1
    return changed


def run_analysis_pipeline(
    *,
    project: str,
    spp_path: str | Path,
    inventory_path: str | Path,
    output_path: str | None = None,
    period_month: str | None = None,
    api_key: str | None = None,
    no_ai: bool = False,
    force_ai_matching: bool = True,
    auto_map: bool = True,
    spp_mapping_override: dict[str, Any] | None = None,
    inv_mapping_override: dict[str, Any] | None = None,
    project_prompt_override: str | None = None,
    rules_xlsm_path: str | Path | None = None,
    nomenclature_path: str | Path | None = None,
    nf45_path: str | Path | None = None,
    overrides: dict[str, dict] | None = None,
    alias_map: dict[str, str] | None = None,
    category_rules: list[dict[str, Any]] | None = None,
    generate_excel: bool = True,
    include_export_artifacts: bool = False,
) -> dict[str, Any]:
    """End-to-end analysis pipeline with optional NF-45 validation."""
    pipeline_started_at = time.perf_counter()
    spp_path = Path(spp_path)
    inventory_path = Path(inventory_path)
    if not spp_path.exists():
        raise FileNotFoundError(f"SPP not found: {spp_path}")
    if not inventory_path.exists():
        raise FileNotFoundError(f"Inventory not found: {inventory_path}")

    config = load_project_config(project)
    system_prompt = load_system_prompt()
    if config.get("notes"):
        system_prompt += f"\n\n## Project-Specific Notes\n{config.get('notes')}"
    if project_prompt_override:
        system_prompt += f"\n\n## User Project Prompt\n{project_prompt_override}"

    # region agent log
    _debug_log(
        "H2",
        "services/pipeline_service.py:240",
        "Pipeline run started",
        {
            "project": project,
            "generate_excel": generate_excel,
            "no_ai": no_ai,
            "force_ai_matching": force_ai_matching,
            "nf45_present": bool(nf45_path and Path(nf45_path).exists()),
            "rules_present": bool(rules_xlsm_path and Path(rules_xlsm_path).exists()),
            "nomenclature_present": bool(nomenclature_path and Path(nomenclature_path).exists()),
        },
    )
    # endregion

    client = None
    if not no_ai:
        client = ClaudeClient(api_key=api_key)

    spp_mapping = mapping_from_dict(spp_mapping_override) or build_column_mapping(config, "spp")
    inv_mapping = mapping_from_dict(inv_mapping_override) or build_column_mapping(config, "inventory")
    if spp_mapping is None and auto_map and client:
        spp_mapping = auto_detect_mapping(spp_path, client, "spp")
    if inv_mapping is None and auto_map and client:
        inv_mapping = auto_detect_mapping(inventory_path, client, "inventory")

    spp_sheets = config.get("spp", {}).get("sheets")
    spp_items_all = parse_spp(
        spp_path,
        sheets=spp_sheets,
        mapping=spp_mapping,
        period_month_hint=period_month,
    )
    spp_items = filter_spp_by_month(spp_items_all)
    inventory_items = parse_inventory(inventory_path, mapping=inv_mapping)

    # region agent log
    _debug_log(
        "H2",
        "services/pipeline_service.py:255",
        "Pipeline parsed source files",
        {
            "project": project,
            "spp_count_all": len(spp_items_all),
            "spp_count_active_month": len(spp_items),
            "inventory_count": len(inventory_items),
            "spp_mapping": mapping_to_dict(spp_mapping) if spp_mapping else None,
            "inventory_mapping": mapping_to_dict(inv_mapping) if inv_mapping else None,
            "project_prompt_present": bool(project_prompt_override.strip()) if project_prompt_override else False,
            "rules_path_present": bool(rules_xlsm_path and Path(rules_xlsm_path).exists()),
        },
    )
    # endregion

    runtime_rules = None
    if rules_xlsm_path and Path(rules_xlsm_path).exists():
        catalog_items = parse_rules_catalog(rules_xlsm_path)
        runtime_rules = build_runtime_rules(catalog_items)
        apply_rules_catalog_to_inventory(inventory_items, runtime_rules)

    normalization_count = apply_nomenclature_normalization(inventory_items, nomenclature_path)
    alias_normalized_count = _apply_alias_rules(inventory_items, alias_map or {})
    category_rules_applied = _apply_category_rules(inventory_items, category_rules or [])

    parse_completed_at = time.perf_counter()
    matches = match_all(
        inventory_items,
        spp_items,
        client=client if not no_ai else None,
        system_prompt=system_prompt,
        force_ai=force_ai_matching and not no_ai,
    )

    # region agent log
    _debug_log(
        "H1",
        "services/pipeline_service.py:274",
        "Matching completed",
        {
            "match_count": len(matches),
            "method_counts_raw": dict(Counter(m.match_method.value for m in matches.values())),
            "rows_with_spp": sum(1 for m in matches.values() if m.matched_spp_rows),
            "rows_without_spp": sum(1 for m in matches.values() if not m.matched_spp_rows),
            "sample_no_spp_rows": [row for row, m in list(matches.items()) if not m.matched_spp_rows][:10],
        },
    )
    # endregion

    matching_completed_at = time.perf_counter()
    applied_overrides = 0
    if overrides:
        applied_overrides = apply_overrides_to_matches(matches, inventory_items, overrides)

    recommendations = analyze_all(
        inventory_items,
        spp_items,
        matches,
        rules=config.get("rules", {}),
    )
    summary = get_summary(recommendations, inventory_items)
    rec_by_row = {rec.inventory_row: rec for rec in recommendations}
    kpi_rows = {
        inv.row
        for inv in inventory_items
        if inv.category != MaterialCategory.CONSUMABLE
        and rec_by_row.get(inv.row)
        and rec_by_row[inv.row].status != AnomalyStatus.OUT_OF_SCOPE
    }
    method_counts = Counter(
        m.match_method.value
        for row, m in matches.items()
        if row in kpi_rows
    )
    matched_rows_with_spp = sum(
        1 for row, m in matches.items() if row in kpi_rows and m.matched_spp_rows
    )
    ai_matched_rows = sum(
        1 for row, m in matches.items()
        if row in kpi_rows and m.match_method == MatchMethod.AI and m.matched_spp_rows
    )
    manual_matched_rows = sum(
        1 for row, m in matches.items()
        if row in kpi_rows and m.match_method == MatchMethod.MANUAL and m.matched_spp_rows
    )
    unmatched_rows = sum(
        1
        for rec in recommendations
        if rec.inventory_row in kpi_rows and rec.expected_writeoff is None
    )
    analysis_completed_at = time.perf_counter()

    # region agent log
    _debug_log(
        "H4",
        "services/pipeline_service.py:296",
        "Summary and KPI basis",
        {
            "recommendation_count": len(recommendations),
            "expected_missing_count": sum(1 for r in recommendations if r.expected_writeoff is None),
            "red_flag_count": summary.get("red_flag"),
            "summary_review_count": summary.get("review"),
            "top_anomalies_len": len(summary.get("top_anomalies") or []),
            "matched_percent_formula": round(matched_rows_with_spp / max(len(kpi_rows), 1) * 100, 1),
            "actual_rows_with_spp": matched_rows_with_spp,
            "actual_rows_without_spp": sum(1 for row, m in matches.items() if row in kpi_rows and not m.matched_spp_rows),
            "method_counts": dict(method_counts),
            "excluded_consumables": summary.get("excluded", {}).get("consumables_count", 0),
        },
    )
    # endregion

    validation = None
    validation_completed_at = analysis_completed_at
    if nf45_path and Path(nf45_path).exists():
        nf_items = parse_writeoff(nf45_path)
        validation = validate_against_nf45(recommendations, nf_items)
        validation_completed_at = time.perf_counter()

    # Build reverse coverage: SPP item → matched inventory items
    spp_coverage = _build_spp_coverage(spp_items, inventory_items, matches, recommendations)
    coverage_completed_at = time.perf_counter()

    out: Path | None = None
    if generate_excel:
        if output_path is None:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = str(OUTPUT_DIR / f"analysis_{project}_{ts}.xlsx")
        data_start = config.get("inventory", {}).get("data_start_row", DEFAULT_INVENTORY_DATA_START)
        # region agent log
        _debug_log(
            "H2",
            "services/pipeline_service.py:generate_output",
            "Preparing Excel export",
            {
                "inventory_path": str(inventory_path),
                "inventory_mapping_sheet_name": getattr(inv_mapping, "sheet_name", None) if inv_mapping else None,
                "inventory_mapping_data_start_row": getattr(inv_mapping, "data_start_row", None) if inv_mapping else None,
                "recommendation_count": len(recommendations),
            },
        )
        # endregion
        out = generate_output(
            source_path=inventory_path,
            output_path=output_path,
            recommendations=recommendations,
            data_start_row=data_start,
            sheet_name=getattr(inv_mapping, "sheet_name", None) if inv_mapping else None,
            summary=summary,
            spp_coverage=spp_coverage,
        )
    excel_completed_at = time.perf_counter()

    # region agent log
    _debug_log(
        "H2",
        "services/pipeline_service.py:372",
        "Pipeline phase timings",
        {
            "generate_excel": generate_excel,
            "parse_ms": round((parse_completed_at - pipeline_started_at) * 1000, 1),
            "matching_ms": round((matching_completed_at - parse_completed_at) * 1000, 1),
            "analysis_ms": round((analysis_completed_at - matching_completed_at) * 1000, 1),
            "validation_ms": round((validation_completed_at - analysis_completed_at) * 1000, 1),
            "coverage_ms": round((coverage_completed_at - validation_completed_at) * 1000, 1),
            "excel_ms": round((excel_completed_at - coverage_completed_at) * 1000, 1),
            "total_ms": round((excel_completed_at - pipeline_started_at) * 1000, 1),
            "matched_rows_with_spp": matched_rows_with_spp,
            "inventory_count": len(inventory_items),
            "output_path": str(out) if out else None,
        },
    )
    # endregion

    kpis = {
        "total_items": summary["total_items"],
        "ok": summary["ok"],
        "warning": summary["warning"],
        "red_flag": summary["red_flag"],
        "matched_percent": round(matched_rows_with_spp / max(len(kpi_rows), 1) * 100, 1),
        "red_flags_percent": round(summary["red_flag"] / max(summary["total_items"], 1) * 100, 1),
        "method_counts": dict(method_counts),
        "match_breakdown": {
            "ai_matched": ai_matched_rows,
            "manual_override": manual_matched_rows,
            "unmatched": unmatched_rows,
        },
        "excluded": {
            "consumables_count": summary.get("excluded", {}).get("consumables_count", 0),
            "out_of_scope_count": summary.get("excluded", {}).get("out_of_scope_count", 0),
        },
        "validation": validation,
    }

    result = {
        "project": project,
        "spp_count": len(spp_items),
        "spp_count_all": len(spp_items_all),
        "spp_count_active_month": len(spp_items),
        "inventory_count": len(inventory_items),
        "summary": summary,
        "method_counts": dict(method_counts),
        "kpis": kpis,
        "spp_coverage": spp_coverage,
        "mappings": {
            "spp": mapping_to_dict(spp_mapping) if spp_mapping else None,
            "inventory": mapping_to_dict(inv_mapping) if inv_mapping else None,
        },
        "applied_overrides": applied_overrides,
        "rules_loaded": runtime_rules.get("count", 0) if runtime_rules else 0,
        "normalized_items": normalization_count,
        "alias_normalized_items": alias_normalized_count,
        "category_rules_applied": category_rules_applied,
        "output_path": str(out) if out else None,
        "api_usage": client.get_usage_summary() if client else None,
        "spp_coverage_summary": {
            "total_spp_month": len(spp_items),
            "covered": sum(1 for c in spp_coverage if c["covered"]),
            "not_covered": sum(1 for c in spp_coverage if not c["covered"]),
            "by_sheet": _coverage_by_sheet(spp_coverage),
        },
        "review": _build_review_payload(inventory_items, recommendations, spp_items),
    }
    if include_export_artifacts:
        result["export_artifacts"] = {
            "summary": summary,
            "spp_coverage": spp_coverage,
            "data_start_row": config.get("inventory", {}).get("data_start_row", DEFAULT_INVENTORY_DATA_START),
            "recommendations": [_model_to_dict(rec) for rec in recommendations],
        }
    return result


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    if hasattr(model, "dict"):
        return model.dict()
    raise TypeError(f"Unsupported model type: {type(model).__name__}")


def _build_spp_coverage(
    spp_items: list,
    inventory_items: list,
    matches: dict,
    recommendations: list,
) -> list[dict]:
    """For each active-month SPP item, check which inventory rows matched to it."""
    from models import SPPItem, InventoryItem, MatchResult, WriteoffRecommendation

    inv_by_row = {i.row: i for i in inventory_items}
    # Reverse map: SPP row → list of inventory rows that matched to it
    reverse: dict[int, set[int]] = {}
    for inv_row, m in matches.items():
        inv = inv_by_row.get(inv_row)
        if inv and inv.category == MaterialCategory.CONSUMABLE:
            # Consumables are intentionally excluded from coverage/KPI.
            continue
        for spp_row in m.matched_spp_rows:
            reverse.setdefault(spp_row, set()).add(inv_row)

    coverage: list[dict] = []
    for spp in spp_items:
        qty_month = 0.0
        if spp.quantity and spp.percent_month:
            qty_month = spp.quantity * (spp.percent_month / 100)
        elif spp.total_month and spp.price_per_unit and spp.price_per_unit > 0:
            qty_month = spp.total_month / spp.price_per_unit

        matched_inv_rows = sorted(reverse.get(spp.row, set()))
        inv_names: list[str] = []
        total_deviation = 0.0
        for ir in matched_inv_rows:
            inv = inv_by_row.get(ir)
            if inv:
                inv_names.append(inv.name[:50])
                total_deviation += abs(float(inv.deviation or 0))

        is_covered = len(matched_inv_rows) > 0
        delta = qty_month - total_deviation if qty_month else -total_deviation

        if is_covered and qty_month > 0:
            pct_diff = abs(delta) / qty_month * 100
            if pct_diff <= 15:
                comment = f"Pokryto ({len(matched_inv_rows)} polozek), odchylka {pct_diff:.1f}%"
            elif pct_diff <= 30:
                comment = f"Castecne pokryto ({len(matched_inv_rows)} polozek), odchylka {pct_diff:.1f}%"
            else:
                comment = f"Velka odchylka ({pct_diff:.1f}%), {len(matched_inv_rows)} polozek ze skladu"
        elif is_covered:
            comment = f"Pokryto ({len(matched_inv_rows)} polozek), plan za mesic neni znamy"
        else:
            comment = "Nepokryto — zadna polozka ze skladu nebyla prirazena"

        coverage.append({
            "spp_row": spp.source_row,
            "spp_sheet": spp.sheet,
            "spp_name": spp.name,
            "spp_unit": spp.unit,
            "spp_qty_month": round(qty_month, 2),
            "spp_total_month": spp.total_month,
            "covered": is_covered,
            "inventory_rows": matched_inv_rows,
            "inventory_names": inv_names,
            "inventory_total_deviation": round(total_deviation, 2),
            "delta": round(delta, 2),
            "comment": comment,
        })

    return coverage


def _coverage_by_sheet(coverage: list[dict]) -> dict[str, dict[str, int]]:
    """Build simple per-sheet counters (e.g., ZTI: 9/12 covered)."""
    summary: dict[str, dict[str, int]] = {}
    for item in coverage:
        sheet = str(item.get("spp_sheet") or "UNKNOWN")
        block = summary.setdefault(sheet, {"total": 0, "covered": 0, "not_covered": 0})
        block["total"] += 1
        if item.get("covered"):
            block["covered"] += 1
        else:
            block["not_covered"] += 1
    return summary


def _build_review_payload(inventory_items: list, recommendations: list, spp_items: list) -> dict[str, Any]:
    inv_by_row = {i.row: i for i in inventory_items}
    review_rows: list[dict[str, Any]] = []
    unmatched_count = 0
    out_of_scope_count = 0

    for rec in recommendations:
        inv = inv_by_row.get(rec.inventory_row)
        article = (inv.article or "").strip().upper() if inv and inv.article else ""
        item_key = f"ARTICLE:{article}" if article else f"ROW:{rec.inventory_row}"
        is_out_of_scope = getattr(rec.status, "value", rec.status) == "OUT_OF_SCOPE"
        is_unmatched = rec.expected_writeoff is None
        is_anomaly = getattr(rec.status, "value", rec.status) not in {"OK", "OUT_OF_SCOPE"}
        if is_out_of_scope:
            out_of_scope_count += 1
            continue
        if is_unmatched:
            unmatched_count += 1
        if not (is_unmatched or is_anomaly):
            continue

        review_rows.append(
            {
                "item_key": item_key,
                "inventory_row": rec.inventory_row,
                "article": article or None,
                "name": rec.inventory_name,
                "inventory_unit": inv.unit if inv else None,
                "actual_deviation": inv.deviation if inv else None,
                "price": inv.price if inv else None,
                "expected_writeoff": rec.expected_writeoff,
                "spp_reference": rec.spp_reference,
                "spp_source": _extract_spp_source_label(rec.spp_reference),
                "reason": rec.reason,
                "status": getattr(rec.status, "value", rec.status),
                "method": getattr(rec.match_method, "value", rec.match_method),
                "deviation_percent": rec.deviation_percent,
                "category": (inv.category.value if inv else None),
                "is_unmatched": is_unmatched,
                "is_anomaly": is_anomaly,
                "is_out_of_scope": False,
                "money_impact": round(
                    abs(float(inv.deviation or 0.0)) * abs(float(inv.price or 0.0)),
                    2,
                ) if inv else 0.0,
            }
        )

    spp_options: list[dict[str, Any]] = []
    for spp in spp_items:
        qty_month = None
        if spp.quantity and spp.percent_month:
            qty_month = round(spp.quantity * (spp.percent_month / 100), 2)
        elif spp.total_month and spp.price_per_unit and spp.price_per_unit > 0:
            qty_month = round(spp.total_month / spp.price_per_unit, 2)
        spp_options.append(
            {
                "row": spp.row,
                "source_row": spp.source_row,
                "sheet": spp.sheet,
                "name": spp.name,
                "unit": spp.unit,
                "qty_month": qty_month,
                "label": f"[{spp.sheet}] Row {spp.source_row}: {spp.name[:90]}",
            }
        )

    review_rows.sort(
        key=lambda item: (
            0 if item["is_unmatched"] else 1,
            -abs(float(item.get("money_impact") or 0.0)),
            -abs(float(item.get("deviation_percent") or 0.0)),
            item["inventory_row"],
        ),
    )

    review_top_anomalies = [item for item in review_rows if item["is_anomaly"]]
    review_top_anomalies.sort(
        key=lambda item: (
            abs(float(item.get("money_impact") or 0.0)),
            abs(float(item.get("deviation_percent") or 0.0)),
        ),
        reverse=True,
    )

    return {
        "review_rows": review_rows,
        "top_anomalies": review_top_anomalies[:20],
        "spp_options": spp_options,
        "counts": {
            "unmatched": unmatched_count,
            "review_rows": len(review_rows),
            "top_anomalies": len(review_top_anomalies),
            "out_of_scope": out_of_scope_count,
        },
    }


def _norm(value: str) -> str:
    return " ".join(value.lower().replace("\xa0", " ").split())


def _apply_alias_rules(inventory_items: list, alias_map: dict[str, str]) -> int:
    if not alias_map:
        return 0
    normalized_alias_map = {
        _norm(str(k)): str(v).strip()
        for k, v in alias_map.items()
        if str(k).strip() and str(v).strip()
    }
    if not normalized_alias_map:
        return 0

    changed = 0
    for item in inventory_items:
        original = item.name or ""
        updated = original
        low_updated = _norm(updated)
        for alias, canonical in normalized_alias_map.items():
            pattern = r"\b" + re.escape(alias) + r"\b"
            low_after = re.sub(pattern, canonical, low_updated, flags=re.IGNORECASE)
            if low_after != low_updated:
                low_updated = low_after
                updated = low_after
        if updated != original and updated.strip():
            item.name = updated
            changed += 1
    return changed


def _apply_category_rules(inventory_items: list, rules: list[dict[str, Any]]) -> int:
    if not rules:
        return 0
    changed = 0
    for item in inventory_items:
        item_name = _norm(item.name or "")
        item_article = (item.article or "").strip().upper()
        for rule in rules:
            value = rule.get("rule_value") or {}
            match_mode = str(value.get("match") or "name_contains").strip().lower()
            pattern = str(value.get("pattern") or "").strip()
            target_cat = str(value.get("set_category") or "").strip().upper()
            if not pattern or not target_cat:
                continue
            matched = False
            if match_mode == "article_prefix":
                matched = item_article.startswith(pattern.upper())
            elif match_mode == "name_regex":
                try:
                    matched = bool(re.search(pattern, item_name, flags=re.IGNORECASE))
                except re.error:
                    matched = False
            else:
                matched = pattern.lower() in item_name

            if not matched:
                continue
            try:
                new_cat = MaterialCategory(target_cat)
            except ValueError:
                continue
            if item.category != new_cat:
                item.category = new_cat
                changed += 1
            break
    return changed


def _extract_spp_source_label(spp_reference: str) -> str:
    import re

    if not spp_reference:
        return ""
    parts = re.findall(r"\[([^\]]+)\]\s*Row\s*(\d+)", spp_reference)
    if not parts:
        return spp_reference[:40]
    return ", ".join(f"{sheet} #{row}" for sheet, row in parts)
