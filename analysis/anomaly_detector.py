"""Compare expected write-offs vs actual inventory deviations and flag anomalies."""

from __future__ import annotations

from collections import Counter

from models import (
    InventoryItem,
    SPPItem,
    MatchResult,
    WriteoffRecommendation,
    SPPCoverageRec,
    AnomalyStatus,
    MaterialCategory,
)
from analysis.writeoff_calculator import calculate_expected_writeoff
from config.settings import TOLERANCE_OK, TOLERANCE_WARNING


def _status_human(status: AnomalyStatus) -> str:
    if status == AnomalyStatus.OK:
        return "V poradku"
    if status == AnomalyStatus.WARNING:
        return "Varovani"
    if status == AnomalyStatus.OUT_OF_SCOPE:
        return "Mimo aktivni SPP mesic"
    return "Kriticke"


def _build_one_line_explanation(rec: WriteoffRecommendation, inv: InventoryItem | None) -> str:
    """User-facing one-liner. Prefer the AI-written reason; fall back to a numeric template.

    Emits clean labels (no "%%" artefact) and adds money impact when known.
    """
    ai_reason = (rec.reason or "").strip()
    if ai_reason:
        return ai_reason[:240]

    unit = (inv.unit if inv else None) or ""
    if rec.expected_writeoff is not None:
        expected = f"{round(float(rec.expected_writeoff), 2)} {unit}".strip()
    else:
        expected = "нет оценки"
    actual = f"{round(float(rec.actual_deviation or 0.0), 2)} {unit}".strip()
    if rec.deviation_percent is not None:
        dev = f"{round(float(rec.deviation_percent), 1)}%"
    else:
        dev = "нет %"
    return (
        f"Материал: {rec.inventory_name[:50]}. "
        f"Ожидалось: {expected}. Факт: {actual}. "
        f"Отклонение: {dev}. Статус: {_status_human(rec.status)}."
    )


def analyze_all(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
    matches: dict[int, MatchResult],
    rules: dict | None = None,
) -> list[WriteoffRecommendation]:
    """Run write-off analysis for all inventory items.

    Args:
        inventory: All parsed inventory items.
        spp: All parsed SPP items.
        matches: Match results from material_matcher.

    Returns:
        List of WriteoffRecommendation objects.
    """
    spp_by_row = {item.row: item for item in spp}
    recommendations: list[WriteoffRecommendation] = []

    for inv_item in inventory:
        match = matches.get(inv_item.row)
        if match is None:
            # Create a default unmatched recommendation
            rec = WriteoffRecommendation(
                inventory_row=inv_item.row,
                inventory_name=inv_item.name,
                article=inv_item.article,
                actual_deviation=inv_item.deviation,
                reason="No match attempted",
                status=AnomalyStatus.RED_FLAG,
            )
        else:
            rec = calculate_expected_writeoff(
                inv_item=inv_item,
                match=match,
                spp_items=spp_by_row,
                all_matches=matches,
                all_inventory=inventory,
                rules=rules or {},
            )
        recommendations.append(rec)

    return recommendations


def get_summary(
    recommendations: list[WriteoffRecommendation],
    inventory: list[InventoryItem] | None = None,
    spp_coverage_recs: list[SPPCoverageRec] | None = None,
) -> dict:
    """Generate a summary of the analysis results.

    When spp_coverage_recs is provided, the primary KPI counters (total, ok,
    warning, red_flag, review) are derived from the SPP-centric coverage
    analysis instead of the inventory-centric WriteoffRecommendation list.
    The inventory-centric data is still used for unit/cost totals and
    top-anomaly details (needed by the Excel exporter).

    Returns:
        Dict with counts and totals by status.
    """
    inventory = inventory or []
    inv_by_row = {i.row: i for i in inventory}
    def _is_consumable(rec: WriteoffRecommendation) -> bool:
        inv = inv_by_row.get(rec.inventory_row)
        return bool(inv and inv.category == MaterialCategory.CONSUMABLE)
    def _is_out_of_scope(rec: WriteoffRecommendation) -> bool:
        return rec.status == AnomalyStatus.OUT_OF_SCOPE

    non_consumable = [r for r in recommendations if not _is_consumable(r)]
    excluded_consumables = len(recommendations) - len(non_consumable)
    excluded_out_of_scope = sum(1 for r in non_consumable if _is_out_of_scope(r))
    kpi_recommendations = [r for r in non_consumable if not _is_out_of_scope(r)]

    # --- SPP-centric KPI counters (primary when available) ---
    if spp_coverage_recs is not None:
        total = len(spp_coverage_recs)
        ok = sum(1 for r in spp_coverage_recs if r.status == AnomalyStatus.OK)
        warning = sum(1 for r in spp_coverage_recs if r.status == AnomalyStatus.WARNING)
        red = sum(1 for r in spp_coverage_recs if r.status == AnomalyStatus.RED_FLAG)
        review = sum(1 for r in spp_coverage_recs if len(r.covered_inv_rows) == 0)
    else:
        total = len(kpi_recommendations)
        ok = sum(1 for r in kpi_recommendations if r.status == AnomalyStatus.OK)
        warning = sum(1 for r in kpi_recommendations if r.status == AnomalyStatus.WARNING)
        red = sum(1 for r in kpi_recommendations if r.status == AnomalyStatus.RED_FLAG)
        review = sum(1 for r in kpi_recommendations if r.expected_writeoff is None)

    # Top anomalies by monetary impact first, then by deviation percent.
    anomalies = [
        r for r in kpi_recommendations
        if r.status != AnomalyStatus.OK
    ]
    anomalies.sort(
        key=lambda r: (
            abs(float(r.actual_deviation or 0.0)) * abs(float((inv_by_row.get(r.inventory_row).price if inv_by_row.get(r.inventory_row) else 0.0) or 0.0)),
            abs(float(r.deviation_percent or 0.0)),
        ),
        reverse=True,
    )

    def _norm_unit(v: str | None) -> str | None:
        if not v:
            return None
        unit = str(v).strip().lower()
        if unit in {"bm"}:
            return "m"
        if unit in {"ks", "pcs", "pc"}:
            return "ks"
        if unit in {"kg"}:
            return "kg"
        if unit in {"m"}:
            return "m"
        return None

    unit_totals = {
        "m": {"expected": 0.0, "actual": 0.0},
        "ks": {"expected": 0.0, "actual": 0.0},
        "kg": {"expected": 0.0, "actual": 0.0},
    }
    expected_cost = 0.0
    actual_cost = 0.0
    unmatched_reason_counter: Counter[str] = Counter()

    for rec in kpi_recommendations:
        inv = inv_by_row.get(rec.inventory_row)
        unit = _norm_unit(inv.unit if inv else None)
        exp = float(rec.expected_writeoff or 0.0)
        act = abs(float(rec.actual_deviation or 0.0))

        if unit in unit_totals:
            unit_totals[unit]["expected"] += exp
            unit_totals[unit]["actual"] += act

        price = float(inv.price or 0.0) if inv else 0.0
        if price > 0:
            expected_cost += exp * price
            actual_cost += act * price

        if rec.expected_writeoff is None:
            unmatched_reason_counter[rec.reason or "No reason"] += 1

    unmatched_top_reasons = [
        {
            "reason": reason,
            "count": count,
            "percent": round(count / max(review, 1) * 100, 1),
        }
        for reason, count in unmatched_reason_counter.most_common(5)
    ]

    return {
        "total_items": total,
        "ok": ok,
        "warning": warning,
        "red_flag": red,
        "review": review,
        "ok_percent": round(ok / total * 100, 1) if total > 0 else 0,
        "unit_totals": {
            u: {
                "expected": round(v["expected"], 2),
                "actual": round(v["actual"], 2),
                "delta": round(v["actual"] - v["expected"], 2),
            }
            for u, v in unit_totals.items()
        },
        "money_totals": {
            "expected_cost": round(expected_cost, 2),
            "actual_cost": round(actual_cost, 2),
            "delta_cost": round(actual_cost - expected_cost, 2),
        },
        "unmatched": {
            "count": review,
            "percent": round(review / max(total, 1) * 100, 1),
            "top_reasons": unmatched_top_reasons,
        },
        "top_anomalies": [
            {
                "row": a.inventory_row,
                "article": a.article,
                "name": a.inventory_name,
                "unit": (inv_by_row.get(a.inventory_row).unit if inv_by_row.get(a.inventory_row) else None),
                "expected_writeoff": a.expected_writeoff,
                "actual_deviation": a.actual_deviation,
                "deviation_percent": a.deviation_percent,
                "reason": a.reason,
                "status": a.status.value,
                "method": a.match_method.value,
                "price": (inv_by_row.get(a.inventory_row).price if inv_by_row.get(a.inventory_row) else None),
                "money_impact": round(
                    abs(float(a.actual_deviation or 0.0))
                    * abs(float((inv_by_row.get(a.inventory_row).price if inv_by_row.get(a.inventory_row) else 0.0) or 0.0)),
                    2,
                ),
                "one_line_explanation": _build_one_line_explanation(a, inv_by_row.get(a.inventory_row)),
            }
            for a in anomalies[:10]
        ],
        "excluded": {
            "consumables_count": excluded_consumables,
            "out_of_scope_count": excluded_out_of_scope,
        },
    }


def analyze_spp_centric(
    spp_items: list[SPPItem],
    inventory: list[InventoryItem],
    matches: dict[int, MatchResult],
    rules: dict | None = None,
) -> list[SPPCoverageRec]:
    """SPP-centric coverage analysis: for each active-month SPP row, check
    whether inventory items cover it.

    RED_FLAG = SPP row with monthly consumption > 0 and no inventory coverage.
    WARNING / OK = covered, but deviation outside/within tolerance.

    Inventory items with no active SPP link are simply not flagged (out of scope).

    Args:
        spp_items: Active-month SPP items (already filtered by filter_spp_by_month).
        inventory: All inventory items.
        matches: match_all() results keyed by inventory row.
        rules: Optional project rules (tolerance overrides).

    Returns:
        List of SPPCoverageRec, one per active SPP row.
    """
    rules = rules or {}
    tolerances = rules.get("tolerance", {})
    try:
        tol_ok = float(tolerances.get("ok") or TOLERANCE_OK)
    except (TypeError, ValueError):
        tol_ok = TOLERANCE_OK
    try:
        tol_warning = float(tolerances.get("warning") or TOLERANCE_WARNING)
    except (TypeError, ValueError):
        tol_warning = TOLERANCE_WARNING

    inv_by_row = {i.row: i for i in inventory}

    # Build reverse map: spp_row → set of inventory rows matched to it.
    # Exclude consumables — they are always OK and don't affect SPP coverage.
    reverse: dict[int, set[int]] = {}
    for inv_row, m in matches.items():
        inv = inv_by_row.get(inv_row)
        if inv and inv.category == MaterialCategory.CONSUMABLE:
            continue
        for spp_row in m.matched_spp_rows:
            reverse.setdefault(spp_row, set()).add(inv_row)

    results: list[SPPCoverageRec] = []

    for spp in spp_items:
        # Monthly expected quantity
        qty_month: float | None = None
        if spp.quantity and spp.percent_month:
            qty_month = spp.quantity * (spp.percent_month / 100)
        elif spp.total_month and spp.price_per_unit and spp.price_per_unit > 0:
            qty_month = spp.total_month / spp.price_per_unit

        matched_inv_rows = sorted(reverse.get(spp.row, set()))
        covered_names: list[str] = []
        total_deviation = 0.0
        for ir in matched_inv_rows:
            inv = inv_by_row.get(ir)
            if inv:
                covered_names.append(inv.name[:70])
                total_deviation += abs(float(inv.deviation or 0.0))

        is_covered = len(matched_inv_rows) > 0

        # Compute delta and status
        delta: float | None = None
        deviation_pct: float | None = None
        if qty_month is not None and qty_month > 0:
            delta = qty_month - total_deviation
            deviation_pct = abs(delta) / qty_month * 100

        if not is_covered:
            if qty_month is not None and qty_month > 0:
                # We know how much should be there AND nothing was found → real anomaly
                status = AnomalyStatus.RED_FLAG
                reason = "Nepokryto — žádná položka ze skladu nebyla přiřazena k této práci"
            else:
                # No quantity calculable (service item / equipment / kpl row) → can't verify
                status = AnomalyStatus.WARNING
                reason = "Nelze ověřit — plán za měsíc neobsahuje množství (možná služba nebo zařízení bez zásoby)"
        elif deviation_pct is not None:
            if deviation_pct <= tol_ok * 100:
                status = AnomalyStatus.OK
                reason = f"Pokryto, odchylka {deviation_pct:.1f}%"
            elif deviation_pct <= tol_warning * 100:
                status = AnomalyStatus.WARNING
                reason = f"Částečně pokryto, odchylka {deviation_pct:.1f}% (limit {tol_ok*100:.0f}–{tol_warning*100:.0f}%)"
            else:
                status = AnomalyStatus.RED_FLAG
                reason = f"Velká odchylka {deviation_pct:.1f}% (limit >{tol_warning*100:.0f}%)"
        else:
            # Covered but no qty_month to compare against
            status = AnomalyStatus.OK
            reason = f"Pokryto ({len(matched_inv_rows)} pol.), plán za měsíc není znám"

        results.append(
            SPPCoverageRec(
                spp_row=spp.row,
                spp_source_row=spp.source_row,
                spp_sheet=spp.sheet,
                spp_name=spp.name,
                spp_unit=spp.unit,
                spp_qty_month=round(qty_month, 3) if qty_month is not None else None,
                spp_total_month=spp.total_month,
                covered_inv_rows=matched_inv_rows,
                covered_inv_names=covered_names,
                total_inv_deviation=round(total_deviation, 3),
                delta=round(delta, 3) if delta is not None else None,
                deviation_percent=round(deviation_pct, 1) if deviation_pct is not None else None,
                status=status,
                reason=reason,
            )
        )

    return results
