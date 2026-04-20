"""Compare expected write-offs vs actual inventory deviations and flag anomalies."""

from __future__ import annotations

from collections import Counter

from models import (
    InventoryItem,
    SPPItem,
    MatchResult,
    WriteoffRecommendation,
    AnomalyStatus,
    MaterialCategory,
)
from analysis.writeoff_calculator import calculate_expected_writeoff


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
) -> dict:
    """Generate a summary of the analysis results.

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
                "one_line_explanation": (
                    f"Материал: {a.inventory_name[:50]}. "
                    f"Ожидалось: {round(float(a.expected_writeoff or 0), 2) if a.expected_writeoff is not None else 'нет оценки'} "
                    f"{(inv_by_row.get(a.inventory_row).unit if inv_by_row.get(a.inventory_row) else '') or ''}. "
                    f"Факт: {round(float(a.actual_deviation or 0), 2)} "
                    f"{(inv_by_row.get(a.inventory_row).unit if inv_by_row.get(a.inventory_row) else '') or ''}. "
                    f"Отклонение: {round(float(a.deviation_percent or 0), 1) if a.deviation_percent is not None else 'нет %'}%. "
                    f"Статус: {a.status.value}."
                ),
            }
            for a in anomalies[:10]
        ],
        "excluded": {
            "consumables_count": excluded_consumables,
            "out_of_scope_count": excluded_out_of_scope,
        },
    }
