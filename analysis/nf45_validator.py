"""Validation against factual write-off document (NF-45)."""

from __future__ import annotations

from collections import defaultdict

from models import WriteoffRecommendation
from parsers.writeoff_parser import WriteoffItem


def validate_against_nf45(
    recommendations: list[WriteoffRecommendation],
    nf45_items: list[WriteoffItem],
) -> dict:
    """Compare expected write-off with factual NF-45 quantities.

    Matching priority:
    1) by article
    2) by normalized name
    """
    rec_by_article: dict[str, WriteoffRecommendation] = {}
    rec_by_name: dict[str, WriteoffRecommendation] = {}
    for rec in recommendations:
        if rec.article:
            rec_by_article[rec.article.strip().upper()] = rec
        rec_by_name[_norm(rec.inventory_name)] = rec

    total = 0
    matched = 0
    abs_pct_errors: list[float] = []
    by_status = defaultdict(int)
    details = []

    for item in nf45_items:
        total += 1
        rec = None
        if item.article:
            rec = rec_by_article.get(item.article.strip().upper())
        if rec is None:
            rec = rec_by_name.get(_norm(item.name))
        if rec is None or rec.expected_writeoff is None or item.quantity is None or rec.expected_writeoff == 0:
            continue

        matched += 1
        actual = float(item.quantity)
        expected = float(rec.expected_writeoff)
        pct_error = abs(actual - expected) / expected * 100
        abs_pct_errors.append(pct_error)
        by_status[rec.status.value] += 1
        details.append(
            {
                "article": item.article,
                "name": item.name,
                "actual_nf45": actual,
                "expected_ai": expected,
                "pct_error": round(pct_error, 2),
                "status": rec.status.value,
                "method": rec.match_method.value,
            }
        )

    mape = sum(abs_pct_errors) / len(abs_pct_errors) if abs_pct_errors else None
    within_15 = (
        sum(1 for x in abs_pct_errors if x <= 15) / len(abs_pct_errors) * 100
        if abs_pct_errors
        else None
    )

    return {
        "nf45_rows": total,
        "matched_rows": matched,
        "match_rate_percent": round((matched / total * 100), 1) if total else 0.0,
        "mape_percent": round(mape, 2) if mape is not None else None,
        "accuracy_within_15_percent": round(within_15, 1) if within_15 is not None else None,
        "status_distribution": dict(by_status),
        "sample": details[:30],
    }


def _norm(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).lower().replace("\xa0", " ").split())
