"""Parser for write-off rules workbook (SPP_Chirana_...pravidla...xlsm)."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import openpyxl
from pydantic import BaseModel


UNIT_VALUES = {"m", "ks", "kg", "bm", "l", "bal", "m2", "m3"}


class RuleItem(BaseModel):
    category_code: str
    category_name: str
    nomenclature: str
    unit: str | None = None


def normalize_name(value: str) -> str:
    txt = value.strip().lower()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()


def parse_rules_catalog(filepath: str | Path) -> list[RuleItem]:
    """Parse hierarchical rules file:
    - category row
    - nomenclature row
    - unit row
    repeat until next category.
    """
    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    ws = wb.active

    current_code = ""
    current_category = ""
    last_nomenclature: str | None = None
    items: list[RuleItem] = []

    for row in ws.iter_rows(min_row=1):
        value = None
        for cell in row:
            try:
                if cell.value is not None and str(cell.value).strip():
                    value = str(cell.value).strip()
                    break
            except AttributeError:
                continue
        if not value:
            continue

        # Category line, e.g. "0001 TRUBKY PPR"
        cat_match = re.match(r"^(\d{4})\s+(.+)$", value)
        if cat_match:
            current_code = cat_match.group(1)
            current_category = cat_match.group(2).strip()
            last_nomenclature = None
            continue

        # Unit line
        if value.lower() in UNIT_VALUES:
            if last_nomenclature and current_code:
                items.append(
                    RuleItem(
                        category_code=current_code,
                        category_name=current_category,
                        nomenclature=last_nomenclature,
                        unit=value.lower(),
                    )
                )
            continue

        # Nomenclature line
        last_nomenclature = value
        if current_code:
            # Keep also unit-less item as fallback (some files may skip explicit unit row)
            items.append(
                RuleItem(
                    category_code=current_code,
                    category_name=current_category,
                    nomenclature=last_nomenclature,
                    unit=None,
                )
            )

    wb.close()
    return items


def build_runtime_rules(rule_items: list[RuleItem]) -> dict:
    """Build runtime structure used by analysis/matching."""
    by_category: dict[str, list[dict]] = {}
    for item in rule_items:
        key = item.category_name
        by_category.setdefault(key, []).append(
            {
                "name": item.nomenclature,
                "name_norm": normalize_name(item.nomenclature),
                "unit": item.unit,
                "category_code": item.category_code,
            }
        )
    return {"catalog": by_category, "count": len(rule_items)}
