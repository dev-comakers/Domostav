"""Parser for the nomenclature reference list (список номенклатури по групам)."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import openpyxl

from models import NomenclatureItem


UNIT_VALUES = {"m", "ks", "kg", "bm", "l", "bal", "m2", "m3"}


def parse_nomenclature(filepath: str | Path) -> list[NomenclatureItem]:
    """Parse the nomenclature reference file.

    The file has a specific structure:
    - Group header rows: "0001 TRUBKY PPR", "0002 TRUBKY PE" etc.
    - Under each group: alternating name/unit rows for items.

    Args:
        filepath: Path to the nomenclature Excel file.

    Returns:
        List of NomenclatureItem objects.
    """
    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    ws = wb.active

    items: list[NomenclatureItem] = []
    current_group: str | None = None
    current_item: NomenclatureItem | None = None

    for row in ws.iter_rows(min_row=1):
        value = None
        for cell in row:
            if cell.value is not None and str(cell.value).strip():
                value = str(cell.value).strip()
                break
        if not value:
            continue

        # Group line, e.g. 0001 TRUBKY PPR
        if re.match(r"^\d{4}\s+.+$", value):
            current_group = value
            current_item = None
            continue

        # Unit line follows nomenclature line in this workbook format
        if value.lower() in UNIT_VALUES:
            if current_item is not None:
                current_item.unit = value.lower()
            continue

        # Nomenclature line
        current_item = NomenclatureItem(
            group=current_group,
            name=value,
            unit=None,
            article=None,
        )
        items.append(current_item)

    wb.close()
    return items


def build_nomenclature_index(
    items: list[NomenclatureItem],
) -> dict[str, list[NomenclatureItem]]:
    """Build a lookup index from nomenclature items.

    Returns:
        Dict mapping article codes and normalized names to items.
    """
    index: dict[str, list[NomenclatureItem]] = {}
    for item in items:
        if item.article:
            key = item.article.upper().strip()
            index.setdefault(key, []).append(item)
        # Also index by normalized name
        name_key = normalize_name(item.name)
        index.setdefault(name_key, []).append(item)
    return index


def normalize_name(value: str) -> str:
    txt = value.strip().lower()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()
