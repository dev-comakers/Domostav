"""Parser for actual write-off documents (e.g., NF-45)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import openpyxl
from pydantic import BaseModel


UNIT_VALUES = {"m", "ks", "kg", "bm", "l", "bal", "m2", "m3"}


class WriteoffItem(BaseModel):
    row: int
    number: str | None = None
    article: str | None = None
    name: str
    quantity: float | None = None
    unit: str | None = None
    warehouse: str | None = None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = str(value).replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
        if cleaned in ("", "-", "—"):
            return None
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _is_article(value: str) -> bool:
    text = value.strip()
    if len(text) < 4 or len(text) > 30:
        return False
    if " " in text:
        return False
    alnum = re.sub(r"[-_/]", "", text)
    return alnum.isalnum() and any(ch.isdigit() for ch in alnum) and any(ch.isalpha() for ch in alnum)


def parse_writeoff(filepath: str | Path, sheet_name: str | None = None) -> list[WriteoffItem]:
    """Parse write-off Excel document into normalized rows.

    The document can have merged/visual columns, so this parser uses robust heuristics:
    - row number in first columns
    - article-like code in early columns
    - long material name text
    - numeric quantity and unit on the right side
    """
    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active
    items: list[WriteoffItem] = []

    for row in ws.iter_rows(min_row=1):
        values = []
        for cell in row:
            try:
                values.append(cell.value)
            except AttributeError:
                values.append(None)

        text_cells: list[tuple[int, str]] = []
        numeric_cells: list[tuple[int, float]] = []
        for idx, raw in enumerate(values):
            if raw is None:
                continue
            if isinstance(raw, (int, float)):
                numeric_cells.append((idx, float(raw)))
            else:
                text = str(raw).strip()
                if text:
                    text_cells.append((idx, text))

        if not text_cells and not numeric_cells:
            continue

        # detect row number (first small integer in first columns)
        row_number: str | None = None
        for idx, num in numeric_cells:
            if idx <= 4 and float(num).is_integer() and 0 < num < 5000:
                row_number = str(int(num))
                break

        # detect article and name
        article: str | None = None
        name: str | None = None
        for idx, text in text_cells:
            if article is None and idx <= 12 and _is_article(text):
                article = text
                continue
            if name is None and len(text) >= 6 and not text.lower().startswith(("sklad", "komirka", "запаси", "№")):
                # likely material name
                name = text
            elif name is not None and len(text) > len(name):
                name = text

        # detect quantity + unit
        qty: float | None = None
        unit: str | None = None
        for idx, num in numeric_cells:
            if idx >= 10 and num >= 0:
                qty = num
                # look for unit near quantity column
                for t_idx, text in text_cells:
                    if abs(t_idx - idx) <= 2 and text.lower() in UNIT_VALUES:
                        unit = text.lower()
                        break
                if qty is not None:
                    break

        # detect warehouse (contains "sklad")
        warehouse: str | None = None
        for _, text in text_cells:
            low = text.lower()
            if "sklad" in low:
                warehouse = text
                break

        if name and (qty is not None or article or row_number):
            items.append(
                WriteoffItem(
                    row=getattr(row[0], "row", 0) or 0,
                    number=row_number,
                    article=article,
                    name=name,
                    quantity=qty,
                    unit=unit,
                    warehouse=warehouse,
                )
            )

    wb.close()
    return items
