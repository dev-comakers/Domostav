"""Parser for SPP (Souhrn Provedených Prací) Excel files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import openpyxl

from models import SPPItem, ColumnMapping


def col_letter_to_index(letter: str) -> int:
    """Convert column letter to 1-based index."""
    result = 0
    for char in letter.upper():
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result


def to_float(val: Any) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        cleaned = str(val).replace(",", ".").replace(" ", "").replace("\xa0", "").strip()
        if cleaned in ("", "-", "—"):
            return None
        # Handle percentage like "15%" or "0.15"
        if cleaned.endswith("%"):
            return float(cleaned[:-1])
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_spp(
    filepath: str | Path,
    sheets: list[dict] | None = None,
    mapping: ColumnMapping | None = None,
    prefer_adaptive: bool = False,
) -> list[SPPItem]:
    """Parse an SPP Excel file into structured SPPItem objects.

    Args:
        filepath: Path to the Excel file (.xlsm or .xlsx).
        sheets: List of sheet configs [{"name": "...", "category_hint": "..."}].
                 If None, uses Chirana defaults.
        mapping: Column mapping. If None, uses Chirana defaults.

    Returns:
        List of SPPItem objects from all specified sheets.
    """
    if mapping is None:
        mapping = ColumnMapping(
            name="I",
            unit="K",
            quantity="L",
            price="M",       # price_per_unit
            total="N",
            percent_month="R",
            total_month="S",
            header_row=5,
            data_start_row=6,
        )

    if sheets is None:
        sheets = _auto_select_spp_sheets(filepath)

    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    all_items: list[SPPItem] = []
    month_hint = _extract_month_hint(str(filepath))

    for sheet_cfg in sheets:
        sheet_name = sheet_cfg["name"]
        if sheet_name not in wb.sheetnames:
            # Try partial match
            matched = [s for s in wb.sheetnames if sheet_name.lower() in s.lower()]
            if not matched:
                continue
            sheet_name = matched[0]

        ws = wb[sheet_name]
        adaptive = _detect_spp_mapping_from_headers(ws)
        if adaptive and (prefer_adaptive or not _mapping_has_enough_rows(ws, mapping)):
            active_mapping = adaptive
        else:
            active_mapping = mapping
        month_total_col = _detect_month_total_column(ws, active_mapping.header_row, month_hint)
        qty_col = active_mapping.quantity

        def get_cell(row_data: tuple, col_letter: str | None) -> Any:
            if not col_letter:
                return None
            idx = col_letter_to_index(col_letter) - 1
            if idx < len(row_data):
                cell = row_data[idx]
                try:
                    return cell.value if cell else None
                except AttributeError:
                    return None
            return None

        row_num = active_mapping.data_start_row - 1
        for row in ws.iter_rows(min_row=active_mapping.data_start_row):
            row_num += 1
            # Try to get actual row number from cell
            try:
                if row[0] and hasattr(row[0], 'row') and row[0].row:
                    row_num = row[0].row
            except (AttributeError, TypeError):
                pass

            name_val = get_cell(row, active_mapping.name)
            if not name_val or not str(name_val).strip():
                continue

            name_str = str(name_val).strip()

            # Skip summary/total rows
            lower = name_str.lower()
            if any(kw in lower for kw in ["celkem", "součet", "total", "mezisoučet"]):
                continue

            quantity = to_float(get_cell(row, active_mapping.quantity))
            price_per = to_float(get_cell(row, active_mapping.price))
            total = to_float(get_cell(row, active_mapping.total))
            pct = to_float(get_cell(row, active_mapping.percent_month))
            total_m = to_float(get_cell(row, active_mapping.total_month))

            # Fallback for SPP formats that use month columns (e.g. "Unor 2026")
            # instead of percent_month/total_month fields.
            if total_m is None and month_total_col:
                total_m = to_float(get_cell(row, month_total_col))
            if quantity is None and month_total_col and month_total_col != qty_col:
                # Better than empty quantity when only monthly columns are available.
                quantity = to_float(get_cell(row, month_total_col))

            item = SPPItem(
                row=row_num,
                sheet=sheet_cfg.get("category_hint", sheet_name),
                name=name_str,
                unit=str(get_cell(row, active_mapping.unit) or "").strip() or None,
                quantity=quantity,
                price_per_unit=price_per,
                total=total,
                percent_month=pct,
                total_month=total_m,
            )
            all_items.append(item)

    wb.close()
    # If configured sheet names do not exist in an unknown format, fallback to auto sheet detection.
    if not all_items and sheets is not None:
        return parse_spp(filepath=filepath, sheets=None, mapping=mapping, prefer_adaptive=True)
    return all_items


def filter_spp_by_month(items: list[SPPItem]) -> list[SPPItem]:
    """Keep only SPP items that have actual work for the period.

    Business rule: active month rows are only those with percent_month > 0.
    """
    # Preserve legacy Chirana behavior when percent_month is available:
    # active row <=> percent_month > 0.
    has_percent_data = any(item.percent_month is not None for item in items)
    result: list[SPPItem] = []
    for item in items:
        pct = item.percent_month or 0
        total_month = item.total_month or 0
        if has_percent_data:
            if pct > 0:
                result.append(item)
        else:
            if total_month > 0:
                result.append(item)
    return result


def get_spp_preview(
    filepath: str | Path,
    sheet_name: str | None = None,
    num_rows: int = 10,
) -> list[list[Any]]:
    """Get first N rows from an SPP sheet for preview."""
    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(max_row=num_rows)):
        rows.append([cell.value for cell in row])
    wb.close()
    return rows


def _extract_month_hint(path_value: str) -> str:
    low = path_value.lower()
    month_aliases = {
        "leden": "leden",
        "led": "leden",
        "january": "leden",
        "unor": "unor",
        "únor": "unor",
        "feb": "unor",
        "february": "unor",
        "brezen": "brezen",
        "březen": "brezen",
        "mar": "brezen",
        "march": "brezen",
        "duben": "duben",
        "apr": "duben",
        "kveten": "kveten",
        "květen": "kveten",
        "may": "kveten",
        "cerven": "cerven",
        "červen": "cerven",
        "jun": "cerven",
        "cervenec": "cervenec",
        "červenec": "cervenec",
        "jul": "cervenec",
        "srpen": "srpen",
        "aug": "srpen",
        "zari": "zari",
        "září": "zari",
        "sep": "zari",
        "rijen": "rijen",
        "říjen": "rijen",
        "oct": "rijen",
        "listopad": "listopad",
        "nov": "listopad",
        "prosinec": "prosinec",
        "dec": "prosinec",
    }
    for alias, normalized in month_aliases.items():
        if alias in low:
            return normalized
    return ""


def _detect_month_total_column(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    header_row: int,
    month_hint: str,
) -> str | None:
    month_row = max(1, header_row - 1)
    max_scan_col = min(260, ws.max_column)
    candidates: list[tuple[int, int]] = []
    # score, column index (col points to "Celkem" inside month block)
    for col in range(1, max_scan_col + 1):
        month_cell = ws.cell(month_row, col).value
        sub_cell = ws.cell(header_row, col).value
        sub_next = ws.cell(header_row, col + 1).value if col + 1 <= ws.max_column else None
        month_text = (str(month_cell or "")).strip().lower()
        sub_text = (str(sub_cell or "")).strip().lower()
        sub_next_text = (str(sub_next or "")).strip().lower()
        score = 0
        if month_text:
            if month_hint and month_hint in month_text:
                score += 100
            if any(k in month_text for k in ["2025", "2026", "2027", "leden", "unor", "únor", "brezen", "březen", "duben", "kveten", "květen", "cerven", "červen", "cervenec", "červenec", "srpen", "zari", "září", "rijen", "říjen", "listopad", "prosinec"]):
                score += 25
        # Pattern: month label at col N, then row header has "Montáž" and "Celkem" at N+1.
        if "celkem" in sub_text:
            score += 15
        if "celkem" in sub_next_text:
            score += 30
        if "mont" in sub_text and "celkem" in sub_next_text:
            score += 20
        if score > 0:
            target_col = col + 1 if "celkem" in sub_next_text else col
            if target_col <= ws.max_column:
                candidates.append((score, target_col))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    best_col = candidates[0][1]
    return openpyxl.utils.get_column_letter(best_col)


def _auto_select_spp_sheets(filepath: str | Path) -> list[dict[str, str]]:
    wb = openpyxl.load_workbook(str(filepath), data_only=True, read_only=True)
    result: list[dict[str, str]] = []
    try:
        for name in wb.sheetnames:
            ws = wb[name]
            hit = False
            for row_idx in range(1, 16):
                row_values = [ws.cell(row_idx, c).value for c in range(1, min(40, ws.max_column) + 1)]
                row_text = " | ".join(str(v) for v in row_values if v not in (None, "")).lower()
                if "název položky" in row_text or "nazev polozky" in row_text:
                    hit = True
                    break
            if hit:
                normalized = _normalize_sheet_hint(name)
                result.append({"name": name, "category_hint": normalized})
    finally:
        wb.close()
    if result:
        return result
    # Safe fallback to historical defaults if no headers were recognized.
    return [
        {"name": "Fakturace SoD - ZTI", "category_hint": "ZTI"},
        {"name": "Fakturace SoD - ÚT", "category_hint": "UT"},
    ]


def _normalize_sheet_hint(sheet_name: str) -> str:
    low = sheet_name.lower()
    if "zti" in low:
        return "ZTI"
    if "ut" in low or "út" in low or "vytáp" in low:
        return "UT"
    if "kanal" in low:
        return "KAN"
    if "vod" in low:
        return "VOD"
    if "vzt" in low:
        return "VZT"
    m = re.sub(r"[^A-Za-z0-9]+", " ", sheet_name).strip()
    return m[:20] if m else sheet_name[:20]


def _detect_spp_mapping_from_headers(
    ws: openpyxl.worksheet.worksheet.Worksheet,
) -> ColumnMapping | None:
    max_col = min(ws.max_column, 120)
    header_row = None
    name_col = None
    for row_idx in range(1, min(ws.max_row, 30) + 1):
        normalized = []
        for c in range(1, max_col + 1):
            val = ws.cell(row_idx, c).value
            normalized.append((c, str(val).strip().lower() if val not in (None, "") else ""))
        for c, text in normalized:
            if "název položky" in text or "nazev polozky" in text:
                header_row = row_idx
                name_col = c
                break
        if header_row:
            break
    if not header_row or not name_col:
        return None

    def _find_col(keywords: tuple[str, ...], default: int | None = None) -> int | None:
        for c in range(1, max_col + 1):
            text = str(ws.cell(header_row, c).value or "").strip().lower()
            if any(k in text for k in keywords):
                return c
        return default

    unit_col = _find_col(("mj", "jednotk", "unit"), default=name_col + 1)
    qty_col = _find_col(("množství", "mnozstvi", "qty"), default=name_col + 2)
    price_col = _find_col(("j.cena", "jedn.cena", "dodávka", "dodavka", "price"), default=name_col + 3)
    total_col = _find_col(("cena s dph", "cena celkem", "celk.", "total"), default=name_col + 4)
    return ColumnMapping(
        name=openpyxl.utils.get_column_letter(name_col),
        unit=openpyxl.utils.get_column_letter(unit_col) if unit_col else None,
        quantity=openpyxl.utils.get_column_letter(qty_col) if qty_col else None,
        price=openpyxl.utils.get_column_letter(price_col) if price_col else None,
        total=openpyxl.utils.get_column_letter(total_col) if total_col else None,
        percent_month=None,
        total_month=None,
        header_row=header_row,
        data_start_row=header_row + 1,
    )


def _mapping_has_enough_rows(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    mapping: ColumnMapping,
    min_rows: int = 5,
) -> bool:
    if not mapping.name:
        return False
    idx = col_letter_to_index(mapping.name) - 1
    non_empty = 0
    for row in ws.iter_rows(min_row=max(1, mapping.data_start_row), max_row=min(ws.max_row, mapping.data_start_row + 60)):
        if idx < len(row):
            val = row[idx].value
            if val not in (None, "") and str(val).strip():
                non_empty += 1
                if non_empty >= min_rows:
                    return True
    return False
