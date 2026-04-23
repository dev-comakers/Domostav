from __future__ import annotations

import re
from pathlib import Path

from .html_utils import (
    clean_display_name,
    clean_text,
    flatten_rows_html_parser,
    flatten_rows_regex,
    normalize_name,
    parse_money,
)
from .models import ParsedPayrollRow


PERIOD_RE = re.compile(r"\b\d{2}/\d{4}\b")


def read_report_content(path: str | Path) -> str:
    data = Path(path).read_bytes()
    for encoding in ("cp1250", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def detect_report_type(content: str, filename: str) -> str:
    low = content.lower()
    name = filename.lower()
    if "přehled mezd" in low or "prehled mezd" in name:
        return "prehled_mezd"
    if "soupis sociálního poji" in low or "socialka" in name:
        return "socialka"
    if "soupis zdravotního poji" in low or "zdravotka" in name:
        return "zdravotka"
    raise ValueError(f"Unsupported report type for {filename}")


def _first_period(rows: list[list[str]]) -> str:
    for row in rows:
        for cell in row:
            match = PERIOD_RE.search(cell)
            if match:
                return match.group(0)
    return ""


def _extract_company_from_row(row: list[str]) -> str | None:
    values = [value for value in row if value]
    if not values:
        return None
    if any(value.startswith("Firma") for value in values):
        idx = next(i for i, value in enumerate(values) if value.startswith("Firma"))
        for value in values[idx + 1 :]:
            if not value.startswith("Firma"):
                return value
    if any(value.startswith("I") and ":" in value for value in values):
        first_value = values[0]
        first_lower = first_value.lower()
        if not first_value.startswith("I") and not first_lower.startswith(("přehled", "prehled", "soupis", "poji", "strana")):
            return first_value
    return None


def _is_prehled_data_row(row: list[str]) -> bool:
    return len(row) > 40 and bool(PERIOD_RE.fullmatch(row[1] if len(row) > 1 else ""))


def _is_socialka_data_row(row: list[str]) -> bool:
    return (
        len(row) > 23
        and bool(row[1] and row[3])
        and "Soupis" not in row[3]
        and "Jm" not in row[3]
        and not row[3].startswith("Firma")
        and parse_money(row[18]) is not None
        and parse_money(row[22]) is not None
    )


def _is_zdravotka_data_row(row: list[str]) -> bool:
    return (
        len(row) > 23
        and bool(row[1] and row[4])
        and "Soupis" not in row[4]
        and "Jm" not in row[4]
        and not row[4].startswith("Firma")
        and parse_money(row[21]) is not None
        and parse_money(row[22]) is not None
    )


def _parse_prehled_rows(rows: list[list[str]], filename: str, parser_mode: str) -> list[ParsedPayrollRow]:
    current_company = ""
    period = _first_period(rows)
    result: list[ParsedPayrollRow] = []
    for row_index, row in enumerate(rows):
        company = _extract_company_from_row(row)
        if company:
            current_company = company
        if not _is_prehled_data_row(row):
            continue
        display = clean_display_name(row[4])
        if not display or display == "Jméno":
            continue
        result.append(
            ParsedPayrollRow(
                report_type="prehled_mezd",
                company_name=current_company,
                period=period,
                employee_name=display,
                normalized_name=normalize_name(display),
                person_code=clean_text(row[8]) or None,
                gross_wage=_money(row, 16),
                social_employee=_money(row, 19),
                health_employee=_money(row, 26),
                tax_amount=_money(row, 27),
                payout_amount=_money(row, 10),
                settlement_amount=_money(row, 40),
                srazky=_money(row, 33),
                zaloha=_money(row, 35),
                source_file=filename,
                source_row_index=row_index,
                parser_mode=parser_mode,
                raw_payload={"row": row},
            )
        )
    return result


def _parse_socialka_rows(rows: list[list[str]], filename: str, parser_mode: str) -> list[ParsedPayrollRow]:
    current_company = ""
    period = _first_period(rows)
    result: list[ParsedPayrollRow] = []
    for row_index, row in enumerate(rows):
        company = _extract_company_from_row(row)
        if company:
            current_company = company
        if not _is_socialka_data_row(row):
            continue
        display = clean_display_name(row[3])
        if not display or display == "Jméno":
            continue
        result.append(
            ParsedPayrollRow(
                report_type="socialka",
                company_name=current_company,
                period=period,
                employee_name=display,
                normalized_name=normalize_name(display),
                person_code=clean_text(row[1]) or None,
                gross_wage=_money(row, 11),
                social_employee=_money(row, 18),
                social_employer=_money(row, 22),
                source_file=filename,
                source_row_index=row_index,
                parser_mode=parser_mode,
                raw_payload={"row": row},
            )
        )
    return result


def _parse_zdravotka_rows(rows: list[list[str]], filename: str, parser_mode: str) -> list[ParsedPayrollRow]:
    current_company = ""
    current_insurance = ""
    period = _first_period(rows)
    result: list[ParsedPayrollRow] = []
    for row_index, row in enumerate(rows):
        company = _extract_company_from_row(row)
        if company:
            current_company = company
        values = [value for value in row if value]
        if any(value.startswith("Poji") for value in values):
            idx = next(i for i, value in enumerate(values) if value.startswith("Poji"))
            for value in values[idx + 1 :]:
                if not value.startswith("Poji"):
                    current_insurance = value
                    break
        if not _is_zdravotka_data_row(row):
            continue
        display = clean_display_name(row[4])
        if not display or display == "Jméno":
            continue
        result.append(
            ParsedPayrollRow(
                report_type="zdravotka",
                company_name=current_company,
                period=period,
                employee_name=display,
                normalized_name=normalize_name(display),
                person_code=clean_text(row[1]) or None,
                gross_wage=_money(row, 14),
                health_employee=_money(row, 21),
                health_employer=_money(row, 22),
                health_insurance_name=current_insurance or None,
                source_file=filename,
                source_row_index=row_index,
                parser_mode=parser_mode,
                raw_payload={"row": row},
            )
        )
    return result


def _money(row: list[str], idx: int) -> float | None:
    if idx >= len(row):
        return None
    return parse_money(row[idx])


def parse_report_file(path: str | Path) -> tuple[str, str, str, list[ParsedPayrollRow]]:
    content = read_report_content(path)
    filename = Path(path).name
    report_type = detect_report_type(content, filename)

    rows = flatten_rows_regex(content)
    parser_mode = "regex"
    parsed = _parse_rows(report_type, rows, filename, parser_mode)
    if parsed:
        period = parsed[0].period
        company = parsed[0].company_name
        return report_type, company, period, parsed

    rows = flatten_rows_html_parser(content)
    parser_mode = "html_parser"
    parsed = _parse_rows(report_type, rows, filename, parser_mode)
    if parsed:
        period = parsed[0].period
        company = parsed[0].company_name
        return report_type, company, period, parsed

    raise ValueError(f"Could not parse {filename}")


def _parse_rows(
    report_type: str,
    rows: list[list[str]],
    filename: str,
    parser_mode: str,
) -> list[ParsedPayrollRow]:
    if report_type == "prehled_mezd":
        return _parse_prehled_rows(rows, filename, parser_mode)
    if report_type == "socialka":
        return _parse_socialka_rows(rows, filename, parser_mode)
    if report_type == "zdravotka":
        return _parse_zdravotka_rows(rows, filename, parser_mode)
    return []
