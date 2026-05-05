from __future__ import annotations

import html
import re
import unicodedata
from html.parser import HTMLParser


_POHODA_NAME_PREFIX = re.compile(r"^nen[i\u00ed]\s+", re.IGNORECASE)


def clean_text(value: str) -> str:
    """Whitespace/nbsp cleanup, preserves case and diacritics.

    Suitable for generic fields like person codes, company names, etc.
    """

    if not value:
        return ""
    value = value.replace("\xa0", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return value


def clean_display_name(value: str) -> str:
    """Cleans a human name coming from POHODA HTML reports for display.

    Strips the "ne\u00ed " / "neni " prefix that POHODA occasionally prepends to
    employees who have a "ne\u00ed v evidenci" / "ne\u00ed poji\u0161t\u011bn" flag.
    """

    cleaned = clean_text(value)
    if not cleaned:
        return ""
    stripped = _POHODA_NAME_PREFIX.sub("", cleaned, count=1)
    return stripped.strip()


def normalize_name(value: str) -> str:
    """Matching key for employee names.

    Case-insensitive, diacritic-insensitive, whitespace-collapsed and without
    the "ne\u00ed " POHODA prefix. Used to match parsed rows against the
    employee database and as a UNIQUE key on ``payroll_employees``.
    """

    display = clean_display_name(value)
    if not display:
        return ""
    lowered = display.lower()
    decomposed = unicodedata.normalize("NFKD", lowered)
    stripped = "".join(
        ch
        for ch in decomposed
        if not unicodedata.combining(ch) and unicodedata.category(ch) not in {"Cf", "Cc"}
    )
    stripped = re.sub(r"[^0-9a-zA-Z\s'-]+", " ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped


def normalize_name_variants(value: str) -> set[str]:
    """Return conservative matching keys for Czech/Ukrainian employee names.

    POHODA exports and the employee seed file are not always consistent about
    `Surname Name` vs. `Name Surname`. We keep the variants limited to exact and
    reversed token order so similarly named people are not over-matched.
    """

    normalized = normalize_name(value)
    if not normalized:
        return set()
    variants = {normalized}
    parts = normalized.split()
    if len(parts) >= 2:
        variants.add(" ".join(reversed(parts)))
    return variants


def normalize_name_token_key(value: str) -> str:
    normalized = normalize_name(value)
    if not normalized:
        return ""
    return " ".join(sorted(normalized.split()))


def parse_money(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.replace("\xa0", " ").strip()
    cleaned = cleaned.replace(" ", "").replace(",", ".")
    cleaned = cleaned.replace("+", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_html_text(value: str) -> str:
    value = re.sub(r"<br[^>]*>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def flatten_rows_regex(content: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", content, flags=re.S | re.I):
        row: list[str] = []
        for attrs, inner in re.findall(r"<td([^>]*)>(.*?)</td>", tr, flags=re.S | re.I):
            span_match = re.search(r'colspan="(\d+)"', attrs)
            span = int(span_match.group(1)) if span_match else 1
            text = clean_html_text(inner)
            row.extend([text] * span)
        rows.append(row)
    return rows


class _TableHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._in_tr = False
        self._in_td = False
        self._current_row: list[str] = []
        self._current_cell: list[str] = []
        self._current_span = 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "tr":
            self._in_tr = True
            self._current_row = []
        elif tag == "td" and self._in_tr:
            self._in_td = True
            self._current_cell = []
            self._current_span = int(attr_map.get("colspan", "1") or "1")
        elif tag == "br" and self._in_td:
            self._current_cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self._in_td:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._in_td:
            text = clean_text(html.unescape("".join(self._current_cell)))
            self._current_row.extend([text] * self._current_span)
            self._in_td = False
        elif tag == "tr" and self._in_tr:
            self.rows.append(self._current_row)
            self._in_tr = False


def flatten_rows_html_parser(content: str) -> list[list[str]]:
    parser = _TableHTMLParser()
    parser.feed(content)
    return parser.rows
