"""Unit tests for SPP active-month filtering."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from models import SPPItem
from parsers.spp_parser import filter_spp_by_month


def _item(row: int, pct: float | None, tm: float | None) -> SPPItem:
    return SPPItem(
        row=row,
        source_row=row,
        sheet="S",
        name="test",
        percent_month=pct,
        total_month=tm,
    )


def test_percent_positive_wins():
    items = [_item(1, 10.0, 0.0), _item(2, 0.0, 999.0)]
    out = filter_spp_by_month(items)
    assert len(out) == 1 and out[0].row == 1


def test_fallback_when_all_percent_zero_but_total_month():
    """CHIRANA (NOVECON) layout: % column mapped but all zeros → use month totals."""
    items = [_item(1, 0.0, 50.0), _item(2, 0.0, 0.0), _item(3, None, 0.0)]
    out = filter_spp_by_month(items)
    assert [i.row for i in out] == [1]


def test_no_percent_column_uses_total_month():
    items = [_item(1, None, 50.0), _item(2, None, 0.0)]
    out = filter_spp_by_month(items)
    assert len(out) == 1 and out[0].row == 1


if __name__ == "__main__":
    test_percent_positive_wins()
    test_fallback_when_all_percent_zero_but_total_month()
    test_no_percent_column_uses_total_month()
    print("ok")
