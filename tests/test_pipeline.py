"""Integration test: run the pipeline on Chirana test data and validate results."""

from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from rich.console import Console
from rich.table import Table

from parsers.inventory_parser import parse_inventory
from parsers.spp_parser import parse_spp
from matching.material_matcher import match_all, enrich_items
from matching.diameter_extractor import extract_diameter
from matching.category_classifier import classify_category
from analysis.anomaly_detector import analyze_all, get_summary
from models import MaterialCategory, MatchMethod, AnomalyStatus

console = Console()

# Paths to test data (relative to this test file):
# tests/ -> domostav-ai/ -> Domostav Calude code/ -> Domostav x Fajnwork/
DATA_ROOT = Path(__file__).resolve().parents[3]
SPP_FILE = DATA_ROOT / "SPP Chirana 02-26.xlsm"
INVENTORY_FILE = (
    DATA_ROOT
    / "Інвентаризація запасів Chirana 02-26"
    / "Інвентаризація запасів за групами № NF-30 від 25.02.2026.xlsx"
)
WRITEOFF_FILE = (
    DATA_ROOT
    / "списання Chirana 02-26"
    / "Бланк товарного наповнення № NF-45 від 27.02.2026.xlsx"
)


def test_diameter_extraction():
    """Test diameter extraction from various name formats."""
    console.print("\n[bold]Test: Diameter Extraction[/bold]")
    test_cases = [
        ("Trubka PPR d20", 20),
        ("Trubka PPR d25 PN20", 25),
        ("Koleno PPR d32", 32),
        ("Redukce 20-25", 20),  # first diameter
        ("trubka DN50", 50),
        ("20x2.3 PPR", 20),
        ("Izolace Tubolit průměr 25", 25),
        ("Silikon sanitární", None),
        ("Ventil kulový d20", 20),
    ]
    passed = 0
    for name, expected in test_cases:
        result = extract_diameter(name)
        ok = result == expected
        passed += ok
        status = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {status} '{name}' → d{result} (expected d{expected})")

    console.print(f"  Passed: {passed}/{len(test_cases)}")
    return passed == len(test_cases)


def test_category_classification():
    """Test category classification."""
    console.print("\n[bold]Test: Category Classification[/bold]")
    test_cases = [
        ("Trubka PPR d20", MaterialCategory.PIPE),
        ("Koleno PPR d20 90°", MaterialCategory.FITTING),
        ("Redukce PPR 25-20", MaterialCategory.FITTING),
        ("Izolace Tubolit d25", MaterialCategory.INSULATION),
        ("Silikon sanitární", MaterialCategory.CONSUMABLE),
        ("Ventil kulový d20", MaterialCategory.VALVE),
        ("T-kus PPR d25", MaterialCategory.FITTING),
    ]
    passed = 0
    for name, expected in test_cases:
        result = classify_category(name)
        ok = result == expected
        passed += ok
        status = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {status} '{name}' → {result.value} (expected {expected.value})")

    console.print(f"  Passed: {passed}/{len(test_cases)}")
    return passed == len(test_cases)


def test_parsing():
    """Test parsing of real Chirana files."""
    console.print("\n[bold]Test: File Parsing[/bold]")

    ok = True

    # Parse SPP
    if SPP_FILE.exists():
        spp_items = parse_spp(str(SPP_FILE))
        console.print(f"  SPP items parsed: {len(spp_items)}")
        if len(spp_items) < 100:
            console.print(f"  [yellow]Warning: Expected ~540 items, got {len(spp_items)}[/yellow]")
            ok = False
        else:
            console.print(f"  [green]✓[/green] SPP: {len(spp_items)} items")

        # Show sample
        for item in spp_items[:3]:
            console.print(f"    Row {item.row} [{item.sheet}]: {item.name[:60]}")
    else:
        console.print(f"  [red]✗ SPP file not found: {SPP_FILE}[/red]")
        ok = False

    # Parse inventory
    if INVENTORY_FILE.exists():
        inv_items = parse_inventory(str(INVENTORY_FILE))
        console.print(f"  Inventory items parsed: {len(inv_items)}")
        if len(inv_items) < 100:
            console.print(f"  [yellow]Warning: Expected ~286 items, got {len(inv_items)}[/yellow]")
            ok = False
        else:
            console.print(f"  [green]✓[/green] Inventory: {len(inv_items)} items")

        for item in inv_items[:3]:
            console.print(
                f"    Row {item.row}: [{item.article or '—'}] {item.name[:50]} "
                f"dev={item.deviation}"
            )
    else:
        console.print(f"  [red]✗ Inventory file not found: {INVENTORY_FILE}[/red]")
        ok = False

    return ok


def test_matching_no_ai():
    """Test matching layers 1+2 (no AI) on real data."""
    console.print("\n[bold]Test: Material Matching (no AI)[/bold]")

    if not SPP_FILE.exists() or not INVENTORY_FILE.exists():
        console.print("  [red]✗ Test data files not found[/red]")
        return False

    spp_items = parse_spp(str(SPP_FILE))
    inv_items = parse_inventory(str(INVENTORY_FILE))

    matches = match_all(inv_items, spp_items, client=None)

    from collections import Counter
    methods = Counter(m.match_method.value for m in matches.values())

    table = Table(title="Matching Results (Layers 1+2)")
    table.add_column("Method")
    table.add_column("Count", justify="right")
    table.add_column("%", justify="right")

    total = len(matches)
    for method, count in methods.most_common():
        table.add_row(method, str(count), f"{count/total*100:.1f}%")
    console.print(table)

    # We expect at least some regex matches
    matched = total - methods.get("UNMATCHED", 0)
    console.print(f"  Total matched: {matched}/{total} ({matched/total*100:.1f}%)")

    return matched > 0


def test_full_pipeline_no_ai():
    """Test the full pipeline without AI."""
    console.print("\n[bold]Test: Full Pipeline (no AI)[/bold]")

    if not SPP_FILE.exists() or not INVENTORY_FILE.exists():
        console.print("  [red]✗ Test data files not found[/red]")
        return False

    spp_items = parse_spp(str(SPP_FILE))
    inv_items = parse_inventory(str(INVENTORY_FILE))
    matches = match_all(inv_items, spp_items, client=None)
    recommendations = analyze_all(inv_items, spp_items, matches)
    summary = get_summary(recommendations)

    table = Table(title="Pipeline Results")
    table.add_column("Metric")
    table.add_column("Value", justify="right")

    table.add_row("Total items", str(summary["total_items"]))
    table.add_row("[green]OK[/green]", str(summary["ok"]))
    table.add_row("[yellow]WARNING[/yellow]", str(summary["warning"]))
    table.add_row("[red]RED FLAG[/red]", str(summary["red_flag"]))

    console.print(table)
    return True


def main():
    """Run all tests."""
    console.print("[bold blue]═══ Domostav AI Pipeline Tests ═══[/bold blue]\n")

    results = {
        "Diameter extraction": test_diameter_extraction(),
        "Category classification": test_category_classification(),
    }

    # File-dependent tests
    if SPP_FILE.exists() and INVENTORY_FILE.exists():
        results["File parsing"] = test_parsing()
        results["Matching (no AI)"] = test_matching_no_ai()
        results["Full pipeline (no AI)"] = test_full_pipeline_no_ai()
    else:
        console.print(
            "\n[yellow]Skipping file-dependent tests — test data not found.[/yellow]"
            f"\n  Expected SPP: {SPP_FILE}"
            f"\n  Expected Inv: {INVENTORY_FILE}"
        )

    # Summary
    console.print("\n[bold]═══ Test Summary ═══[/bold]")
    all_pass = True
    for name, passed in results.items():
        status = "[green]PASS[/green]" if passed else "[red]FAIL[/red]"
        console.print(f"  {status} {name}")
        if not passed:
            all_pass = False

    if all_pass:
        console.print("\n[bold green]All tests passed![/bold green]")
    else:
        console.print("\n[bold red]Some tests failed.[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
