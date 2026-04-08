"""Domostav AI Write-Off Analysis — CLI entry point."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

from config.settings import (
    CONFIG_DIR,
    OUTPUT_DIR,
    DEFAULT_INVENTORY_DATA_START,
)
from models import ColumnMapping, AnomalyStatus
from llm.client import ClaudeClient
from parsers.inventory_parser import parse_inventory
from parsers.spp_parser import parse_spp
from parsers.mapping_engine import auto_detect_mapping, display_mapping
from matching.material_matcher import match_all
from analysis.anomaly_detector import analyze_all, get_summary
from output.excel_generator import generate_output

console = Console()


def load_project_config(project: str) -> dict:
    """Load project-specific configuration from YAML."""
    config_path = CONFIG_DIR / "projects" / f"{project}.yaml"
    if not config_path.exists():
        console.print(f"[yellow]Warning: Project config {config_path} not found, using defaults.[/yellow]")
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_system_prompt() -> str:
    """Load the system prompt from file."""
    prompt_path = CONFIG_DIR / "system_prompt.txt"
    if not prompt_path.exists():
        return ""
    return prompt_path.read_text(encoding="utf-8")


def build_column_mapping(config: dict, file_type: str) -> ColumnMapping | None:
    """Build ColumnMapping from project config."""
    section = config.get(file_type)
    if not section or "columns" not in section:
        return None

    cols = section["columns"]
    return ColumnMapping(
        row_number=cols.get("number"),
        article=cols.get("article"),
        name=cols.get("name"),
        unit=cols.get("unit"),
        quantity=cols.get("quantity", cols.get("quantity_fact")),
        quantity_accounting=cols.get("quantity_accounting"),
        deviation=cols.get("deviation"),
        price=cols.get("price", cols.get("price_per_unit")),
        total=cols.get("total"),
        percent_month=cols.get("percent_month"),
        total_month=cols.get("total_month"),
        header_row=section.get("header_row", 1),
        data_start_row=section.get("data_start_row", 2),
    )


@click.command()
@click.option("--spp", required=True, help="Path to SPP Excel file (.xlsm/.xlsx)")
@click.option("--inventory", required=True, help="Path to inventory Excel file (.xlsx)")
@click.option("--project", default="chirana", help="Project config name (default: chirana)")
@click.option("--output", default=None, help="Output file path (default: auto-generated)")
@click.option(
    "--api-key",
    default=None,
    help="LLM API key (OPENAI_API_KEY preferred, ANTHROPIC_API_KEY fallback)",
)
@click.option("--no-ai", is_flag=True, help="Skip AI matching (layers 1+2 only)")
@click.option("--auto-confirm", is_flag=True, help="Skip mapping confirmation prompt")
@click.option(
    "--auto-map/--no-auto-map",
    default=True,
    help="Use AI to auto-detect mapping when project mapping is missing",
)
def main(
    spp: str,
    inventory: str,
    project: str,
    output: str | None,
    api_key: str | None,
    no_ai: bool,
    auto_confirm: bool,
    auto_map: bool,
) -> None:
    """Domostav AI Write-Off Analysis Pipeline.

    Analyzes performed construction works (SPP) vs inventory to detect
    anomalies in material write-offs.
    """
    console.print(Panel.fit(
        "[bold blue]Domostav AI Write-Off Analysis[/bold blue]\n"
        f"Project: {project} | Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        border_style="blue",
    ))

    # Validate input files
    spp_path = Path(spp)
    inv_path = Path(inventory)
    if not spp_path.exists():
        console.print(f"[red]Error: SPP file not found: {spp_path}[/red]")
        sys.exit(1)
    if not inv_path.exists():
        console.print(f"[red]Error: Inventory file not found: {inv_path}[/red]")
        sys.exit(1)

    # Load config
    config = load_project_config(project)
    system_prompt = load_system_prompt()

    # Project-specific prompt
    project_notes = config.get("notes", "")
    if project_notes:
        system_prompt += f"\n\n## Project-Specific Notes\n{project_notes}"

    # Initialize LLM client (if needed)
    client = None
    if not no_ai:
        try:
            client = ClaudeClient(api_key=api_key)
            console.print("[green]✓[/green] AI provider connected")
        except ValueError as e:
            console.print(f"[yellow]Warning: {e}. Running without AI matching.[/yellow]")
            no_ai = True

    # ─── STEP 1: Parse input files ─────────────────────────────
    console.print("\n[bold]Step 1/4: Loading and parsing files...[/bold]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        # Parse SPP
        task = progress.add_task("Parsing SPP...", total=1)
        spp_mapping = build_column_mapping(config, "spp")
        spp_sheets = None
        if config.get("spp", {}).get("sheets"):
            spp_sheets = config["spp"]["sheets"]
        if spp_mapping is None and auto_map and client:
            console.print("  [dim]No SPP mapping in project config; auto-detecting...[/dim]")
            spp_mapping = auto_detect_mapping(spp_path, client, file_type="spp")
        spp_items = parse_spp(spp_path, sheets=spp_sheets, mapping=spp_mapping)
        progress.update(task, completed=1)
        console.print(f"  SPP: {len(spp_items)} work items loaded")

        # Parse inventory
        task = progress.add_task("Parsing inventory...", total=1)
        inv_mapping = build_column_mapping(config, "inventory")
        if inv_mapping is None and auto_map and client:
            console.print("  [dim]No inventory mapping in project config; auto-detecting...[/dim]")
            inv_mapping = auto_detect_mapping(inv_path, client, file_type="inventory")
        inv_items = parse_inventory(inv_path, mapping=inv_mapping)
        progress.update(task, completed=1)
        console.print(f"  Inventory: {len(inv_items)} items loaded")

    # ─── STEP 2: Show mapping and confirm ───────────────────────
    console.print("\n[bold]Step 2/4: Column mapping[/bold]")

    if inv_mapping:
        display_mapping_table("Inventory", inv_mapping)
    if spp_mapping:
        display_mapping_table("SPP", spp_mapping)

    if not auto_confirm:
        confirm = console.input("\n[yellow]Proceed with this mapping? [Y/n]: [/yellow]")
        if confirm.lower() in ("n", "no"):
            console.print("[red]Aborted by user.[/red]")
            sys.exit(0)

    # ─── STEP 3: Matching and analysis ──────────────────────────
    console.print("\n[bold]Step 3/4: Matching and analysis...[/bold]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        # Material matching
        task = progress.add_task("Matching materials...", total=1)
        matches = match_all(
            inv_items,
            spp_items,
            client=client if not no_ai else None,
            system_prompt=system_prompt,
        )
        progress.update(task, completed=1)

        # Count match methods
        from collections import Counter
        method_counts = Counter(m.match_method.value for m in matches.values())
        for method, count in method_counts.most_common():
            console.print(f"  {method}: {count} items")

        # Analysis
        task = progress.add_task("Calculating write-offs...", total=1)
        recommendations = analyze_all(
            inv_items,
            spp_items,
            matches,
            rules=config.get("rules", {}),
        )
        progress.update(task, completed=1)

    # Show summary
    summary = get_summary(recommendations)
    _print_summary(summary)

    # ─── STEP 4: Generate output ────────────────────────────────
    console.print("\n[bold]Step 4/4: Generating output...[/bold]")

    if output is None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = str(OUTPUT_DIR / f"analysis_{project}_{timestamp}.xlsx")

    data_start = (
        config.get("inventory", {}).get("data_start_row", DEFAULT_INVENTORY_DATA_START)
    )
    output_path = generate_output(
        source_path=inv_path,
        output_path=output,
        recommendations=recommendations,
        data_start_row=data_start,
    )

    console.print(f"\n[green]✓ Output saved to:[/green] {output_path}")

    # API usage
    if client:
        console.print(f"[dim]{client.get_usage_summary()}[/dim]")

    console.print(Panel.fit("[bold green]Analysis complete![/bold green]", border_style="green"))


def display_mapping_table(label: str, mapping: ColumnMapping) -> None:
    """Display a column mapping as a rich table."""
    table = Table(title=f"{label} Column Mapping")
    table.add_column("Field", style="cyan")
    table.add_column("Column", style="green")

    fields = {
        "Name": mapping.name,
        "Article": mapping.article,
        "Unit": mapping.unit,
        "Quantity": mapping.quantity,
        "Qty Accounting": mapping.quantity_accounting,
        "Deviation": mapping.deviation,
        "Price": mapping.price,
        "Header Row": str(mapping.header_row),
        "Data Start": str(mapping.data_start_row),
    }
    for name, val in fields.items():
        if val:
            table.add_row(name, val)

    console.print(table)


def _print_summary(summary: dict) -> None:
    """Print analysis summary as a rich panel."""
    total = summary["total_items"]
    ok = summary["ok"]
    warning = summary["warning"]
    red = summary["red_flag"]

    table = Table(title="Analysis Summary")
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Percent", justify="right")

    table.add_row("[green]OK[/green]", str(ok), f"{ok/total*100:.1f}%" if total else "0%")
    table.add_row("[yellow]WARNING[/yellow]", str(warning), f"{warning/total*100:.1f}%" if total else "0%")
    table.add_row("[red]RED FLAG[/red]", str(red), f"{red/total*100:.1f}%" if total else "0%")
    table.add_row("[bold]Total[/bold]", str(total), "100%")

    console.print(table)

    if summary["top_anomalies"]:
        console.print("\n[bold red]Top anomalies:[/bold red]")
        for a in summary["top_anomalies"][:5]:
            console.print(
                f"  Row {a['row']}: {a['name'][:50]} — "
                f"deviation {a['deviation_percent']:.1f}%"
            )


if __name__ == "__main__":
    main()
