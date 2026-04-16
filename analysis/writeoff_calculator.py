"""Calculate expected write-off amounts based on SPP work and rules."""

from __future__ import annotations

from models import (
    InventoryItem,
    SPPItem,
    MatchResult,
    MaterialCategory,
    WriteoffRecommendation,
    AnomalyStatus,
    MatchMethod,
)
from config.settings import (
    PIPE_WASTE_PERCENT,
    INSULATION_WASTE_PERCENT,
    FITTING_PIPE_COST_RATIO,
    TOLERANCE_OK,
    TOLERANCE_WARNING,
)
from matching.diameter_extractor import extract_all_diameters


def calculate_expected_writeoff(
    inv_item: InventoryItem,
    match: MatchResult,
    spp_items: dict[int, SPPItem],
    all_matches: dict[int, MatchResult],
    all_inventory: list[InventoryItem],
    rules: dict | None = None,
) -> WriteoffRecommendation:
    """Calculate expected write-off for a single inventory item.

    Args:
        inv_item: The inventory item to analyze.
        match: The match result linking it to SPP items.
        spp_items: Dict of row -> SPPItem for lookup.
        all_matches: All match results (needed for proportional calculations).
        all_inventory: All inventory items (needed for context).

    Returns:
        WriteoffRecommendation with expected values.
    """
    rules = rules or {}
    tolerances = rules.get("tolerance", {})
    tol_ok = _to_float_or_default(tolerances.get("ok"), TOLERANCE_OK)
    tol_warning = _to_float_or_default(tolerances.get("warning"), TOLERANCE_WARNING)
    pipe_waste = _to_float_or_default(rules.get("pipes", {}).get("waste_percent"), PIPE_WASTE_PERCENT)
    insulation_waste = _to_float_or_default(
        rules.get("insulation", {}).get("waste_percent"),
        INSULATION_WASTE_PERCENT,
    )
    fitting_ratio = _extract_fitting_ratio(
        rules.get("fittings", {}).get("ratio_to_pipes"),
        FITTING_PIPE_COST_RATIO,
    )

    # Get matched SPP items
    matched_spp = [spp_items[r] for r in match.matched_spp_rows if r in spp_items]

    # Consumables: write to zero
    if inv_item.category == MaterialCategory.CONSUMABLE:
        expected = abs(inv_item.deviation) if inv_item.deviation else 0.0
        return WriteoffRecommendation(
            inventory_row=inv_item.row,
            inventory_name=inv_item.name,
            article=inv_item.article,
            expected_writeoff=expected,
            actual_deviation=inv_item.deviation,
            spp_reference="Расходник — списать в ноль",
            reason="Consumable material — auto write-off to zero",
            status=AnomalyStatus.OK,
            match_method=match.match_method,
            deviation_percent=0.0,
        )

    if not matched_spp:
        return WriteoffRecommendation(
            inventory_row=inv_item.row,
            inventory_name=inv_item.name,
            article=inv_item.article,
            expected_writeoff=None,
            actual_deviation=inv_item.deviation,
            spp_reference="",
            reason=_humanize_match_reason(match.match_reason, match.match_method, has_match=False),
            status=AnomalyStatus.RED_FLAG,
            match_method=match.match_method,
        )

    # Calculate based on category
    expected = 0.0
    spp_refs = []

    if inv_item.category == MaterialCategory.PIPE:
        expected = _calc_pipe_writeoff(inv_item, matched_spp, pipe_waste)
        spp_refs = _format_spp_refs(matched_spp)

    elif inv_item.category == MaterialCategory.FITTING:
        expected = _calc_fitting_writeoff(inv_item, matched_spp, all_inventory, fitting_ratio)
        spp_refs = _format_spp_refs(matched_spp)

    elif inv_item.category == MaterialCategory.INSULATION:
        expected = _calc_insulation_writeoff(inv_item, matched_spp, insulation_waste)
        spp_refs = _format_spp_refs(matched_spp)

    elif inv_item.category == MaterialCategory.VALVE:
        expected = _calc_valve_writeoff(inv_item, matched_spp)
        spp_refs = _format_spp_refs(matched_spp)

    else:
        expected = _calc_generic_writeoff(inv_item, matched_spp)
        spp_refs = _format_spp_refs(matched_spp)

    # Determine status
    status = _determine_status(expected, inv_item.deviation, tol_ok, tol_warning)

    # Calculate deviation percentage
    deviation_pct = None
    if expected and expected != 0 and inv_item.deviation is not None:
        deviation_pct = abs(abs(inv_item.deviation) - expected) / expected * 100

    reason_text = _build_reason(inv_item, expected, matched_spp)
    ai_reason_text = _humanize_match_reason(match.match_reason, match.match_method, has_match=True)
    if match.match_method == MatchMethod.AI and ai_reason_text:
        reason_text = f"{ai_reason_text} | {reason_text}" if reason_text else ai_reason_text
    if match.match_method == MatchMethod.MANUAL and (match.match_reason or "").strip():
        # Preserve user-entered manual note so it appears in review and final Excel.
        reason_text = match.match_reason.strip()

    return WriteoffRecommendation(
        inventory_row=inv_item.row,
        inventory_name=inv_item.name,
        article=inv_item.article,
        expected_writeoff=round(expected, 2) if expected else None,
        actual_deviation=inv_item.deviation,
        spp_reference=" | ".join(spp_refs),
        reason=reason_text,
        status=status,
        match_method=match.match_method,
        deviation_percent=round(deviation_pct, 1) if deviation_pct is not None else None,
    )


def _calc_pipe_writeoff(
    inv_item: InventoryItem,
    matched_spp: list[SPPItem],
    waste_percent: float,
) -> float:
    """Pipes: quantity from SPP + waste percentage."""
    total_qty = 0.0
    for spp in matched_spp:
        if spp.quantity and spp.percent_month:
            # Use monthly portion
            total_qty += spp.quantity * (spp.percent_month / 100)
        elif spp.quantity:
            total_qty += spp.quantity

    # Add waste
    return total_qty * (1 + waste_percent / 100)


def _calc_fitting_writeoff(
    inv_item: InventoryItem,
    matched_spp: list[SPPItem],
    all_inventory: list[InventoryItem],
    fitting_ratio: float,
) -> float:
    """Fittings: 50/50 cost ratio with pipes of same diameter."""
    # Find total pipe cost for matching diameter
    pipe_cost = 0.0
    for spp in matched_spp:
        if spp.total_month:
            pipe_cost += spp.total_month
        elif spp.total:
            pipe_cost += spp.total

    # Fittings should be ~50% of pipe cost
    target_fitting_cost = pipe_cost * fitting_ratio

    if inv_item.price and inv_item.price > 0:
        return target_fitting_cost / inv_item.price

    # Fallback: rough estimate based on pipe quantity.
    # For reductions like 20-25, we treat both diameters evenly (50/50).
    diameters = extract_all_diameters(inv_item.name)
    reduction_factor = 0.5 if len(diameters) >= 2 else 1.0
    total_qty = sum(
        (s.quantity or 0) * (s.percent_month or 100) / 100
        for s in matched_spp
    )
    return total_qty * 0.3 * reduction_factor  # rough ratio


def _calc_insulation_writeoff(
    inv_item: InventoryItem,
    matched_spp: list[SPPItem],
    waste_percent: float,
) -> float:
    """Insulation: match to pipe length + waste."""
    total_qty = 0.0
    for spp in matched_spp:
        if spp.quantity and spp.percent_month:
            total_qty += spp.quantity * (spp.percent_month / 100)
        elif spp.quantity:
            total_qty += spp.quantity

    return total_qty * (1 + waste_percent / 100)


def _calc_valve_writeoff(
    inv_item: InventoryItem,
    matched_spp: list[SPPItem],
) -> float:
    """Valves: 1:1 from SPP count."""
    total = 0.0
    for spp in matched_spp:
        if spp.quantity and spp.percent_month:
            total += spp.quantity * (spp.percent_month / 100)
        elif spp.quantity:
            total += spp.quantity
    return total


def _calc_generic_writeoff(
    inv_item: InventoryItem,
    matched_spp: list[SPPItem],
) -> float:
    """Generic: SPP quantity + 10% waste."""
    total_qty = 0.0
    for spp in matched_spp:
        if spp.quantity and spp.percent_month:
            total_qty += spp.quantity * (spp.percent_month / 100)
        elif spp.quantity:
            total_qty += spp.quantity

    return total_qty * 1.10


def _determine_status(
    expected: float,
    actual_deviation: float | None,
    tol_ok: float,
    tol_warning: float,
) -> AnomalyStatus:
    """Determine anomaly status based on expected vs actual."""
    if actual_deviation is None:
        return AnomalyStatus.RED_FLAG

    if expected == 0:
        if abs(actual_deviation) < 1:
            return AnomalyStatus.OK
        return AnomalyStatus.RED_FLAG

    actual = abs(actual_deviation)
    deviation_ratio = abs(actual - expected) / expected

    if deviation_ratio <= tol_ok:
        return AnomalyStatus.OK
    elif deviation_ratio <= tol_warning:
        return AnomalyStatus.WARNING
    else:
        return AnomalyStatus.RED_FLAG


def _to_float_or_default(value: object, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_fitting_ratio(value: object, default: float) -> float:
    """Parse ratio that can come as float or '50/50 by cost' style string."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if "/" in text:
        parts = text.split("/", 1)
        try:
            left = float(parts[0].strip())
            right_part = "".join(ch for ch in parts[1] if ch.isdigit() or ch == ".")
            right = float(right_part) if right_part else 0.0
            denom = left + right
            if denom > 0:
                return left / denom
        except ValueError:
            pass

    # Last resort: try direct float conversion
    return _to_float_or_default(text, default)


def _build_reason(
    inv_item: InventoryItem,
    expected: float,
    matched_spp: list[SPPItem],
) -> str:
    """Build a human-readable reason string."""
    parts = []
    if inv_item.category != MaterialCategory.OTHER:
        parts.append(f"Category: {inv_item.category.value}")
    if inv_item.diameter:
        parts.append(f"d{inv_item.diameter}")
    if expected:
        parts.append(f"Expected: {expected:.1f}")
    if inv_item.deviation is not None:
        parts.append(f"Actual deviation: {inv_item.deviation:.1f}")
    if matched_spp:
        parts.append(f"Matched {len(matched_spp)} SPP item(s)")
    return " | ".join(parts)


def _format_spp_refs(matched_spp: list[SPPItem], limit: int = 3) -> list[str]:
    refs = [f"[{s.sheet}] Row {s.source_row}: {s.name}" for s in matched_spp[:limit]]
    if len(matched_spp) > limit:
        refs.append(f"+{len(matched_spp) - limit} dalsi radky SPP")
    return refs


def _humanize_match_reason(raw_reason: str, match_method: MatchMethod, *, has_match: bool) -> str:
    text = (raw_reason or "").strip()
    low = text.lower()

    if match_method == MatchMethod.MANUAL and text:
        return text

    if "429" in low or "quota" in low or "exceeded your current quota" in low:
        return (
            "AI matching se nepodaril: byl vycerpan limit OpenAI API (429). "
            "Zkontrolujte billing, plan a API klic."
        )
    if "no ai client configured" in low:
        return "AI matching nebyl spusten: chybi nakonfigurovany AI klient nebo API klic."
    if "ai did not return match for this row" in low:
        return "Bez shody: AI pro tento radek nevratila zadny pouzitelny vysledek."
    if "candidate score" in low or "below threshold" in low or "below hard floor" in low:
        return "Bez shody: AI si nebyla dost jista navrzenou vazbou."
    if "diameter mismatch" in low:
        return "Bez shody: nesouhlasi prumer materialu a SPP prace."
    if "material type mismatch" in low:
        return "Bez shody: nesouhlasi typ materialu."
    if "category mismatch" in low:
        return "Bez shody: nesouhlasi kategorie materialu a typu prace."
    if low.startswith("bez shody:"):
        return text
    if low.startswith("ai matching failed:"):
        detail = text.split(":", 1)[1].strip() if ":" in text else text
        return f"AI matching se nepodaril: {detail}"
    if has_match and match_method == MatchMethod.AI:
        if text:
            return f"AI vazba: {text}"
        return "AI vazba byla prijata podle nazvu, prumeru a typu materialu."
    if text:
        return text
    return "Bez shody: AI nenasla dostatecne jistou vazbu na SPP."
