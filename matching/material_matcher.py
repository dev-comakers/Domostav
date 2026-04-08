"""Three-layer material matching: article → regex → AI."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from typing import Any
from urllib import request

from rapidfuzz import fuzz

from config.settings import BATCH_SIZE
from models import (
    InventoryItem,
    SPPItem,
    MatchMethod,
    MatchResult,
    MaterialCategory,
)
from matching.diameter_extractor import extract_diameter, extract_all_diameters
from matching.category_classifier import classify_category, extract_material_type
from llm.client import ClaudeClient


_DEBUG_LOG_PATH = "/Users/dmytriivezerian/Desktop/Domostav x Fajnwork/.cursor/debug-f07731.log"


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": "f07731",
        "runId": "initial",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass
    try:
        req = request.Request(
            "http://127.0.0.1:7897/ingest/d0e90649-22bc-4799-98cf-38260af08d14",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Debug-Session-Id": "f07731",
            },
            method="POST",
        )
        request.urlopen(req, timeout=2).read()
    except Exception:
        pass


AI_MATCHING_PROMPT = """You are matching inventory items to performed construction works (SPP items).

For each inventory item, find ONE most likely SPP item it corresponds to.
Materials are consumed when work is performed — e.g., a pipe in inventory is used for pipe installation work in SPP.

## Inventory items to match:
{batch_json}

## Available SPP items:
{spp_json}

For each inventory item, respond with a JSON array of objects:
[
  {{
    "inventory_row": <row number>,
    "matched_spp_rows": [<single best SPP row number, max length 1>],
    "confidence": <0.0 to 1.0>,
    "match_reason": "<brief explanation>"
  }},
  ...
]

Consider:
- Diameter matching (d20 pipe → d20 installation work)
- Material type matching (PPR pipe → PPR installation)
- Category logic (fittings match to pipe work of same diameter)
- Czech construction terminology
"""


def enrich_items(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
) -> None:
    """Enrich inventory and SPP items with extracted features (in-place)."""
    for item in inventory:
        item.diameter = extract_diameter(item.name)
        item.category = classify_category(item.name)
        item.material_type = extract_material_type(item.name)

    for item in spp:
        item.diameter = extract_diameter(item.name)
        item.category = classify_category(item.name)
        item.material_type = extract_material_type(item.name)


def match_by_article(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
) -> dict[int, MatchResult]:
    """Layer 1: Match by exact article code."""
    results: dict[int, MatchResult] = {}

    # Build article index from SPP (if SPP items have articles embedded in names)
    # In practice, SPP items are work descriptions, not materials with articles.
    # This layer primarily catches cases where article codes appear in both sources.
    spp_name_index: dict[str, list[int]] = {}
    for item in spp:
        # Extract potential article-like codes from SPP names
        words = item.name.split()
        for w in words:
            clean = w.strip("(),.-")
            if len(clean) >= 5 and clean.isalnum() and not clean.isdigit():
                spp_name_index.setdefault(clean.upper(), []).append(item.row)

    for inv_item in inventory:
        if not inv_item.article:
            continue
        art = inv_item.article.upper().strip()
        if art in spp_name_index:
            results[inv_item.row] = MatchResult(
                inventory_row=inv_item.row,
                matched_spp_rows=[spp_name_index[art][0]],
                match_method=MatchMethod.ARTICLE,
                confidence=0.95,
                match_reason=f"Article code match: {art}",
            )

    return results


def match_by_regex(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
    already_matched: set[int],
) -> dict[int, MatchResult]:
    """Layer 2: Match by diameter + category + material type."""
    results: dict[int, MatchResult] = {}

    # Group SPP items by diameter
    spp_by_diameter: dict[int, list[SPPItem]] = {}
    for item in spp:
        if item.diameter:
            spp_by_diameter.setdefault(item.diameter, []).append(item)

    for inv_item in inventory:
        if inv_item.row in already_matched:
            continue

        diameters = extract_all_diameters(inv_item.name)
        if not diameters and not inv_item.diameter:
            continue

        # Find SPP items with matching diameter.
        # For reductions (e.g. 20-25), evaluate both diameters.
        if not diameters and inv_item.diameter:
            diameters = [inv_item.diameter]

        candidates: list[SPPItem] = []
        for d in diameters:
            candidates.extend(spp_by_diameter.get(d, []))
        # Deduplicate by row
        candidates = list({c.row: c for c in candidates}.values())
        if not candidates:
            continue

        # Score candidates by category and material type similarity
        best_rows = []
        best_reason_parts = []

        for spp_item in candidates:
            score = 0.5  # base score for diameter match

            # Category match bonus
            if inv_item.category == spp_item.category:
                score += 0.2
            elif (
                inv_item.category == MaterialCategory.FITTING
                and spp_item.category == MaterialCategory.PIPE
            ):
                score += 0.15  # fittings go with pipes

            # Material type match bonus
            if (
                inv_item.material_type
                and spp_item.material_type
                and inv_item.material_type == spp_item.material_type
            ):
                score += 0.15

            # Fuzzy name similarity bonus
            name_sim = fuzz.token_sort_ratio(inv_item.name, spp_item.name) / 100
            if name_sim > 0.4:
                score += name_sim * 0.1

            if score >= 0.5:
                best_rows.append((spp_item.row, score))

        if best_rows:
            best_rows.sort(key=lambda x: x[1], reverse=True)
            top_rows = [best_rows[0][0]]
            top_score = best_rows[0][1]
            results[inv_item.row] = MatchResult(
                inventory_row=inv_item.row,
                matched_spp_rows=top_rows,
                match_method=MatchMethod.REGEX,
                confidence=min(top_score, 0.9),
                match_reason=(
                    f"Diameter(s) {','.join(f'd{d}' for d in diameters)} "
                    f"+ category {inv_item.category.value}"
                ),
            )

    return results


def match_by_ai(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
    already_matched: set[int],
    client: ClaudeClient,
    system_prompt: str = "",
    batch_size: int = BATCH_SIZE,
) -> dict[int, MatchResult]:
    """Layer 3: Use Claude AI for remaining unmatched items."""
    unmatched = [i for i in inventory if i.row not in already_matched]
    if not unmatched:
        return {}

    # Prepare SPP summary for context
    spp_data = [
        {
            "row": s.row,
            "sheet": s.sheet,
            "name": s.name,
            "diameter": s.diameter,
            "category": s.category.value,
            "unit": s.unit,
            "quantity": s.quantity,
        }
        for s in spp
    ]
    spp_json = json.dumps(spp_data, ensure_ascii=False)

    results: dict[int, MatchResult] = {}

    def process_batch(batch_start: int, batch: list[InventoryItem]) -> tuple[int, list[InventoryItem], list[dict[str, Any]] | None, Exception | None, float, int, int]:
        batch_started_at = time.perf_counter()
        batch_data = [
            {
                "row": item.row,
                "name": item.name,
                "article": item.article,
                "diameter": item.diameter,
                "category": item.category.value,
                "unit": item.unit,
                "deviation": item.deviation,
            }
            for item in batch
        ]

        prompt = AI_MATCHING_PROMPT.format(
            batch_json=json.dumps(batch_data, ensure_ascii=False, indent=2),
            spp_json=spp_json,
        )
        try:
            batch_client = ClaudeClient(
                api_key=client.api_key,
                model=client.model,
                provider=client.provider,
            )
            ai_results = batch_client.ask_json(prompt, system_prompt)
            if not isinstance(ai_results, list):
                ai_results = [ai_results]
            return (
                batch_start,
                batch,
                ai_results,
                None,
                round((time.perf_counter() - batch_started_at) * 1000, 1),
                batch_client.total_input_tokens,
                batch_client.total_output_tokens,
            )
        except Exception as e:
            return (
                batch_start,
                batch,
                None,
                e,
                round((time.perf_counter() - batch_started_at) * 1000, 1),
                0,
                0,
            )

    batches = [
        (i, unmatched[i : i + batch_size])
        for i in range(0, len(unmatched), batch_size)
    ]
    max_workers = min(3, len(batches))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_batch, batch_start, batch)
            for batch_start, batch in batches
        ]
        for future in as_completed(futures):
            batch_start, batch, ai_results, error, elapsed_ms, input_tokens, output_tokens = future.result()
            client.total_calls += 1
            client.total_input_tokens += input_tokens
            client.total_output_tokens += output_tokens

            if error is None:
                # region agent log
                _debug_log(
                    "H1",
                    "matching/material_matcher.py:244",
                    "AI batch response parsed",
                    {
                        "batch_start": batch_start,
                        "batch_size": len(batch),
                        "returned_items": len(ai_results or []),
                        "returned_with_spp": sum(
                            1 for r in (ai_results or []) if isinstance(r, dict) and (r.get("matched_spp_rows") or [])
                        ),
                        "returned_empty_spp": sum(
                            1 for r in (ai_results or []) if isinstance(r, dict) and not (r.get("matched_spp_rows") or [])
                        ),
                        "elapsed_ms": elapsed_ms,
                    },
                )
                # endregion

                for r in ai_results or []:
                    row = r.get("inventory_row")
                    if row is None:
                        continue
                    raw_rows = r.get("matched_spp_rows", [])
                    if isinstance(raw_rows, (int, float, str)):
                        raw_rows = [raw_rows]
                    elif isinstance(raw_rows, dict):
                        raw_rows = list(raw_rows.values())
                    elif not isinstance(raw_rows, list):
                        raw_rows = []
                    normalized_rows: list[int] = []
                    for x in raw_rows:
                        try:
                            normalized_rows.append(int(x))
                        except (TypeError, ValueError):
                            continue
                    normalized_rows = normalized_rows[:1]
                    results[row] = MatchResult(
                        inventory_row=row,
                        matched_spp_rows=normalized_rows,
                        match_method=MatchMethod.AI,
                        confidence=float(r.get("confidence", 0.5)),
                        match_reason=r.get("match_reason", "AI match"),
                    )
                continue

            # region agent log
            _debug_log(
                "H1",
                "matching/material_matcher.py:265",
                "AI batch request failed",
                {
                    "batch_start": batch_start,
                    "batch_size": len(batch),
                    "error_type": type(error).__name__,
                    "error_text": str(error)[:500],
                    "elapsed_ms": elapsed_ms,
                },
            )
            # endregion
            for item in batch:
                results[item.row] = MatchResult(
                    inventory_row=item.row,
                    matched_spp_rows=[],
                    match_method=MatchMethod.AI,
                    confidence=0.0,
                    match_reason=f"AI matching failed: {error}",
                )

    return results


def match_all(
    inventory: list[InventoryItem],
    spp: list[SPPItem],
    client: ClaudeClient | None = None,
    system_prompt: str = "",
    force_ai: bool = False,
) -> dict[int, MatchResult]:
    """Run all three matching layers and return combined results.

    Args:
        inventory: Parsed inventory items.
        spp: Parsed SPP items.
        client: ClaudeClient for AI matching (optional, skips layer 3 if None).
        system_prompt: System prompt for AI context.

    Returns:
        Dict mapping inventory row numbers to MatchResult.
    """
    # Enrich with extracted features
    enrich_items(inventory, spp)

    # Strict AI mode: all rows go through AI matching.
    if force_ai and client:
        results = match_by_ai(inventory, spp, set(), client, system_prompt)
        for item in inventory:
            if item.row not in results:
                results[item.row] = MatchResult(
                    inventory_row=item.row,
                    matched_spp_rows=[],
                    match_method=MatchMethod.AI,
                    confidence=0.0,
                    match_reason="AI did not return match for this row",
                )
            elif results[item.row].match_method != MatchMethod.AI:
                results[item.row].match_method = MatchMethod.AI
        _normalize_single_spp_link(results)
        rows_with_spp = sum(1 for m in results.values() if m.matched_spp_rows)
        # region agent log
        _debug_log(
            "H3",
            "matching/material_matcher.py:326",
            "Strict AI mode result distribution",
            {
                "inventory_count": len(inventory),
                "result_count": len(results),
                "rows_with_spp": rows_with_spp,
                "rows_without_spp": sum(1 for m in results.values() if not m.matched_spp_rows),
                "force_ai": force_ai,
                "client_present": bool(client),
            },
        )
        # endregion
        return results

    results = {}
    if client:
        results = match_by_ai(inventory, spp, set(), client, system_prompt)

    for item in inventory:
        if item.row not in results:
            results[item.row] = MatchResult(
                inventory_row=item.row,
                match_method=MatchMethod.UNMATCHED if not client else MatchMethod.AI,
                match_reason="AI did not return match for this row" if client else "No AI client configured",
            )

    _normalize_single_spp_link(results)
    return results


def _normalize_single_spp_link(results: dict[int, MatchResult]) -> None:
    """Keep only one SPP row per inventory row for cleaner 1:1 output."""
    for match in results.values():
        if not match.matched_spp_rows:
            continue
        try:
            first_row = int(match.matched_spp_rows[0])
        except (TypeError, ValueError):
            match.matched_spp_rows = []
            continue
        match.matched_spp_rows = [first_row]
