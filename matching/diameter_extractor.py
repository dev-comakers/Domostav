"""Extract diameter values from material/work item names."""

from __future__ import annotations

import re


# Patterns for diameter extraction, ordered by specificity
DIAMETER_PATTERNS = [
    # "d20", "D20", "d 20"
    re.compile(r"\bd\s*(\d{2,3})\b", re.IGNORECASE),
    # "D50x5,6", "D 50x5.6"
    re.compile(r"\bD\s*(\d{2,3})\s*[xX×]", re.IGNORECASE),
    # "DN20", "DN 20", "Dn20"
    re.compile(r"\bDN\s*(\d{2,3})\b", re.IGNORECASE),
    # "20x2.3", "20x3,2", "25x2.3" (pipe dimensions: diameter x wall thickness)
    re.compile(r"\b(\d{2,3})\s*[xX×]\s*\d"),
    # "průměr 20", "prumner 20", "Průměr 20"
    re.compile(r"pr[uůú]m[eě][rř]\s*(\d{2,3})\b", re.IGNORECASE),
    # "PPR 20", "PE 25" (material type followed by diameter)
    re.compile(r"\b(?:PPR|PE|PVC|Cu)\s+(\d{2,3})\b", re.IGNORECASE),
    # "trubka 20", "trub. 20"
    re.compile(r"\btrub\w*\.?\s+(\d{2,3})\b", re.IGNORECASE),
    # "20mm", "25 mm"
    re.compile(r"\b(\d{2,3})\s*mm\b", re.IGNORECASE),
    # Standalone common diameters preceded by space/hyphen: " 20", "-20"
    # Only match common pipe diameters to avoid false positives
    re.compile(r"[\s\-/](\d{2,3})(?:\s|$|[,;.\-/])"),
]

# Common pipe diameters (to validate extracted values)
COMMON_DIAMETERS = {12, 15, 16, 18, 20, 25, 26, 32, 40, 50, 63, 75, 90, 110, 125, 160}


def extract_diameter(name: str) -> int | None:
    """Extract the primary diameter from a material/work item name.

    Args:
        name: Material or work item name (Czech/Slovak).

    Returns:
        Diameter as integer, or None if not found.
    """
    if not name:
        return None

    for pattern in DIAMETER_PATTERNS:
        match = pattern.search(name)
        if match:
            try:
                diameter = int(match.group(1))
                # Validate it's a reasonable pipe diameter
                if 10 <= diameter <= 200:
                    return diameter
            except (ValueError, IndexError):
                continue

    return None


def extract_all_diameters(name: str) -> list[int]:
    """Extract all diameters from a name (useful for reductions like '20-25').

    Args:
        name: Material name.

    Returns:
        List of unique diameters found.
    """
    if not name:
        return []

    diameters = set()

    # Special pattern for reductions: "20-25", "20/25", "20-32"
    reduction_pattern = re.compile(r"\b(\d{2,3})\s*[-/]\s*(\d{2,3})\b")
    for match in reduction_pattern.finditer(name):
        d1, d2 = int(match.group(1)), int(match.group(2))
        if 10 <= d1 <= 200:
            diameters.add(d1)
        if 10 <= d2 <= 200:
            diameters.add(d2)

    # Also try standard extraction
    for pattern in DIAMETER_PATTERNS:
        for match in pattern.finditer(name):
            try:
                d = int(match.group(1))
                if 10 <= d <= 200:
                    diameters.add(d)
            except (ValueError, IndexError):
                continue

    return sorted(diameters)
