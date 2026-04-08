"""Rule-based category classification for construction materials."""

from __future__ import annotations

import re

from models import MaterialCategory


# Category keyword rules (Czech/Slovak terms)
CATEGORY_RULES: list[tuple[MaterialCategory, list[str]]] = [
    (MaterialCategory.PIPE, [
        "trubka", "trub", "potrubí", "trubk", "rúrka", "rúr",
        "roura", "rour", "pipe",
    ]),
    (MaterialCategory.FITTING, [
        "tvarovka", "tvarov", "koleno", "kolen",
        "redukce", "redukc", "spojka", "spojk",
        "t-kus", "t kus", "tkus", "odbočka", "odboč",
        "přechod", "prechod", "nátrubek", "natrubek",
        "zaslepka", "zátka", "zatka", "záslepka",
        "vsuvka", "objímka", "objimka",
    ]),
    (MaterialCategory.INSULATION, [
        "izolace", "izolac", "tubolit", "mirelon",
        "tepelná izolace", "tepelna izolace",
        "obalení", "obaleni", "izol",
    ]),
    (MaterialCategory.CONSUMABLE, [
        "silikon", "silicon", "tmel", "tmely",
        "páska", "paska", "konopí", "konopi",
        "loctite", "teflonová", "teflonova", "teflon",
        "lepidlo", "čistič", "cistic", "odmašťovač",
        "hemp", "lněn", "lnen",
        # PPE / expendables / tooling
        "rukavice", "перчат", "glove", "pracovní oděv", "pracovni odev",
        "respirátor", "respirator", "maska", "ochranný", "ochranny",
        "nářadí", "naradi", "tool", "šroub", "sroub", "hmožd", "hmozd",
        "vrták", "vrtak", "bit ", "pilka", "brusn", "fix", "značkovač", "znackovac",
        "metr ", "pytel", "rukáv", "rukav",
    ]),
    (MaterialCategory.VALVE, [
        "ventil", "kohout", "uzávěr", "uzaver",
        "kulový", "kulovy", "kulov",
        "zpětná klapka", "zpetna klapka",
        "zpětný ventil", "zpetny ventil",
        "regulační", "regulacni",
        "termostatick", "termohlavice",
    ]),
]


def classify_category(name: str) -> MaterialCategory:
    """Classify a material/work item into a category based on its name.

    Args:
        name: Material or work item name (Czech/Slovak).

    Returns:
        MaterialCategory enum value.
    """
    if not name:
        return MaterialCategory.OTHER

    lower = name.lower()
    # Remove diacritics-insensitive check by just doing lowercase comparison
    for category, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in lower:
                return category

    return MaterialCategory.OTHER


def extract_material_type(name: str) -> str | None:
    """Extract material type (PPR, PE, Cu, etc.) from name.

    Args:
        name: Material name.

    Returns:
        Material type string or None.
    """
    if not name:
        return None

    upper = name.upper()
    patterns = [
        (r"\bPPR\b", "PPR"),
        (r"\bPE\b", "PE"),
        (r"\bPE-?X\b", "PEX"),
        (r"\bPVC\b", "PVC"),
        (r"\bCu\b", "Cu"),
        (r"\bOCEL\b", "OCEL"),
        (r"\bNEREZ\b", "NEREZ"),
        (r"\bALUPEX\b", "ALUPEX"),
        (r"\bPP\b", "PP"),
    ]
    for pattern, material in patterns:
        if re.search(pattern, upper):
            return material
    return None
