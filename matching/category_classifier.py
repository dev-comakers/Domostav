"""Rule-based category classification for construction materials."""

from __future__ import annotations

import re

from models import MaterialCategory


# Category keyword rules (Czech/Slovak terms)
CATEGORY_RULES: list[tuple[MaterialCategory, list[str]]] = [
    (MaterialCategory.INSULATION, [
        "izolace", "izolac", "tubolit", "mirelon",
        "tepelná izolace", "tepelna izolace",
        "obalení", "obaleni", "izotub", "kaučuk", "kaucuk", "armaflex",
        "trubková izolace", "trubkova izolace", "izolační", "izolacni",
    ]),
    (MaterialCategory.PIPE, [
        "trubka", "trub", "potrubí", "potrubi", "trubk", "rúrka", "rúr",
        "roura", "rour", "pipe", "vodovod", "studená voda", "studena voda",
        "teplá voda", "tepla voda", "cirkulace", "kanaliz", "odpadní", "odpadni",
        "kg potrub", "ht potrub", "master 3", "kgem", "htem",
    ]),
    (MaterialCategory.FITTING, [
        "tvarovka", "tvarov", "koleno", "kolen",
        "redukce", "redukc", "spojka", "spojk",
        "t-kus", "t kus", "tkus", "odbočka", "odboč",
        "přechod", "prechod", "nátrubek", "natrubek",
        "zaslepka", "zátka", "zatka", "záslepka",
        "vsuvka", "objímka", "objimka",
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
    if any(k in lower for k in ["trubková izolace", "trubkova izolace", "tubolit", "mirelon", "izotub", "armaflex"]):
        return MaterialCategory.INSULATION
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
        (r"\bPP-?RCT\b", "PPR"),
        (r"\bPPRCT\b", "PPR"),
        (r"\bPPR\b", "PPR"),
        (r"\bPE\b", "PE"),
        (r"\bPE-?X\b", "PEX"),
        (r"\bPVC\b", "PVC"),
        (r"\bHTEM\b", "HT"),
        (r"\bHT\b", "HT"),
        (r"\bKGEM\b", "KG"),
        (r"\bKG\b", "KG"),
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
