from __future__ import annotations

import re


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    text = value.strip().lower()
    text = text.replace("á", "a").replace("č", "c").replace("ď", "d")
    text = text.replace("é", "e").replace("ě", "e").replace("í", "i")
    text = text.replace("ň", "n").replace("ó", "o").replace("ř", "r")
    text = text.replace("š", "s").replace("ť", "t").replace("ú", "u")
    text = text.replace("ů", "u").replace("ý", "y").replace("ž", "z")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def classify_company(company_name: str | None, company_code: str | None = None) -> str:
    normalized = _normalize(company_name)
    code = (company_code or "").strip().upper()

    if code == "DM":
        return "domostav_tzb"

    if "domostav" not in normalized:
        return "other"

    if "stavebni" in normalized:
        return "domostav_stavebni"

    if "tzb" in normalized or "tzw" in normalized:
        return "domostav_tzb"

    return "domostav_other"


def is_domostav_tzb(company_name: str | None, company_code: str | None = None) -> bool:
    return classify_company(company_name, company_code) == "domostav_tzb"


def is_domostav_company(company_name: str | None, company_code: str | None = None) -> bool:
    return classify_company(company_name, company_code).startswith("domostav_")


def companies_compatible(
    row_company_name: str | None,
    employee_company_name: str | None,
    employee_company_code: str | None = None,
) -> bool:
    row_class = classify_company(row_company_name)
    employee_class = classify_company(employee_company_name, employee_company_code)

    if row_class.startswith("domostav_") and employee_class.startswith("domostav_"):
        return row_class == employee_class

    if row_class.startswith("domostav_") != employee_class.startswith("domostav_"):
        return False

    return True
