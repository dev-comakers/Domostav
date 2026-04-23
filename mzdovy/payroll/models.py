from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


def utc_now() -> str:
    return datetime.utcnow().isoformat()


class ParsedPayrollRow(BaseModel):
    report_type: str
    company_name: str
    period: str
    employee_name: str
    normalized_name: str
    person_code: str | None = None
    gross_wage: float | None = None
    social_employee: float | None = None
    social_employer: float | None = None
    health_employee: float | None = None
    health_employer: float | None = None
    tax_amount: float | None = None
    payout_amount: float | None = None
    settlement_amount: float | None = None
    srazky: float | None = None
    zaloha: float | None = None
    health_insurance_name: str | None = None
    source_file: str
    source_row_index: int
    parser_mode: str = "regex"
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class PreviewRow(BaseModel):
    id: int | None = None
    import_id: int
    period: str
    display_name: str
    normalized_name: str
    company_name: str
    employee_id: int | None = None
    project_name: str | None = None
    coordinator_name: str | None = None
    company_code: str | None = None
    gross_wage: float = 0.0
    social_employee: float = 0.0
    social_employer: float = 0.0
    health_employee: float = 0.0
    health_employer: float = 0.0
    tax_amount: float = 0.0
    odvody_platime: float = 0.0
    odvody_strhavame: float = 0.0
    mesicni_mzda: float = 0.0
    control_sum_parsed: float = 0.0
    control_sum_expected: float = 0.0
    match_status: str = "missing"
    warnings: list[str] = Field(default_factory=list)
    source_files: list[str] = Field(default_factory=list)


class EmployeeInput(BaseModel):
    full_name: str
    project_name: str | None = None
    coordinator_name: str | None = None
    company_code: str | None = None
    company_name: str | None = None
    odvody_strhavame: float = 0.0
    mesicni_mzda: float = 0.0


class EmployeeRecord(EmployeeInput):
    id: int
    normalized_name: str
    created_at: str
    updated_at: str


class ImportSummary(BaseModel):
    import_id: int
    period: str
    file_count: int
    parsed_rows: int
    preview_rows: int
    matched_rows: int
    missing_rows: int
    created_at: str
