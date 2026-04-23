from __future__ import annotations

import uuid
from pathlib import Path

from .models import EmployeeInput
from .parsers import parse_report_file
from ..storage.payroll_store import PayrollStore


class PayrollService:
    def __init__(self, store: PayrollStore, upload_dir: Path):
        self.store = store
        self.upload_dir = upload_dir
        self.upload_dir.mkdir(parents=True, exist_ok=True)

    def import_html_files(self, files: list, period: str | None = None) -> int:
        import_id = self.store.create_import(period or "")
        session_dir = self.upload_dir / f"payroll_{import_id}_{uuid.uuid4().hex[:8]}"
        session_dir.mkdir(parents=True, exist_ok=True)

        effective_period = period or ""
        for file_storage in files:
            filename = file_storage.filename or f"upload_{uuid.uuid4().hex}.htm"
            saved_path = session_dir / filename
            file_storage.save(saved_path)
            report_type, company_name, detected_period, rows = parse_report_file(saved_path)
            parser_mode = rows[0].parser_mode if rows else "regex"
            file_id = self.store.save_import_file(
                import_id=import_id,
                filename=filename,
                report_type=report_type,
                company_name=company_name,
                period=detected_period,
                parser_mode=parser_mode,
                saved_path=str(saved_path),
            )
            self.store.save_parsed_rows(import_id, file_id, [row.model_dump() for row in rows])
            effective_period = effective_period or detected_period

        if effective_period:
            self.store.update_import_period(import_id, effective_period)
        self.store.rebuild_preview_rows(import_id)
        return import_id

    def create_employee_from_preview(
        self,
        *,
        preview_row_id: int,
        full_name: str,
        project_name: str | None,
        coordinator_name: str | None,
        company_code: str | None,
        company_name: str | None,
        odvody_strhavame: float = 0.0,
        mesicni_mzda: float = 0.0,
    ) -> int:
        employee_id = self.store.create_employee(
            EmployeeInput(
                full_name=full_name,
                project_name=project_name,
                coordinator_name=coordinator_name,
                company_code=company_code,
                company_name=company_name,
                odvody_strhavame=odvody_strhavame,
                mesicni_mzda=mesicni_mzda,
            )
        )
        self.store.attach_employee_to_preview_row(preview_row_id, employee_id)
        return employee_id
