"""Flask blueprint exposing the Mzdov\u00fd p\u0159ehled routes under /mzdovy.

All user-facing copy uses formal Czech (vyk\u00e1n\u00ed): "nahrajte",
"zkontrolujte", "st\u00e1hn\u011bte" instead of the casual "nahraj", etc.
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path

from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from . import APP_TITLE, DB_PATH, EXPORT_DIR, UPLOAD_DIR
from .payroll.employee_seed import load_employees_from_xlsx
from .payroll.exporter import build_export
from .payroll.models import EmployeeInput
from .payroll.service import PayrollService
from .storage.payroll_store import PayrollStore


blueprint = Blueprint(
    "mzdovy",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/mzdovy",
)


_store = PayrollStore()
_service = PayrollService(_store, UPLOAD_DIR)


def _render_page(template_name: str, **context):
    return render_template(template_name, title=APP_TITLE, app_title=APP_TITLE, **context)


# ---------- Pages ----------


@blueprint.get("/")
def home():
    return redirect(url_for("mzdovy.wizard_new"))


@blueprint.get("/wizard")
def wizard_root():
    return redirect(url_for("mzdovy.wizard_new"))


@blueprint.get("/wizard/new")
def wizard_new():
    return _render_page(
        "wizard.html",
        page_id="wizard",
        page_title="Nov\u00fd mzdov\u00fd p\u0159ehled",
        page_description=(
            "Za\u010dn\u011bte nov\u00fdm importem HTML report\u016f z Pamica. "
            "Obdob\u00ed na\u010dteme automaticky z dokument\u016f."
        ),
        step="upload",
        import_id=None,
        summary=None,
    )


@blueprint.get("/wizard/<int:import_id>")
def wizard_default(import_id: int):
    return redirect(url_for("mzdovy.wizard_step", import_id=import_id, step="review"))


@blueprint.get("/wizard/<int:import_id>/<step>")
def wizard_step(import_id: int, step: str):
    if step not in {"review", "recompute", "export"}:
        return redirect(url_for("mzdovy.wizard_default", import_id=import_id))
    summary = _store.get_import_summary(import_id)
    if not summary:
        return redirect(url_for("mzdovy.wizard_new"))

    titles = {
        "review": (
            "Kontrola p\u0159ehledu",
            "Zkontrolujte zam\u011bstnance, firmy, odvody a upozorn\u011bn\u00ed p\u0159ed dal\u0161\u00edm krokem.",
        ),
        "recompute": (
            "P\u0159epo\u010d\u00edt\u00e1n\u00ed p\u0159ehledu",
            "Po \u00faprav\u011b datab\u00e1ze synchronizujte p\u0159ehled s aktu\u00e1ln\u00edmi zam\u011bstnanci, projekty a koordin\u00e1tory.",
        ),
        "export": (
            "St\u00e1hnout XLSX",
            "Po posledn\u00ed kontrole st\u00e1hn\u011bte XLSX soubory jen pro firmy, kter\u00e9 jsou v aktu\u00e1ln\u00edm importu.",
        ),
    }
    page_title, page_description = titles[step]
    return _render_page(
        "wizard.html",
        page_id="wizard",
        page_title=page_title,
        page_description=page_description,
        step=step,
        import_id=import_id,
        summary=summary.model_dump(),
    )


@blueprint.get("/employees")
def employees_page():
    return _render_page(
        "employees.html",
        page_id="employees",
        page_title="Datab\u00e1ze zam\u011bstnanc\u016f",
        page_description=(
            "Spravujte zam\u011bstnance, projekty, koordin\u00e1tory a firmy. "
            "Odvody strh\u00e1v\u00e1me a m\u011bs\u00ed\u010dn\u00ed mzdu vypl\u0148ujte ru\u010dn\u011b."
        ),
    )


@blueprint.get("/history")
def history_page():
    imports = _store.list_imports()
    selected_import_id = request.args.get("import_id", type=int)
    if selected_import_id is None and imports:
        selected_import_id = imports[0]["id"]
    return _render_page(
        "history.html",
        page_id="history",
        page_title="Historie import\u016f",
        page_description="Vra\u0165te se k p\u0159edchoz\u00edm zpracov\u00e1n\u00edm, otev\u0159ete jejich p\u0159ehled a zopakujte export.",
        selected_import_id=selected_import_id,
    )


# ---------- API ----------


@blueprint.get("/api/imports")
def list_imports():
    return jsonify({"imports": _store.list_imports()})


@blueprint.post("/api/imports")
def create_import():
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "Nahrajte alespo\u0148 jeden HTML soubor."}), 400
    try:
        import_id = _service.import_html_files(files)
    except Exception as exc:
        return jsonify({"error": f"Nepoda\u0159ilo se zpracovat soubory: {exc}"}), 400
    summary = _store.get_import_summary(import_id)
    preview_rows = _store.list_preview_rows(import_id)
    return jsonify(
        {
            "import_id": import_id,
            "summary": summary.model_dump() if summary else None,
            "preview_rows": preview_rows,
        }
    )


@blueprint.get("/api/imports/<int:import_id>/preview")
def get_preview(import_id: int):
    summary = _store.get_import_summary(import_id)
    if not summary:
        return jsonify({"error": "Import nebyl nalezen."}), 404
    return jsonify(
        {
            "summary": summary.model_dump(),
            "preview_rows": _store.list_preview_rows(import_id),
        }
    )


@blueprint.post("/api/imports/<int:import_id>/recompute")
def recompute_preview(import_id: int):
    summary = _store.get_import_summary(import_id)
    if not summary:
        return jsonify({"error": "Import nebyl nalezen."}), 404
    _store.rebuild_preview_rows(import_id)
    updated_summary = _store.get_import_summary(import_id)
    return jsonify(
        {
            "summary": updated_summary.model_dump() if updated_summary else None,
            "preview_rows": _store.list_preview_rows(import_id),
        }
    )


@blueprint.get("/api/employees")
def list_employees():
    return jsonify({"employees": _store.list_employees()})


@blueprint.get("/api/meta")
def get_metadata():
    return jsonify(_store.list_employee_metadata())


def _employee_payload_to_input(payload: dict, *, fallback_company_name: str | None = None) -> EmployeeInput | None:
    full_name = (payload.get("full_name") or "").strip()
    if not full_name:
        return None

    company_name = (payload.get("company_name") or fallback_company_name or "").strip() or None
    company_code = (payload.get("company_code") or company_name or "").strip() or None

    def _num(value) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return float(str(value).replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    return EmployeeInput(
        full_name=full_name,
        project_name=(payload.get("project_name") or "").strip() or None,
        coordinator_name=(payload.get("coordinator_name") or "").strip() or None,
        company_code=company_code,
        company_name=company_name,
        odvody_strhavame=_num(payload.get("odvody_strhavame")),
        mesicni_mzda=_num(payload.get("mesicni_mzda")),
    )


@blueprint.post("/api/employees")
def create_employee_manual():
    payload = request.get_json(silent=True) or {}
    data = _employee_payload_to_input(payload)
    if data is None:
        return jsonify({"error": "Jm\u00e9no a p\u0159\u00edjmen\u00ed je povinn\u00e9."}), 400
    employee_id = _store.create_employee(data)
    return jsonify({"employee_id": employee_id, "employees": _store.list_employees()})


@blueprint.put("/api/employees/<int:employee_id>")
def update_employee(employee_id: int):
    payload = request.get_json(silent=True) or {}
    data = _employee_payload_to_input(payload)
    if data is None:
        return jsonify({"error": "Jm\u00e9no a p\u0159\u00edjmen\u00ed je povinn\u00e9."}), 400
    try:
        _store.update_employee(employee_id, data)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"employees": _store.list_employees()})


@blueprint.delete("/api/employees/<int:employee_id>")
def delete_employee(employee_id: int):
    _store.delete_employee(employee_id)
    return jsonify({"employees": _store.list_employees()})


@blueprint.post("/api/employees/clear")
def clear_employees():
    removed = _store.clear_employees()
    return jsonify({"removed": removed, "employees": _store.list_employees()})


@blueprint.post("/api/employees/import-xlsx")
def import_employees_from_xlsx():
    uploaded = request.files.get("file")
    if not uploaded or not uploaded.filename:
        return jsonify({"error": "Nahrajte XLSX soubor."}), 400
    try:
        buffer = BytesIO(uploaded.read())
        items = load_employees_from_xlsx(buffer)
    except Exception as exc:
        return jsonify({"error": f"Nepoda\u0159ilo se na\u010d\u00edst XLSX: {exc}"}), 400
    stats = _store.bulk_upsert_employees(items)
    return jsonify(
        {
            "stats": stats,
            "total_rows": len(items),
            "employees": _store.list_employees(),
        }
    )


@blueprint.post("/api/preview/<int:preview_row_id>/employees")
def create_employee_from_preview(preview_row_id: int):
    payload = request.get_json(silent=True) or {}
    preview_row = _store.get_preview_row(preview_row_id)
    if not preview_row:
        return jsonify({"error": "\u0158\u00e1dek p\u0159ehledu nebyl nalezen."}), 404

    data = _employee_payload_to_input(payload, fallback_company_name=preview_row["company_name"])
    if data is None:
        return jsonify({"error": "Jm\u00e9no a p\u0159\u00edjmen\u00ed je povinn\u00e9."}), 400

    employee_id = _service.create_employee_from_preview(
        preview_row_id=preview_row_id,
        full_name=data.full_name,
        project_name=data.project_name,
        coordinator_name=data.coordinator_name,
        company_code=data.company_code,
        company_name=data.company_name,
        odvody_strhavame=data.odvody_strhavame,
        mesicni_mzda=data.mesicni_mzda,
    )
    updated_summary = _store.get_import_summary(preview_row["import_id"])
    return jsonify(
        {
            "employee_id": employee_id,
            "summary": updated_summary.model_dump() if updated_summary else None,
            "preview_rows": _store.list_preview_rows(preview_row["import_id"]),
        }
    )


@blueprint.post("/api/exports/<int:import_id>")
def create_export(import_id: int):
    summary = _store.get_import_summary(import_id)
    if not summary:
        return jsonify({"error": "Import nebyl nalezen."}), 404
    preview_rows = _store.list_preview_rows(import_id)
    if not preview_rows:
        return jsonify({"error": "P\u0159ehled je pr\u00e1zdn\u00fd, nen\u00ed co exportovat."}), 400

    from .payroll.exporter import detect_export_variant

    period = summary.period or datetime.utcnow().strftime("%m/%Y")
    safe_period = period.replace("/", "_")
    variant = detect_export_variant(preview_rows)
    slug = "Domostav" if variant == "dm" else "Ostatni_firmy"
    output_path = EXPORT_DIR / f"Vydvody_po_objektach_{slug}_{safe_period}_{import_id}.xlsx"
    build_export(preview_rows, str(output_path))
    _store.save_export_run(import_id, str(output_path))
    download_urls = {
        variant: url_for("mzdovy.download_file", filename=output_path.name),
    }
    return jsonify({"download_urls": download_urls, "variant": variant})


@blueprint.get("/downloads/<path:filename>")
def download_file(filename: str):
    return send_from_directory(str(EXPORT_DIR), filename, as_attachment=True)
