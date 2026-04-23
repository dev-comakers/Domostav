# Mzdovy Prehled Mini

Separate payroll mini-project built to match the existing Domostav stack:

- Python
- Flask
- SQLite
- openpyxl
- HTML/CSS/vanilla JS

## What it does

1. Uploads POHODA HTML reports:
   - `Prehled mezd`
   - `Socialka`
   - `Zdravotka`
2. Parses the files with `regex` first.
3. Falls back to an HTML parser if regex parsing fails.
4. Aggregates rows by employee full name.
5. Matches employees against a local SQLite database.
6. Lets you create missing employees from the preview screen.
7. Exports a new XLSX workbook with formulas.

## Project structure

- `app.py` - Flask entry point and API routes
- `config.py` - paths and runtime folders
- `storage/payroll_store.py` - SQLite schema and data access
- `payroll/parsers.py` - report detection and parsing
- `payroll/service.py` - import and employee creation orchestration
- `payroll/exporter.py` - XLSX export generation
- `templates/index.html` - main UI
- `static/` - CSS and frontend JS

## Run locally

```bash
cd mzdovy_prehled_mini
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python app.py
```

Open [http://127.0.0.1:5050](http://127.0.0.1:5050).

## Current implementation notes

- `Odvody platime = social employee + social employer + health employee + health employer + tax`
- `Odvody strhavame = Odvody platime`
- matching is exact by normalized full name
- payroll values are not edited in preview
- bonus columns stay present in export but are currently filled with `0`

## Integration note

This mini-project is intentionally structured so it can later be moved into the main `domostav-ai` Flask project with minimal changes:

- the storage layer already mirrors the existing `SessionStore` style
- Flask routes are isolated in one entrypoint
- the UI already follows the same dark dashboard direction
