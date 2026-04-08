# Domostav AI

Local AI-assisted platform for checking material write-offs against SPP.

## What It Does

- Upload SPP and inventory documents.
- Match inventory rows to SPP work rows with AI.
- Review unmatched/anomaly rows in web UI before export.
- Save project/system matching rules and recalculate.
- Export final Excel report with:
  - `TDSheet`
  - `AI Summary`
  - `SPP Coverage`

## Tech Stack

- Python 3.9+
- Flask
- OpenAI API
- openpyxl
- SQLite (local state)

## Project Structure

- `webapp.py` - Flask app and API endpoints
- `services/` - end-to-end pipeline orchestration
- `parsers/` - SPP/inventory/rules parsers
- `matching/` - AI matching logic
- `analysis/` - anomaly/write-off calculations
- `output/` - Excel generation
- `storage/` - SQLite session and rule storage
- `design/` - dashboard UI
- `config/` - project configs and prompts

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export OPENAI_API_KEY="YOUR_KEY"
export OPENAI_MODEL="gpt-5.4"
python3 webapp.py
```

Open:

- `http://127.0.0.1:8000`
- `http://127.0.0.1:8000/health`
- `http://127.0.0.1:8000/api/version`

## Notes

- The repository intentionally ignores local input/output Excel files and local DB state.
- Upload your own SPP/inventory files in the UI for each run.

