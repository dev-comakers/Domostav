"""Mzdovy prehled module - payroll workflow bundled as a Flask blueprint.

The module is self-contained: templates, static assets, SQLite storage and
uploads all live under mzdovy/ so the main Domostav AI app can register it
with a single url_prefix.
"""

from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent
DATA_DIR = PACKAGE_ROOT / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
EXPORT_DIR = DATA_DIR / "exports"
# DB_PATH is kept for backwards compatibility only; Postgres is now the backend.
DB_PATH = DATA_DIR / "payroll.db"

APP_TITLE = "Mzdový přehled"
URL_PREFIX = "/mzdovy"

for _path in (DATA_DIR, UPLOAD_DIR, EXPORT_DIR):
    _path.mkdir(parents=True, exist_ok=True)

from .blueprint import blueprint  # noqa: E402

__all__ = ["blueprint", "APP_TITLE", "URL_PREFIX", "DATA_DIR", "UPLOAD_DIR", "EXPORT_DIR", "DB_PATH"]
