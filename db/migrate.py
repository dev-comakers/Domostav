"""Apply idempotent DDL files to each module's schema at application startup."""

from __future__ import annotations

from pathlib import Path

from .connection import get_conn

DDL_DIR = Path(__file__).resolve().parent / "ddl"


def _read_sql(name: str) -> str:
    return (DDL_DIR / name).read_text(encoding="utf-8")


def apply_schemas() -> None:
    """Ensure `spp` and `mzdovy` schemas exist and all DDL is applied.

    Safe to call repeatedly; all statements use `IF NOT EXISTS`.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS spp")
            cur.execute("CREATE SCHEMA IF NOT EXISTS mzdovy")

    with get_conn(schema="spp") as conn:
        with conn.cursor() as cur:
            cur.execute(_read_sql("spp.sql"))

    with get_conn(schema="mzdovy") as conn:
        with conn.cursor() as cur:
            cur.execute(_read_sql("mzdovy.sql"))
