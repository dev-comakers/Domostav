"""Lazy-initialised Postgres connection pool shared by both modules."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Iterator

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_ENV_LOADED = False
_POOL: ConnectionPool | None = None
_POOL_LOCK = Lock()


def _load_env() -> None:
    """Load .env from the application root (next to webapp.py)."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    _ENV_LOADED = True


def require_database_url() -> str:
    _load_env()
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example to .env and configure the connection string."
        )
    return url


def _get_pool() -> ConnectionPool:
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            dsn = require_database_url()
            _POOL = ConnectionPool(
                conninfo=dsn,
                min_size=1,
                max_size=10,
                timeout=10,
                kwargs={"row_factory": dict_row},
                open=True,
            )
    return _POOL


@contextmanager
def get_conn(schema: str | None = None) -> Iterator[psycopg.Connection]:
    """Yield a pooled connection with optional `search_path` pinned to `schema`.

    The connection auto-commits on successful exit and rolls back on exception.
    """
    pool = _get_pool()
    with pool.connection() as conn:
        if schema:
            with conn.cursor() as cur:
                cur.execute(f'SET LOCAL search_path TO "{schema}", public')
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()


def close_pool() -> None:
    """Gracefully close the shared pool (used by tests / shutdown hooks)."""
    global _POOL
    with _POOL_LOCK:
        if _POOL is not None:
            _POOL.close()
            _POOL = None
