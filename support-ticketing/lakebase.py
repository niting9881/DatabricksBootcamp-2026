"""
Lakebase (Databricks-managed Postgres) connection helper.

Connects using a Databricks Secret (scope: lakebase, key: url) or
falls back to the LAKEBASE_URL environment variable if set directly.
"""

import base64
import os
from contextlib import contextmanager

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2.extras import RealDictCursor

_w = WorkspaceClient()

# Secret scope + key are injected by app.yaml env vars.
# Defaults match the setup instructions in app.yaml.
_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "lakebase")
_KEY   = os.environ.get("LAKEBASE_SECRET_KEY",   "url")


def _lakebase_url() -> str:
    direct = os.environ.get("LAKEBASE_URL")
    if direct:
        return direct
    secret = _w.secrets.get_secret(scope=_SCOPE, key=_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


@contextmanager
def get_connection():
    conn = psycopg2.connect(_lakebase_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params=None) -> list[dict]:
    """Execute SQL and return rows as list[dict].

    Always commits after execute so that INSERT/UPDATE/DELETE ... RETURNING
    statements are persisted. A commit after a plain SELECT is a harmless no-op.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            conn.commit()   # persist DML (INSERT/UPDATE/DELETE RETURNING)
            return rows


def run_write(sql: str, params=None) -> int:
    """Execute INSERT/UPDATE/DELETE, commit, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
