"""
Lakebase (Databricks-managed Postgres) connection helper.

Connects using a Databricks Secret (scope: lakebase, key: url) or
falls back to the LAKEBASE_URL environment variable if set directly.

Includes DDL helpers for:
  - tickets / ticket_messages  (support ticketing system)
  - weather_documents          (weather harvest — Homework 2)
  - weather_embeddings         (pgvector 384-dim — Homework 2)
"""

import base64
import os
from contextlib import contextmanager

import psycopg2
from databricks.sdk import WorkspaceClient
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

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


def get_engine():
    return create_engine(_lakebase_url())


def run_query(sql: str, params=None) -> list[dict]:
    """Execute a SELECT and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params=None) -> int:
    """Execute INSERT/UPDATE/DELETE, commit, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


# ── Weather DDL ──────────────────────────────────────────────────────────────────

def ensure_weather_tables():
    """Idempotent DDL: create weather_documents + weather_embeddings if absent."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # pgvector extension (already enabled, but safe to re-run)
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_documents (
                    id            VARCHAR(255) PRIMARY KEY,
                    location      VARCHAR(100) NOT NULL,
                    source_type   VARCHAR(20)  NOT NULL,
                    headline      VARCHAR(255),
                    narrative_text TEXT        NOT NULL,
                    issued_at     TIMESTAMPTZ,
                    effective_at  TIMESTAMPTZ,
                    expires_at    TIMESTAMPTZ,
                    payload       JSONB,
                    synced_at     TIMESTAMPTZ  DEFAULT NOW(),
                    updated_at    TIMESTAMPTZ  DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_embeddings (
                    id          BIGSERIAL    PRIMARY KEY,
                    document_id VARCHAR(255) NOT NULL
                                REFERENCES weather_documents(id) ON DELETE CASCADE,
                    chunk_index INT          NOT NULL,
                    chunk_text  TEXT         NOT NULL,
                    embedding   vector(384),
                    model_name  VARCHAR(100) DEFAULT 'all-MiniLM-L6-v2',
                    created_at  TIMESTAMPTZ  DEFAULT NOW(),
                    UNIQUE(document_id, chunk_index)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_location    ON weather_documents(location)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_source_type ON weather_documents(source_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_issued_at   ON weather_documents(issued_at DESC)")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_weather_emb_hnsw
                ON weather_embeddings USING hnsw (embedding vector_cosine_ops)
            """)
            conn.commit()


# ── Weather upsert (Stretch Goal 2: deduplication) ─────────────────────────────

def upsert_weather_documents(docs: list[dict]) -> dict:
    """
    Upsert weather documents into weather_documents.
    - INSERT ... ON CONFLICT (id) DO UPDATE: no duplicates on re-sync.
    - When narrative_text changes, stale embeddings are deleted so the
      ingestion script re-embeds on next run.
    Returns {"inserted": N, "updated": N}.
    """
    import json as _json
    inserted = 0
    updated  = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for doc in docs:
                cur.execute("""
                    INSERT INTO weather_documents
                        (id, location, source_type, headline, narrative_text,
                         issued_at, effective_at, expires_at, payload, synced_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET location       = EXCLUDED.location,
                            headline       = EXCLUDED.headline,
                            narrative_text = EXCLUDED.narrative_text,
                            issued_at      = EXCLUDED.issued_at,
                            effective_at   = EXCLUDED.effective_at,
                            expires_at     = EXCLUDED.expires_at,
                            payload        = EXCLUDED.payload,
                            synced_at      = EXCLUDED.synced_at,
                            updated_at     = NOW()
                    RETURNING
                        (xmax = 0) AS was_inserted,
                        (xmax <> 0 AND narrative_text IS DISTINCT FROM EXCLUDED.narrative_text) AS text_changed
                """, (
                    doc["id"], doc["location"], doc["source_type"],
                    doc.get("headline"), doc["narrative_text"],
                    doc.get("issued_at"), doc.get("effective_at"), doc.get("expires_at"),
                    _json.dumps(doc.get("payload") or {}),
                    doc.get("synced_at"),
                ))
                row = cur.fetchone()
                if row and row["was_inserted"]:
                    inserted += 1
                else:
                    updated += 1
                    # Invalidate stale embeddings if narrative changed
                    if row and row.get("text_changed"):
                        cur.execute(
                            "DELETE FROM weather_embeddings WHERE document_id = %s",
                            (doc["id"],)
                        )
        conn.commit()

    return {"inserted": inserted, "updated": updated}
