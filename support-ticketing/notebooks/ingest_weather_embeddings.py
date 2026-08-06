"""
notes/ingest_weather_embeddings.py

Batch embedding ingestion pipeline for weather_documents → weather_embeddings.

Pipeline:
  1. Connect to Lakebase via psycopg2 (reads LAKEBASE_URL from Databricks Secret)
  2. Query unembedded rows from weather_documents
  3. Chunk narrative_text with sliding-window (CHUNK_SIZE=800 words, OVERLAP=100)
  4. Embed each chunk with sentence-transformers/all-MiniLM-L6-v2 (384-dim)
  5. Batch-insert into weather_embeddings via psycopg2 execute_values + ::vector cast
  6. Report stats (documents, chunks, errors)

Does NOT use Spark JDBC (unsupported against Lakebase in this environment).

Usage (Databricks notebook):
    %run ./notebooks/ingest_weather_embeddings

Usage (plain Python):
    LAKEBASE_URL=postgresql://... python notebooks/ingest_weather_embeddings.py
"""

import base64
import logging
import os
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ingest_weather")

MODEL_NAME    = "all-MiniLM-L6-v2"
CHUNK_SIZE    = 800   # words per chunk
CHUNK_OVERLAP = 100   # word overlap between consecutive chunks
BATCH_SIZE    = 50    # documents per embedding batch
DEFAULT_LIMIT = 1000  # max documents to process per run


# ── Connection ──────────────────────────────────────────────────────────────

def _get_db_url() -> str:
    """Resolve Lakebase connection URL from env var or Databricks Secret."""
    direct = os.environ.get("LAKEBASE_URL")
    if direct:
        return direct
    try:
        from databricks.sdk import WorkspaceClient
        _w = WorkspaceClient()
        scope  = os.environ.get("LAKEBASE_SECRET_SCOPE", "lakebase")
        key    = os.environ.get("LAKEBASE_SECRET_KEY",   "url")
        secret = _w.secrets.get_secret(scope=scope, key=key)
        return base64.b64decode(secret.value).decode("utf-8")
    except Exception as exc:
        logger.error("Could not read Databricks Secret: %s", exc)
    raise RuntimeError(
        "Set LAKEBASE_URL env var or configure Databricks Secret scope=lakebase key=url"
    )


def get_connection():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(_get_db_url(), cursor_factory=RealDictCursor)


# ── Chunking ───────────────────────────────────────────────────────────────────

def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> list[str]:
    """
    Sliding-window word-level chunking.

    Most NWS forecast text is 300–600 words per location, so a single chunk
    per document is common. Chunking kicks in for combined alert+instruction
    text that can reach 800+ words.

    Parameters:
        chunk_size: max words per chunk (default 800)
        overlap:    words repeated at the start of each new chunk (default 100)
                    reduces information loss at chunk boundaries
    """
    if not text or not text.strip():
        return []
    words = text.split()
    if len(words) <= chunk_size:
        return [text.strip()]

    chunks = []
    step = max(1, chunk_size - overlap)
    start = 0
    while start < len(words):
        chunk = " ".join(words[start : start + chunk_size])
        if chunk.strip():
            chunks.append(chunk.strip())
        start += step
    return chunks


# ── Main ingestion function ─────────────────────────────────────────────────────

def ingest_weather_embeddings(
    batch_size: int = BATCH_SIZE,
    limit: int = DEFAULT_LIMIT,
) -> dict:
    """
    Read unembedded weather_documents rows, chunk, embed, and insert into
    weather_embeddings. Returns stats dict.
    """
    from sentence_transformers import SentenceTransformer
    from psycopg2.extras import execute_values

    # Load model once
    logger.info("Loading sentence-transformers model: %s", MODEL_NAME)
    t0 = time.time()
    model = SentenceTransformer(MODEL_NAME)
    logger.info("Model loaded in %.1fs", time.time() - t0)

    conn = get_connection()
    total_docs   = 0
    total_chunks = 0
    total_errors = 0

    try:
        # Query documents not yet in weather_embeddings
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, narrative_text
                FROM   weather_documents
                WHERE  id NOT IN (
                    SELECT DISTINCT document_id FROM weather_embeddings
                )
                ORDER BY synced_at DESC
                LIMIT  %s
            """, (limit,))
            docs = cur.fetchall()

        total_docs = len(docs)
        logger.info("Found %d unembedded documents", total_docs)

        if not docs:
            logger.info("Nothing to embed — run POST /weather/sync first.")
            return {"documents": 0, "chunks": 0, "errors": 0}

        # Process in batches
        for batch_start in range(0, len(docs), batch_size):
            batch  = docs[batch_start : batch_start + batch_size]
            records = []

            for doc in batch:
                try:
                    chunks = chunk_text(doc["narrative_text"])
                    if not chunks:
                        continue

                    t1 = time.time()
                    embeddings = model.encode(chunks, show_progress_bar=False)
                    logger.debug(
                        "Embedded %d chunks for doc %s in %.2fs",
                        len(chunks), doc["id"][:12], time.time() - t1
                    )

                    for idx, (chunk, emb) in enumerate(zip(chunks, embeddings)):
                        # Convert numpy array to pgvector literal: "[0.1,0.2,...]"
                        vec_str = "[" + ",".join(f"{v:.8f}" for v in emb.tolist()) + "]"
                        records.append((
                            doc["id"],   # document_id
                            idx,          # chunk_index
                            chunk,        # chunk_text
                            vec_str,      # embedding (cast to ::vector in SQL)
                            MODEL_NAME,   # model_name
                        ))

                except Exception as exc:
                    logger.error("Error embedding doc %s: %s", doc["id"][:16], exc)
                    total_errors += 1

            if not records:
                continue

            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO weather_embeddings
                            (document_id, chunk_index, chunk_text, embedding, model_name)
                        VALUES %s
                        ON CONFLICT (document_id, chunk_index) DO NOTHING
                        """,
                        records,
                        template="(%s, %s, %s, %s::vector, %s)",
                        page_size=500,
                    )
                    conn.commit()

                total_chunks += len(records)
                batch_num = batch_start // batch_size + 1
                logger.info(
                    "Batch %d: inserted %d embeddings (docs %d–%d)",
                    batch_num, len(records),
                    batch_start + 1, min(batch_start + batch_size, total_docs)
                )

            except Exception as exc:
                conn.rollback()
                logger.error("Batch insert failed: %s", exc)
                total_errors += len(batch)

    finally:
        conn.close()

    stats = {"documents": total_docs, "chunks": total_chunks, "errors": total_errors}
    logger.info("Ingestion complete: %s", stats)
    return stats


# ── Entry point (run as script or notebook cell) ─────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest weather embeddings into Lakebase")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Documents per embedding batch (default {BATCH_SIZE})")
    parser.add_argument("--limit",      type=int, default=DEFAULT_LIMIT,
                        help=f"Max documents to process (default {DEFAULT_LIMIT})")
    args = parser.parse_args()
    stats = ingest_weather_embeddings(batch_size=args.batch_size, limit=args.limit)
    print(stats)

# ── Databricks notebook usage ─────────────────────────────────────────────────
# Uncomment and run the following cell in a Databricks notebook:
#
# stats = ingest_weather_embeddings(batch_size=50, limit=1000)
# print(stats)
