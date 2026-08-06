# Weather Intelligence — Vector Retrieval Service

DataExpert.io Bootcamp — Homework 2  
**Pipeline:** NWS API → Lakebase Postgres + pgvector → Flask Semantic Search

---

## Overview

This module extends the Support Ticketing app with a weather intelligence pipeline:

1. **Harvest** — Fetch unstructured weather text (active alerts + multi-day forecasts) from the National Weather Service API (`api.weather.gov`) for a set of US locations.
2. **Vectorize** — Chunk narrative text, embed with `all-MiniLM-L6-v2` (384-dim), and store vectors in Lakebase (`pgvector`).
3. **Retrieve** — POST `/weather/search` embeds the user’s query and returns the most semantically relevant weather documents, ranked by cosine similarity.

```
 NWS API (alerts + forecasts)
         │
         │  weather_client.py
         │  (requests, Nominatim geocoding)
         ▼
 POST /weather/sync → weather_documents (Lakebase Postgres)
         │
         │  notebooks/ingest_weather_embeddings.py
         │  (sentence-transformers, psycopg2)
         ▼
  weather_embeddings.embedding vector(384)  ← HNSW index
         │
         │  pgvector <=> cosine distance
         ▼
 POST /weather/search → top-k ranked results + optional LLM summary
```

---

## Data Source: National Weather Service (NWS) API

**Why NWS?**
- Free, no API key, generous rate limits
- Returns rich unstructured narrative text ideal for embedding
- Two complementary data types: real-time alerts + multi-day forecasts
- US-wide coverage via standardised REST endpoints

**Endpoints used:**
| Endpoint | Data | Document type |
|----------|------|---------------|
| `GET /alerts/active?area={STATE}` | Active weather alerts with description + instruction text | `source_type = "alert"` |
| `GET /points/{lat},{lon}` | Resolve location to NWS grid (office, gridX, gridY) | Internal |
| `GET /gridpoints/{office}/{x},{y}/forecast` | 7-day forecast with `detailedForecast` per period | `source_type = "forecast"` |

**Geocoding:** City/state strings (e.g., `"Chicago, IL"`) are resolved to lat/lon via [Nominatim / OpenStreetMap](https://nominatim.org/) — free, no API key.

---

## Schema Design

### `weather_documents`

```sql
CREATE TABLE weather_documents (
    id            VARCHAR(255) PRIMARY KEY,   -- SHA-256 of alert_id or location|type|date
    location      VARCHAR(100) NOT NULL,       -- e.g. "Chicago, IL"
    source_type   VARCHAR(20)  NOT NULL,       -- "alert" | "forecast"
    headline      VARCHAR(255),                -- e.g. "Flash Flood Warning"
    narrative_text TEXT        NOT NULL,       -- free-text body to embed
    issued_at     TIMESTAMPTZ,
    effective_at  TIMESTAMPTZ,
    expires_at    TIMESTAMPTZ,
    payload       JSONB,                       -- raw API response for provenance
    synced_at     TIMESTAMPTZ  DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  DEFAULT NOW()
);
```

**Index rationale:**
- `idx_weather_location` — filter by city
- `idx_weather_source_type` — filter alerts vs. forecasts
- `idx_weather_issued_at DESC` — sort/filter by recency

### `weather_embeddings`

```sql
CREATE TABLE weather_embeddings (
    id          BIGSERIAL    PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL REFERENCES weather_documents(id) ON DELETE CASCADE,
    chunk_index INT          NOT NULL,
    chunk_text  TEXT         NOT NULL,
    embedding   vector(384),                   -- all-MiniLM-L6-v2 (384 dimensions)
    model_name  VARCHAR(100) DEFAULT 'all-MiniLM-L6-v2',
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(document_id, chunk_index)
);

-- HNSW index for fast cosine similarity search
CREATE INDEX idx_weather_emb_hnsw
    ON weather_embeddings USING hnsw (embedding vector_cosine_ops);
```

**Index choice — HNSW vs IVFFlat:**  
HNSW (`Hierarchical Navigable Small World`) was chosen for production retrieval because it delivers sub-millisecond query latency on small-to-medium datasets without requiring a minimum row count before creation (IVFFlat requires at least `lists` rows to build). For the homework dataset (~50–500 embeddings) HNSW builds instantly and returns results in <50ms.

---

## Chunking Strategy

| Parameter | Value | Rationale |
|-----------|-------|----------|
| `CHUNK_SIZE` | 800 words | Most NWS forecast text is 300–600 words; this fits in a single chunk. Combined alert+instruction text can reach 700–900 words. |
| `CHUNK_OVERLAP` | 100 words | Preserves context at chunk boundaries, reducing information loss for multi-period forecast documents. |

**Algorithm:** Sliding-window, word-level. Short documents (≤800 words) produce exactly one chunk. Most NWS weather documents will produce 1–2 chunks.

---

## Embedding Model

| Property | Value |
|----------|-------|
| Model | `sentence-transformers/all-MiniLM-L6-v2` |
| Dimensionality | 384 |
| Inference (CPU) | ~15–50ms per chunk |
| Download size | ~90 MB |

**Why this model:** Matches the existing reference app’s news pipeline (same dimensionality), strong general-purpose semantic similarity, fast CPU inference, no API key required.

---

## How to Run (End-to-End)

### Prerequisites

```bash
pip install sentence-transformers psycopg2-binary requests flask databricks-sdk torch
```

Set up the Databricks Secret (one-time):
```bash
databricks secrets create-scope lakebase
databricks secrets put-secret lakebase url \
  --string-value "postgresql://student:<password>@<host>:5432/databricks_postgres?sslmode=require"
```

### Step 1 — Sync weather data

```bash
curl -X POST https://support-ticketing-7474648653109871.aws.databricksapps.com/weather/sync \
  -H "Content-Type: application/json" \
  -d '{"locations": ["Chicago, IL", "Austin, TX", "Miami, FL"], "limit": 50}'
```

Expected response:
```json
{
  "status": "success",
  "synced_count": 14,
  "data": {"alerts": 8, "forecasts": 5, "new_documents": 14, "updated_documents": 0}
}
```

### Step 2 — Run embedding ingestion (Databricks notebook)

Open a Databricks notebook and run:
```python
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "sentence-transformers"])

import sys
sys.path.insert(0, "/Workspace/Users/learndatabricks31@gmail.com/support-ticketing")
from notebooks.ingest_weather_embeddings import ingest_weather_embeddings
stats = ingest_weather_embeddings(batch_size=50, limit=1000)
print(stats)
```

Or from the command line:
```bash
cd /Workspace/Users/<user>/support-ticketing
python notebooks/ingest_weather_embeddings.py --batch-size 50 --limit 1000
```

### Step 3 — Search

```bash
# Basic search
curl -X POST https://support-ticketing-7474648653109871.aws.databricksapps.com/weather/search \
  -H "Content-Type: application/json" \
  -d '{"query": "flash flood risk near rivers", "top_k": 5}'

# Filter by source type
curl -X POST .../weather/search \
  -H "Content-Type: application/json" \
  -d '{"query": "severe thunderstorm", "top_k": 3, "source_type": "alert"}'

# GET variant
curl "https://support-ticketing-7474648653109871.aws.databricksapps.com/weather/search?query=flooding&top_k=5"

# With LLM summary (Stretch Goal 1)
curl -X POST .../weather/search \
  -H "Content-Type: application/json" \
  -d '{"query": "tornado warning", "top_k": 5, "generate_summary": true}'
```

Expected response:
```json
{
  "status": "success",
  "query": "flash flood risk near rivers",
  "results_count": 5,
  "results": [
    {
      "rank": 1,
      "document_id": "a1b2c3d4...",
      "location": "Chicago, IL",
      "source_type": "alert",
      "headline": "Flash Flood Warning",
      "chunk_text": "A Flash Flood Warning means that flooding is imminent...",
      "similarity_score": 0.87,
      "issued_at": "2026-08-05T08:00:00Z"
    }
  ]
}
```

---

## Stretch Goals Implemented

### Stretch Goal 1: LLM-Powered Search Summaries
Pass `"generate_summary": true` in the `/weather/search` request body. The top-3 chunk texts are sent to `databricks-meta-llama-3-3-70b-instruct` via the Databricks Foundation Models API, and a 2–3 sentence natural-language summary is returned in the `summary` field.

### Stretch Goal 2: Deduplication & Upsert
`POST /weather/sync` uses `INSERT ... ON CONFLICT (id) DO UPDATE` — re-running sync never creates duplicate rows. When `narrative_text` changes (e.g., an alert is updated), stale embeddings are automatically deleted from `weather_embeddings` so the ingestion script re-embeds on the next run.

### Stretch Goal 3: Scheduled Sync Job
A Lakeflow Job (`weather-sync-job`) is configured to run `ingest_weather_embeddings.py` every 15 minutes. See the Scheduled Jobs section in the main README for the job ID.

---

## Known Limitations

1. **NWS coverage is US-only** — the API only serves US weather data. International locations will fail geocoding or return no grid.
2. **Active alerts are ephemeral** — NWS alerts expire. A search immediately after sync may return 0 results if no alerts are currently active for the selected locations.
3. **Embeddings are a manual step** — after `/weather/sync`, the embedding ingestion script must be run separately. The scheduled job (Stretch Goal 3) automates this in production.
4. **Cold-start latency** — the first `/weather/search` request loads the `all-MiniLM-L6-v2` model (∼90 MB, ∼15s on cold start). Subsequent searches are fast (<100ms embedding + <50ms vector search).
5. **Forecast deduplication is daily** — forecast IDs are keyed on `location + date`. Re-syncing intra-day creates a new document if the forecast is updated (intended behaviour — keeps the latest narrative).
6. **No minimum similarity threshold** — results are ranked by cosine distance but not filtered by a minimum score. Low-relevance results may appear if the embeddings table has few documents.

---

## API Reference

| Method | Endpoint | Body / Params | Response |
|--------|----------|---------------|----------|
| POST | `/weather/sync` | `{locations, limit}` | `{status, synced_count, data: {alerts, forecasts, new, updated}}` |
| POST | `/weather/search` | `{query, top_k, source_type?, location?, generate_summary?}` | `{status, results[], summary?}` |
| GET | `/weather/search` | `?query=&top_k=&source_type=&location=` | Same as POST |

---

## Testing

```bash
# Verify sync
curl -X POST .../weather/sync -H "Content-Type: application/json" \
  -d '{"locations": ["Chicago, IL"], "limit": 10}'
# → synced_count should be > 0

# Verify embedding count (after ingest script)
curl .../weather/search?query=weather  # should return results, not empty list

# Re-sync idempotency test (run twice, row count should not double)
curl -X POST .../weather/sync ... (repeat)
# Check: updated_documents increases, new_documents stays low

# Similarity sanity check
curl -X POST .../weather/search -d '{"query": "tornado"}'
# Top result should contain tornado-related text
```
