"""
Support Ticketing System + Weather Intelligence — Databricks App
Backed by Lakebase Postgres (psycopg2) + pgvector semantic search.

Ticketing routes:
  GET  /                          Serve UI
  GET  /healthz                   Health check
  GET  /tickets[?status=]         List tickets (optional status filter)
  POST /tickets                   Create ticket
  GET  /tickets/<id>              Get ticket + messages
  PATCH /tickets/<id>/status      Update ticket status
  POST /tickets/<id>/messages     Add message
  DELETE /tickets/<id>            Delete ticket (with cascade)
  GET  /stats                     Count tickets by status

Weather Intelligence routes (Homework 2):
  POST /weather/sync              Harvest NWS alerts + forecasts into Lakebase
  POST /weather/search            Semantic similarity search over weather docs
  GET  /weather/search            Same as POST (query param variant)
"""

import logging
import os
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase
import weather_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ticketing-app")

app = Flask(__name__)
_w = WorkspaceClient()

VALID_STATUSES   = ("open", "in_progress", "resolved")
VALID_PRIORITIES = ("low", "medium", "high")

# ── Embedding model (lazy-loaded once on first weather search) ──────────────────
_EMBED_MODEL = None

def _get_embed_model():
    """Load sentence-transformers model once and cache globally."""
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        from sentence_transformers import SentenceTransformer
        logger.info("Loading embedding model all-MiniLM-L6-v2 ...")
        _EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Embedding model ready")
    return _EMBED_MODEL


def _current_user() -> str:
    """Resolve caller identity from Databricks App header or SDK fallback."""
    return request.headers.get("X-Forwarded-Email") or _w.current_user.me().user_name


def ensure_tables():
    """Create tickets and ticket_messages tables if they don't exist."""
    lakebase.run_write("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id   SERIAL PRIMARY KEY,
            title       TEXT        NOT NULL,
            status      TEXT        NOT NULL DEFAULT 'open',
            priority    TEXT        NOT NULL DEFAULT 'medium',
            created_by  TEXT        NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    lakebase.run_write("""
        CREATE TABLE IF NOT EXISTS ticket_messages (
            message_id   SERIAL PRIMARY KEY,
            ticket_id    INTEGER     NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
            message_text TEXT        NOT NULL,
            author       TEXT        NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


# ── Health ────────────────────────────────────────────────────────────────────

@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"})


# ── UI ────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    ensure_tables()
    return render_template("index.html")


# ── Tickets ───────────────────────────────────────────────────────────────────

@app.route("/tickets", methods=["GET"])
def list_tickets():
    status = request.args.get("status", "").strip()
    if status and status not in VALID_STATUSES:
        return jsonify({"error": f"Invalid status: {status}"}), 400
    if status:
        rows = lakebase.run_query(
            "SELECT * FROM tickets WHERE status = %s ORDER BY created_at DESC",
            (status,)
        )
    else:
        rows = lakebase.run_query("SELECT * FROM tickets ORDER BY created_at DESC")
    return jsonify([dict(r) for r in rows])


@app.route("/tickets", methods=["POST"])
def create_ticket():
    data = request.get_json() or {}
    title = (data.get("title") or "").strip()
    priority = (data.get("priority") or "medium").strip()

    if not title:
        return jsonify({"error": "Title is required"}), 400
    if priority not in VALID_PRIORITIES:
        return jsonify({"error": f"Priority must be one of: {', '.join(VALID_PRIORITIES)}"}), 400

    created_by = _current_user()
    rows = lakebase.run_query(
        "INSERT INTO tickets (title, status, priority, created_by) "
        "VALUES (%s, 'open', %s, %s) RETURNING *",
        (title, priority, created_by)
    )
    return jsonify(dict(rows[0])), 201


@app.route("/tickets/<int:ticket_id>", methods=["GET"])
def get_ticket(ticket_id):
    tickets = lakebase.run_query(
        "SELECT * FROM tickets WHERE ticket_id = %s", (ticket_id,)
    )
    if not tickets:
        return jsonify({"error": "Ticket not found"}), 404
    messages = lakebase.run_query(
        "SELECT * FROM ticket_messages WHERE ticket_id = %s ORDER BY created_at ASC",
        (ticket_id,)
    )
    return jsonify({"ticket": dict(tickets[0]), "messages": [dict(m) for m in messages]})


@app.route("/tickets/<int:ticket_id>/status", methods=["PATCH"])
def update_status(ticket_id):
    data = request.get_json() or {}
    new_status = (data.get("status") or "").strip()
    if new_status not in VALID_STATUSES:
        return jsonify({"error": f"Status must be one of: {', '.join(VALID_STATUSES)}"}), 400
    updated = lakebase.run_write(
        "UPDATE tickets SET status = %s WHERE ticket_id = %s",
        (new_status, ticket_id)
    )
    if not updated:
        return jsonify({"error": "Ticket not found"}), 404
    return jsonify({"ticket_id": ticket_id, "status": new_status})


@app.route("/tickets/<int:ticket_id>/messages", methods=["POST"])
def add_message(ticket_id):
    data = request.get_json() or {}
    message_text = (data.get("message_text") or "").strip()
    if not message_text:
        return jsonify({"error": "Message text is required"}), 400

    exists = lakebase.run_query(
        "SELECT 1 FROM tickets WHERE ticket_id = %s", (ticket_id,)
    )
    if not exists:
        return jsonify({"error": "Ticket not found"}), 404

    author = _current_user()
    rows = lakebase.run_query(
        "INSERT INTO ticket_messages (ticket_id, message_text, author) "
        "VALUES (%s, %s, %s) RETURNING *",
        (ticket_id, message_text, author)
    )
    return jsonify(dict(rows[0])), 201


@app.route("/tickets/<int:ticket_id>", methods=["DELETE"])
def delete_ticket(ticket_id):
    deleted = lakebase.run_write(
        "DELETE FROM tickets WHERE ticket_id = %s", (ticket_id,)
    )
    if not deleted:
        return jsonify({"error": "Ticket not found"}), 404
    return jsonify({"deleted": ticket_id})


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.route("/stats")
def stats():
    rows = lakebase.run_query(
        "SELECT status, COUNT(*) AS count FROM tickets GROUP BY status"
    )
    totals = {s: 0 for s in VALID_STATUSES}
    for r in rows:
        totals[r["status"]] = r["count"]
    totals["total"] = sum(totals.values())
    return jsonify(totals)


# ── Weather: Sync ──────────────────────────────────────────────────────────────

DEFAULT_LOCATIONS = ["Chicago, IL", "Austin, TX", "Miami, FL", "Seattle, WA", "New York, NY"]

@app.route("/weather/sync", methods=["POST"])
def weather_sync():
    """
    POST /weather/sync
    Body: {"locations": ["Chicago, IL", "Austin, TX"], "limit": 50}
    Harvests NWS alerts + forecasts, upserts into weather_documents.
    Stretch Goal 2: ON CONFLICT DO UPDATE — re-sync never creates duplicates;
    changed narrative_text triggers deletion of stale embeddings.
    """
    data      = request.get_json() or {}
    locations = data.get("locations") or DEFAULT_LOCATIONS
    limit     = int(data.get("limit") or 50)

    # Validate
    if not isinstance(locations, list) or not locations:
        return jsonify({"status": "error", "message": "locations must be a non-empty list"}), 400
    limit = max(1, min(limit, 500))

    # Ensure tables exist
    lakebase.ensure_weather_tables()

    # Harvest
    try:
        docs = weather_client.fetch_weather_documents(locations, limit=limit)
    except Exception as exc:
        logger.exception("Weather harvest failed")
        return jsonify({"status": "error", "message": str(exc)}), 500

    if not docs:
        return jsonify({
            "status": "success", "synced_count": 0,
            "message": "No weather documents returned (no active alerts or forecast data)",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    # Upsert into Lakebase (with stale-embedding invalidation)
    try:
        counts = lakebase.upsert_weather_documents(docs)
    except Exception as exc:
        logger.exception("Weather upsert failed")
        return jsonify({"status": "error", "message": str(exc)}), 500

    alerts_count    = sum(1 for d in docs if d["source_type"] == "alert")
    forecasts_count = sum(1 for d in docs if d["source_type"] == "forecast")

    return jsonify({
        "status":        "success",
        "synced_count":  len(docs),
        "message":       f"Synced {len(docs)} weather documents",
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "data": {
            "alerts":            alerts_count,
            "forecasts":         forecasts_count,
            "new_documents":     counts.get("inserted", 0),
            "updated_documents": counts.get("updated", 0),
        },
    })


# ── Weather: Search ─────────────────────────────────────────────────────────────

@app.route("/weather/search", methods=["POST", "GET"])
def weather_search():
    """
    POST /weather/search
    Body: {"query": "flash flood risk", "top_k": 5,
           "source_type": "alert",  # optional filter
           "location": "Chicago, IL",  # optional filter
           "generate_summary": true}   # optional LLM summary (Stretch Goal 1)

    GET  /weather/search?query=flooding&top_k=5&source_type=alert

    Embeds query with all-MiniLM-L6-v2 and runs pgvector cosine similarity search.
    """
    # Accept both POST JSON body and GET query params
    if request.method == "POST":
        body = request.get_json() or {}
    else:
        body = request.args

    raw_query = (body.get("query") or "").strip()
    if not raw_query:
        return jsonify({"status": "error", "message": "query is required"}), 400
    if len(raw_query) > 1000:
        return jsonify({"status": "error",
                        "message": "query must be 1–1000 characters"}), 400

    try:
        top_k = int(body.get("top_k") or 5)
        top_k = max(1, min(top_k, 20))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "top_k must be an integer 1–20"}), 400

    source_type      = (body.get("source_type") or "").strip() or None
    location_filter  = (body.get("location") or "").strip() or None
    generate_summary = str(body.get("generate_summary") or "").lower() in ("true", "1", "yes")

    # Check embeddings table is non-empty
    count_rows = lakebase.run_query("SELECT COUNT(*) AS n FROM weather_embeddings")
    if not count_rows or count_rows[0]["n"] == 0:
        return jsonify({
            "status":        "success",
            "query":         raw_query,
            "results_count": 0,
            "results":       [],
            "message":       "No weather documents indexed yet. Run POST /weather/sync then the embedding ingestion script.",
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        })

    # Embed query
    try:
        model     = _get_embed_model()
        query_vec = model.encode([raw_query])[0].tolist()
        vec_str   = "[" + ",".join(f"{v:.8f}" for v in query_vec) + "]"
    except Exception as exc:
        logger.exception("Embedding model error")
        return jsonify({"status": "error", "message": f"Embedding failed: {exc}"}), 500

    # Build cosine similarity query with optional filters
    where_clauses = []
    params: list = []
    if source_type:
        where_clauses.append("d.source_type = %s")
        params.append(source_type)
    if location_filter:
        where_clauses.append("d.location ILIKE %s")
        params.append(f"%{location_filter}%")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # pgvector <=> = cosine distance; 1 - distance = similarity
    sql = f"""
        SELECT
            d.id            AS document_id,
            d.location,
            d.source_type,
            d.headline,
            d.issued_at,
            e.chunk_text,
            1 - (e.embedding <=> %s::vector) AS similarity_score
        FROM weather_embeddings e
        JOIN weather_documents  d ON d.id = e.document_id
        {where_sql}
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
    """
    # vec_str appears twice (SELECT + ORDER BY) then top_k
    query_params = [vec_str] + params + [vec_str, top_k]

    try:
        rows = lakebase.run_query(sql, query_params)
    except Exception as exc:
        logger.exception("Vector search failed")
        return jsonify({"status": "error", "message": str(exc)}), 500

    results = [
        {
            "rank":             i + 1,
            "document_id":      r["document_id"],
            "location":         r["location"],
            "source_type":      r["source_type"],
            "headline":         r["headline"],
            "chunk_text":       r["chunk_text"],
            "similarity_score": round(float(r["similarity_score"]), 4),
            "issued_at":        r["issued_at"].isoformat() if r["issued_at"] else None,
        }
        for i, r in enumerate(rows)
    ]

    response = {
        "status":        "success",
        "query":         raw_query,
        "top_k":         top_k,
        "results_count": len(results),
        "results":       results,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

    # Stretch Goal 1: LLM-generated summary using Databricks Foundation Models
    if generate_summary and results:
        try:
            context = "\n\n".join(r["chunk_text"] for r in results[:3])
            prompt  = (
                f'Based on these weather documents, provide a brief (2–3 sentence) '
                f'summary of the key weather risks or conditions relevant to '
                f'\"{ raw_query }\":\n\n{context}'
            )
            completion = _w.serving_endpoints.query(
                name="databricks-meta-llama-3-3-70b-instruct",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
            )
            summary_text = completion.choices[0].message.content.strip()
            response["summary"] = summary_text
        except Exception as exc:
            logger.warning("LLM summary failed (non-fatal): %s", exc)
            response["summary"] = None

    return jsonify(response)


# ── Error handler ──────────────────────────────────────────────────────────────

@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception")
    code = getattr(err, "code", 500)
    if not isinstance(code, int):
        code = 500
    return jsonify({"error": str(err)}), code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
