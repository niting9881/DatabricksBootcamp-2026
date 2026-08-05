"""
Support Ticketing System — Databricks App
Backed by Lakebase Postgres via lakebase.py

Routes:
  GET  /                          Serve UI
  GET  /healthz                   Health check
  GET  /tickets[?status=]         List tickets (optional status filter)
  POST /tickets                   Create ticket
  GET  /tickets/<id>              Get ticket + messages
  PATCH /tickets/<id>/status      Update ticket status
  POST /tickets/<id>/messages     Add message
  DELETE /tickets/<id>            Delete ticket (with cascade)
  GET  /stats                     Count tickets by status
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template, request

import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ticketing-app")

app = Flask(__name__)
_w = WorkspaceClient()

VALID_STATUSES = ("open", "in_progress", "resolved")
VALID_PRIORITIES = ("low", "medium", "high")


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


# ── Error handler ─────────────────────────────────────────────────────────────

@app.errorhandler(Exception)
def handle_exception(err):
    logger.exception("Unhandled exception")
    code = getattr(err, "code", 500)
    if not isinstance(code, int):
        code = 500
    return jsonify({"error": str(err)}), code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
