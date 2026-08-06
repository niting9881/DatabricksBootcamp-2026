# Support Ticketing System

A Databricks App-powered internal support ticketing system backed by **Lakebase Postgres** (Databricks-managed OLTP database). Built as Day 1 homework for the DataExpert.io boot camp.

---

## Live App

**URL:** https://support-ticketing-7474648653109871.aws.databricksapps.com

---

## Project Overview

Users can create support tickets, add threaded messages, track resolution status, and filter tickets by status — all backed by a live Postgres database. Every read and write goes through Lakebase; no data is hard-coded.

### Features

| Feature | Description |
|---------|-------------|
| View tickets | List all tickets with status + priority badges |
| Filter by status | Dropdown to filter `open`, `in_progress`, `resolved` |
| Ticket detail | Click a ticket to view its full message thread |
| Create ticket | Modal form with title + priority |
| Add message | Reply to any ticket |
| Update status | Change status inline from the detail panel |
| Delete ticket | Delete with browser confirmation dialog |
| Stats bar | Live counts of tickets per status at the top |
| Input validation | Client-side + server-side checks with error messages |

---

## Architecture

```
 Browser (Bootstrap 5 SPA)
       │
       │  fetch() — JSON API calls
       │
 ┌─────────────────────────┐
 │  Databricks App           │
 │  Flask (Python 3.11)      │
 │  app.py — 8 REST routes   │
 └─────────────────────────┘
       │
       │  psycopg2 (static password URL)
       │
 ┌─────────────────────────┐
 │  Lakebase Postgres        │
 │  Project: stock-watchlist │
 │  DB: databricks_postgres  │
 │  Branch: production       │
 └─────────────────────────┘
       │
       │  Lakebase CDF (WAL streaming)
       │
 ┌─────────────────────────┐
 │  Unity Catalog (Delta)    │
 │  cdf_catalog.lakebase_cdf │
 │  s3://databricks-external3257 │
 └─────────────────────────┘
```

---

## Data Flow

### Write path (user action → Lakebase)
1. User submits a form in the browser (create ticket, add message, update status)
2. Browser calls a Flask REST endpoint via `fetch()` with a JSON body
3. Flask validates the input server-side (status enum, non-empty fields)
4. `lakebase.py` opens a psycopg2 connection using the `LAKEBASE_URL` environment variable
5. The SQL is executed and committed; the response row is returned as JSON
6. Browser updates the UI without a page reload

### Read path (Lakebase → browser)
1. Browser calls `GET /tickets` or `GET /tickets/<id>`
2. Flask queries Lakebase and serializes the rows to JSON
3. Browser renders the ticket list and detail panel

### CDC path (Lakebase → Unity Catalog)
1. **Lakebase CDF** (powered by the `wal2delta` Postgres extension) reads the Postgres Write-Ahead Log
2. Every INSERT / UPDATE / DELETE on `tickets` and `ticket_messages` is captured
3. Changes are written as Delta tables to `cdf_catalog.lakebase_cdf` on S3 (`s3://databricks-external3257`)
4. Delta tables can be queried with Spark SQL for analytics, auditing, or AI/ML pipelines

---

## Project Structure

```
support-ticketing/
├── app.py               # Flask application — all routes and business logic
├── app.yaml             # Databricks App config (command + env vars)
├── lakebase.py          # Postgres connection helper via psycopg2
├── requirements.txt     # Python dependencies
├── README.md            # This file
└── templates/
    └── index.html         # Bootstrap 5 single-page frontend
```

---

## API Routes

| Method | Route | Description |
|--------|-------|-------------|
| `GET` | `/` | Serve the UI |
| `GET` | `/healthz` | Health check |
| `GET` | `/tickets` | List all tickets (optional `?status=` filter) |
| `POST` | `/tickets` | Create a new ticket |
| `GET` | `/tickets/<id>` | Get a ticket and all its messages |
| `PATCH` | `/tickets/<id>/status` | Update ticket status |
| `POST` | `/tickets/<id>/messages` | Add a message to a ticket |
| `DELETE` | `/tickets/<id>` | Delete a ticket (cascades to messages) |
| `GET` | `/stats` | Ticket counts grouped by status |

---

## Database Schema

```sql
-- Tickets (operational OLTP table in Lakebase Postgres)
CREATE TABLE tickets (
    ticket_id   SERIAL PRIMARY KEY,
    title       TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'open',   -- open | in_progress | resolved
    priority    TEXT        NOT NULL DEFAULT 'medium', -- low | medium | high
    created_by  TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Messages (threaded replies, FK to tickets)
CREATE TABLE ticket_messages (
    message_id   SERIAL PRIMARY KEY,
    ticket_id    INTEGER     NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
    message_text TEXT        NOT NULL,
    author       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Lakebase Endpoint

| Property | Value |
|----------|-------|
| Host | `ep-polished-forest-d2ql5e9j.database.us-east-1.cloud.databricks.com` |
| Database | `databricks_postgres` |
| Schema | `public` |
| Role | `student` (native password login) |
| Branch | `production` |
| Project | `stock-watchlist` |

---

## Lakebase CDF (Change Data Feed)

Both tables stream to Unity Catalog via Lakebase CDF:

| Property | Value |
|----------|-------|
| Source schema | `databricks_postgres.public` |
| Destination catalog | `cdf_catalog` |
| Destination schema | `lakebase_cdf` |
| Storage | `s3://databricks-external3257` |
| Status | `STREAMING` |
| Delta tables | `lb_watchlist_history`, `lb_tickets_history`, `lb_ticket_messages_history` |

> **Prerequisite:** Both tables must have `REPLICA IDENTITY FULL` set before CDF is started. This was set with `ALTER TABLE tickets REPLICA IDENTITY FULL` and `ALTER TABLE ticket_messages REPLICA IDENTITY FULL`.

---

## Credentials & Security

- The Lakebase connection string is stored in `app.yaml` as an environment variable (`LAKEBASE_URL`) — never in source code
- The app uses the Databricks Apps `X-Forwarded-Email` header to resolve the current user’s identity without any manual auth logic
- No passwords or secrets are stored in any source file — the Lakebase URL is read from a **Databricks Secret** at runtime
- `app.yaml` passes only `LAKEBASE_SECRET_SCOPE=lakebase` and `LAKEBASE_SECRET_KEY=url` (plain names, no credentials)
- **Setup (one-time):** `databricks secrets create-scope lakebase` then `databricks secrets put-secret lakebase url --string-value "<connection-url>"`
- **Safe to commit and submit:** no file in this repository contains a password, token, or secret value

---

## Key Challenges Encountered

### 1. Lakebase endpoint did not exist
The GitHub reference app had a placeholder `LAKEBASE_URL` pointing to an endpoint in a different workspace. A new Lakebase Autoscaling project (`stock-watchlist`) had to be provisioned from scratch and `app.yaml` updated with the new endpoint host.

### 2. Native Postgres password login disabled by default
Lakebase projects default to OAuth-only authentication. The `student` role with a static password requires `enable_pg_native_login=True` on the project, which had to be explicitly set via the SDK before the connection string would work.

### 3. Lakebase CDF rejected Databricks-managed storage
CDF requires the destination Unity Catalog catalog to be backed by user-controlled external storage. The workspace had only Databricks-managed storage buckets. This required:
- Creating an AWS IAM role (`arn:aws:iam::432680881096:role/databricks-uc-cdf-role`)
- A 3-statement IAM trust policy (Databricks UC master role + Unity Catalog IAM ARN + self-assume)
- Registering a UC storage credential and external location
- Creating a new catalog (`cdf_catalog`) backed by `s3://databricks-external3257`

### 4. REPLICA IDENTITY FULL must be set before starting CDF
CDF skipped the `watchlist` table silently because `REPLICA IDENTITY` was still at `DEFAULT` when CDF first started. The `wal2delta.tables` diagnostic query revealed `status: SKIPPED / Does not have REPLICA IDENTITY FULL`. Fix: set `FULL` on all tables, then stop and restart CDF.

### 5. IAM role self-assume requirement
The storage credential validation failed with “non self-assuming” until a third trust statement was added allowing the role to assume itself (`sts:AssumeRole` on its own ARN). This is a Databricks Unity Catalog security requirement.

---

## Reflection

**What was the most difficult part?**

The most challenging aspect was setting up the Lakebase Change Data Feed (CDF) pipeline to stream Postgres changes into Unity Catalog Delta tables. CDF requires the destination catalog to be backed by user-managed external S3 storage, which involved provisioning an AWS IAM role with a three-statement trust policy, registering a Unity Catalog storage credential and external location, and creating a brand-new catalog from scratch — none of which was needed for the application itself. A particularly hard-to-diagnose issue was that `REPLICA IDENTITY FULL` must be set on source tables *before* CDF starts; if set afterwards, CDF silently marks the table as `SKIPPED` with no visible error, and the only fix is to stop and fully restart the CDF pipeline. Debugging this required querying the internal `wal2delta.tables` diagnostic view, which surfaced the `status: SKIPPED / Does not have REPLICA IDENTITY FULL` root cause.

**How is Lakebase different from storing this data in a traditional analytics table?**

Lakebase is a fully managed, transactional Postgres database built for low-latency row-level reads and writes — exactly the workload that powers web applications and REST APIs. A traditional Unity Catalog Delta table is a columnar, append-optimised store designed for large-scale analytical queries, batch pipelines, and machine learning, not for sub-millisecond point lookups or enforcing relational constraints. Lakebase natively supports ACID transactions, serial primary keys, foreign key constraints with `ON DELETE CASCADE`, and standard Postgres drivers (psycopg2, JDBC) without needing Spark at all. The real power comes from combining both: operational data lives in Lakebase for the application layer, while Lakebase CDF continuously replicates every change to Delta tables in Unity Catalog, making the same data available for real-time application reads *and* full analytical SQL queries.

**What feature would you add next?**

The next feature I would add is ticket assignment with automated notifications — each ticket would gain an `assigned_to` column linking it to a support agent, and a Lakeflow Job triggered by the Lakebase CDF Delta table would send an email alert whenever a ticket is assigned or its status changes. This would close the loop between the transactional application layer and the Databricks data platform, turning the CDF change stream into an active event trigger rather than a passive audit log. Longer term, I would integrate Databricks AI Functions (`ai_classify`) to automatically categorise and prioritise incoming tickets based on their description, routing high-priority issues to senior agents without any manual triage.

---

## Dependencies

```
databricks-sdk>=0.30.0
psycopg2-binary>=2.9.9
sqlalchemy>=2.0.30
flask>=3.0.3
requests>=2.32.3
python-dotenv>=1.0.1
```

---

## Deployment

```python
# Deploy via Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment, AppDeploymentMode

w = WorkspaceClient()
w.apps.deploy(
    app_name="support-ticketing",
    app_deployment=AppDeployment(
        source_code_path="/Workspace/Users/<your-email>/support-ticketing",
        mode=AppDeploymentMode.SNAPSHOT,
    )
).result()
```
