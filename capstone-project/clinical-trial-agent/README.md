# Clinical Trial Matching & Recruitment Agent

AI-powered system that matches patients to clinical trials using semantic search, LLM reasoning, drug interaction safety checks, and PubMed evidence retrieval — with a Streamlit frontend and full LLMOps observability.

---

## Quick Deploy to Any Databricks Workspace

### Prerequisites
- Databricks workspace with **Lakebase** enabled
- Python compute (Serverless or cluster with `databricks-sdk >= 0.118.0`)
- No API keys required (ClinicalTrials.gov and PubMed are free)

### One-Command Deployment

```bash
# From a Databricks notebook or terminal:
python deploy_infrastructure.py
```

This creates:
- A Lakebase project (`clinical-trial-agent`) with PostgreSQL 17
- pgvector extension (384-dim embeddings)
- 13 tables (8 core + 2 vector + 3 LLMOps)
- 6 indexes for query performance

---

## Project Structure

```
clinical-trial-agent/
├── README.md                   # This file
├── config.py                   # Shared config with get_lakebase_connection()
├── deploy_infrastructure.py    # One-shot idempotent deployment
└── app/
    ├── app.py                  # Streamlit frontend (5 tabs)
    ├── app.yaml                # Databricks App config
    └── requirements.txt        # App dependencies
```

Main notebook (all 8 phases implemented as sequential cells):
```
Capstone Project - Clinical Trial Matching Agent.py
```

---

## Implementation Phases (All Complete ✅)

### Phase 1: Lakebase Infrastructure
- Created Lakebase project with PostgreSQL 17 + pgvector
- 13 tables: `patients`, `patient_conditions`, `clinical_trials`, `trial_eligibility_criteria`, `trial_documents`, `patient_trial_matches`, `enrollment_recommendations`, `patient_communications`, `trial_eligibility_embeddings`, `pubmed_embeddings`, `agent_traces`, `agent_feedback`, `agent_prompts`
- 6 indexes for query performance
- Idempotent `deploy_infrastructure.py` for portable deployment

### Phase 2: ClinicalTrials.gov ETL
- Ingested **747 trials** across 5 disease areas (T2D, Breast Cancer, COPD, Lupus, Alzheimer's)
- Parsed **1,447 eligibility criteria** (inclusion + exclusion split)
- API: `https://clinicaltrials.gov/api/v2/studies` (no auth, paginated)

### Phase 3: PubMed Literature ETL
- Ingested **289 articles** from PubMed/MEDLINE
- Stored title, abstract, MeSH terms, publication date
- API: eutils (esearch + efetch XML)

### Phase 4: Embedding Generation (pgvector)
- Generated **4,322 eligibility criteria vectors** (500-char chunks, normalized cosine)
- Generated **289 PubMed abstract vectors**
- Model: `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)
- Cosine similarity search verified (top result: 0.649 similarity)

### Phase 5: AI Agent + MLflow Tracing
- **6 Agent Tools**:
  1. `semantic_trial_search` — pgvector cosine search on eligibility embeddings
  2. `structured_trial_filter` — SQL filter by condition/phase/status/age/gender
  3. `check_drug_interactions` — FDA Drug Label API safety check
  4. `retrieve_pubmed_evidence` — pgvector search on PubMed embeddings
  5. `score_patient_trial_match` — Store match with confidence + reasoning
  6. `generate_enrollment_recommendation` — Create actionable next steps
- **LLM**: `databricks-llama-4-maverick` via Foundation Model API (OpenAI-compatible)
- **MLflow Tracing**: `@mlflow.trace` decorator with per-span instrumentation
- **5 Synthetic Patients**: Maria Garcia (T2D), James Thompson (NSCLC), Sarah Chen (Lupus), Robert Williams (Alzheimer's), Angela Johnson (TNBC)
- Agent logs to `agent_traces` table for LLMOps monitoring

### Phase 6: Databricks App (Streamlit)
- **5-Tab Frontend**:
  1. 👤 Patient Match — Select patient → view AI-matched trials → approve/reject
  2. 🔍 Trial Browser — Filter by status/phase, text search across 747 trials
  3. 📄 Upload Records — Vision AI medical image/PDF upload
  4. 📊 Eval Dashboard — Key metrics, agent latency, confidence distribution
  5. ⚙️ Admin / LLMOps — Agent traces, prompt versions, human feedback loop
- OAuth-based Lakebase connection (no credentials needed)
- Deploy: `databricks apps create clinical-trial-agent && databricks apps deploy clinical-trial-agent --source-code-path ./app`

### Phase 7: Vision Model Integration
- **ai_query (multimodal)**: Analyze X-ray images → extract findings → trial keywords
- **ai_parse_document v2**: Parse PDF medical records → structured VARIANT
- **ai_extract v2**: Extract diagnosis, tumor markers, lab values from reports
- **End-to-end pipeline**: Image → Extract findings → Build patient description → Semantic trial search → Evidence retrieval
- Demo: Chest X-ray (8mm bilateral pulmonary nodules) → NSCLC trial match (similarity 0.587)

### Phase 8: Evaluation & Testing
- **4-Level Evaluation Framework**:
  - Level 1: End-to-end agent (5 patients, 100% precision, 5/5 success)
  - Level 2: Retrieval quality (P@10: 74% avg, best: 100% for Alzheimer's/TNBC)
  - Level 3: Safety guardrails (100% NCT integrity, drug interactions detected, confidence [0.40–0.85])
  - Level 4: MLflow experiment logging (all metrics + params + eval_report.json artifact)

---

## Configuration

All settings live in `config.py`. After running `deploy_infrastructure.py`, the host is auto-populated:

```python
LAKEBASE_CONFIG = {
    "project_id": "clinical-trial-agent",
    "branch_id": "production",
    "endpoint_id": "primary",
    "host": "ep-cool-thunder-d1jvz502.database.us-west-2.cloud.databricks.com",
    "database": "databricks_postgres",
    "port": 5432,
}
```

### Authentication Pattern
```python
from databricks.sdk import WorkspaceClient
import psycopg2

w = WorkspaceClient()
cred = w.postgres.generate_database_credential(
    endpoint=f"projects/{PROJECT_ID}/branches/production/endpoints/primary"
)
conn = psycopg2.connect(
    host=HOST, port=5432, dbname="databricks_postgres",
    user=w.current_user.me().user_name,  # Must use email, not "oauth"
    password=cred.token, sslmode="require"
)
```

---

## Tech Stack

| Component | Technology |
| --- | --- |
| Database | Lakebase (PostgreSQL 17 + pgvector) |
| Vector Search | pgvector cosine similarity (384-dim) |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 |
| Agent | LangChain + databricks-llama-4-maverick |
| Vision | ai_parse_document v2 + ai_query (multimodal) |
| Frontend | Streamlit (Databricks App, 5 tabs) |
| Observability | MLflow Tracing (@mlflow.trace) |
| Evaluation | 4-level framework (precision, retrieval, safety, MLflow) |
| Safety | FDA Drug API, NCT integrity checks, confidence bounds |

---

## Data Sources (No Auth Required)

| API | Records Ingested | Purpose |
| --- | --- | --- |
| ClinicalTrials.gov v2 | 747 trials | Trial metadata + eligibility criteria |
| PubMed/MEDLINE (eutils) | 289 articles | Evidence retrieval for matches |
| FDA Drug Labels (OpenAPI) | On-demand | Drug interaction safety checks |

---

## Evaluation Results

| Metric | Value |
| --- | --- |
| Agent Precision@k | **100%** |
| Success Rate | 5/5 patients |
| Avg Matches/Patient | 2.8 |
| Retrieval P@10 | 74% |
| NCT ID Integrity | 100% |
| Confidence Bounds | [0.40, 0.85] ✅ |
| Avg Latency | ~10s |
| Tools per Run | 11 |
| MLflow Experiment | `clinical-trial-agent-eval` |

---

## External APIs Used

| API | Endpoint | Auth |
| --- | --- | --- |
| ClinicalTrials.gov v2 | `https://clinicaltrials.gov/api/v2/studies` | None |
| PubMed Search | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi` | None |
| PubMed Fetch | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi` | None |
| FDA Drug Label | `https://api.fda.gov/drug/label.json` | None |
| Databricks Foundation Models | `{workspace_host}/serving-endpoints` | OAuth (auto) |
