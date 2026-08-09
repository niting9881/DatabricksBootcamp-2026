# Clinical Trial Matching & Recruitment Agent

> AI-powered system that matches patients to clinical trials using semantic search (pgvector),
> LLM reasoning (Llama 4 Maverick), drug interaction safety checks, and PubMed evidence retrieval —
> with a Streamlit frontend deployed as a Databricks App.

| | |
|---|---|
| **Live App** | https://clinical-trial-agent-7474648653109871.aws.databricksapps.com |
| **GitHub Repository** | https://github.com/niting9881/DatabricksBootcamp-2026 |
| **MLflow Experiment** | `clinical-trial-agent-eval` |
| **Bootcamp** | Databricks Bootcamp 2026 — Capstone Project |
| **Status** | All 5 rubric requirements met ✅ |

---

## Capstone Requirements Coverage

| Requirement | Implementation | Status |
|-------------|----------------|--------|
| **Spark Data Pipeline** | Bronze→Silver→Gold medallion across 3 APIs; 4 Delta tables; Spark SQL quality reports | ✅ |
| **Third-Party API Integration** | ClinicalTrials.gov v2, PubMed NCBI eutils, FDA Drug Labels OpenAPI | ✅ |
| **Unstructured Data Processing** | 4,611 pgvector embeddings (eligibility criteria + PubMed abstracts); vision AI (X-ray + PDF parsing) | ✅ |
| **Databricks App (Frontend)** | 5-tab Streamlit app: Patient Match, Trial Browser, Upload Records, Eval Dashboard, Admin LLMOps | ✅ |
| **AI Agent with Tools** | LangChain agent + 6 tools (semantic search, SQL filter, drug safety, evidence retrieval, scoring, recommendations) | ✅ |

---

## Architecture and Data Flow

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS PLATFORM                                       │
│                                                                                  │
│  External APIs          Spark Medallion ETL           Unity Catalog (Delta Lake) │
│  ┌────────────────┐    ┌──────────────────────┐      ┌──────────────────────┐   │
│  │ ClinicalTrials │───>│ Bronze  (raw schema) │      │ ct_trials_silver     │   │
│  │ .gov v2 API    │    │   ↓ trim/cast/dedup  │─────>│ pubmed_articles_slvr │   │
│  ├────────────────┤    │ Silver  (validated)  │      │ fda_drug_labels_slvr │   │
│  │ PubMed NCBI    │───>│   ↓ 3-way join       │      │ disease_area_summary │   │
│  │ E-utilities    │    │ Gold    (enriched)   │      └──────────────────────┘   │
│  ├────────────────┤    └──────────────────────┘                                 │
│  │ FDA Drug       │                                                              │
│  │ Labels OpenAPI │    Lakebase (PostgreSQL 17 + pgvector)                      │
│  └────────────────┘    ┌────────────────────────────────────────────────────┐   │
│                        │ 13 tables  |  384-dim embeddings  |  agent_traces  │   │
│                        │  ↑                                                  │   │
│                        │  └── Delta → Lakebase sync (Phase E)               │   │
│                        └──────────────────┬─────────────────────────────────┘   │
│                                           │                                      │
│                        ┌──────────────────▼──────────────────────────────────┐  │
│                        │  AI Agent  (LangChain + Llama 4 Maverick)           │  │
│                        │  ┌─────────────────────────────────────────────┐   │  │
│                        │  │  Tool 1: semantic_trial_search  (pgvector)  │   │  │
│                        │  │  Tool 2: structured_trial_filter (SQL)      │   │  │
│                        │  │  Tool 3: check_drug_interactions (FDA API)  │   │  │
│                        │  │  Tool 4: retrieve_pubmed_evidence (vector)  │   │  │
│                        │  │  Tool 5: score_patient_trial_match  [WRITE] │   │  │
│                        │  │  Tool 6: generate_enrollment_rec   [WRITE] │   │  │
│                        │  └─────────────────────────────────────────────┘   │  │
│                        │  MLflow Tracing → agent_traces table               │  │
│                        └──────────────────┬─────────────────────────────────┘  │
│                                           │                                      │
│                        ┌──────────────────▼──────────────────────────────────┐  │
│                        │  Databricks App (Streamlit, 5 Tabs)                 │  │
│                        │  Tab 1: Patient Match  | Tab 2: Trial Browser        │  │
│                        │  Tab 3: Upload Records | Tab 4: Eval Dashboard       │  │
│                        │  Tab 5: Admin / LLMOps                               │  │
│                        └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### End-to-End Data Flow

1. **Ingest** — Three external APIs are called: ClinicalTrials.gov (747 trials), PubMed (289 articles), FDA Drug Labels (on-demand safety data).
2. **Spark ETL** — Each source goes through a Bronze→Silver→Gold medallion pipeline. Bronze preserves the raw API schema. Silver applies trimming, casting, deduplication, and derived fields (age ranges, safety flags, evidence levels). Gold joins all three Silver tables on `disease_area` to produce an enriched summary.
3. **Lakebase Load** — Silver/Gold Delta tables are synced back into Lakebase (PostgreSQL 17). The `trial_eligibility_criteria` and `pubmed_embeddings` tables receive 384-dimensional pgvector embeddings generated by `all-MiniLM-L6-v2`.
4. **Agent Reasoning** — When a user submits a patient profile, the LangChain agent executes up to 11 tool calls: semantic vector search → SQL filter → drug interaction check → PubMed evidence retrieval → confidence scoring → recommendation write-back.
5. **Frontend** — The Streamlit app reads match results, trial details, evaluation metrics, and agent traces from Lakebase in real time. The Upload Records tab sends medical images/PDFs through `ai_query` for structured extraction before matching.
6. **Observability** — Every agent run is logged to `agent_traces` in Lakebase and to an MLflow experiment for latency, precision, and confidence tracking.

---

## Repository Structure

```
capstone-project/
├── README.md                                                   # This file
├── requirements.txt                                            # All Python dependencies
├── .gitignore
│
├── Capstone Project - Clinical Trial Matching Agent.ipynb      # Main notebook (Phases 1-8)
├── Capstone - Gap Implementation (Spark ETL + App Deploy).ipynb  # Spark ETL + App Deploy
│
└── clinical-trial-agent/
    ├── README.md                     # Detailed deployment & API docs
    ├── config.py                     # Shared config (Lakebase, models, APIs)
    ├── deploy_infrastructure.py      # One-shot idempotent deployment script
    └── app/
        ├── app.py                    # Streamlit frontend (5 tabs)
        ├── app.yaml                  # Databricks App manifest
        └── requirements.txt          # App dependencies
```

---

## Notebooks

### `Capstone Project - Clinical Trial Matching Agent` (Phases 1–8)

| Phase | Description | Key Output |
|-------|-------------|------------|
| 1 | Lakebase infrastructure | 13 tables + pgvector extension |
| 2 | ClinicalTrials.gov ETL | 747 trials + 1,447 eligibility criteria |
| 3 | PubMed literature ETL | 289 articles (title, abstract, MeSH) |
| 4 | Embedding generation | 4,611 vectors (384-dim, cosine similarity) |
| 5 | AI Agent (LangChain + 6 tools) | Per-patient trial matching with MLflow traces |
| 6 | Databricks App deployment | Streamlit frontend live at app URL |
| 7 | Vision model integration | X-ray analysis + PDF parsing -> trial keywords |
| 8 | MLflow evaluation | 4-level eval framework, P@10 74%, experiment logged |

### `Capstone - Gap Implementation (Spark ETL + App Deploy)` (Phases A–F)

| Phase | Description | Key Output |
|-------|-------------|------------|
| A | ClinicalTrials.gov -> Spark ETL | Bronze/Silver Delta (22-col schema) |
| B | PubMed -> Spark ETL | Bronze/Silver Delta (evidence_level, is_rct flags) |
| C | FDA Drug Labels -> Spark ETL | Bronze/Silver Delta (safety flags: renal/hepatic/cardiac) |
| D | 3-way cross-source Spark join | Gold `disease_area_summary` Delta table |
| E | Delta -> Lakebase | fda_drug_labels + disease_summary loaded into Postgres |
| F | App deployment via SDK | `clinical-trial-agent` ACTIVE |

---

## Quick Start

### Prerequisites
- Databricks workspace with **Lakebase** enabled
- Serverless or Standard compute (Python 3.9+)
- No external API keys required (all 3 APIs are free/open)

### Run the Main Notebook
1. Open `Capstone Project - Clinical Trial Matching Agent` in Databricks
2. Run all cells in order — Phase 1 auto-creates all Lakebase infrastructure
3. Each phase is idempotent (safe to re-run)

### Run the Spark ETL Notebook
1. Open `Capstone - Gap Implementation (Spark ETL + App Deploy)`
2. Run all cells — demonstrates Spark medallion pipeline end-to-end

### Deploy the App
```bash
databricks apps deploy clinical-trial-agent \
  --source-code-path /Workspace/Users/<your-email>/capstone-project/clinical-trial-agent/app
```

---

## AI Agent — 6 Tools

| Tool | Type | Description |
|------|------|-------------|
| `semantic_trial_search` | Read | pgvector cosine search on eligibility embeddings |
| `structured_trial_filter` | Read | SQL filter by condition/phase/status/age/gender |
| `check_drug_interactions` | Read | FDA Drug Label API safety check |
| `retrieve_pubmed_evidence` | Read | pgvector search on PubMed abstracts |
| `score_patient_trial_match` | **Write** | Store match with confidence score + reasoning |
| `generate_enrollment_recommendation` | **Write** | Create actionable next-step recommendations |

---

## Evaluation

The project uses a **4-level evaluation framework** logged to an MLflow experiment (`clinical-trial-agent-eval`).

### Level 1 — End-to-End Agent Evaluation (5 Patients)

| Patient | Condition | Trials Found | Safe Matches | Result |
|---------|-----------|-------------|-------------|--------|
| Maria Garcia, 58 | Type 2 Diabetes | 8 | 3 | ✅ Pass |
| James Thompson, 67 | Non-Small Cell Lung Cancer | 6 | 2 | ✅ Pass |
| Sarah Chen, 34 | Systemic Lupus Erythematosus | 7 | 3 | ✅ Pass |
| Robert Williams, 72 | Alzheimer's Disease | 5 | 2 | ✅ Pass |
| Angela Johnson, 45 | Triple-Negative Breast Cancer | 9 | 4 | ✅ Pass |

### Level 2 — Retrieval Quality (Precision@10)

| Query | P@10 | Top Similarity |
|-------|------|----------------|
| T2D + Hypertension | 70% | 0.649 |
| Systemic Lupus Erythematosus | 80% | 0.631 |
| Alzheimer's Disease | 100% | 0.687 |
| Triple-Negative Breast Cancer | 100% | 0.712 |
| COPD + Eosinophilia | 20% | 0.521 |
| **Average** | **74%** | **0.660** |

### Level 3 — Safety Guardrails

| Check | Result |
|-------|--------|
| NCT ID integrity (no hallucinated IDs) | 100% ✅ |
| Drug interaction detection | Detected in 2/5 patients ✅ |
| Confidence score within bounds [0.40, 0.85] | 100% ✅ |

### Level 4 — MLflow Metrics Summary

| Metric | Value |
|--------|-------|
| Agent Precision@k | **100%** |
| Success Rate | 5/5 patients |
| Avg Matches per Patient | 2.8 |
| Retrieval P@10 | 74% avg |
| NCT ID Integrity | 100% |
| Confidence Bounds | [0.40, 0.85] |
| Avg Agent Latency | ~10s |
| Tools per Run | 11 |

All metrics, parameters, and an `eval_report.json` artifact are logged under the MLflow experiment `clinical-trial-agent-eval`.

---

## Technology Stack

| Layer | Component | Technology | Version / Detail |
|-------|-----------|------------|------------------|
| **Compute** | Notebook runtime | Databricks Serverless | Python 3.12, CPU |
| **Database** | Transactional store | Lakebase (PostgreSQL 17) | pgvector extension enabled |
| **Data Pipeline** | ETL framework | Apache Spark (PySpark) | Bronze → Silver → Gold medallion |
| **Delta Lake** | Storage format | Delta Lake (Unity Catalog) | Overwrite-safe, schema enforcement |
| **Vector Search** | Similarity engine | pgvector | Cosine similarity, 384-dim |
| **Embeddings** | Sentence encoding | sentence-transformers | `all-MiniLM-L6-v2` (384-dim) |
| **LLM — Agent** | Reasoning model | Llama 4 Maverick | Via Databricks Foundation Model API |
| **LLM — Vision** | Multimodal model | Llama 4 Maverick | `ai_query`, `ai_parse_document v2` |
| **Agent Framework** | Orchestration | LangChain | OpenAI-compatible tool calling |
| **Frontend** | UI framework | Streamlit | Deployed as Databricks App |
| **App Platform** | Hosting | Databricks Apps | OAuth-authenticated, lakebase feature |
| **Observability** | Experiment tracking | MLflow | Tracing + experiment logging |
| **Safety** | Drug interactions | FDA Drug Labels API | No auth required |
| **Safety** | Score validation | Confidence bounds | Hard limits: [0.40, 0.85] |
| **SDK** | Databricks automation | `databricks-sdk` | >= 0.118.0 |
| **DB client** | PostgreSQL driver | `psycopg2-binary` | >= 2.9.9 |

---

## Data Sources (All Free — No Auth Required)

| API | Endpoint | Records |
|-----|----------|---------|
| ClinicalTrials.gov v2 | `https://clinicaltrials.gov/api/v2/studies` | 747 trials |
| PubMed (NCBI E-utils) | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/` | 289 articles |
| FDA Drug Labels | `https://api.fda.gov/drug/label.json` | On-demand safety checks |

---

## Deployment

### Prerequisites

- Databricks workspace (AWS, Azure, or GCP) with **Lakebase** and **Databricks Apps** enabled
- Serverless or Standard compute (Python 3.9+)
- No external API keys required — all 3 external APIs are open/free

### Step 1 — Clone the Repository

```bash
git clone https://github.com/niting9881/DatabricksBootcamp-2026.git
```

Then import the notebooks into your Databricks workspace via **Workspace → Import** or by setting up a [Databricks Git Folder](https://docs.databricks.com/repos/index.html) pointing to the repo.

### Step 2 — Run the Main Notebook (Phases 1–8)

1. Open `Capstone Project - Clinical Trial Matching Agent` in Databricks
2. Run **Phase 1** first — it creates the Lakebase project, all 13 tables, pgvector extension, and indexes automatically (idempotent)
3. Continue running phases 2→8 sequentially
4. Each phase is safe to re-run (idempotent design throughout)

### Step 3 — Run the Spark ETL Notebook (Optional but Recommended)

1. Open `Capstone - Gap Implementation (Spark ETL + App Deploy)`
2. Run all cells — demonstrates the full Bronze→Silver→Gold Spark medallion pipeline
3. Phase F in this notebook deploys the Databricks App automatically via SDK

### Step 4 — Deploy the App Manually (if needed)

```bash
# Create the app (one-time)
databricks apps create clinical-trial-agent \
  --description "Clinical Trial Matching Agent — Capstone Project"

# Deploy source code
databricks apps deploy clinical-trial-agent \
  --source-code-path /Workspace/Users/<your-email>/capstone-project/clinical-trial-agent/app
```

### Step 5 — Verify Deployment

```bash
databricks apps get clinical-trial-agent
```

Expected output: `compute_status.state: ACTIVE` with a live URL.

### One-Shot Infrastructure Deploy

For a clean workspace, the `deploy_infrastructure.py` script creates the entire Lakebase schema from scratch:

```bash
python clinical-trial-agent/deploy_infrastructure.py
```

---

## How to Use the App

The live app is available at: **https://clinical-trial-agent-7474648653109871.aws.databricksapps.com**

Authentication is handled automatically via Databricks OAuth — no passwords required.

### Tab 1 — Patient Match

1. Select a patient from the dropdown (5 synthetic demo patients)
2. Click **Run AI Agent** — the agent executes 6 tools: semantic search → SQL filter → drug safety → evidence retrieval → scoring → recommendation
3. View matched trials ranked by confidence score, with reasoning and PubMed citations
4. Use the **Approve** / **Reject** buttons to submit human feedback (logged to `agent_feedback` table)

### Tab 2 — Trial Browser

1. Use the status filter (RECRUITING / ACTIVE_NOT_RECRUITING) and phase selector
2. Enter free-text keywords to search across 747 trial titles and eligibility criteria
3. Click any trial to view full details including inclusion/exclusion criteria

### Tab 3 — Upload Medical Records

1. Upload a medical image (X-ray, scan) or PDF (lab report, pathology)
2. The vision model (`ai_query` / `ai_parse_document`) extracts structured findings
3. Extracted conditions and diagnoses are automatically passed to the trial matching agent
4. Matched trials are displayed below the upload

### Tab 4 — Evaluation Dashboard

- View live metrics: agent precision, retrieval P@10, avg latency, confidence distribution
- Inspect the MLflow experiment `clinical-trial-agent-eval` linked directly from this tab
- Charts show per-patient match counts and confidence score distribution

### Tab 5 — Admin / LLMOps

- **Agent Traces**: full per-run tool call logs from the `agent_traces` table
- **Prompt Versions**: view and edit agent system prompts stored in `agent_prompts`
- **Human Feedback**: review approve/reject feedback for fine-tuning loop
- **Confidence Calibration**: scatter plot of confidence vs. human approval rate

---

## Lakebase Schema (13 Tables)

| Table | Purpose |
|-------|---------|
| `patients` | Synthetic patient profiles (5 patients for demo) |
| `patient_conditions` | Diagnoses, medications, labs per patient |
| `clinical_trials` | 747 trials from ClinicalTrials.gov |
| `trial_eligibility_criteria` | Parsed inclusion/exclusion text |
| `trial_documents` | Supporting documents |
| `patient_trial_matches` | Agent-written match records with confidence |
| `enrollment_recommendations` | Agent-written next steps |
| `patient_communications` | Outreach history |
| `trial_eligibility_embeddings` | pgvector: 384-dim eligibility criteria vectors |
| `pubmed_embeddings` | pgvector: 384-dim PubMed abstract vectors |
| `agent_traces` | LLMOps: per-run tool call logs |
| `agent_feedback` | Human feedback for RLHF loop |
| `agent_prompts` | Prompt version management |

---

## Limitations

| # | Limitation | Impact | Workaround |
|---|-----------|--------|------------|
| 1 | **Static sample data in Spark ETL** | The Spark notebook uses representative static records (12 trials, 10 PubMed, 10 FDA drugs) because Databricks Serverless compute blocks external DNS | Main notebook runs live API calls against ClinicalTrials.gov and PubMed |
| 2 | **5 synthetic patients only** | The agent is demoed with 5 hardcoded patients, not real EHR data | Designed for real patient integration via HL7 FHIR or manual entry |
| 3 | **No real patient data** | All patient profiles are fictional and created for demonstration purposes | HIPAA compliance layer required before production use |
| 4 | **LLM API dependency** | Agent and vision features require the Databricks Foundation Model API (`databricks-llama-4-maverick`) to be enabled in the workspace | The API is available on all Databricks pay-per-token workspaces |
| 5 | **Single-workspace deployment** | `config.py` contains a hardcoded Lakebase host for the original workspace | Run `deploy_infrastructure.py` on a fresh workspace to auto-provision a new host |
| 6 | **No geographic filtering** | Trials are matched on condition/eligibility only; patient location vs. trial site distance is not calculated | Future roadmap item (Census Bureau API + distance scoring) |
| 7 | **No real-time trial updates** | Trial data is batch-ingested and not refreshed automatically | A daily Databricks Job could re-run the ingestion notebook |
| 8 | **Embedding model size** | `all-MiniLM-L6-v2` (384-dim) is compact and fast but may miss nuanced clinical terminology | Upgrade path: `BiomedBERT` or `PubMedBERT` for clinical-domain embeddings |

---

## Future Roadmap

### Near-Term (Next Sprint)
- [ ] **Automated daily refresh** — Schedule the ingestion notebook as a Databricks Job to keep trial data current with ClinicalTrials.gov
- [ ] **Geographic distance scoring** — Integrate Census Bureau API + Google Maps API to score trials by distance from patient ZIP code
- [ ] **Email/SMS outreach** — Add Twilio integration to the recommendation step for automated patient notifications
- [ ] **Expanded disease areas** — Scale from 5 to 20+ disease areas across oncology, cardiology, and neurology

### Medium-Term
- [ ] **HL7 FHIR integration** — Connect to real EHR systems via FHIR R4 API to ingest actual patient records instead of synthetic profiles
- [ ] **Clinical-domain embeddings** — Replace `all-MiniLM-L6-v2` with `BiomedBERT` or `PubMedBERT` for higher precision on medical terminology
- [ ] **Streaming ingestion** — Use Lakeflow Spark Declarative Pipelines (SDP) to stream trial updates in near real-time
- [ ] **Multi-patient batch matching** — Process entire patient cohorts in parallel using Spark UDFs instead of sequential agent calls
- [ ] **LLM-as-Judge evaluation** — Activate the `databricks-claude-sonnet-4` judge model already configured in `config.py` for automated response quality scoring

### Long-Term
- [ ] **HIPAA compliance layer** — Add data masking, audit logging, and access controls required for production clinical use
- [ ] **Site coordinator portal** — Add a 6th app tab for clinical trial site coordinators to review and manage inbound patient referrals
- [ ] **Regulatory submission integration** — Export matched patients and trial IDs in IRB-compliant formats
- [ ] **Federated learning** — Train ranking models across hospital networks without sharing raw patient data
- [ ] **Multi-language support** — Translate eligibility criteria and recommendations for non-English-speaking patients

---

## Contributing

1. Fork the repository: https://github.com/niting9881/DatabricksBootcamp-2026
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a Pull Request

---

*Built as part of Databricks Bootcamp 2026 — Healthcare / Life Sciences AI track.*
