#!/usr/bin/env python3
"""
============================================================================
Clinical Trial Matching Agent - Infrastructure Deployment Script
============================================================================
Deploys ALL Lakebase resources to ANY Databricks workspace.

Usage:
  1. Open this file in a Databricks notebook or run via `databricks jobs`
  2. Optionally change PROJECT_ID below
  3. Run all cells / execute the script

What it creates:
  - Lakebase project (PostgreSQL 17 + pgvector)
  - 13 tables (8 core + 2 vector + 3 LLMOps)
  - 6 indexes
  - Connection config printed at the end

Prerequisites:
  - Databricks workspace with Lakebase enabled
  - databricks-sdk >= 0.118.0 (auto-installed)
  - User must have permission to create Lakebase projects

Time to deploy: ~2 minutes
============================================================================
"""

# ===================== CONFIGURATION =====================
PROJECT_ID = "clinical-trial-agent"       # Change this for different deployments
DISPLAY_NAME = "Clinical Trial Agent"      # Human-readable name
PG_VERSION = 17                            # Postgres version
# =========================================================

import subprocess, sys, importlib.metadata as md

# --- Auto-install/upgrade SDK ---
def ensure_sdk():
    try:
        before = md.version("databricks-sdk")
    except md.PackageNotFoundError:
        before = None
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--upgrade", "databricks-sdk>=0.118.0"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    after = md.version("databricks-sdk")
    print(f"[SDK] databricks-sdk: {before} -> {after}")
    if before != after:
        print("[SDK] Upgraded! If running in a notebook, restart Python and re-run.")
        return False
    return True

# --- Auto-install psycopg2 ---
def ensure_psycopg2():
    try:
        import psycopg2
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "psycopg2-binary"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    print("[OK] psycopg2 available")


def create_project(w):
    """Create or get existing Lakebase project."""
    from databricks.sdk.service.postgres import Project, ProjectSpec
    
    try:
        project = w.postgres.get_project(name=f"projects/{PROJECT_ID}")
        print(f"\n[SKIP] Project already exists: {project.name}")
    except Exception:
        print(f"\n[CREATE] Creating project '{PROJECT_ID}'...")
        op = w.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=DISPLAY_NAME, pg_version=PG_VERSION)),
            project_id=PROJECT_ID,
        )
        project = op.wait()
        print(f"[OK] Project created: {project.name}")
    
    return project


def get_connection_info(w):
    """Get branch, endpoint, and host info."""
    branches = list(w.postgres.list_branches(parent=f"projects/{PROJECT_ID}"))
    branch = branches[0]
    endpoints = list(w.postgres.list_endpoints(parent=branch.name))
    endpoint = endpoints[0]
    host = endpoint.status.hosts.host
    
    config = {
        "project_id": PROJECT_ID,
        "branch_id": "production",
        "endpoint_id": "primary",
        "host": host,
        "database": "databricks_postgres",
        "port": 5432,
    }
    return config


def deploy_schema(w, config):
    """Deploy all tables, indexes, and extensions."""
    import psycopg2
    
    # Get OAuth token
    cred = w.postgres.generate_database_credential(
        endpoint=f"projects/{config['project_id']}/branches/{config['branch_id']}/endpoints/{config['endpoint_id']}"
    )
    
    # Connect (user = Databricks email, password = OAuth token)
    username = w.current_user.me().user_name
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        dbname=config['database'],
        user=username,
        password=cred.token,
        sslmode="require"
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # DDL Statements
    DDL = [
        "CREATE EXTENSION IF NOT EXISTS vector",
        
        """CREATE TABLE IF NOT EXISTS patients (
            patient_id SERIAL PRIMARY KEY, first_name VARCHAR(100), last_name VARCHAR(100),
            age INTEGER, gender VARCHAR(20), race_ethnicity VARCHAR(100),
            location_state VARCHAR(50), location_zip VARCHAR(10),
            travel_willingness_miles INTEGER DEFAULT 100, created_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS patient_conditions (
            condition_id SERIAL PRIMARY KEY, patient_id INTEGER REFERENCES patients(patient_id),
            condition_name VARCHAR(255), icd10_code VARCHAR(20),
            condition_status VARCHAR(50) DEFAULT 'active', diagnosed_date DATE,
            medications TEXT[], lab_values JSONB, genetic_markers TEXT[], notes TEXT)""",
        
        """CREATE TABLE IF NOT EXISTS clinical_trials (
            trial_id SERIAL PRIMARY KEY, nct_id VARCHAR(20) UNIQUE NOT NULL,
            title TEXT, brief_summary TEXT, detailed_description TEXT,
            status VARCHAR(50), phase VARCHAR(20), sponsor VARCHAR(255),
            conditions TEXT[], interventions TEXT[], enrollment_count INTEGER,
            start_date DATE, completion_date DATE, locations JSONB,
            contact_info JSONB, last_updated DATE, source_url TEXT,
            ingested_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS trial_eligibility_criteria (
            criteria_id SERIAL PRIMARY KEY, nct_id VARCHAR(20) REFERENCES clinical_trials(nct_id),
            criteria_type VARCHAR(20), criteria_text TEXT NOT NULL,
            min_age INTEGER, max_age INTEGER, gender_required VARCHAR(20),
            healthy_volunteers BOOLEAN DEFAULT FALSE)""",
        
        """CREATE TABLE IF NOT EXISTS trial_documents (
            document_id SERIAL PRIMARY KEY, nct_id VARCHAR(20) REFERENCES clinical_trials(nct_id),
            document_type VARCHAR(50), document_title TEXT, document_text TEXT,
            source_url TEXT, ingested_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS patient_trial_matches (
            match_id SERIAL PRIMARY KEY, patient_id INTEGER REFERENCES patients(patient_id),
            nct_id VARCHAR(20) REFERENCES clinical_trials(nct_id),
            confidence_score FLOAT NOT NULL, match_reasoning TEXT NOT NULL,
            matching_criteria TEXT[], risk_flags TEXT[], evidence_pmids TEXT[],
            status VARCHAR(30) DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW(),
            reviewed_by VARCHAR(100), reviewed_at TIMESTAMP)""",
        
        """CREATE TABLE IF NOT EXISTS enrollment_recommendations (
            recommendation_id SERIAL PRIMARY KEY,
            match_id INTEGER REFERENCES patient_trial_matches(match_id),
            patient_id INTEGER REFERENCES patients(patient_id), nct_id VARCHAR(20),
            recommendation_text TEXT NOT NULL, risk_assessment TEXT,
            next_steps TEXT[], requires_specialist_review BOOLEAN DEFAULT FALSE,
            specialist_type VARCHAR(100), created_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS patient_communications (
            communication_id SERIAL PRIMARY KEY, patient_id INTEGER REFERENCES patients(patient_id),
            nct_id VARCHAR(20), communication_type VARCHAR(50), subject TEXT,
            body TEXT NOT NULL, sent_at TIMESTAMP, status VARCHAR(30) DEFAULT 'draft')""",
        
        """CREATE TABLE IF NOT EXISTS trial_eligibility_embeddings (
            embedding_id SERIAL PRIMARY KEY,
            nct_id VARCHAR(20) REFERENCES clinical_trials(nct_id),
            criteria_id INTEGER REFERENCES trial_eligibility_criteria(criteria_id),
            chunk_text TEXT NOT NULL, embedding vector(384) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS pubmed_embeddings (
            embedding_id SERIAL PRIMARY KEY, pmid VARCHAR(20), title TEXT,
            abstract_text TEXT, mesh_terms TEXT[], embedding vector(384) NOT NULL,
            publication_date DATE, created_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS agent_traces (
            trace_id SERIAL PRIMARY KEY, request_id UUID DEFAULT gen_random_uuid(),
            patient_id INTEGER, prompt_version VARCHAR(20), tools_called TEXT[],
            total_latency_ms INTEGER, input_tokens INTEGER, output_tokens INTEGER,
            total_cost FLOAT, error_message TEXT, guardrail_violations TEXT[],
            created_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS agent_feedback (
            feedback_id SERIAL PRIMARY KEY,
            match_id INTEGER REFERENCES patient_trial_matches(match_id),
            action VARCHAR(30), rejection_reason TEXT, edited_reasoning TEXT,
            feedback_by VARCHAR(100), feedback_at TIMESTAMP DEFAULT NOW())""",
        
        """CREATE TABLE IF NOT EXISTS agent_prompts (
            prompt_id SERIAL PRIMARY KEY, version VARCHAR(20) NOT NULL,
            prompt_name VARCHAR(100), prompt_text TEXT NOT NULL, description TEXT,
            eval_precision FLOAT, eval_recall FLOAT, eval_safety_score FLOAT,
            is_active BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW(),
            created_by VARCHAR(100))""",
    ]
    
    # Execute DDL
    print("\n[SCHEMA] Deploying tables...")
    for i, ddl in enumerate(DDL, 1):
        try:
            cur.execute(ddl)
            name = ddl.split("EXISTS ")[-1].split(" ")[0].split("(")[0] if "EXISTS" in ddl else "pgvector"
            print(f"  [{i:2d}/{len(DDL)}] OK - {name}")
        except Exception as e:
            print(f"  [{i:2d}/{len(DDL)}] WARN - {e}")
    
    # Indexes
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_trials_status ON clinical_trials(status)",
        "CREATE INDEX IF NOT EXISTS idx_trials_phase ON clinical_trials(phase)",
        "CREATE INDEX IF NOT EXISTS idx_trials_nct ON clinical_trials(nct_id)",
        "CREATE INDEX IF NOT EXISTS idx_criteria_nct ON trial_eligibility_criteria(nct_id)",
        "CREATE INDEX IF NOT EXISTS idx_matches_patient ON patient_trial_matches(patient_id)",
        "CREATE INDEX IF NOT EXISTS idx_matches_status ON patient_trial_matches(status)",
    ]
    print("\n[INDEXES] Creating indexes...")
    for idx in INDEXES:
        try:
            cur.execute(idx)
        except:
            pass
    print(f"  OK - {len(INDEXES)} indexes")
    
    # Verify
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
    tables = [row[0] for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return tables


def main():
    """Main deployment entry point."""
    print("="*60)
    print("CLINICAL TRIAL AGENT - INFRASTRUCTURE DEPLOYMENT")
    print("="*60)
    
    # Check dependencies
    if not ensure_sdk():
        print("\n[!] SDK was upgraded. Please restart Python and re-run this script.")
        return
    ensure_psycopg2()
    
    # Initialize client
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    
    # Create project
    create_project(w)
    
    # Get connection info
    config = get_connection_info(w)
    print(f"\n[CONFIG] Connection details:")
    for k, v in config.items():
        print(f"  {k}: {v}")
    
    # Deploy schema
    tables = deploy_schema(w, config)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"DEPLOYMENT COMPLETE")
    print(f"{'='*60}")
    print(f"  Project:    {config['project_id']}")
    print(f"  Host:       {config['host']}")
    print(f"  Database:   {config['database']}")
    print(f"  Tables:     {len(tables)}")
    print(f"  Extensions: pgvector (384-dim)")
    print(f"  Indexes:    6")
    print(f"\n  To connect from another notebook:")
    print(f"  ----------------------------------------")
    print(f"  LAKEBASE_CONFIG = {config}")
    print(f"{'='*60}")
    
    return config


if __name__ == "__main__":
    config = main()
