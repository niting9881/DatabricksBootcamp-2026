"""
============================================================================
Clinical Trial Matching Agent - Shared Configuration
============================================================================
All notebooks import from this file to ensure consistent connection details.
Change these values when deploying to a different workspace.
============================================================================
"""

# ======================== LAKEBASE CONNECTION ========================
LAKEBASE_CONFIG = {
    "project_id": "clinical-trial-agent",
    "branch_id": "production",
    "endpoint_id": "primary",
    "host": "ep-cool-thunder-d1jvz502.database.us-west-2.cloud.databricks.com",
    "database": "databricks_postgres",
    "port": 5432,
}

# ======================== API ENDPOINTS ========================
CLINICALTRIALS_API = "https://clinicaltrials.gov/api/v2/studies"
PUBMED_SEARCH_API = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_FETCH_API = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
FDA_DRUG_API = "https://api.fda.gov/drug/label.json"

# ======================== MODEL CONFIG ========================
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
AGENT_LLM = "databricks-llama-4-maverick"  # For agent reasoning
VISION_MODEL = "databricks-llama-4-maverick"  # For image analysis
JUDGE_MODEL = "databricks-claude-sonnet-4"  # For LLM-as-Judge evaluation

# ======================== DISEASE AREAS ========================
# Trials are ingested for these 5 disease areas
DISEASE_AREAS = [
    "Type 2 Diabetes",
    "Breast Cancer",
    "COPD",
    "Lupus",
    "Alzheimer's Disease",
]

# ======================== AGENT CONFIG ========================
AGENT_CONFIG = {
    "max_tool_calls": 8,
    "confidence_threshold": 0.5,  # Minimum score to show a match
    "max_matches_returned": 10,
    "require_interaction_check": True,
    "require_evidence_citation": True,
}

# ======================== EVALUATION CONFIG ========================
EVAL_CONFIG = {
    "golden_test_size": 30,      # Number of labeled test cases
    "precision_target": 0.70,    # Minimum Precision@10
    "recall_target": 0.80,       # Minimum Recall@10
    "agent_pass_rate": 0.85,     # Minimum agent test pass rate
    "hallucination_max": 0.05,   # Maximum hallucination rate (5%)
    "safety_min": 1.00,          # 100% safety compliance
}

# ======================== HELPER FUNCTION ========================
def get_lakebase_connection():
    """Get a psycopg2 connection to Lakebase using OAuth."""
    import psycopg2
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    cred = w.postgres.generate_database_credential(
        endpoint=f"projects/{LAKEBASE_CONFIG['project_id']}/branches/{LAKEBASE_CONFIG['branch_id']}/endpoints/{LAKEBASE_CONFIG['endpoint_id']}"
    )
    
    username = w.current_user.me().user_name
    conn = psycopg2.connect(
        host=LAKEBASE_CONFIG['host'],
        port=LAKEBASE_CONFIG['port'],
        dbname=LAKEBASE_CONFIG['database'],
        user=username,
        password=cred.token,
        sslmode="require"
    )
    conn.autocommit = True
    return conn
