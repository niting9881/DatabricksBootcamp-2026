"""
Clinical Trial Matching & Recruitment Agent
Databricks App — Streamlit Frontend (5 Tabs)
"""

import streamlit as st
import psycopg2
import json
import os
from datetime import datetime

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_connection():
    """Get Lakebase connection using OAuth (from Databricks App env)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    
    project_id = os.environ.get("LAKEBASE_PROJECT_ID", "clinical-trial-agent")
    branch_id = os.environ.get("LAKEBASE_BRANCH_ID", "production")
    endpoint_id = os.environ.get("LAKEBASE_ENDPOINT_ID", "primary")
    database = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
    
    endpoint = w.postgres.get_endpoint(
        name=f"projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}"
    )
    host = endpoint.status.hosts.host
    
    cred = w.postgres.generate_database_credential(
        endpoint=f"projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}"
    )
    username = w.current_user.me().user_name
    
    conn = psycopg2.connect(
        host=host, port=5432, dbname=database,
        user=username, password=cred.token, sslmode="require"
    )
    conn.autocommit = True
    return conn


def get_cursor():
    """Get a cursor, reconnecting if needed."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return cur
    except Exception:
        st.cache_resource.clear()
        conn = get_connection()
        return conn.cursor()


# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Clinical Trial Matching Agent",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🧬 Clinical Trial Matching & Recruitment Agent")
st.caption("AI-powered patient-trial matching with semantic search and safety verification")

# ============================================================================
# TABS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "👤 Patient Match", "🔍 Trial Browser", "📄 Upload Records",
    "📊 Eval Dashboard", "⚙️ Admin / LLMOps"
])

# ======================== TAB 1: PATIENT MATCH ========================
with tab1:
    st.header("Patient-Trial Matching")
    st.write("Select a patient to find matching clinical trials using semantic AI.")
    
    cur = get_cursor()
    
    # Load patients
    cur.execute("SELECT patient_id, first_name, last_name, age, gender FROM patients ORDER BY patient_id")
    patients = cur.fetchall()
    
    if patients:
        patient_options = {f"{p[1]} {p[2]} (Age {p[3]}, {p[4]})": p[0] for p in patients}
        selected = st.selectbox("Select Patient", list(patient_options.keys()))
        patient_id = patient_options[selected]
        
        # Show patient details
        cur.execute("""
            SELECT condition_name, icd10_code, medications, lab_values
            FROM patient_conditions WHERE patient_id = %s
        """, (patient_id,))
        conditions = cur.fetchall()
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Conditions")
            for cond in conditions:
                st.write(f"**{cond[0]}** ({cond[1]})")
                if cond[2]:
                    st.write(f"  Meds: {', '.join(cond[2])}")
        with col2:
            st.subheader("Lab Values")
            for cond in conditions:
                if cond[3]:
                    labs = cond[3] if isinstance(cond[3], dict) else json.loads(cond[3])
                    for k, v in labs.items():
                        st.metric(k, v)
        
        # Run matching
        if st.button("🚀 Find Matching Trials", type="primary"):
            with st.spinner("Running AI agent..."):
                # Query existing matches or run new search
                cur.execute("""
                    SELECT ptm.nct_id, ct.title, ptm.confidence_score, ptm.match_reasoning, ptm.status
                    FROM patient_trial_matches ptm
                    JOIN clinical_trials ct ON ptm.nct_id = ct.nct_id
                    WHERE ptm.patient_id = %s
                    ORDER BY ptm.confidence_score DESC LIMIT 10
                """, (patient_id,))
                matches = cur.fetchall()
                
                if matches:
                    st.success(f"Found {len(matches)} trial matches!")
                    for match in matches:
                        with st.expander(f"✅ {match[0]} — Confidence: {match[2]:.0%}"):
                            st.write(f"**Trial:** {match[1]}")
                            st.write(f"**Reasoning:** {match[3]}")
                            st.write(f"**Status:** {match[4]}")
                            col_a, col_b = st.columns(2)
                            with col_a:
                                st.button(f"✅ Approve", key=f"approve_{match[0]}")
                            with col_b:
                                st.button(f"❌ Reject", key=f"reject_{match[0]}")
                else:
                    st.info("No matches found yet. Run the agent notebook first.")
    else:
        st.warning("No patients in database. Run the Phase 5 notebook first.")


# ======================== TAB 2: TRIAL BROWSER ========================
with tab2:
    st.header("Clinical Trial Browser")
    
    cur = get_cursor()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox("Status", ["All", "RECRUITING", "NOT_YET_RECRUITING", "ACTIVE_NOT_RECRUITING"])
    with col2:
        phase_filter = st.selectbox("Phase", ["All", "PHASE1", "PHASE2", "PHASE3", "PHASE4", "EARLY_PHASE1"])
    with col3:
        search_term = st.text_input("Search", placeholder="e.g. diabetes, COPD...")
    
    # Build query
    query = "SELECT nct_id, title, status, phase, conditions, enrollment_count FROM clinical_trials WHERE 1=1"
    params = []
    if status_filter != "All":
        query += " AND status = %s"
        params.append(status_filter)
    if phase_filter != "All":
        query += " AND phase = %s"
        params.append(phase_filter)
    if search_term:
        query += " AND (LOWER(title) LIKE LOWER(%s) OR EXISTS (SELECT 1 FROM unnest(conditions) c WHERE LOWER(c) LIKE LOWER(%s)))"
        params.extend([f"%{search_term}%", f"%{search_term}%"])
    query += " ORDER BY ingested_at DESC LIMIT 50"
    
    cur.execute(query, params)
    trials = cur.fetchall()
    
    st.write(f"Showing {len(trials)} trials")
    for trial in trials:
        with st.expander(f"{trial[0]} — {trial[1][:80]}..."):
            st.write(f"**Status:** {trial[2]} | **Phase:** {trial[3]} | **Enrollment:** {trial[5] or 'N/A'}")
            st.write(f"**Conditions:** {', '.join(trial[4][:5]) if trial[4] else 'N/A'}")
            st.link_button("View on ClinicalTrials.gov", f"https://clinicaltrials.gov/study/{trial[0]}")


# ======================== TAB 3: UPLOAD RECORDS ========================
with tab3:
    st.header("📄 Upload Medical Records")
    st.write("Upload X-rays, lab reports, or PDF medical records for AI analysis.")
    
    uploaded_file = st.file_uploader(
        "Upload medical record (PDF, JPG, PNG)",
        type=["pdf", "jpg", "jpeg", "png"],
        help="Supported: X-rays, lab reports, pathology reports"
    )
    
    if uploaded_file:
        st.image(uploaded_file, caption=uploaded_file.name, width=400) if uploaded_file.type.startswith("image") else None
        
        if st.button("🤖 Analyze with Vision AI", type="primary"):
            st.info("""
            **Vision AI Integration** (requires Databricks SQL endpoint):
            
            This feature uses `ai_query('databricks-llama-4-maverick', prompt, files => content)` 
            to extract findings from medical images and `ai_parse_document()` for PDFs.
            
            **Demo flow:**
            1. Upload X-ray → Extract "8mm bilateral pulmonary nodules"
            2. Auto-match to NSCLC trials
            3. Generate enrollment recommendation
            
            Run the Vision notebook (Phase 7) to enable this feature.
            """)


# ======================== TAB 4: EVAL DASHBOARD ========================
with tab4:
    st.header("📊 Evaluation Dashboard")
    
    cur = get_cursor()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    cur.execute("SELECT COUNT(*) FROM clinical_trials")
    trial_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM patient_trial_matches")
    match_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM trial_eligibility_embeddings")
    embedding_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM pubmed_embeddings WHERE embedding IS NOT NULL")
    pubmed_count = cur.fetchone()[0]
    
    col1.metric("Clinical Trials", trial_count)
    col2.metric("Patient Matches", match_count)
    col3.metric("Trial Embeddings", embedding_count)
    col4.metric("PubMed Articles", pubmed_count)
    
    st.divider()
    
    # Agent performance
    st.subheader("Agent Performance")
    cur.execute("""
        SELECT prompt_version, COUNT(*) as runs, 
               AVG(total_latency_ms) as avg_latency,
               array_length(tools_called, 1) as avg_tools
        FROM agent_traces
        GROUP BY prompt_version
        ORDER BY COUNT(*) DESC
    """)
    traces = cur.fetchall()
    
    if traces:
        for trace in traces:
            st.write(f"**Prompt {trace[0]}**: {trace[1]} runs, avg latency {trace[2]:.0f}ms")
    else:
        st.info("No agent traces yet. Run the agent to see metrics.")
    
    # Match confidence distribution
    st.subheader("Match Confidence Distribution")
    cur.execute("""
        SELECT confidence_score FROM patient_trial_matches ORDER BY confidence_score DESC
    """)
    scores = [row[0] for row in cur.fetchall()]
    if scores:
        st.bar_chart(scores)


# ======================== TAB 5: ADMIN / LLMOps ========================
with tab5:
    st.header("⚙️ Admin & LLMOps")
    
    cur = get_cursor()
    
    subtab1, subtab2, subtab3 = st.tabs(["Agent Traces", "Prompt Versions", "Feedback"])
    
    with subtab1:
        st.subheader("Recent Agent Traces")
        cur.execute("""
            SELECT trace_id, patient_id, prompt_version, tools_called, 
                   total_latency_ms, guardrail_violations, created_at
            FROM agent_traces ORDER BY created_at DESC LIMIT 20
        """)
        traces = cur.fetchall()
        for trace in traces:
            with st.expander(f"Trace #{trace[0]} — Patient {trace[1]} ({trace[6]})" ):
                st.write(f"**Prompt:** {trace[2]}")
                st.write(f"**Tools:** {trace[3]}")
                st.write(f"**Latency:** {trace[4]}ms")
                if trace[5]:
                    st.error(f"Guardrail violations: {trace[5]}")
    
    with subtab2:
        st.subheader("Prompt Version Management")
        cur.execute("""
            SELECT version, prompt_name, is_active, eval_precision, eval_recall, created_at
            FROM agent_prompts ORDER BY created_at DESC
        """)
        prompts = cur.fetchall()
        if prompts:
            for p in prompts:
                status = "🟢 Active" if p[2] else "⚪ Inactive"
                st.write(f"{status} **v{p[0]}** — {p[1]} (P:{p[3]}, R:{p[4]})")
        else:
            st.info("No prompt versions registered yet.")
    
    with subtab3:
        st.subheader("Human Feedback")
        cur.execute("""
            SELECT f.feedback_id, f.action, f.rejection_reason, f.feedback_by, f.feedback_at,
                   ptm.nct_id, ptm.confidence_score
            FROM agent_feedback f
            JOIN patient_trial_matches ptm ON f.match_id = ptm.match_id
            ORDER BY f.feedback_at DESC LIMIT 20
        """)
        feedback = cur.fetchall()
        if feedback:
            for fb in feedback:
                icon = "✅" if fb[1] == "approve" else "❌" if fb[1] == "reject" else "⚠️"
                st.write(f"{icon} {fb[5]} (conf: {fb[6]:.0%}) — {fb[1]} by {fb[3]}")
                if fb[2]:
                    st.caption(f"Reason: {fb[2]}")
        else:
            st.info("No feedback recorded yet. Approve/reject matches in the Patient Match tab.")


# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.header("About")
    st.write("""
    **Clinical Trial Matching Agent**
    
    AI-powered system that matches patients to clinical trials using:
    - Semantic search (pgvector + MiniLM-L6)
    - Drug interaction safety checks (FDA API)
    - LLM reasoning (Llama-4-Maverick)
    - PubMed evidence retrieval
    """)
    
    st.divider()
    st.write("**Data Sources**")
    st.write("- ClinicalTrials.gov (747 trials)")
    st.write("- PubMed/MEDLINE (289 articles)")
    st.write("- FDA Drug Labels")
    
    st.divider()
    st.write("**Tech Stack**")
    st.write("- Lakebase (PostgreSQL + pgvector)")
    st.write("- sentence-transformers/all-MiniLM-L6-v2")
    st.write("- MLflow Tracing")
    st.write("- Databricks Foundation Models")
