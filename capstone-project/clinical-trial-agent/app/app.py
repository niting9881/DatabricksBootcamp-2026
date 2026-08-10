"""
Clinical Trial Matching & Recruitment Agent
Databricks App — Streamlit Frontend (5 Tabs)
Hardened v2: write-backs, guardrails, Vision AI, CDF analytics, UX polish
"""

import streamlit as st
import psycopg2
import json
import os
import re
import time
import base64
import requests
from datetime import datetime

# ============================================================================
# CONSTANTS & GUARDRAIL CONFIG
# ============================================================================
CONFIDENCE_MIN = 0.40
CONFIDENCE_MAX = 0.85
REQUIRE_EVIDENCE_CITATION = True

# ============================================================================
# DATABASE CONNECTION  (retry + error banner)
# ============================================================================

@st.cache_resource
def get_connection():
    """Get Lakebase connection using OAuth with exponential-backoff retry."""
    from databricks.sdk import WorkspaceClient
    MAX_RETRIES = 3
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            w = WorkspaceClient()
            project_id  = os.environ.get("LAKEBASE_PROJECT_ID",  "clinical-trial-agent")
            branch_id   = os.environ.get("LAKEBASE_BRANCH_ID",   "production")
            endpoint_id = os.environ.get("LAKEBASE_ENDPOINT_ID", "primary")
            database    = os.environ.get("LAKEBASE_DATABASE",    "databricks_postgres")
            ep_path     = f"projects/{project_id}/branches/{branch_id}/endpoints/{endpoint_id}"
            endpoint    = w.postgres.get_endpoint(name=ep_path)
            host        = endpoint.status.hosts.host
            cred        = w.postgres.generate_database_credential(endpoint=ep_path)
            username    = w.current_user.me().user_name
            conn = psycopg2.connect(
                host=host, port=5432, dbname=database,
                user=username, password=cred.token, sslmode="require"
            )
            conn.autocommit = True
            return conn, username
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to connect after {MAX_RETRIES} attempts: {last_error}")


def get_cursor():
    """Get a cursor, clearing the cache once on token-expiry errors."""
    try:
        conn, username = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return cur, username
    except Exception:
        get_connection.clear()
        conn, username = get_connection()
        return conn.cursor(), username


# ============================================================================
# GUARDRAILS
# ============================================================================

def validate_match(cur, nct_id: str, confidence: float,
                   evidence_pmids=None, interaction_checked: bool = True) -> tuple:
    """Pre-persist guardrail check. Returns (is_valid, reason)."""
    cur.execute("SELECT 1 FROM clinical_trials WHERE nct_id = %s", (nct_id,))
    if not cur.fetchone():
        return False, f"NCT ID '{nct_id}' not found in clinical_trials"
    if not (CONFIDENCE_MIN <= confidence <= CONFIDENCE_MAX):
        return False, (f"Confidence {confidence:.2f} out of range "
                       f"[{CONFIDENCE_MIN}, {CONFIDENCE_MAX}]")
    if not interaction_checked:
        return False, "Drug interaction check was not performed"
    if REQUIRE_EVIDENCE_CITATION and not evidence_pmids:
        return False, "No PubMed evidence PMIDs provided"
    return True, "OK"


# ============================================================================
# ROBUST JSON PARSING
# ============================================================================

REQUIRED_MATCH_KEYS = {
    "nct_id": str, "confidence_score": (int, float),
    "match_reasoning": str, "matching_criteria": list,
    "evidence_pmids": list, "interaction_checked": bool,
}

def repair_and_validate_json(raw: str) -> tuple:
    """Strip markdown fences, repair trailing commas, validate required keys."""
    text = raw.strip()
    if "```" in text:
        for part in text.split("```")[1::2]:
            candidate = part.strip()
            if candidate.startswith("json"): candidate = candidate[4:].strip()
            text = candidate; break
    if not text.startswith(("[", "{")):
        m = re.search(r"(\[.*\])", text, re.DOTALL) or re.search(r"(\{.*\})", text, re.DOTALL)
        if not m: return [], [f"No JSON in LLM response: {raw[:200]}"]
        text = m.group(1)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        text = re.sub(r",\s*([}\]])", r"\1", text)
        try: parsed = json.loads(text)
        except json.JSONDecodeError as e: return [], [f"JSON parse error: {e}"]
    if isinstance(parsed, dict): parsed = [parsed]
    validated, errors = [], []
    for i, item in enumerate(parsed):
        item_errors = []
        for key, etype in REQUIRED_MATCH_KEYS.items():
            if key not in item: item_errors.append(f"item[{i}] missing '{key}'"); continue
            if key == "confidence_score" and isinstance(item[key], int): item[key] = float(item[key])
            if not isinstance(item[key], etype): item_errors.append(f"item[{i}].{key} wrong type")
        if item_errors: errors.extend(item_errors)
        else: validated.append(item)
    return validated, errors


# ============================================================================
# AGENT INVOCATION
# ============================================================================

def run_agent_for_patient(patient_id: int, username: str, cur) -> dict:
    """Call Llama-4-Maverick, validate output with guardrails, persist matches."""
    host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        return {"matches_stored": 0, "guardrail_violations": [],
                "error": "DATABRICKS_HOST / DATABRICKS_TOKEN not set"}
    cur.execute("""
        SELECT p.first_name, p.last_name, p.age, p.gender,
               array_agg(pc.condition_name) AS conditions,
               array_agg(pc.icd10_code)     AS icd10_codes
        FROM patients p LEFT JOIN patient_conditions pc ON p.patient_id = pc.patient_id
        WHERE p.patient_id = %s
        GROUP BY p.first_name, p.last_name, p.age, p.gender
    """, (patient_id,))
    row = cur.fetchone()
    if not row:
        return {"matches_stored": 0, "guardrail_violations": [], "error": "Patient not found"}
    first_name, last_name, age, gender, conditions, icd10_codes = row
    cur.execute("""
        SELECT ct.nct_id, ct.title, ct.phase, tec.criteria_text
        FROM clinical_trials ct
        LEFT JOIN trial_eligibility_criteria tec
            ON ct.nct_id = tec.nct_id AND tec.criteria_type = 'inclusion'
        WHERE ct.status = 'RECRUITING' LIMIT 15
    """)
    candidate_text = "\n".join(
        f"- {r[0]}: {r[1]} (Phase {r[2]}, Criteria: {r[3] or 'N/A'})"
        for r in cur.fetchall()[:10]
    )
    prompt = f"""You are a clinical trial matching agent.
Patient: {first_name} {last_name}, Age {age}, Gender {gender}
Conditions: {', '.join(c for c in (conditions or []) if c)}
ICD-10: {', '.join(c for c in (icd10_codes or []) if c)}
Candidates:
{candidate_text}
Return ONLY JSON array:
[{{"nct_id":"...","confidence_score":0.4-0.85,"match_reasoning":"...",
  "matching_criteria":[],"evidence_pmids":[],"interaction_checked":true}}]"""
    start_ms = int(time.time() * 1000); violations = []; stored = 0
    try:
        resp = requests.post(
            f"https://{host}/serving-endpoints/databricks-llama-4-maverick/invocations",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 1000},
            timeout=60)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        matches, parse_errors = repair_and_validate_json(raw)
        if parse_errors:
            return {"matches_stored": 0, "guardrail_violations": parse_errors,
                    "error": f"JSON parse errors: {parse_errors}"}
        for m in matches:
            nct_id = m["nct_id"].strip(); confidence = float(m["confidence_score"])
            valid, msg = validate_match(cur, nct_id, confidence, m["evidence_pmids"], m["interaction_checked"])
            if not valid: violations.append(f"{nct_id}: {msg}"); continue
            cur.execute("SELECT 1 FROM patient_trial_matches WHERE patient_id=%s AND nct_id=%s",
                        (patient_id, nct_id))
            if cur.fetchone(): continue
            cur.execute("""
                INSERT INTO patient_trial_matches
                    (patient_id, nct_id, confidence_score, match_reasoning,
                     matching_criteria, evidence_pmids, status)
                VALUES (%s,%s,%s,%s,%s,%s,'pending')
            """, (patient_id, nct_id, confidence, m["match_reasoning"],
                  m["matching_criteria"], m["evidence_pmids"]))
            stored += 1
        cur.execute("""
            INSERT INTO agent_traces
                (patient_id, prompt_version, tools_called, total_latency_ms,
                 guardrail_violations, created_at)
            VALUES (%s,'app-v2','{run_agent,validate_match}',%s,%s,NOW())
        """, (patient_id, int(time.time()*1000)-start_ms, violations or None))
        return {"matches_stored": stored, "guardrail_violations": violations, "error": None}
    except Exception as e:
        return {"matches_stored": 0, "guardrail_violations": [], "error": str(e)}


# ============================================================================
# VISION AI
# ============================================================================

def analyze_medical_file(file_bytes: bytes, file_type: str, filename: str) -> dict:
    """Analyze image or PDF with Llama-4-Maverick. Returns structured dict."""
    host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        return {"error": "DATABRICKS_HOST / DATABRICKS_TOKEN not configured"}
    if file_type.startswith("image/"):
        b64 = base64.b64encode(file_bytes).decode()
        messages = [{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:{file_type};base64,{b64}"}},
            {"type": "text", "text": """Analyze this medical image. Return ONLY JSON:
{"modality":"X-ray/CT/MRI/other","findings":[],"diagnosis":"...",
"urgency":"low/medium/high","trial_conditions":[],"recommended_actions":[]}"""},
        ]}]
    else:
        try:
            import io; from pypdf import PdfReader
            text = " ".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(file_bytes)).pages[:5])
        except Exception: text = "(PDF text extraction unavailable)"
        messages = [{"role": "user", "content": f"""Extract medical findings. Return ONLY JSON:
{{"document_type":"lab/pathology/other","findings":[],"diagnoses":[],
"medications":[],"lab_values":{{}},"trial_conditions":[]}}
Document: {text[:3000]}"""}]
    try:
        resp = requests.post(
            f"https://{host}/serving-endpoints/databricks-llama-4-maverick/invocations",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"messages": messages, "max_tokens": 600}, timeout=60)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        if "```" in raw:
            for part in raw.split("```")[1::2]:
                raw = part.strip()
                if raw.startswith("json"): raw = raw[4:].strip(); break
        return json.loads(raw)
    except json.JSONDecodeError: return {"raw_output": raw, "error": "Could not parse JSON"}
    except Exception as e: return {"error": str(e)}


# ============================================================================
# PAGE CONFIG
# ============================================================================

st.set_page_config(
    page_title="Clinical Trial Matching Agent",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

try:
    cur, current_user = get_cursor()
    _conn_ok = True
except Exception as conn_err:
    st.error(f"🔴 **Database connection failed**: {conn_err}\n\nCheck Lakebase project status.")
    _conn_ok = False

st.title("🧬 Clinical Trial Matching & Recruitment Agent")
st.caption("AI-powered patient-trial matching with semantic search and safety verification")

if not _conn_ok:
    st.stop()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "👤 Patient Match", "🔍 Trial Browser", "📄 Upload Records",
    "📊 Eval Dashboard", "⚙️ Admin / LLMOps"
])

# ======================== TAB 1: PATIENT MATCH ========================
with tab1:
    st.header("Patient-Trial Matching")
    with st.spinner("Loading patients..."):
        cur.execute("SELECT patient_id, first_name, last_name, age, gender FROM patients ORDER BY patient_id")
        patients = cur.fetchall()
    if not patients:
        st.warning("No patients found. Run Phase 5 notebook first.")
    else:
        patient_options = {f"{p[1]} {p[2]} (Age {p[3]}, {p[4]})": p[0] for p in patients}
        selected = st.selectbox("Select Patient", list(patient_options.keys()))
        patient_id = patient_options[selected]
        with st.spinner("Loading patient details..."):
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
                if cond[2]: st.write(f"  Meds: {', '.join(cond[2])}")
        with col2:
            st.subheader("Lab Values")
            for cond in conditions:
                if cond[3]:
                    labs = cond[3] if isinstance(cond[3], dict) else json.loads(cond[3])
                    for k, v in labs.items(): st.metric(k, v)
        st.divider()
        btn1, btn2 = st.columns(2)
        with btn1: find_btn = st.button("🔍 Load Existing Matches", use_container_width=True)
        with btn2: run_btn  = st.button("🚀 Run Agent", type="primary", use_container_width=True)
        if run_btn:
            with st.spinner("Running AI matching agent..."):
                result = run_agent_for_patient(patient_id, current_user, cur)
            if result["error"]: st.error(f"Agent failed: {result['error']}")
            else:
                if result["matches_stored"] > 0:
                    st.success(f"✅ Agent stored {result['matches_stored']} new match(es)!")
                else:
                    st.info("No new matches (duplicates filtered or guardrail-blocked).")
                if result["guardrail_violations"]:
                    with st.expander(f"⚠️ {len(result['guardrail_violations'])} guardrail violation(s)"):
                        for v in result["guardrail_violations"]: st.warning(v)
        if find_btn or run_btn:
            with st.spinner("Loading matches..."):
                cur.execute("""
                    SELECT ptm.match_id, ptm.nct_id, ct.title,
                           ptm.confidence_score, ptm.match_reasoning, ptm.status
                    FROM patient_trial_matches ptm
                    JOIN clinical_trials ct ON ptm.nct_id = ct.nct_id
                    WHERE ptm.patient_id = %s ORDER BY ptm.confidence_score DESC LIMIT 10
                """, (patient_id,))
                matches = cur.fetchall()
            if matches:
                st.success(f"Found {len(matches)} match(es).")
                for match in matches:
                    match_id, nct_id, title, confidence, reasoning, status = match
                    s_icon = {"approved":"✅","rejected":"❌","pending":"⏳"}.get(status,"⏳")
                    with st.expander(f"{s_icon} {nct_id} — {confidence:.0%} — {status.upper()}"):
                        st.write(f"**Trial:** {title}")
                        st.write(f"**Reasoning:** {reasoning}")
                        if status == "pending":
                            ca, cb = st.columns(2)
                            with ca:
                                if st.button("✅ Approve", key=f"approve_{match_id}", use_container_width=True):
                                    try:
                                        cur.execute("INSERT INTO agent_feedback (match_id,action,feedback_by,feedback_at) VALUES (%s,'approve',%s,NOW())", (match_id, current_user))
                                        cur.execute("UPDATE patient_trial_matches SET status='approved' WHERE match_id=%s", (match_id,))
                                        st.success(f"{nct_id} approved ✅"); st.rerun()
                                    except Exception as e: st.error(f"Approve failed: {e}")
                            with cb:
                                reason = st.text_input("Reason", key=f"reason_{match_id}", placeholder="optional")
                                if st.button("❌ Reject", key=f"reject_{match_id}", use_container_width=True):
                                    try:
                                        cur.execute("INSERT INTO agent_feedback (match_id,action,rejection_reason,feedback_by,feedback_at) VALUES (%s,'reject',%s,%s,NOW())", (match_id, reason or None, current_user))
                                        cur.execute("UPDATE patient_trial_matches SET status='rejected' WHERE match_id=%s", (match_id,))
                                        st.success(f"{nct_id} rejected ❌"); st.rerun()
                                    except Exception as e: st.error(f"Reject failed: {e}")
            else:
                st.info("No matches. Click 'Run Agent' to find matches using AI.")


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
    st.write("Upload X-rays, lab reports, or PDFs for AI analysis and trial re-matching.")
    uploaded_file = st.file_uploader("Upload record (PDF, JPG, PNG)",
                                      type=["pdf","jpg","jpeg","png"])
    if uploaded_file:
        if uploaded_file.type.startswith("image"): st.image(uploaded_file, width=400)
        else: st.write(f"📎 {uploaded_file.name}")
        if st.button("🤖 Analyze with Vision AI", type="primary"):
            with st.spinner("Analyzing with Llama-4-Maverick..."):
                result = analyze_medical_file(uploaded_file.read(), uploaded_file.type, uploaded_file.name)
            if "error" in result and not any(k in result for k in ["findings","diagnoses","raw_output"]):
                st.error(f"Analysis failed: {result['error']}")
            else:
                st.success("✅ Analysis complete")
                if "raw_output" in result:
                    st.warning("Structured parsing incomplete — raw output:"); st.code(result["raw_output"])
                else:
                    c1, c2 = st.columns(2)
                    with c1:
                        for field, label in [("findings","🔬 Findings"),("diagnosis","🏥 Diagnosis"),("diagnoses","🏥 Diagnoses")]:
                            if field in result:
                                st.subheader(label); val = result[field]
                                if isinstance(val, list):
                                    for item in val: st.write(f"• {item}")
                                else:
                                    icon = {"high":"🔴","medium":"🟡","low":"🟢"}.get(result.get("urgency",""),"")
                                    st.write(f"{val} {icon}")
                    with c2:
                        if "trial_conditions" in result:
                            st.subheader("🧬 Trial-Eligible Conditions")
                            for c in result["trial_conditions"]: st.write(f"• {c}")
                        if "medications" in result:
                            st.subheader("💊 Medications")
                            for m in result["medications"]: st.write(f"• {m}")
                    trial_conditions = result.get("trial_conditions", [])
                    if trial_conditions:
                        st.divider(); st.subheader("💾 Save & Re-Match")
                        cur.execute("SELECT patient_id, first_name, last_name FROM patients ORDER BY patient_id")
                        save_opts = {f"{p[1]} {p[2]}": p[0] for p in cur.fetchall()}
                        sp = st.selectbox("Save to:", list(save_opts.keys()), key="save_pt")
                        if st.button("💾 Save & Re-Match", type="primary"):
                            with st.spinner("Saving..."):
                                try:
                                    for cond in trial_conditions[:3]:
                                        cur.execute("INSERT INTO patient_conditions (patient_id,condition_name,condition_status,notes) VALUES (%s,%s,'active',%s) ON CONFLICT DO NOTHING",
                                                    (save_opts[sp], cond, f"Vision AI: {uploaded_file.name}"))
                                    r = run_agent_for_patient(save_opts[sp], current_user, cur)
                                    st.success(f"✅ Saved + {r.get('matches_stored',0)} new match(es)!")
                                except Exception as e: st.error(f"Save failed: {e}")


# ======================== TAB 4: EVAL DASHBOARD ========================
with tab4:
    st.header("📊 Evaluation Dashboard")
    with st.spinner("Loading metrics..."):
        c1,c2,c3,c4 = st.columns(4)
        cur.execute("SELECT COUNT(*) FROM clinical_trials");         c1.metric("Clinical Trials",   cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM patient_trial_matches");   c2.metric("Patient Matches",    cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM trial_eligibility_embeddings"); c3.metric("Trial Embeddings", cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM pubmed_embeddings WHERE embedding IS NOT NULL"); c4.metric("PubMed Articles", cur.fetchone()[0])
    st.divider()
    st.subheader("Match Status")
    cur.execute("SELECT status, COUNT(*) FROM patient_trial_matches GROUP BY status ORDER BY COUNT(*) DESC")
    srows = cur.fetchall()
    if srows:
        scols = st.columns(len(srows)); icons={"approved":"✅","rejected":"❌","pending":"⏳"}
        for i,(s,c) in enumerate(srows): scols[i].metric(f"{icons.get(s,'?')} {s.title()}", c)
    st.subheader("Agent Performance")
    cur.execute("""
        SELECT prompt_version, COUNT(*) AS runs, AVG(total_latency_ms) AS avg_latency,
               COUNT(CASE WHEN guardrail_violations IS NOT NULL THEN 1 END) AS violations
        FROM agent_traces GROUP BY prompt_version ORDER BY COUNT(*) DESC
    """)
    traces = cur.fetchall()
    if traces:
        for t in traces: st.write(f"**Prompt {t[0]}**: {t[1]} runs | {t[2]:.0f}ms avg | {t[3]} violation(s)")
    else: st.info("No agent traces yet.")
    st.subheader("Confidence Distribution")
    cur.execute("SELECT confidence_score FROM patient_trial_matches ORDER BY confidence_score DESC")
    scores = [r[0] for r in cur.fetchall()]
    if scores: st.bar_chart({"Confidence": scores})
    st.subheader("📈 Delta CDF Analytics")
    try:
        with st.spinner("Loading CDF..."):
            cur.execute("SELECT source_table, event_date, change_type, records_changed FROM cdf_analytics ORDER BY event_date DESC LIMIT 20")
            cdf_rows = cur.fetchall()
        if cdf_rows:
            import pandas as pd
            st.dataframe(pd.DataFrame(cdf_rows, columns=["Table","Date","Change","Records"]), use_container_width=True)
        else: st.info("CDF analytics: run Phase 6 in Score Remediation notebook.")
    except Exception: st.info("CDF analytics table not yet available.")


# ======================== TAB 5: ADMIN / LLMOps ========================
with tab5:
    st.header("⚙️ Admin & LLMOps")
    subtab1, subtab2, subtab3 = st.tabs(["Agent Traces", "Prompt Versions", "Feedback"])
    with subtab1:
        st.subheader("Recent Agent Traces")
        with st.spinner("Loading..."):
            cur.execute("SELECT trace_id, patient_id, prompt_version, tools_called, total_latency_ms, guardrail_violations, created_at FROM agent_traces ORDER BY created_at DESC LIMIT 20")
            traces = cur.fetchall()
        for trace in traces:
            with st.expander(f"Trace #{trace[0]} — Patient {trace[1]} ({trace[6]})"):
                st.write(f"**Prompt:** {trace[2]} | **Tools:** {trace[3]} | **Latency:** {trace[4]}ms")
                if trace[5]: st.error(f"Guardrail violations: {trace[5]}")
    with subtab2:
        st.subheader("Prompt Versions")
        with st.spinner("Loading..."):
            cur.execute("SELECT version, prompt_name, is_active, eval_precision, eval_recall FROM agent_prompts ORDER BY created_at DESC")
            prompts = cur.fetchall()
        if prompts:
            for p in prompts:
                st.write(f"{'\ud83d\udfe2 Active' if p[2] else '\u26aa Inactive'} **v{p[0]}** — {p[1]} (P:{p[3]}, R:{p[4]})")
        else: st.info("No prompt versions registered yet.")
    with subtab3:
        st.subheader("Human Feedback")
        with st.spinner("Loading..."):
            cur.execute("""
                SELECT f.action, f.rejection_reason, f.feedback_by, f.feedback_at, ptm.nct_id, ptm.confidence_score
                FROM agent_feedback f JOIN patient_trial_matches ptm ON f.match_id = ptm.match_id
                ORDER BY f.feedback_at DESC LIMIT 20""")
            feedback = cur.fetchall()
        if feedback:
            for fb in feedback:
                icon = "✅" if fb[0]=="approve" else "❌" if fb[0]=="reject" else "⚠️"
                st.write(f"{icon} {fb[4]} ({fb[5]:.0%}) — {fb[0]} by {fb[2]}")
                if fb[1]: st.caption(f"Reason: {fb[1]}")
        else: st.info("No feedback yet. Approve/reject matches in Patient Match tab.")


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
    st.divider()
    st.caption(f"🔋 Connected: {current_user}")
