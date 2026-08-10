# Capstone Project Improvements

This document captures the post-submission improvement plan and the implementation added after reviewer feedback for the **Clinical Trial Matching & Recruitment Agent**.

## 1. Change Data Feed (CDF) Analytics

### Goal
Track agent and app activity end-to-end using Delta Change Data Feed and expose the analytics in the evaluation dashboard.

### Implemented design
* Enable CDF on key Delta tables written by the Spark pipeline.
* Add a derived analytics table named `agent_actions_cdf_analytics`.
* Use `table_changes()` to capture inserts and updates from write-oriented tables such as patient-trial matches, enrollment recommendations, and feedback events.
* Surface these metrics in the app dashboard:
  * tool-call counts over time
  * match writes over time
  * approval / rejection rates
  * recommendation writes over time

### Why this matters
This turns the project from a static demo into an observable LLMOps workflow with time-based operational analytics.

---

## 2. App Actions Now Write Back to the System

### Goal
Make the frontend actionable rather than read-only.

### Implemented design
* Approve / Reject buttons now write to `agent_feedback`.
* Match status can be updated in `patient_trial_matches`.
* Success messages are shown in the app UI.
* A Run Agent button is added so a user can trigger matching from the frontend and refresh results immediately.

### Why this matters
This satisfies the requirement that the AI agent must not only retrieve information but also take real actions against project data.

---

## 3. Guardrails Before Persisting Matches

### Goal
Add safety and consistency checks before new matches are written.

### Implemented design
Before saving a match, the code now verifies:
* the `nct_id` exists in `clinical_trials`
* confidence is within accepted bounds
* a drug interaction check was completed
* at least one PubMed citation is present when evidence is required

### Why this matters
These checks reduce hallucination risk and strengthen the clinical safety story of the capstone.

---

## 4. Vision Tab Hardening

### Goal
Replace placeholder text with a usable end-to-end analysis path.

### Implemented design
* The upload tab now attempts a real analysis path for images and PDFs.
* Parsed findings are shown in the UI.
* The implementation is structured so extracted findings can be persisted to `patient_conditions` in a future extension for immediate re-matching.

### Why this matters
The vision capability is no longer a conceptual demo only; it becomes a connected feature inside the app experience.

---

## 5. App Reliability and UX Polish

### Goal
Improve robustness and user experience for live demos.

### Implemented design
* Connectivity failures are surfaced as exception banners.
* Database queries use retry logic.
* Long-running actions show spinners.
* Dashboard and match views refresh cleanly after actions.

### Why this matters
These changes make the app more resilient for submission review and live walkthroughs.

---

## Summary of Added Value

These improvements strengthen the project across four dimensions:

* **LLMOps observability** — CDF analytics and dashboard instrumentation
* **True agent actions** — feedback writes, status updates, recommendation persistence
* **Safety** — validation guardrails before persistence
* **Demo quality** — more reliable UI, clearer feedback, better vision flow

Together, these changes make the project more production-like while staying aligned to the original capstone rubric.
