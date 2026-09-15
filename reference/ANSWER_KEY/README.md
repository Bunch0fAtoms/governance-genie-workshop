# Tandem Workshop: Verified Answer Key

These eight notebooks are the SA-only answer key. They are verbatim copies of the
production notebooks that ran green in the workshop validation environment.

**Do not hand these to participants before they attempt the TODOs.**
Distribute the blanked scaffold in `notebooks/todo/` instead.

## Verified grain (foundation job run 67614706136489)

| Table | Rows | Key constraint |
|---|---|---|
| patients | 300 | patient_id unique |
| claims | 4,800 | claim_id unique (300 PSA, 300 pump_on_body, 3,600 consumable_shipments) |
| revenue_summary | 3,600 | (patient_id, month) unique |
| gold_patients | 300 | patient_id unique |
| gold_claims | 3,900 | claim_id unique (pump_on_body and consumable_shipment only; PSA filtered) |
| gold_revenue_summary | 3,600 | (patient_id, month) unique |

Note: BUILD_FACTS.md lists grain as 300/3900/3600 for the gold tables, which matches.

## Oracle values (verified run 430679350084871)

Level 1 gate: analyst@tandem.com active grants = **6**
Level 2 gate: rows blocked by region row filter (non-US West patients) = **195**
Level 3 gate: DTC average cycle time, Q1 2026, Pump on Body = **30** days

## Notebooks

| File | Layer | What it builds |
|---|---|---|
| `00_load_foundation.py` | Foundation | Generates synthetic patients, claims, revenue_summary (seed 42) |
| `01_load_bronze.py` | Bronze | Raw copy with technical metadata (_loaded_at, _source, _has_phi, _has_pii) |
| `02_build_silver.py` | Silver | Deduplication, enrichment, cycle-time calculation, MoM growth |
| `03_build_gold.py` | Gold | Business-ready filtered tables with grain constraints |
| `04_metric_views.py` | Metric Views | Five views backing the Genie space (certified data only) |
| `phase_1_access_control.py` | Governance Phase 1 | access_control table, audit table, seeded grants, sandbox copies |
| `phase_2_classification_masks.py` | Governance Phase 2 | PHI mask functions, masked views, region row filter |
| `apply_access_control.py` | Governance Phase 1 | Idempotent apply job: GRANT active rows, REVOKE expired rows |

All data is 100% synthetic for capability demonstration. No real Tandem patient data.
