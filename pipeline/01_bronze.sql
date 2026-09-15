-- =============================================================================
-- Tandem Channel-Shift Foundation Pipeline (Spark Declarative Pipelines) - BRONZE
-- =============================================================================
-- Bronze layer: a raw copy of the synthetic foundation seed tables, plus the
-- technical metadata columns (_loaded_at, _source, _has_phi, _has_pii).
--
-- The seeds (patients, claims, revenue_summary) are generated imperatively by
-- src/00_load_foundation.py and read here by their fully qualified name via the
-- pipeline configuration values source_catalog and source_schema. Every layer
-- below (silver, gold) is fully declarative and references pipeline datasets by
-- their plain name, so the pipeline resolves the dependency graph on its own.
-- All data is 100% synthetic, for capability demonstration only.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW bronze_patients
  COMMENT 'Bronze patients: raw synthetic patient records with technical metadata.'
AS
SELECT
  patient_id,
  device_type,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  referring_hcp_id,
  current_timestamp() AS _loaded_at,
  'foundation' AS _source,
  true  AS _has_phi,
  true  AS _has_pii
FROM ${source_catalog}.${source_schema}.patients;

CREATE OR REFRESH MATERIALIZED VIEW bronze_claims
  COMMENT 'Bronze claims: raw synthetic claim events with technical metadata.'
AS
SELECT
  claim_id,
  patient_id,
  claim_date,
  claim_type,
  channel,
  product_line,
  product_sku,
  claim_amount_usd,
  units,
  current_timestamp() AS _loaded_at,
  'foundation' AS _source,
  false AS _has_phi,
  false AS _has_pii
FROM ${source_catalog}.${source_schema}.claims;

CREATE OR REFRESH MATERIALIZED VIEW bronze_revenue_summary
  COMMENT 'Bronze revenue summary: raw synthetic per-patient-month revenue with technical metadata.'
AS
SELECT
  patient_id,
  month,
  upfront_recognized_usd,
  recurring_recognized_usd,
  gross_margin_pct,
  fulfillment_cost_usd,
  total_recognized_usd,
  margin_usd,
  channel,
  device_type,
  current_timestamp() AS _loaded_at,
  'foundation' AS _source,
  false AS _has_phi,
  false AS _has_pii
FROM ${source_catalog}.${source_schema}.revenue_summary;

CREATE OR REFRESH MATERIALIZED VIEW bronze_hcps
  COMMENT 'Bronze HCPs: raw synthetic healthcare provider records with commercial attributes.'
AS
SELECT
  hcp_id,
  hcp_name,
  facility,
  specialty,
  role_type,
  region,
  segment_priority,
  primary_device_referred,
  nbrx_12m,
  trx_12m,
  current_timestamp() AS _loaded_at,
  'foundation' AS _source,
  false AS _has_phi,
  false AS _has_pii
FROM ${source_catalog}.${source_schema}.hcps;
