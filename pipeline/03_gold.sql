-- =============================================================================
-- Tandem Channel-Shift Foundation Pipeline (Spark Declarative Pipelines) - GOLD
-- =============================================================================
-- Gold layer: business-ready, certified data products. These three materialized
-- views are the certified surface the workshop governs (masks, row filter, tags
-- applied in governance/phase_2_classification_masks.py via ALTER MATERIALIZED
-- VIEW) and that the five metric views read.
--
-- Grain: gold_patients (patient_id), gold_claims (claim_id, filtered to
-- pump_on_body + consumable_shipment), gold_revenue_summary (patient_id, month).
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_patients
  COMMENT 'Gold patients: one row per patient, with lifecycle, primary channel, HCP referral, and 12-month revenue totals.'
AS
WITH patient_claims AS (
  SELECT
    p.patient_id,
    p.device_type,
    p.region,
    p.ssn,
    p.dob,
    p.patient_name,
    p.enrolled_date,
    p.referring_hcp_id,
    p.device_category,
    CASE
      WHEN MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.claim_date END) >= CURRENT_DATE - INTERVAL 12 MONTH
      THEN 'active'
      ELSE 'lapsed'
    END AS patient_lifecycle_status,
    MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.claim_date END) AS pump_on_body_date,
    MAX(CASE WHEN c.claim_type = 'pump_on_body' THEN c.channel END) AS channel_primary,
    ROUND(SUM(CASE WHEN r.upfront_recognized_usd > 0 THEN r.upfront_recognized_usd ELSE 0 END), 2) AS total_upfront_12m,
    ROUND(SUM(CASE WHEN r.recurring_recognized_usd > 0 THEN r.recurring_recognized_usd ELSE 0 END), 2) AS total_consumable_spend_12m
  FROM silver_patients p
  LEFT JOIN silver_claims c          ON p.patient_id = c.patient_id
  LEFT JOIN silver_revenue_summary r ON p.patient_id = r.patient_id
  GROUP BY
    p.patient_id, p.device_type, p.region, p.ssn, p.dob, p.patient_name,
    p.enrolled_date, p.referring_hcp_id, p.device_category
)
SELECT
  patient_id,
  device_type,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  referring_hcp_id,
  device_category,
  patient_lifecycle_status,
  pump_on_body_date,
  channel_primary,
  total_upfront_12m,
  total_consumable_spend_12m
FROM patient_claims;

CREATE OR REFRESH MATERIALIZED VIEW gold_claims
  COMMENT 'Gold claims: pump_on_body and consumable_shipment events only, with month offset from placement.'
AS
WITH filtered_claims AS (
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
    device_type,
    region,
    quarter_num,
    year_num,
    days_to_pump_on_body,
    is_high_value,
    ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY _loaded_at DESC) AS rn
  FROM silver_claims
  WHERE claim_type IN ('pump_on_body', 'consumable_shipment')
),
with_month_offset AS (
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
    device_type,
    region,
    quarter_num,
    year_num,
    days_to_pump_on_body,
    is_high_value,
    CASE
      WHEN claim_type = 'pump_on_body' THEN 0
      WHEN claim_type = 'consumable_shipment'
      THEN MONTH(claim_date) - MONTH(MAX(CASE WHEN claim_type = 'pump_on_body' THEN claim_date END)
                                     OVER (PARTITION BY patient_id)) +
           12 * (YEAR(claim_date) - YEAR(MAX(CASE WHEN claim_type = 'pump_on_body' THEN claim_date END)
                                             OVER (PARTITION BY patient_id)))
      ELSE NULL
    END AS month_offset_from_placement
  FROM filtered_claims
  WHERE rn = 1
)
SELECT * FROM with_month_offset;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_summary
  COMMENT 'Gold revenue summary: certified per-patient-month revenue with cumulative and growth metrics.'
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
  region,
  margin_calculated_usd,
  is_break_even,
  quarter_num,
  year_num,
  cumulative_usd,
  cumulative_margin_usd,
  moM_growth_pct
FROM silver_revenue_summary;

CREATE OR REFRESH MATERIALIZED VIEW gold_hcp
  COMMENT 'Gold HCPs: certified healthcare provider commercial profile with referral performance metrics.'
AS
SELECT
  h.hcp_id,
  h.hcp_name,
  h.facility,
  h.specialty,
  h.role_type,
  h.region,
  h.segment_priority,
  h.primary_device_referred,
  h.nbrx_12m,
  h.trx_12m,
  h.retention_rate,
  COUNT(DISTINCT p.patient_id) AS patients_referred,
  COUNT(DISTINCT CASE WHEN p.patient_lifecycle_status = 'active' THEN p.patient_id END) AS active_placements,
  ROUND(CAST(COUNT(DISTINCT CASE WHEN p.patient_lifecycle_status = 'active' THEN p.patient_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT p.patient_id), 0), 3) AS placement_success_rate
FROM silver_hcps h
LEFT JOIN gold_patients p ON h.hcp_id = p.referring_hcp_id
GROUP BY
  h.hcp_id, h.hcp_name, h.facility, h.specialty, h.role_type, h.region, h.segment_priority,
  h.primary_device_referred, h.nbrx_12m, h.trx_12m, h.retention_rate;
