-- =============================================================================
-- Channel-Shift Foundation Pipeline (Spark Declarative Pipelines) - SILVER
-- =============================================================================
-- Silver layer: deduplicate on grain keys, enrich, and add quality flags.
-- Reads the bronze pipeline datasets by their plain name; the pipeline resolves
-- bronze -> silver dependencies automatically.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_patients
  COMMENT 'Silver patients: deduplicated on patient_id, enriched with age, device_category, and HCP referral.'
AS
WITH deduplicated AS (
  SELECT
    patient_id,
    device_type,
    region,
    ssn,
    dob,
    patient_name,
    enrolled_date,
    referring_hcp_id,
    _loaded_at,
    _source,
    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY _loaded_at DESC) AS rn
  FROM bronze_patients
)
SELECT
  patient_id,
  device_type,
  LOWER(CASE WHEN device_type = 'Mobi' THEN 'mobi' ELSE 'slim' END) AS device_category,
  region,
  ssn,
  dob,
  patient_name,
  enrolled_date,
  referring_hcp_id,
  CAST((DATEDIFF(enrolled_date, dob) / 365.25) AS INT) AS patient_age_at_enrollment,
  _loaded_at,
  _source,
  1.0 AS _quality_score
FROM deduplicated
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_hcps
  COMMENT 'Silver HCPs: deduplicated and enriched with derived metrics.'
AS
WITH deduplicated AS (
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
    _loaded_at,
    _source,
    ROW_NUMBER() OVER (PARTITION BY hcp_id ORDER BY _loaded_at DESC) AS rn
  FROM bronze_hcps
)
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
  CAST((trx_12m - nbrx_12m) AS DOUBLE) / CAST(trx_12m AS DOUBLE) AS retention_rate,
  _loaded_at,
  _source,
  1.0 AS _quality_score
FROM deduplicated
WHERE rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_claims
  COMMENT 'Silver claims: enriched with quarter/year, cycle time, and high-value flag.'
AS
WITH claim_cte AS (
  SELECT
    c.claim_id,
    c.patient_id,
    c.claim_date,
    c.claim_type,
    c.channel,
    c.product_line,
    c.product_sku,
    c.claim_amount_usd,
    c.units,
    c._loaded_at,
    c._source,
    p.device_type,
    p.region,
    QUARTER(c.claim_date) AS quarter_num,
    YEAR(c.claim_date) AS year_num
  FROM bronze_claims c
  JOIN silver_patients p ON c.patient_id = p.patient_id
),
psa_pump_join AS (
  SELECT
    c1.claim_id,
    c1.patient_id,
    c1.claim_date,
    c1.claim_type,
    c1.channel,
    c1.product_line,
    c1.product_sku,
    c1.claim_amount_usd,
    c1.units,
    c1._loaded_at,
    c1._source,
    c1.device_type,
    c1.region,
    c1.quarter_num,
    c1.year_num,
    DATEDIFF(c_pump.claim_date, c_psa.claim_date) AS days_to_pump_on_body,
    CASE WHEN c1.claim_amount_usd > 500 THEN true ELSE false END AS is_high_value
  FROM claim_cte c1
  LEFT JOIN claim_cte c_psa  ON c1.patient_id = c_psa.patient_id  AND c_psa.claim_type  = 'psa_intake'
  LEFT JOIN claim_cte c_pump ON c1.patient_id = c_pump.patient_id AND c_pump.claim_type = 'pump_on_body'
)
SELECT * FROM psa_pump_join;

CREATE OR REFRESH MATERIALIZED VIEW silver_revenue_summary
  COMMENT 'Silver revenue summary: margins, cumulative totals, and month-over-month growth.'
AS
WITH rev_cte AS (
  SELECT
    r.patient_id,
    r.month,
    r.upfront_recognized_usd,
    r.recurring_recognized_usd,
    r.gross_margin_pct,
    r.fulfillment_cost_usd,
    r.total_recognized_usd,
    r.margin_usd,
    r.channel,
    r.device_type,
    r._loaded_at,
    r._source,
    p.region,
    (r.total_recognized_usd * r.gross_margin_pct / 100) AS margin_calculated_usd,
    CASE WHEN r.margin_usd > 0 THEN true ELSE false END AS is_break_even,
    QUARTER(r.month) AS quarter_num,
    YEAR(r.month) AS year_num
  FROM bronze_revenue_summary r
  JOIN silver_patients p ON r.patient_id = p.patient_id
),
with_cumulative AS (
  SELECT
    *,
    SUM(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) AS cumulative_usd,
    SUM(margin_usd)           OVER (PARTITION BY patient_id ORDER BY month) AS cumulative_margin_usd
  FROM rev_cte
),
with_mom_growth AS (
  SELECT
    *,
    LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) AS prev_month_total,
    CASE
      WHEN LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) > 0
      THEN ROUND((total_recognized_usd - LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month))
                 / LAG(total_recognized_usd) OVER (PARTITION BY patient_id ORDER BY month) * 100, 2)
      ELSE 0
    END AS moM_growth_pct
  FROM with_cumulative
)
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
  moM_growth_pct,
  _loaded_at,
  _source
FROM with_mom_growth;
