-- Oracle 1: cycle time from Patient Services Agreement (PSA) intake to Pump on Body, Q1 2026.
--
-- Verified on a serverless workspace (Genie space 1 generated the equivalent MEASURE() query):
-- DTC + t:slim X2 is fastest at about 25 days; DTC leads, then Pharmacy, then DME.
--
-- mv_fulfillment_rate is a Unity Catalog Metric View, so aggregate with MEASURE(), not AVG().
-- Substitute {{CATALOG}} with your catalog. The schema stays workshop_analytics on any workspace
-- (on a two-catalog metastore use workshop_certified as the catalog).

SELECT
  Channel,
  `Device Type`,
  ROUND(MEASURE(`Avg Cycle Time Days`)) AS avg_cycle_days,
  MEASURE(`Patient Count`)              AS patient_count
FROM {{CATALOG}}.workshop_analytics.mv_fulfillment_rate
WHERE `Claim Type` = 'pump_on_body' AND Quarter = 1 AND Year = 2026
GROUP BY ALL
ORDER BY avg_cycle_days ASC;
