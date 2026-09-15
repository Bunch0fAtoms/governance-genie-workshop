-- Oracle 2: gross margin percent by channel.
--
-- Verified on a serverless workspace: DTC 50 percent > Pharmacy 45 > DME 40. A shift of volume from DME
-- toward DTC lifts blended margin. Any such shift is a modeled projection, not a recorded outcome.
--
-- mv_gross_margin is a Unity Catalog Metric View, so aggregate with MEASURE(). Substitute
-- {{CATALOG}} with your catalog (schema stays workshop_analytics).

SELECT
  Channel,
  ROUND(MEASURE(`Gross Margin Pct`), 1) AS gross_margin_pct
FROM {{CATALOG}}.workshop_analytics.mv_gross_margin
GROUP BY ALL
ORDER BY gross_margin_pct DESC;
