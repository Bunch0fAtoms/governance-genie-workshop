-- Step 10 / 12 wall answer: gross margin by payer segment.
--
-- This is the question Genie space 1 refuses (no payer dimension in the five certified views) and
-- Genie space 2 answers after the room builds the derived product plus its metric view.
-- Verified on a serverless workspace: Cash-pay 50 percent (best) > Commercial 45 > Medicare 40.
--
-- mv_payer_segment_revenue is a Unity Catalog Metric View over the uncertified sandbox derived
-- product. Substitute {{CATALOG}} with your catalog (sandbox schema stays workshop_sandbox).

SELECT
  `Payer Segment`,
  MEASURE(`Patient Count`)              AS patients,
  ROUND(MEASURE(`Total Revenue USD`))   AS revenue_usd,
  ROUND(MEASURE(`Avg Margin Pct`), 1)   AS avg_gross_margin_pct
FROM {{CATALOG}}.workshop_sandbox.mv_payer_segment_revenue
GROUP BY ALL
ORDER BY avg_gross_margin_pct DESC;
