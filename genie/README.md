# Genie spaces: the governed commercial agent (steps 7 to 12)

Participants leave with two working governed artificial intelligence (AI) agents. Each answers
business questions from certified data and refuses raw patient records on its own.

Genie Code is the Databricks in-workspace AI coding agent. The prompt-to-genie skill is a helper it
loads to build a Genie space from a plain-language description. A certified semantic layer is the set
of approved metric views that carry the business definitions. Each Genie space reads only those
views, so it cannot reach raw patient records.

The room builds the two spaces by prompting Genie Code with the
[prompt-to-genie](https://github.com/sean-zhang-dbx/prompt-to-genie) skill.

- **Space 1, "Order and Revenue Analytics"** reads the five certified metric views in
  `workshop_analytics`: `mv_revenue_by_channel`, `mv_fulfillment_rate`, `mv_recognized_revenue`,
  `mv_recurring_revenue`, `mv_gross_margin`. It answers the cycle-time and margin questions, and it
  cannot answer a payer-segment question, because no certified view carries a payer dimension.
- **Space 2, "Order and Revenue Analytics plus Payer"** clones space 1 and adds the step-10 metric
  view `workshop_sandbox.mv_payer_segment_revenue`. It now answers the payer-segment question that
  space 1 refused.

## The build path
1. Open Genie Code on a notebook and load prompt-to-genie. Describe space 1 in plain language: the
   five metric views, the business instructions, and the serverless warehouse. Genie Code profiles
   the views, excludes any patient identifier, and creates the space.
2. Interact with space 1, then encounter a payer-segment question the space cannot answer.
3. Build the step-10 derived product and its metric view (`notebooks/todo/`).
4. Prompt Genie Code again to clone space 1 and add the payer metric view. Interact with space 2 and
   see that payer-segment question answered.

## Files here (the facilitator fallback)
The two JavaScript Object Notation (JSON) files are the tested serialized-space definitions. If Genie
Code or prompt-to-genie is unavailable on the day, create the spaces directly. Substitute
`{{CATALOG}}`, `{{WAREHOUSE_ID}}`, and `{{PARENT_PATH}}` first.

```bash
databricks genie create-space --json @genie_space.json       --profile <profile>   # space 1
databricks genie create-space --json @genie_space_payer.json --profile <profile>   # space 2
```

- `genie_space.json` : space 1, the five certified metric views only.
- `genie_space_payer.json` : space 2, the five plus `mv_payer_segment_revenue`.
- `verify_cycle_time_by_channel.sql` : oracle 1. Cycle time, DTC fastest.
- `verify_margin_impact.sql` : oracle 2. Gross margin, DTC 50 percent, Pharmacy 45, DME 40.
- `verify_payer_segment_margin.sql` : the wall answer. Cash-pay 50, Commercial 45, Medicare 40.

The metric views are queried with `MEASURE()`. On a two-catalog metastore, use `workshop_certified`
as the catalog; keep `workshop_analytics` and `workshop_sandbox` as the schema names.
