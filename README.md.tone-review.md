# Tone review: README.md

Reviewed: `/Users/morgan.williams/Vibe/Tandem/governance-genie-workshop/README.md`  
Date: 2026-09-11

```
DOC-TONE-CRITIC REVIEW
File: /Users/morgan.williams/Vibe/Tandem/governance-genie-workshop/README.md
Reviewed: 2026-09-11
GRADE: 4.4/5, FAIL, 1 blocker
Companion executive review: NOT REQUIRED

Scores
  Structure      4/5
  Readability    4/5
  Client framing 5/5
  Design system  5/5
  Mechanics      4/5

Findings (most severe first)
  [blocker] (2026-09-11) Line 3: "Databricks Asset Bundle (DAB)" uses the stale pre-rename
    product name. The current official name, confirmed live 2026-09-01 against the Databricks
    docs, is "Declarative Automation Bundles." CLAUDE.md rule: "Known definitions must be
    exact." Fix: Change to "Declarative Automation Bundles (DAB)" (acronym stays DAB; a brief
    "(formerly Databricks Asset Bundles)" note is acceptable if the audience knows the old
    name, per the memory note, but keep it short and do not belabor the rename).

  [improvement] (2026-09-11) Line 3 (opening sentence): The doc leads with the format
    description ("This is a minimal Declarative Automation Bundles (DAB)") rather than the
    goal. CLAUDE.md rule: "Lead with the goal. Say what we are trying to do, then explain
    how." Fix: Swap the order. Example: "This repo confirms your workspace is ready for the
    workshop. It uses a minimal Declarative Automation Bundles (DAB) configuration to run a
    connection and capability test." The goal lands first; the format explains how.

  [improvement] (2026-09-11) Lines 38-40 ("The quickest way to eyeball it" section): Four
    sequential actions are strung together in one sentence: open, set, attach, click. The
    sentence runs to roughly 30 words. CLAUDE.md rule: one idea per sentence, roughly 15 to
    20 words. Fix: Split into three sentences: "Open `src/connection_test.py`. Set the
    **catalog** box at the top to a catalog where you can create a schema. Attach
    **Serverless** and click **Run all**."

  [nit] (2026-09-11) Line 29 (Step 2, "Run it in the workspace"): "run the **Connection and
    capability probe** job it created" - the pronoun "it" is ambiguous; the Deploy action
    creates the job but the sentence reads as if "databricks.yml" creates it. Fix: "run the
    **Connection and capability probe** job that Deploy creates."

  [nit] (2026-09-11) Lines 32-33 (Step 3): "Open that run (Jobs and Pipelines, or the link
    the panel shows), click the task, and read the **Output**." - three actions in one sentence
    with an interrupting parenthetical. Fix: "Open that run. You can find it under Jobs and
    Pipelines, or click the link the panel shows. Click the task and read the **Output**."

Verdict
  FAIL. Fix the DAB expansion first: "Databricks Asset Bundle" is the stale name; the current
  official product name is "Declarative Automation Bundles." The rest of the document is clean,
  direct, and well suited to its technical audience.

Review saved: /Users/morgan.williams/Vibe/Tandem/governance-genie-workshop/README.md.tone-review.md
```

## Findings detail

- (2026-09-11) **[blocker]** Line 3: Stale acronym expansion. "Databricks Asset Bundle (DAB)"
  must be "Declarative Automation Bundles (DAB)." Rule: known definitions must be exact;
  current official name confirmed live 2026-09-01.

- (2026-09-11) **[improvement]** Line 3: Opening leads with format, not goal. Swap so the goal
  ("confirm your workspace is ready") comes first, and the format ("Declarative Automation
  Bundles configuration") follows as the how.

- (2026-09-11) **[improvement]** Lines 38-40: Four actions in one 30-word sentence. Split into
  three sentences (open / set catalog / attach Serverless and run).

- (2026-09-11) **[nit]** Line 29: Ambiguous pronoun "it" in "job it created." Change to "job
  that Deploy creates."

- (2026-09-11) **[nit]** Lines 32-33: Three actions plus a parenthetical in one sentence.
  Split into three short sentences.
