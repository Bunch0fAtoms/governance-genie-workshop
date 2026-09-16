# Governance and Genie Workshop

A one-day, hands-on workshop that builds a governed analytics foundation on Databricks and a
governed Genie assistant on top of it. Everything runs on serverless compute with synthetic data, so
nothing here touches real records.

The workshop has two layers:

- **A pre-built green foundation** you deploy and run once. It lands synthetic data, a
  bronze / silver / gold medallion, six Unity Catalog metric views, a seeded access-control table,
  and one worked governance example (a column mask on `ssn` and a region row filter).
- **A participant arc** the room builds on top. It extends the governance pattern, then builds two
  governed Genie spaces with Genie Code.

> Running the pre-session connection and capability check? That bundle now lives on the
> [`connection-check`](../../tree/connection-check) branch.

## Deploy and run the green foundation (in the workspace, no local terminal)

**1. Pull this repo as a Git folder.** In the workspace, go to Workspace, then Create, then Git
folder, and paste `https://github.com/Bunch0fAtoms/governance-genie-workshop.git`.

**2. Set your catalog.** Open `databricks.yml` and replace the `update_here` placeholder on the
`catalog` variable with a catalog where you can create schemas. You can instead pass
`--var catalog=<your_catalog>` from the command line, or set it in
`.databricks/bundle/dev/variable-overrides.json`.

**3. Deploy and run.** Open `databricks.yml` to open the bundle panel, click **Deploy**, then run the
**Foundation Build (pre-built, green)** job it creates. It runs five tasks and takes about five to
eight minutes on serverless.

**4. Confirm it worked.** You should see grain of 300 patients, 3,900 gold claims, and 3,600 revenue
rows, the access-control table seeded with 6 analyst and 8 steward grants, `ssn` masked to
`***-**-NNNN`, and the region row filter in effect.

### From a local terminal instead

```bash
databricks bundle deploy -t dev --profile <your-profile> --var "catalog=<your_catalog>"
databricks bundle run foundation_build -t dev --profile <your-profile> --var "catalog=<your_catalog>"
```

## Who sees masked data

The column mask and row filter exempt the account groups `workshop_stewards` and `workshop_admins`.
Create those in the account console before the session. On a workspace where those groups are not set
up, exempt your own user instead with `--var "privileged_principals=you@example.com"`. An empty value
exempts no one, so every caller sees the masked view.

## What the room builds (the participant arc)

The green foundation demonstrates the governance pattern once. The room then works the notebooks in
`notebooks/todo/`:

1. **Extend the masks** (`phase_2_classification_masks.py`): apply the same pattern to `dob`,
   `patient_name`, and the `patient_id` join key. The join key becomes a deterministic token, so a
   masked analyst can still join on it and get correct matches.
2. **Apply access control** (`apply_access_control.py`): reconcile the seeded policy table to real
   Unity Catalog grants, with auto-expiry.
3. **Build a derived product** (`step10_payer_segment_metric_view.py`): add the payer-segment
   dimension the certified data lacks, then expose it as a Unity Catalog metric view.
4. **Build two Genie spaces** with Genie Code and the prompt-to-genie skill: one on the certified
   metric views, and a clone that also includes the derived payer metric view, which answers a
   question the certified layer alone cannot.

## The Genie Code happy path

The room builds the two Genie spaces by prompting Genie Code with the prompt-to-genie skill. The
serialized space specifications and the verify queries live in `genie/`: `genie_space.json`,
`genie_space_payer.json`, the `verify_*.sql` oracles, and a walkthrough in `genie/README.md`.

## Everything is synthetic

All patients, claims, and revenue are generated with a fixed seed and are fully reproducible. No real
records are used anywhere in this kit.
