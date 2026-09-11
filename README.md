# Governance and Genie Workshop: connection and capability check

This repo confirms your workspace is ready for the workshop build before the
session. It uses a minimal Declarative Automation Bundles (DAB, formerly Databricks
Asset Bundles) configuration to run a connection and capability test. The full
workshop materials will be added here before the day, and you will re-pull to get
the latest.

## What it checks

It runs a single notebook that creates a temporary schema, then runs each
governance action the workshop uses, recording a pass or fail for each:

1. Create a schema.
2. Create a table and write a row.
3. Apply a column mask (`SET MASK`).
4. Apply a row filter (`SET ROW FILTER`).
5. Set tags (`SET TAGS`).
6. Grant and revoke a privilege.

It then drops the schema, so it leaves nothing behind.

## Run it in the workspace (no local terminal needed)

**1. Pull the repo as a Git folder.**
In the workspace, go to Workspace, then Create, then Git folder, and paste
`https://github.com/Bunch0fAtoms/governance-genie-workshop.git`.

**2. Deploy and run the bundle.**
Open `databricks.yml`. The bundle (Deployments) panel opens on the left. Click
**Deploy**, then run the **Connection and capability probe** job that Deploy creates.

**3. See the result on the run page.**
Open that run. You can find it under Jobs and Pipelines, or click the link the
panel shows. Click the task and read the **Output**. The result is a table of each
capability with PASS or FAIL, followed by a `RESULT:` summary line.

### The quickest way to eyeball it

Open `src/connection_test.py`. Attach **Serverless** and click **Run all**. The
result table appears inline at the bottom of the notebook. This runs the same
checks without deploying the job.

By default it uses your workspace's current catalog. If the create-schema check
fails with a permission error, set the **catalog** box at the top to a catalog
where you can create a schema, then run again.

## Where the result shows, and where it does not

- **Shows:** on the run page (the task Output), and inline in the notebook cells
  when you use Run all.
- **Does not show:** in the notebook editor before or without a run. A cell that
  reads only "Command ran successfully" with nothing under it means you are looking
  at the editor, not a run. Open the run, or use Run all.

A note on the logs: serverless startup lines such as
`[SnapStart] Environment variable POD_HOSTNAME is not set` are normal and harmless.
They are not errors. The `RESULT:` report is the line that matters.

## Send the result back

Reply with the `RESULT:` summary (or a screenshot of the result table). That tells
us your workspace is ready, or exactly which capability to sort out before the day.

## Optional: from a local terminal

If you prefer the command line, with the Databricks command-line interface (CLI)
authenticated to your workspace:

```bash
git clone https://github.com/Bunch0fAtoms/governance-genie-workshop.git
cd governance-genie-workshop
databricks bundle deploy -t dev --profile <your-profile>
databricks bundle run connection_test -t dev --profile <your-profile>
```

The `RESULT:` report prints at the end of the run. It uses your workspace's current
catalog by default. To test a specific catalog, add
`--var="catalog=<your_catalog>"` to the deploy and run commands.
