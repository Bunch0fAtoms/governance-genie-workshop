# Governance and Genie Workshop: connection and capability check

This is a minimal Databricks Asset Bundle (DAB). It confirms your workspace is
ready for the workshop build before the session. The full workshop materials will
be added here before the day, and you will re-pull to get the latest.

## What it checks

It deploys one job that runs a single notebook. The notebook creates a temporary
schema and then runs each governance action the workshop uses, recording a pass or
fail for each:

1. Create a schema.
2. Create a table and write a row.
3. Apply a column mask (`SET MASK`).
4. Apply a row filter (`SET ROW FILTER`).
5. Set tags (`SET TAGS`).
6. Grant and revoke a privilege.

It then drops the schema, so it leaves nothing behind.

## Before you start

- The Databricks command-line interface (CLI), installed and authenticated to your
  workspace, with a profile you can pass as `--profile <your-profile>`.
- Serverless jobs enabled in the workspace.
- A catalog where you can create a schema. Pass it as `--var catalog=<your_catalog>`.
  Point it at the catalog you plan to use for the workshop.

## Run it

```bash
# 1. Clone the repo
git clone https://github.com/Bunch0fAtoms/governance-genie-workshop.git
cd governance-genie-workshop

# 2. Validate (expect "Validation OK!")
databricks bundle validate -t dev --profile <your-profile> --var="catalog=<your_catalog>"

# 3. Deploy
databricks bundle deploy -t dev --profile <your-profile> --var="catalog=<your_catalog>"

# 4. Run
databricks bundle run connection_test -t dev --profile <your-profile> --var="catalog=<your_catalog>"
```

## What success looks like

The `bundle run` command prints the report at the end:

```
RESULT: PASSED  (6/6 checks passed)
Target: <catalog>.connection_test
  PASS  create schema
  PASS  create table and write
  PASS  column mask (SET MASK)
  PASS  row filter (SET ROW FILTER)
  PASS  tags (SET TAGS)
  PASS  grant / revoke
```

If a check fails, the run ends FAILED and the same report prints with `FAIL` on the
line that did not pass, and a short reason next to it.

A note on the logs: serverless startup lines such as
`[SnapStart] Environment variable POD_HOSTNAME is not set` are normal and harmless.
They are not errors. The `RESULT:` report is the line that matters.

## Send the result back

Reply with that `RESULT:` block. That tells us your workspace is ready, or exactly
which capability to sort out before the day.
