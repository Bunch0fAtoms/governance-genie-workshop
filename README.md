# Governance and Genie Workshop: connection test

This is a minimal Databricks Asset Bundle (DAB). Its only job is to confirm three
things work on your side before the workshop: you can pull this repo, deploy a
bundle to your workspace, and run a job. The full workshop materials will be added
here before the session, and you will re-pull to get the latest.

## What it does

It deploys one job with a single notebook. The notebook prints a success message
and runs a small read-only query to confirm Spark and Unity Catalog are reachable.
It creates and changes nothing in your data.

## Before you start

- The Databricks command-line interface (CLI) installed and authenticated to your
  workspace, with a profile you can pass as `--profile <your-profile>`.
- Serverless jobs enabled in the workspace.

## Run it

```bash
# 1. Clone the repo
git clone https://github.com/Bunch0fAtoms/governance-genie-workshop.git
cd governance-genie-workshop

# 2. Validate the bundle (expect "Validation OK!")
databricks bundle validate -t dev --profile <your-profile>

# 3. Deploy it
databricks bundle deploy -t dev --profile <your-profile>

# 4. Run the job
databricks bundle run connection_test -t dev --profile <your-profile>
```

## What success looks like

The job finishes and its output ends with "Connection test passed." That is the
thumbs-up we are looking for.

## If something trips

Reply with the output of the step that failed and we will sort it out.
