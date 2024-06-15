# snowflake_dagster_asset_check

This example uses ODP as a dagster Asset Check to flag an asset as unhealthy if it has not been used in the last 30 days, or (soon) if it has significantly declining usage across different moving averages.

In this project, we generate two tables in snowflake, one of which is built from the other. Incidentally, this will cause one of them to appear as unused.

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
pip install -e ../.. # install local odp package
```

You'll need to configure snowflake credentials to use the asset in this project:

```bash
cp .env.example .env
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Assets

This project builds two tables

- `ISSUE_HISTORY` - a generic events data table meant to track issue statuses from a tool like JIRA, Linear, or GitHub Issues. States will be something like "open", "in progress", "closed", etc.

Here's an example:

| issue_id | new_state   | updated_at |
| -------- | ----------- | ---------- |
| 1        | open        | 2021-01-01 |
| 1        | in progress | 2021-01-02 |
| 2        | open        | 2021-01-02 |
| 1        | closed      | 2021-01-03 |
| 2        | in progress | 2021-01-03 |
| 2        | closed      | 2021-01-05 |

- `ISSUE_COUNTS_BY_DAY` - a cumulative flow aggregation that shows # of issues in each state on a given day

Here's an example:

| date       | open | in progress | closed |
| ---------- | ---- | ----------- | ------ |
| 2021-01-01 | 1    | 0           | 0      |
| 2021-01-02 | 1    | 1           | 0      |
| 2021-01-03 | 0    | 1           | 1      |
| 2021-01-04 | 0    | 1           | 1      |
| 2021-01-05 | 0    | 0           | 2      |

## reviewing the check
