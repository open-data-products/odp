# snowflake_dagster_metadata_job

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

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

The project exposes a single asset, `unused_tables` - if you materialize this asset, you'll get a file at `data/staging/unused_tables.txt` that has a basic example of the CLI output:

```
Most common tables (20):
('CATALOG', 'PUBLIC', 'AGG_DAILY'): 30
('CATALOG', 'PUBLIC', 'BRAND'): 27
...
Unused tables (27):
('CATALOG', 'PUBLIC', 'AD_CLICK')
('CATALOG', 'PUBLIC', 'AD_COMBINED')
('CATALOG', 'PUBLIC', 'AD_CONVERSION')
```
