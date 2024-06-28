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

The project will scan for assets that have Snowflake table identifiers, query or metadata using `odp` and inject the relevant metadata back onto the asset.

<img width="1288" alt="image" src="https://github.com/cmpadden/odp/assets/5807118/5f83fbef-32c6-46e1-9b37-8c0e7cbe0d7f">
