# odp

<!--
[![Release](https://img.shields.io/github/v/release/open-data-products/odp)](https://img.shields.io/github/v/release/open-data-products/odp)
[![Build status](https://img.shields.io/github/actions/workflow/status/open-data-products/odp/main.yml?branch=main)](https://github.com/open-data-products/odp/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/open-data-products/odp/branch/main/graph/badge.svg)](https://codecov.io/gh/open-data-products/odp)
[![Commit activity](https://img.shields.io/github/commit-activity/m/open-data-products/odp)](https://img.shields.io/github/commit-activity/m/open-data-products/odp)
[![License](https://img.shields.io/github/license/open-data-products/odp)](https://img.shields.io/github/license/open-data-products/odp)
-->

Detect unused tables and columns in your SQL database.

Currently supports Snowflake and BigQuery, support for Redshift and other warehouses [coming soon](/open-data-products/odp/issues).

- **Github repository**: <https://github.com/open-data-products/odp/>
- **Documentation** <https://open-data-products.github.io/odp/>

## Dev Usage

    poetry install

    poetry run python -m odp detect-unused --info_schema_file=examples/snowflake_info_schema.csv --queries_file=examples/snowflake_query.csv

or, run with snowflake env:

    cat <<EOF > .env
    ODP_SNOWFLAKE_ACCOUNT=your_account
    ODP_SNOWFLAKE_USER=your_user
    ODP_SNOWFLAKE_PASSWORD=your_password
    ODP_SNOWFLAKE_DATABASE=your_database
    ODP_SNOWFLAKE_WAREHOUSE=your_warehouse # optional
    ODP_SNOWFLAKE_ROLE=your_role           # optional
    EOF

    poetry run python -m odp detect-unused

or, run with bigquery env:

    cat <<EOF > .env
    ODP_GOOGLE_APPLICATION_CREDENTIALS=your_credential_file
    ODP_GOOGLE_PROJECT=your_project
    EOF

    poetry run python -m odp detect-unused --dialect=bigquery

## Maintainers

Made with :heart: by

- [metalytics.dev](https://metalytics.dev)
- [Twing Data](https://twingdata.com).
