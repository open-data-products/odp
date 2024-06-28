# odp

- **Announcement Post**: <https://github.com/open-data-products/odp/blob/main/docs/announcement.md>

Open Data Products (ODP) is a toolkit that helps data practitioners and data leaders better understand the value of their data. It can help teams understand what data is in use and who to talk to in order to understand which data is driving business value and why. For example, the detect-unused command can help find tables or columns that are unused within a certain time range (e.g. 60 days).

```
pip install odp
```

```
odp detect-unused --dialect=snowflake --grain=table --since-days=60
```

```
Read 63 queries from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
Read 116 information schema rows from ANALYTICS_PROD.INFORMATION_SCHEMA.COLUMNS
Most common tables (20):
('CATALOG', 'PUBLIC', 'AGG_DAILY'): 30
('CATALOG', 'PUBLIC', 'BRAND'): 27
...
Unused tables (27):
('CATALOG', 'PUBLIC', 'AD_CLICK')
('CATALOG', 'PUBLIC', 'AD_COMBINED')
('CATALOG', 'PUBLIC', 'AD_CONVERSION')
...
```

- **Github repository**: <https://github.com/open-data-products/odp/>

## Dev Usage

    poetry install

    poetry run python -m odp detect-unused --schema-file=examples/snowflake/info-schema.csv --queries-file=examples/snowflake/query-history.csv

or, run with snowflake env:

    cat <<EOF > .env
    ODP_SNOWFLAKE_ACCOUNT=your_account
    ODP_SNOWFLAKE_USERNAME=your_user
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
