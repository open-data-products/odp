import os
from datetime import datetime, timedelta

import click
from dotenv import load_dotenv

from odp.core.detect_unused import (
    build_info_schema,
    detect_unused_columns,
    detect_unused_tables,
    read_info_schema_from_file,
    read_queries,
)
from odp.core.snowflake import (
    get_snowflake_connection,
    get_snowflake_queries,
    get_snowflake_schema,
    load_snowflake_credentials,
)
from odp.core.types import Dialect, Grain, validate_dialect, validate_grain

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))


@click.group(name="odp")
def cli() -> None:
    pass


@cli.command(
    "detect-unused",
    help="""
Detect unused columns in SQL queries.
Requires two files: a query file and an information schema file.
Run `show-queries` to see the SQL queries to run against your
Snowflake instance to generate the two files.""",
)
@click.option("--queries-file", help="The SQL query file to analyze.")
@click.option("--schema-file", help="The file containing the information schema for the database.")
@click.option(
    "--dialect",
    type=click.Choice([d.value for d in Dialect]),
    callback=validate_dialect,
    default="snowflake",
    help="The type of warehouse to connect to. Currently only snowflake is supported.",
)
@click.option(
    "--grain",
    type=click.Choice([g.value for g in Grain]),
    callback=validate_grain,
    default="table",
    help="the grain to search for, e.g. use --grain=column to search for unused tables. Default is table.",
)
@click.option(
    "--since-days",
    type=click.INT,
    default=60,
    help="Count as 'unused' any asset that has not been queried in the last N days. Defaults to 60 days",
)
def cli_detect_unused_columns(
    queries_file: str | None,
    schema_file: str | None,
    dialect: Dialect,
    grain: Grain,
    since_days: int,
) -> None:
    if queries_file and schema_file:
        queries = read_queries(queries_file, since_days)
        print(f"Read {len(queries)} queries from {queries_file}")

        info_schema, info_schema_flat = read_info_schema_from_file(schema_file)
        print(f"Read {len(info_schema_flat)} information schema rows from {schema_file}")
        if grain == Grain.column:
            unused, most_common = detect_unused_columns(queries, info_schema, info_schema_flat, dialect)
            print_results(most_common, unused, grain)
        elif grain == Grain.table:
            unused, most_common = detect_unused_tables(queries, info_schema, info_schema_flat, dialect)
            print_results(most_common, unused, grain)
        elif grain == Grain.schema:
            raise NotImplementedError("Schema grain is not yet supported")

        return

    if dialect == Dialect.snowflake:
        try:
            credentials = load_snowflake_credentials()
        except KeyError as e:
            raise ValueError(
                f"""
Missing or invalid parameters: {e}. Please provide either

   1. both of --info-schema-file and --queries-file (use "odp show-queries" to learn how to generate these)
   2. valid credentials via env, e.g. ODP_SNOWFLAKE_ACCOUNT, ODP_SNOWFLAKE_USERNAME, etc
            """
            ) from e

        conn = get_snowflake_connection(credentials)
        schema = get_snowflake_schema(conn)
        before_date = datetime.combine(datetime.now(), datetime.max.time())
        since_date = before_date - timedelta(days=since_days)
        queries = get_snowflake_queries(
            conn,
            since_datetime=since_date,
            before_datetime=before_date,
        )

        info_schema, info_schema_flat = build_info_schema(schema)
        if grain == Grain.column:
            unused, most_common = detect_unused_columns(queries, info_schema, info_schema_flat, dialect)
            print_results(most_common, unused, grain)
        elif grain == Grain.table:
            unused, most_common = detect_unused_tables(queries, info_schema, info_schema_flat, dialect)
            print_results(most_common, unused, grain)
        elif grain == Grain.schema:
            raise NotImplementedError("Schema grain is not yet supported")
    elif dialect == Dialect.bigquery:
        raise NotImplementedError("Loading BigQuery data via credentials is not yet supported")
    elif dialect == Dialect.redshift:
        raise NotImplementedError("Loading Redshift data via credentials is not yet supported")


def print_results(
    most_common: list[tuple[tuple[str, str, str], int]] | None,
    unused: list[tuple[str, str, str]],
    grain: Grain = Grain.table,
) -> None:
    def render(obj: tuple) -> str:
        """Render a table or column tuple as a string"""
        return ".".join([x.upper() for x in obj])

    if most_common is not None:
        print(f"Most common {grain.plural()}:")
        for asset, count in most_common:
            print(f"{render(asset)}: {count}")
    else:
        print(f"No most common {grain.plural()} found in provided date range")
    print(f"Unused {grain.plural()} ({len(unused)}):")
    for asset in sorted(unused):
        print(f"{render(asset)}")


@cli.command("show-queries")
def show_snowflake_queries() -> None:
    print("Run the below against your snowflake instance to generate a dataset you can" "export to CSV for analysis")
    print(
        """
-- query_file

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE 'select%'
ORDER BY START_TIME DESC
LIMIT 10000; -- or start_time > $SOME_DATE to get columns unused in the last N days


-- schema_file

use database MY_DATABASE; -- change me

SELECT
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME
FROM information_schema.columns
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';

        """
    )


if __name__ == "__main__":
    cli()
