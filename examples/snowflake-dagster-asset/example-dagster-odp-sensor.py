"""

USAGE

    dagster dev -f example-dagster-odp-sensor.py

https://github.com/dagster-io/internal/discussions/9937

To determine if an asset is associated with a Snowflake table, we can filter our assets that have
the following metadata present:

    "dagster/storage_kind": "snowflake"
    "dagster/relation_identifier": "my_database.my_schema.my_table"

"""

from datetime import datetime, timedelta

import snowflake.connector

from dagster import (
    AssetKey,
    AssetObservation,
    Definitions,
    EnvVar,
    OpExecutionContext,
    ScheduleDefinition,
    asset,
    job,
    op,
)
from dagster_snowflake import SnowflakeResource
from odp.core.detect_unused import build_info_schema, get_table_counts
from odp.core.snowflake import get_snowflake_queries
from odp.core.types import Dialect
from odp.core.types import Dialect, SchemaRow




META_KEY_STORAGE_KIND = "dagster/storage_kind"
META_KEY_STORAGE_IDENTIFIER = "dagster/relation_identifier"


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "sandbox.toys.orders",
    }
)
def example_asset_1(): ...


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "snowflake.account_usage.query_history",
    }
)
def example_asset_2(): ...


@asset(
    metadata={
        "dagster/storage_kind": "bigquery",  # should be ignored as we only support Snowflake for now
        "dagster/relation_identifier": "my_database.my_schema.my_table3",
    }
)
def example_asset_3_bq(): ...


all_assets = [example_asset_1, example_asset_2, example_asset_3_bq]

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
)


def _get_snowflake_schema_filtered(conn: snowflake.connector.SnowflakeConnection, tables: list[str] = []):
    """Retrieve column-level schema information filtered by `tables`.

    Args:
        conn (SnowflakeConnection): Snowflake connection
        tables (list[str]): List of tables to filtered formatted catalog.schema.table

    Returns:
        list[SchemaRow]: List of filtered schema information

    """
    tables = [t.upper() for t in tables]

    cur = conn.cursor()

    sql = f"""
SELECT
  TABLE_CATALOG,
  TABLE_SCHEMA,
  TABLE_NAME,
  COLUMN_NAME
FROM {conn.database}.information_schema.columns
WHERE
  TABLE_SCHEMA != 'INFORMATION_SCHEMA'
  AND CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) in (%s)
;
    """
    cur.execute(sql, params=(tables,))

    return [
        SchemaRow(
            TABLE_CATALOG=row[0],
            TABLE_SCHEMA=row[1],
            TABLE_NAME=row[2],
            COLUMN_NAME=row[3],
        )
        for row in cur.fetchall()
    ]


@op
def inject_odp_metadata(context: OpExecutionContext, snowflake: SnowflakeResource):
    # TODO - get all assets with `DagsterInstance.get_asset_keys()`
    snowflake_identifier_asset_mapping: dict[str, AssetKey] = {}
    for asset_def in all_assets:
        asset_metadata = asset_def.metadata_by_key[asset_def.key]

        storage_kind = asset_metadata.get(META_KEY_STORAGE_KIND)
        storage_identifier = asset_metadata.get(META_KEY_STORAGE_IDENTIFIER)
        if storage_kind == "snowflake" and storage_identifier is not None:
            snowflake_identifier_asset_mapping[storage_identifier.upper()] = asset_def.key

    with snowflake.get_connection() as conn:
        before_datetime = datetime.combine(datetime.today() + timedelta(days=1), datetime.max.time())
        since_datetime = before_datetime - timedelta(days=5)
        queries = get_snowflake_queries(conn, since_datetime, before_datetime)

        schema = _get_snowflake_schema_filtered(conn, [str(k) for k in snowflake_identifier_asset_mapping.keys()])

        info_schema, _ = build_info_schema(schema)

        table_counts = get_table_counts(
            dialect=Dialect.snowflake,
            info_schema=info_schema,
            queries=queries,
        )

        for identifier, asset_key in snowflake_identifier_asset_mapping.items():
            table_count = table_counts[tuple(identifier.split("."))]
            context.log_event(AssetObservation(asset_key=asset_key, metadata={"odp/table_counts": table_count}))


@job
def insights_odp_job():
    inject_odp_metadata()


insights_odp_schedule = ScheduleDefinition(
    job=insights_odp_job,
    cron_schedule="0 0 * * *",
)


definitions = Definitions(
    assets=all_assets,
    jobs=[insights_odp_job],
    schedules=[insights_odp_schedule],
    resources={"snowflake": snowflake_resource},
)
