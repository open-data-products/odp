from datetime import datetime, timedelta

import snowflake.connector

from dagster import (
    AssetKey,
    AssetObservation,
    OpExecutionContext,
    job,
    op,
)
from dagster_snowflake import SnowflakeResource
from odp.core.detect_unused import build_info_schema, get_table_counts
from odp.core.snowflake import get_snowflake_queries
from odp.core.types import Dialect
from odp.core.types import Dialect, SchemaRow

from snowflake_dagster_metadata_job.assets import all_assets
from snowflake_dagster_metadata_job.constants import META_KEY_STORAGE_KIND, META_KEY_STORAGE_IDENTIFIER


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
