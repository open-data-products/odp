from datetime import datetime, timedelta
from typing import Optional

import pytz
import snowflake.connector
from dagster import (
    AssetKey,
    AssetObservation,
    AssetSelection,
    JobDefinition,
    MetadataValue,
    OpExecutionContext,
    job,
    op,
)
from dagster_snowflake import SnowflakeResource

from odp.core.detect_unused import build_info_schema, get_table_counts
from odp.core.snowflake import get_snowflake_queries
from odp.core.types import Dialect, SchemaRow
from snowflake_dagster_metadata_job.constants import META_KEY_STORAGE_IDENTIFIER, META_KEY_STORAGE_KIND


def _get_snowflake_schema_filtered(conn: snowflake.connector.SnowflakeConnection, tables: Optional[list[str]] = None):
    """Retrieve column-level schema information filtered by `tables`.

    Args:
        conn (SnowflakeConnection): Snowflake connection
        tables (list[str]): List of tables to filtered formatted catalog.schema.table

    Returns:
        list[SchemaRow]: List of filtered schema information

    """
    if tables is None:
        tables = []

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


def build_odp_snowflake_metadata_job(
    name: str, selection: AssetSelection, query_lookback_days: int = 14
) -> JobDefinition:
    """Creates a job that injects Snowflake metadata for given asset `selection`.

    Args:
        name (str): job name
        selection (AssetSelection): assets to inject metadata
        query_lookback_days (int): number of days to look a back for query insights

    Returns:
        JobDefinition: job that injects metadata onto assets
    """

    @op(name="odp_snowflake_metadata")
    def _op(context: OpExecutionContext, snowflake: SnowflakeResource) -> None:
        asset_graph = context.repository_def.asset_graph
        all_specs = [asset_node.to_asset_spec() for asset_node in asset_graph.asset_nodes]
        selected_asset_keys = selection.resolve(asset_graph)
        metadata_by_selected_asset_key = {
            spec.key: spec.metadata for spec in all_specs if spec.key in selected_asset_keys
        }

        snowflake_identifier_asset_mapping: dict[str, AssetKey] = {}
        for asset_key in metadata_by_selected_asset_key:
            asset_metadata = metadata_by_selected_asset_key[asset_key]

            storage_kind = asset_metadata.get(META_KEY_STORAGE_KIND)
            storage_identifier = asset_metadata.get(META_KEY_STORAGE_IDENTIFIER)
            if storage_kind == "snowflake" and storage_identifier is not None:
                snowflake_identifier_asset_mapping[storage_identifier.upper()] = asset_key

        with snowflake.get_connection() as conn:
            before_datetime = datetime.combine(datetime.today() + timedelta(days=1), datetime.max.time())
            since_datetime = before_datetime - timedelta(days=query_lookback_days)

            queries = get_snowflake_queries(conn, since_datetime, before_datetime)
            schema = _get_snowflake_schema_filtered(conn, [str(k) for k in snowflake_identifier_asset_mapping])
            info_schema, _ = build_info_schema(schema)

            table_counts = get_table_counts(
                dialect=Dialect.snowflake,
                info_schema=info_schema,
                queries=queries,
            )

            for identifier, asset_key in snowflake_identifier_asset_mapping.items():
                table_count = table_counts[tuple(identifier.split("."))]
                context.log_event(
                    AssetObservation(
                        asset_key=asset_key,
                        metadata={
                            "odp/table_access_count": table_count,
                            # `MetadataValue.timestamp` requires timezone
                            "odp/metadata_datetime_before": MetadataValue.timestamp(
                                pytz.timezone("UTC").localize(before_datetime)
                            ),
                            "odp/metadata_datetime_since": MetadataValue.timestamp(
                                pytz.timezone("UTC").localize(since_datetime)
                            ),
                        },
                    )
                )

    @job(name=name)
    def _job():
        _op()

    return _job
