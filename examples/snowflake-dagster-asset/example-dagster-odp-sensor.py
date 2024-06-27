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

from dagster import (
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
from odp.core.detect_unused import build_info_schema, detect_unused_tables
from odp.core.snowflake import get_snowflake_queries, get_snowflake_schema
from odp.core.types import Dialect


META_KEY_STORAGE_KIND = "dagster/storage_kind"
META_KEY_STORAGE_IDENTIFIER = "dagster/relation_identifier"


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "my_database.my_schema.my_table",
    }
)
def example_asset_1(): ...


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "my_database.my_schema.my_table2",
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


@op
def inject_odp_metadata(context: OpExecutionContext, snowflake: SnowflakeResource):
    # TODO - get all assets with `DagsterInstance.get_asset_keys()`

    snowflake_identifier_asset_mapping = {}
    for asset_def in all_assets:
        asset_metadata = asset_def.metadata_by_key[asset_def.key]

        storage_kind = asset_metadata.get(META_KEY_STORAGE_KIND)
        storage_identifier = asset_metadata.get(META_KEY_STORAGE_IDENTIFIER)
        if storage_kind == "snowflake" and storage_identifier is not None:
            snowflake_identifier_asset_mapping[storage_identifier.upper()] = asset_def.key

    # with snowflake.get_connection() as conn:
    #     before_datetime = datetime.combine(datetime.today() + timedelta(days=1), datetime.max.time())
    #     since_datetime = before_datetime - timedelta(days=5)
    #     queries = get_snowflake_queries(conn, since_datetime, before_datetime)
    #     schema = get_snowflake_schema(conn)
    #     info_schema, info_schema_flat = build_info_schema(schema)
    #
    #     # filter `info_schema` to tables that have corresponding Dagster assets
    #     filtered_info_schema_flat = [
    #         schema
    #         for schema in info_schema_flat
    #         if schema[1:] in [identifier.split(".") for identifier in snowflake_identifier_asset_mapping.keys()]
    #     ]
    #
    #     # TODO - currently disabled as performance hung on ~9000 table schema
    #
    #     unused_tables, most_common_tables = detect_unused_tables(
    #         queries=queries,
    #         info_schema=info_schema,
    #         info_schema_flat=filtered_info_schema_flat,
    #         dialect=Dialect.snowflake,
    #     )
    #
    #     # TODO - remap metadata from `detect_unused_tables` to asset metadata

    for asset_key in snowflake_identifier_asset_mapping.values():
        context.log_event(
            AssetObservation(
                asset_key=asset_key, metadata={"odp/num_snowflake_queries": 0, "odp/metalytics_url": "todo"}
            )
        )


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
