from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

from odp.core.snowflake import get_snowflake_queries, get_snowflake_schema
from odp.core.detect_unused import detect_unused_tables, build_info_schema
from odp.core.types import Dialect


class UnusedTablesConfig(Config):
    since_days: int = 60


@asset
def unused_tables(config: UnusedTablesConfig, snowflake: SnowflakeResource):
    with snowflake.get_connection() as conn:
        queries = get_snowflake_queries(conn, config.since_days)
        schema = get_snowflake_schema(conn)

    info_schema, info_schema_flat = build_info_schema(schema)

    tables: list[tuple] = detect_unused_tables(
        queries,
        info_schema,
        info_schema_flat,
        Dialect.snowflake,
    )




