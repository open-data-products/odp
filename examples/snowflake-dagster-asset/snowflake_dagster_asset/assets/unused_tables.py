from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

from odp.core.snowflake import get_snowflake_queries, get_snowflake_schema
from odp.core.detect_unused import detect_unused_tables, build_info_schema
from odp.core.types import Dialect

from .constants import UNUSED_TABLES_OUTPUT


class UnusedTablesConfig(Config):
    since_days: int = 60


@asset
def unused_tables(
    config: UnusedTablesConfig,
    snowflake: SnowflakeResource,
) -> None:
    with snowflake.get_connection() as conn:
        queries = get_snowflake_queries(conn, config.since_days)
        schema = get_snowflake_schema(conn)

    info_schema, info_schema_flat = build_info_schema(schema)

    tables, most_common_tables = detect_unused_tables(
        queries=queries,
        info_schema=info_schema,
        info_schema_flat=info_schema_flat,
        dialect=Dialect.snowflake,
    )

    with open(UNUSED_TABLES_OUTPUT, "w") as f:
        for table in tables:
            f.write(f"{table}\n")




