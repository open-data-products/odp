from dagster import asset
from odp.core.snowflake import get_snowflake_queries, get_snowflake_schema

@asset
def unused_tables(snowflake: SnowflakeResource):
    with snowflake.get_connection() as conn:
        get_snowflake_queries()

