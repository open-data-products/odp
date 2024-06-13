from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

from odp.core.snowflake import get_snowflake_queries, get_snowflake_schema
from odp.core.detect_unused import detect_unused_tables, build_info_schema
from odp.core.types import Dialect

from .constants import UNUSED_TABLES_OUTPUT


class UnusedTablesConfig(Config):
    since_days: int = 60
    target_table: str = "dex_dev.dex_dev.unused_tables"


@asset
def unused_tables(
    config: UnusedTablesConfig,
    snowflake: SnowflakeResource,
) -> None:
    with snowflake.get_connection() as conn:
        queries = get_snowflake_queries(conn, config.since_days)
        schema = get_snowflake_schema(conn)

    info_schema, info_schema_flat = build_info_schema(schema)

    print(info_schema)
    print(info_schema_flat)

    unused_tables, most_common_tables = detect_unused_tables(
        queries=queries,
        info_schema=info_schema,
        info_schema_flat=info_schema_flat,
        dialect=Dialect.snowflake,
    )

    with snowflake.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE IDENTIFIER({target_table}) (table_name STRING)", params={
                "target_table": config.target_table,
            })

            for table in unused_tables:
                cur.execute(f"INSERT INTO {config.target_table} VALUES ('{table}')")


    with open(UNUSED_TABLES_OUTPUT, "w") as f:
        if most_common_tables is not None:
            f.write(f"Most common tables:\n")
            for tbl, count in most_common_tables:
                catalog, db, table = (obj.upper() for obj in tbl)
                f.write(f"{catalog}.{db}.{table}: {count}\n")
        else:
            f.write("No most common tables found in provided date range\n")

        f.write(f"Unused tables ({len(unused_tables)}):\n")
        for table in sorted(unused_tables):
            f.write(f"{table}\n")




