import os
from datetime import datetime, timedelta

import snowflake.connector
from pydantic import BaseModel
from snowflake.connector import SnowflakeConnection

from odp.core.types import QueryRow, SchemaRow


class SnowflakeCredentials(BaseModel):
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str
    snowflake_warehouse: str | None = None
    snowflake_role: str | None = None


def load_snowflake_credentials() -> SnowflakeCredentials:
    """
    Load snowflake credentials from env.

    Use ODP_ prefixed vars if present, fall back to standard SNOWFLAKE_ prefix.
    """
    return SnowflakeCredentials(
        snowflake_account=os.getenv("ODP_SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT")),
        snowflake_user=os.getenv("ODP_SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USERNAME")),
        snowflake_password=os.getenv("ODP_SNOWFLAKE_PASSWORD", os.getenv("SNOWFLAKE_PASSWORD")),
        snowflake_database=os.getenv("ODP_SNOWFLAKE_DATABASE", os.getenv("SNOWFLAKE_DATABASE")),
        snowflake_warehouse=os.getenv("ODP_SNOWFLAKE_WAREHOUSE", os.getenv("SNOWFLAKE_WAREHOUSE")),
        snowflake_role=os.getenv("ODP_SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE")),
    )


def get_snowflake_connection(credentials: SnowflakeCredentials) -> SnowflakeConnection:
    return snowflake.connector.connect(
        user=credentials.snowflake_user,
        password=credentials.snowflake_password,
        account=credentials.snowflake_account,
        database=credentials.snowflake_database,
        role=credentials.snowflake_role,
        warehouse=credentials.snowflake_warehouse,
    )


def get_snowflake_queries(conn: SnowflakeConnection, since_days: int) -> list[QueryRow]:
    start_datetime = datetime.now() - timedelta(days=since_days)

    # Create a cursor object.
    cur = conn.cursor()

    # Execute a statement that will generate a result set.
    sql = """
SELECT QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE 'select%%'
    AND DATABASE_NAME = %(database_name)s
    AND START_TIME > %(start_datetime)s
ORDER BY START_TIME DESC
LIMIT 10000;
        """
    cur.execute(
        sql,
        {
            "database_name": conn.database,
            "start_datetime": start_datetime,
        },
    )

    return [
        QueryRow(
            QUERY_TEXT=row[0],
            DATABASE_NAME=row[1],
            SCHEMA_NAME=row[2],
            START_TIME=row[3],
        )
        for row in cur.fetchall()
    ]


def get_snowflake_schema(conn: SnowflakeConnection) -> list[SchemaRow]:
    # Create a cursor object.
    cur = conn.cursor()

    # Execute a statement that will generate a result set.
    sql = f"""
SELECT
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME
FROM {conn.database}.information_schema.columns
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';
    """
    cur.execute(sql)

    return [
        SchemaRow(
            TABLE_CATALOG=row[0],
            TABLE_SCHEMA=row[1],
            TABLE_NAME=row[2],
            COLUMN_NAME=row[3],
        )
        for row in cur.fetchall()
    ]
