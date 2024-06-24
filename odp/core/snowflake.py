import os
from datetime import date, datetime

import snowflake.connector
from pydantic import BaseModel
from snowflake.connector import SnowflakeConnection
from sqlglot import MappingSchema

from odp.core.detect_unused import extract_tables
from odp.core.types import Dialect, EnrichedQueryRow, QueryRow, SchemaRow


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
        snowflake_account=os.getenv("ODP_SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT")),  # type: ignore[arg-type]
        snowflake_user=os.getenv("ODP_SNOWFLAKE_USERNAME", os.getenv("SNOWFLAKE_USERNAME")),  # type: ignore[arg-type]
        snowflake_password=os.getenv("ODP_SNOWFLAKE_PASSWORD", os.getenv("SNOWFLAKE_PASSWORD")),  # type: ignore[arg-type]
        snowflake_database=os.getenv("ODP_SNOWFLAKE_DATABASE", os.getenv("SNOWFLAKE_DATABASE")),  # type: ignore[arg-type]
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

def make_snowflake_mapping_schema(info_schema: dict) -> MappingSchema:
    return MappingSchema(info_schema, dialect="snowflake")


def parse_snowflake_query(query_rows: list[QueryRow], schema: MappingSchema) -> list[EnrichedQueryRow]:
    res = []
    for row in query_rows:
        used_tables = extract_tables(
            query_text=row.QUERY_TEXT,
            dialect=Dialect.snowflake,
            schema=schema,
        )
        enriched_row = EnrichedQueryRow(
            QUERY_TEXT=row.QUERY_TEXT,
            DATABASE_NAME=row.DATABASE_NAME,
            SCHEMA_NAME=row.SCHEMA_NAME,
            START_TIME=row.START_TIME,
            USED_TABLES=[".".join([part.upper() for part in table]) for table in used_tables],
        )
        res.append(enriched_row)
    return res


def get_snowflake_queries(
    conn: SnowflakeConnection,
    since_datetime: datetime | date,
    before_datetime: datetime | date,
    database_name: str | None = None,
) -> list[QueryRow]:
    # Create a cursor object.
    cur = conn.cursor()

    # Execute a statement that will generate a result set.
    sql = f"""
SELECT QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%%select%%'
    {"" if database_name is None else "AND DATABASE_NAME = %(database_name)s"}
    AND START_TIME > %(since_datetime)s
    AND START_TIME < %(before_datetime)s
    AND ERROR_CODE IS NULL
ORDER BY START_TIME DESC
LIMIT 10000;
        """
    cur.execute(
        sql,
        {
            "database_name": database_name,
            "since_datetime": since_datetime,
            "before_datetime": before_datetime,
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
