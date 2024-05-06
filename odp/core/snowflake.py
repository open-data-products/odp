import os

import snowflake.connector
from pydantic import BaseModel

from odp.core.types import SchemaRow, QueryRow


class SnowflakeCredentials(BaseModel):
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_role: str


def load_credentials() -> SnowflakeCredentials:
    """
    Load snowflake credentials from the .env-formatted file object
    """
    return SnowflakeCredentials(
        snowflake_account=os.environ["SNOWFLAKE_ACCOUNT"],
        snowflake_user=os.environ["SNOWFLAKE_USERNAME"],
        snowflake_password=os.environ["SNOWFLAKE_PASSWORD"],
        snowflake_warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        snowflake_database=os.environ["SNOWFLAKE_DATABASE"],
        snowflake_role=os.environ["SNOWFLAKE_ROLE"],
    )


def get_snowflake_queries() -> list[QueryRow]:
    credentials = load_credentials()

    conn = snowflake.connector.connect(
        user=credentials.snowflake_user,
        password=credentials.snowflake_password,
        account=credentials.snowflake_account,
        warehouse=credentials.snowflake_warehouse,
        database=credentials.snowflake_database,
        role=credentials.snowflake_role,
    )

    # Create a cursor object.
    cur = conn.cursor()

    # Execute a statement that will generate a result set.
    sql = """
SELECT QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE 'select%'
ORDER BY START_TIME DESC
LIMIT 10000; -- or start_time > $SOME_DATE to get columns unused in the last N days
        """
    cur.execute(sql)

    return cur.fetchall()


def get_snowflake_schema() -> list[SchemaRow]:
    credentials = load_credentials()

    conn = snowflake.connector.connect(
        user=credentials.snowflake_user,
        password=credentials.snowflake_password,
        account=credentials.snowflake_account,
        warehouse=credentials.snowflake_warehouse,
        database=credentials.snowflake_database,
        role=credentials.snowflake_role,
    )

    # Create a cursor object.
    cur = conn.cursor()

    # Execute a statement that will generate a result set.
    sql = """
SELECT
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME
FROM information_schema.columns
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';
    """
    cur.execute(sql)

    return cur.fetchall()
