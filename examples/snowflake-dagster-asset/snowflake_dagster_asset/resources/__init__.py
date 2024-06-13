from dagster import EnvVar
from dagster_snowflake import SnowflakeResource


snowflake = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),  # required
    user=EnvVar("SNOWFLAKE_USERNAME"),  # required
    password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),  # required
    database=EnvVar("SNOWFLAKE_DATABASE"),  # required
    role=EnvVar("SNOWFLAKE_ROLE"),  # required
)