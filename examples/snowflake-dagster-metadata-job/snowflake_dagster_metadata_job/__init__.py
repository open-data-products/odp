from dagster import Definitions

from snowflake_dagster_metadata_job.assets import all_assets
from snowflake_dagster_metadata_job.jobs import insights_odp_job
from snowflake_dagster_metadata_job.resources import snowflake_resource
from snowflake_dagster_metadata_job.schedules import insights_odp_schedule


definitions = Definitions(
    assets=all_assets,
    jobs=[insights_odp_job],
    schedules=[insights_odp_schedule],
    resources={"snowflake": snowflake_resource},
)
