from dagster import AssetSelection, Definitions, ScheduleDefinition

from snowflake_dagster_metadata_job.assets import all_assets
from snowflake_dagster_metadata_job.jobs import build_odp_snowflake_metadata_job
from snowflake_dagster_metadata_job.resources import snowflake_resource

odp_snowflake_job = build_odp_snowflake_metadata_job(
    name="odp_snowflake_job", selection=AssetSelection.assets(*all_assets)
)

odp_snowflake_schedule = ScheduleDefinition(
    job=odp_snowflake_job,
    cron_schedule="0 0 * * *",
)

definitions = Definitions(
    assets=all_assets,
    jobs=[odp_snowflake_job],
    schedules=[odp_snowflake_schedule],
    resources={"snowflake": snowflake_resource},
)
