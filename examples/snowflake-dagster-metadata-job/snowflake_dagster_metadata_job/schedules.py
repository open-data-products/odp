from dagster import ScheduleDefinition

from snowflake_dagster_metadata_job.jobs import insights_odp_job


insights_odp_schedule = ScheduleDefinition(
    job=insights_odp_job,
    cron_schedule="0 0 * * *",
)
