from dagster import ScheduleDefinition

from ..jobs import issues_job

issues_schedule = ScheduleDefinition(
    job=issues_job,
    cron_schedule="0 16 * * *",  # daily at 9am PT
)
