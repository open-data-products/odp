from dagster import ScheduleDefinition

from ..jobs import unused_tables_job

unused_tables_schedule = ScheduleDefinition(
    job=unused_tables_job,
    cron_schedule="0 15 * * *",  # 8am PT
)
