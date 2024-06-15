from dagster import Definitions, load_assets_from_modules

from .assets import issues
from .jobs import issues_job
from .resources import snowflake
from .schedules import issues_schedule

issues_assets = load_assets_from_modules([issues])

defs = Definitions(
    assets=[*issues_assets],
    resources={
        "snowflake": snowflake,
    },
    jobs=[issues_job],
    schedules=[issues_schedule],
)
