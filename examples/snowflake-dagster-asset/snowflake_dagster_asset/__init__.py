from dagster import Definitions, load_assets_from_modules

from .assets import unused_tables
from .jobs import unused_tables_job
from .resources import snowflake
from .schedules import unused_tables_schedule

unused_tables_assets = load_assets_from_modules([unused_tables])

defs = Definitions(
    assets=[*unused_tables_assets],
    resources={
        "snowflake": snowflake,
    },
    jobs=[unused_tables_job],
    schedules=[unused_tables_schedule],
)
