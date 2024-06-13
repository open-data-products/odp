from dagster import Definitions, load_assets_from_modules

from .assets import unused_tables
from .resources import snowflake

unused_tables_assets = load_assets_from_modules([unused_tables])

defs = Definitions(
    assets=[*unused_tables_assets],
    resources={
        "snowflake": snowflake,
    },
)
