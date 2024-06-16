from datetime import datetime
from enum import Enum
from typing import Any

import click
from pydantic import BaseModel


class QueryRow(BaseModel):
    QUERY_TEXT: str
    START_TIME: datetime
    DATABASE_NAME: str | None
    SCHEMA_NAME: str | None


class EnrichedQueryRow(QueryRow):
    USED_TABLES: list[str]


class SchemaRow(BaseModel):
    TABLE_CATALOG: str
    TABLE_SCHEMA: str
    TABLE_NAME: str
    COLUMN_NAME: str


class Dialect(Enum):
    snowflake = "snowflake"
    bigquery = "bigquery"
    redshift = "redshift"


def validate_dialect(ctx: Any, param: Any, value: str) -> Dialect:
    try:
        return Dialect(value)
    except ValueError:
        raise click.BadParameter(
            f'Invalid dialect value: {value}. Valid values are: {", ".join(d.value for d in Dialect)}'
        ) from None


class Grain(Enum):
    schema = "schema"
    table = "table"
    column = "column"

    def plural(self) -> str:
        return f"{self.value}s"


def validate_grain(ctx: Any, param: Any, value: str) -> Grain:
    try:
        return Grain(value)
    except ValueError:
        raise click.BadParameter(
            f'Invalid grain value: {value}. Valid values are: {", ".join(g.value for g in Grain)}'
        ) from None
