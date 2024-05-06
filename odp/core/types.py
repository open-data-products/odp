from pydantic import BaseModel


class QueryRow(BaseModel):
    QUERY_TEXT: str
    DATABASE_NAME: str
    SCHEMA_NAME: str


class SchemaRow(BaseModel):
    TABLE_CATALOG: str
    TABLE_SCHEMA: str
    TABLE_NAME: str
    COLUMN_NAME: str
