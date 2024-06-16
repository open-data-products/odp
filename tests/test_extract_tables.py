from odp.core.detect_unused import extract_tables
from odp.core.types import Dialect


def test_extract_tables_with_valid_query():
    query_text = "SELECT * FROM test_db.test_table"
    database_name = "test_catalog"
    schema_name = "test_db"
    schema = {"test_catalog": {"test_db": {"test_table": {"column1": "type1"}}}}
    dialect = Dialect.snowflake

    result = extract_tables(
        query_text=query_text,
        catalog_name=database_name,
        database_name=schema_name,
        schema=schema,
        dialect=dialect,
    )

    assert result == [("test_catalog", "TEST_DB", "TEST_TABLE")]


def test_extract_tables_with_no_tables_in_query():
    query_text = "SELECT 1"
    database_name = "test_catalog"
    catalog_name = "test_db"
    schema = {"test_catalog": {"test_db": {"test_table": {"column1": "type1"}}}}
    dialect = Dialect.snowflake

    result = extract_tables(
        query_text=query_text,
        database_name=database_name,
        catalog_name=catalog_name,
        schema=schema,
        dialect=dialect,
    )

    assert result == []


def test_extract_tables_with_multiple_tables_in_query():
    query_text = "SELECT * FROM test_schema.test_table1 JOIN test_schema.test_table2 ON test_table1.id = test_table2.id"
    catalog_name = "test_db"
    database_name = "test_schema"
    schema = {
        "test_db": {
            "test_schema": {
                "test_table1": {"id": "type1"},
                "test_table2": {"id": "type1"},
            }
        }
    }
    dialect = Dialect.snowflake

    result = extract_tables(
        query_text=query_text,
        database_name=database_name,
        catalog_name=catalog_name,
        schema=schema,
        dialect=dialect,
    )

    assert set(result) == {("test_db", "TEST_SCHEMA", "TEST_TABLE1"), ("test_db", "TEST_SCHEMA", "TEST_TABLE2")}


def test_extract_tables_with_invalid_query():
    query_text = "SELECT * FROM"
    database_name = "test_db"
    catalog_name = "test_catalog"
    schema = {
        "test_catalog": {
            "test_db": {
                "test_table1": {"id": "type1"},
                "test_table2": {"id": "type1"},
            }
        }
    }
    dialect = Dialect.snowflake

    result = extract_tables(
        query_text=query_text,
        database_name=database_name,
        catalog_name=catalog_name,
        schema=schema,
        dialect=dialect,
    )

    assert result == []

def test_extract_tables_with_create_table_as():
    query_text = """
      CREATE OR REPLACE TABLE issue_state_history AS
      SELECT
          updated_at AS date,
          SUM(CASE WHEN new_state = 'open' THEN 1 ELSE 0 END) AS open,
          SUM(CASE WHEN new_state = 'in progress' THEN 1 ELSE 0 END) AS in_progress,
          SUM(CASE WHEN new_state = 'closed' THEN 1 ELSE 0 END) AS closed
      FROM issues_by_day
      GROUP BY updated_at
      ORDER BY updated_at;
    """
    catalog_name = "test_catalog"
    database_name = "test_db"
    schema = {
        "test_catalog": {
            "test_db": {
                "issues_by_day": {
                    "new_state": "string",
                    "updated_at": "datettime",
                },
            }
        }
    }
    dialect = Dialect.snowflake

    result = extract_tables(
        query_text=query_text,
        catalog_name=catalog_name,
        database_name=database_name,
        schema=schema,
        dialect=dialect,
    )

    assert result == [
        ("test_catalog", "test_db", "ISSUES_BY_DAY"),
    ]