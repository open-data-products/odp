from odp.core.detect_unused import detect_unused_tables
from odp.core.types import Dialect, QueryRow


def test_unused_tables_detection_with_valid_input():
    queries = [
        QueryRow(
            QUERY_TEXT="SELECT * FROM test_db.test_table",
            DATABASE_NAME="test_db",
            SCHEMA_NAME="test_catalog",
            START_TIME="2024-01-01 00:00:00",
        )
    ]
    info_schema = {
        "test_catalog": {
            "test_db": {
                "test_table": {"column1": "type1"},
                "test_table_unused": {"column1": "type1"},
            },
            "test_db_2": {
                "test_table_unused_2": {"column1": "type1"},
            },
        },
        "test_catalog_2": {
            "test_db": {"test_table_unused_3": {"column1": "type1"}},
        },
    }

    info_schema_flat = [
        ("test_catalog", "test_db", "test_table", "column1"),
        ("test_catalog", "test_db", "test_table_unused", "column1"),
        ("test_catalog", "test_db_2", "test_table_unused_2", "column1"),
        ("test_catalog_2", "test_db", "test_table_unused_3", "column1"),
    ]
    dialect = Dialect.snowflake

    unused_tables = detect_unused_tables(
        queries=queries, info_schema=info_schema, info_schema_flat=info_schema_flat, dialect=dialect
    )
    assert unused_tables == [
        ("TEST_CATALOG", "TEST_DB", "TEST_TABLE_UNUSED"),
        ("TEST_CATALOG", "TEST_DB_2", "TEST_TABLE_UNUSED_2"),
        ("TEST_CATALOG_2", "TEST_DB", "TEST_TABLE_UNUSED_3"),
    ]


def test_unused_tables_detection_with_multiple_tables_in_query():
    queries = [
        QueryRow(
            QUERY_TEXT="SELECT * FROM test_db.test_table1 JOIN test_db.test_table2 ON test_table1.id = test_table2.id",
            DATABASE_NAME="test_db",
            SCHEMA_NAME="test_catalog",
            START_TIME="2024-01-01 00:00:00",
        )
    ]
    info_schema = {
        "test_catalog": {
            "test_db": {
                "test_table1": {"column1": "type1"},
                "test_table2": {"column1": "type1"},
                "test_table_unused": {"column1": "type1"},
            },
        }
    }
    info_schema_flat = [
        ("test_catalog", "test_db", "test_table1", "column1"),
        ("test_catalog", "test_db", "test_table2", "column1"),
        ("test_catalog", "test_db", "test_table_unused", "column1"),
    ]
    dialect = Dialect.snowflake

    unused_tables = detect_unused_tables(
        queries=queries, info_schema=info_schema, info_schema_flat=info_schema_flat, dialect=dialect
    )
    assert unused_tables == [("TEST_CATALOG", "TEST_DB", "TEST_TABLE_UNUSED")]
