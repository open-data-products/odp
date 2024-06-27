from odp.core.detect_unused import get_table_counts
from odp.core.types import Dialect, QueryRow


def test_count_access():
    queries = [
        QueryRow(
            QUERY_TEXT="SELECT * FROM test_table",
            DATABASE_NAME="test_catalog",
            SCHEMA_NAME="test_db",
            START_TIME="2024-01-01 00:00:00",
        ),
        QueryRow(
            QUERY_TEXT="SELECT column1 FROM test_table",
            DATABASE_NAME="test_catalog",
            SCHEMA_NAME="test_db",
            START_TIME="2024-01-02 00:00:00",
        ),
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

    dialect = Dialect.snowflake

    counts = get_table_counts(queries=queries, info_schema=info_schema, dialect=dialect)

    assert counts[("TEST_CATALOG", "TEST_DB", "TEST_TABLE")] == 2
    assert counts[("TEST_CATALOG", "TEST_DB", "TEST_TABLE_UNUSED")] == 0
    assert counts[("TEST_CATALOG", "TEST_DB", "NOT_IN_SCHEMA")] == 0
