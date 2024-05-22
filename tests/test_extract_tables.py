from odp.core.detect_unused import extract_tables
from odp.core.types import Dialect


def test_extract_tables_with_valid_query():
    query_text = "SELECT * FROM test_db.test_table"
    database_name = "test_db"
    catalog_name = "test_catalog"
    schema = {"test_catalog": {"test_db": {"test_table": {"column1": "type1"}}}}
    dialect = Dialect.snowflake

    result = extract_tables(query_text, database_name, catalog_name, schema, dialect)

    assert result == [("test_catalog", "TEST_DB", "TEST_TABLE")]


def test_extract_tables_with_no_tables_in_query():
    query_text = "SELECT 1"
    database_name = "test_db"
    catalog_name = "test_catalog"
    schema = {"test_db": {"test_table": {"column1": "type1"}}}
    dialect = Dialect.snowflake

    result = extract_tables(query_text, database_name, catalog_name, schema, dialect)

    assert result == []


def test_extract_tables_with_multiple_tables_in_query():
    query_text = "SELECT * FROM test_db.test_table1 JOIN test_db.test_table2 ON test_table1.id = test_table2.id"
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

    result = extract_tables(query_text, database_name, catalog_name, schema, dialect)

    assert set(result) == set(("test_catalog", "TEST_DB", "TEST_TABLE1"), ("test_catalog", "TEST_DB", "TEST_TABLE2"))


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

    result = extract_tables(query_text, database_name, catalog_name, schema, dialect)

    assert result == []
