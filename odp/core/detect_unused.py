import csv
from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope, find_all_in_scope

from odp.core.types import Dialect, QueryRow, SchemaRow


def read_queries(
    query_file: str,
    since: int,
) -> list[QueryRow]:
    since_datetime = datetime.now(timezone.utc) - timedelta(days=since)
    # Read queries from a CSV file and return a list of dictionaries where each key is a column in the CSV
    with open(query_file) as f:
        csv_reader = csv.reader(f)
        header = list(map(str.upper, next(csv_reader)))

        rows = []
        for _row in csv_reader:
            row = dict(zip(header,_row))
            row["START_TIME"] = datetime.fromisoformat(row["START_TIME"])
            query_row = QueryRow(**row)
            if query_row.START_TIME > since_datetime:
                rows.append(query_row)

        return rows


def read_info_schema_from_file(info_schema_file) -> tuple[dict, list[tuple]]:
    # Read the info schema from a CSV file and return it as both a nested dictionary and a flat list
    # Format is: catalog -> schema -> table name -> column name
    schema_rows: list[SchemaRow] = []
    with open(info_schema_file) as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            catalog, schema_name, table_name, column_name = map(str.upper, row)
            schema_rows.append(
                SchemaRow(
                    TABLE_CATALOG=catalog,
                    TABLE_SCHEMA=schema_name,
                    TABLE_NAME=table_name,
                    COLUMN_NAME=column_name,
                )
            )

    return build_info_schema(schema_rows)


def build_info_schema(schema_rows: list[SchemaRow]) -> tuple[dict, list[tuple]]:
    sqlglot_mapping_schema = {}
    flat_schema: list[tuple] = []
    for row in schema_rows:
        catalog, schema_name, table_name, column_name = (
            row.TABLE_CATALOG,
            row.TABLE_SCHEMA,
            row.TABLE_NAME,
            row.COLUMN_NAME,
        )

        if catalog not in sqlglot_mapping_schema:
            sqlglot_mapping_schema[catalog] = {}
        if schema_name not in sqlglot_mapping_schema[catalog]:
            sqlglot_mapping_schema[catalog][schema_name] = {}
        if table_name not in sqlglot_mapping_schema[catalog][schema_name]:
            sqlglot_mapping_schema[catalog][schema_name][table_name] = {}
        sqlglot_mapping_schema[catalog][schema_name][table_name][column_name] = "DUMMY"
        flat_schema.append((catalog, schema_name, table_name, column_name))

    return sqlglot_mapping_schema, flat_schema


def extract_columns(
    query_text: str,
    database_name: str,
    catalog_name: str,
    schema: dict,
    dialect: Dialect,
):
    # Extract the columns from a query that map to actual columns in a table
    # Based on https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md
    try:
        parsed = parse_one(query_text, dialect=dialect.value)
        qualified = qualify(
            parsed, schema=schema, dialect=dialect.value
        )  # Qualify (add schema) and expand * to explicit columns
        root = build_scope(qualified)
    except Exception:
        # todo - debug log these / write to file
        # print("Error parsing query", e, query_text)
        return []

    # This is confusing due to naming conventions. We basically want to make sure every table is fully qualified
    # sqlglot has {catalog: {db: {table: {col: type}}}} convention
    # Snowflake has {database_name: {schema_name: {table: {col: type}}}}
    # So we do database_name (SF) -> catalog (sqlglot), schema_name (SF) -> db (sqlglot)
    for source in root.sources:
        s = root.sources[source]
        if type(s) == exp.Table:
            if "db" not in s.args or not s.args["db"]:
                s.set("db", exp.Identifier(this=catalog_name, quoted=True))
            if "catalog" not in s.args or not s.args["catalog"]:
                s.set("catalog", exp.Identifier(this=database_name, quoted=True))

    columns = []
    for column in find_all_in_scope(root.expression, exp.Column):
        if column.table not in root.sources:
            continue

        table = root.sources[column.table]
        if type(table) != exp.Table:
            continue

        columns.append(
            (
                table.catalog,
                table.db,
                table.name,
                column.this.this,
            )
        )
    return columns


def extract_tables(
    query_text: str,
    database_name: str,
    catalog_name: str,
    schema: dict,
    dialect: Dialect,
):
    # Extract the tables from a query that map to actual columns in a table
    # Based on https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md
    try:
        parsed = parse_one(query_text, dialect=dialect.value)
        qualified = qualify(
            parsed,
            db=database_name,
            catalog=catalog_name,
            schema=schema,
            dialect=dialect.value,
            infer_schema=True,
            expand_stars=False,  # we don't care about columns here
            validate_qualify_columns=False, # we don't care about columns here
            qualify_columns=False, # we don't care about columns here
        )
        root = build_scope(qualified)
    except Exception as e:
        # todo - debug log these / write to file
        # print("Error parsing query", e, query_text)
        return []

    table_exps = set()
    for scope in root.traverse():
        for _, (_, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                table_exps.add(source)

    # converting table expressions to a list of tuples
    tables = []
    for table_exp in table_exps:
        tables.append(
            (
                table_exp.catalog,
                table_exp.db,
                str(table_exp.this.name),
            )
        )
    return tables


def summarize_columns(columns):
    # Return a dictionary of column to counts

    # Flatten the col vals
    cols = [item for sublist in columns for item in sublist]
    return Counter(cols)


def detect_unused_columns(
    queries: list[QueryRow],
    info_schema: dict,
    info_schema_flat: list[tuple],
    dialect: Dialect,
):
    cols = [
        extract_columns(
            query.QUERY_TEXT,
            database_name=query.DATABASE_NAME.upper() if query.DATABASE_NAME else None,
            catalog_name=query.SCHEMA_NAME.upper() if query.SCHEMA_NAME else None,
            schema=info_schema,
            dialect=dialect,
        )
        for query in queries
    ]
    col_counts = summarize_columns(cols)

    # Print the most common columns in a human readable format with one column per line
    print("Most common columns (20):")
    for col, count in col_counts.most_common(20):
        print(f"{col}: {count}")

    # Identify columns that are never used by comparing the columns in the info schema to the columns in the queries
    info_schema_cols = set(info_schema_flat)
    used_cols = set(col_counts.keys())
    unused_cols = sorted(info_schema_cols - used_cols)
    print(f"Unused columns ({len(unused_cols)}):")
    for col in unused_cols:
        print(col)


def detect_unused_tables(
    queries: list[QueryRow],
    info_schema: dict,
    info_schema_flat: list[tuple],
    dialect: Dialect,
):
    tables = [
        extract_tables(
            query.QUERY_TEXT,
            database_name=query.DATABASE_NAME.upper() if query.DATABASE_NAME else None,
            catalog_name=query.SCHEMA_NAME.upper() if query.SCHEMA_NAME else None,
            schema=info_schema,
            dialect=dialect,
        )
        for query in queries
    ]

    tbls = [item for sublist in tables for item in sublist]
    table_counts = Counter(tbls)

    # Print the most common tables in a human-readable format with one table per line
    print("Most common tables (20):")
    for tbl, count in table_counts.most_common(20):
        catalog, db, table = (obj.upper() for obj in tbl)
        print(f"{catalog}.{db}.{table}: {count}")

    # Identify tables that are never used by comparing the tables in the info schema to the tables in the queries
    info_schema_tables = set()
    for table in info_schema_flat:
        info_schema_tables.add((table[0].upper(), table[1].upper(), table[2].upper()))

    used_tables = set(table_counts.keys())
    unused_tables = sorted(info_schema_tables - used_tables)
    print(f"Unused tables ({len(unused_tables)}):")
    for table in sorted(unused_tables):
        print(table)
    return unused_tables
