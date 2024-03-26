import csv
from collections import Counter

from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import find_all_in_scope, build_scope


def read_queries(query_file):
    # Read queries from a CSV file and return a list of dictionaries where each key is a column in the CSV
    with open(query_file) as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        return [dict(zip(header, row)) for row in csv_reader]


def read_info_schema(info_schema_file):
    # Read the info schema from a CSV file and return it as both a nested dictionary and a flat list
    # Format is: catalog -> schema -> table name -> column name
    schema = {}
    flat_schema = []
    with open(info_schema_file) as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            catalog, schema_name, table_name, column_name = map(str.upper, row)
            if catalog not in schema:
                schema[catalog] = {}
            if schema_name not in schema[catalog]:
                schema[catalog][schema_name] = {}
            if table_name not in schema[catalog][schema_name]:
                schema[catalog][schema_name][table_name] = {}
            schema[catalog][schema_name][table_name][column_name] = "DUMMY"
            flat_schema.append((catalog, schema_name, table_name, column_name))
    return schema, flat_schema


def extract_columns(query_text, database_name, catalog_name, schema):
    # Extract the columns from a query that map to actual columns in a table
    # Based on https://github.com/tobymao/sqlglot/blob/main/posts/ast_primer.md
    try:
        parsed = parse_one(query_text, dialect="snowflake")
        qualified = qualify(
            parsed, schema=schema, dialect="snowflake"
        )  # Qualify (add schema) and expand * to explicit columns
        root = build_scope(qualified)
    except Exception as e:
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


def summarize_columns(columns):
    # Return a dictionary of column to counts

    # Flatten the col vals
    cols = [item for sublist in columns for item in sublist]
    return Counter(cols)

def detect_unused_columns(query_file: str, info_schema_file: str):

    queries = read_queries(query_file)
    print(f"Read {len(queries)} queries from {query_file}")

    info_schema, info_schema_flat = read_info_schema(info_schema_file)
    print(
        f"Read {len(info_schema_flat)} information schema rows from {info_schema_file}"
    )

    cols = [
        extract_columns(
            query["QUERY_TEXT"],
            database_name=query["DATABASE_NAME"].upper(),
            catalog_name=query["SCHEMA_NAME"].upper(),
            schema=info_schema,
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
