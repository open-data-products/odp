import click
from dotenv import load_dotenv

from odp.core.snowflake import load_snowflake_credentials, get_snowflake_schema, get_snowflake_queries
from odp.core.detect_unused import detect_unused_columns as detect_unused_columns_core, \
    detect_unused_columns_generic, build_info_schema

load_dotenv()

@click.group(name="odp")
def cli():
    pass


@cli.command('detect-unused', help="""
Detect unused columns in SQL queries. 
Requires two files: a query file and an information schema file. 
Run `show-queries` to see the SQL queries to run against your 
Snowflake instance to generate the two files.""")
@click.option('--queries_file', help='The SQL query file to analyze.')
@click.option('--info_schema_file',
              help='The file containing the information schema for the database.')
@click.option('--dialect',
              default='snowflake',
              help='The type of warehouse to connect to. Currently only snowflake is supported.')
def detect_unused_columns(queries_file: str | None, info_schema_file: str | None, dialect: str):
    if queries_file and info_schema_file:
        detect_unused_columns_core(queries_file, info_schema_file, dialect)
        return

    if dialect == 'snowflake':
        try:
            credentials = load_snowflake_credentials()
        except KeyError as e:
            raise ValueError(f"""
Missing or invalid parameters: {e}. Please provide either 
            
   1. both of --info_schema_file and --queries_file (use "odp show-queries" to generate these
   2. valid crendentials via env, e.g. ODP_SNOWFLAKE_ACCOUNT, ODP_SNOWFLAKE_USER, etc
            """)

        schema = get_snowflake_schema(credentials)
        queries = get_snowflake_queries(credentials)

        info_schema, info_schema_flat = build_info_schema(schema)
        detect_unused_columns_generic(queries, info_schema, info_schema_flat, dialect)


@cli.command("show-queries")
def show_snowflake_queries():
    print("Run the below against your snowflake instance to generate a dataset you can"
          "export to CSV for analysis")
    print("""
-- query_file

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE 'select%'
ORDER BY START_TIME DESC
LIMIT 10000; -- or start_time > $SOME_DATE to get columns unused in the last N days


-- info_schema_file

use database MY_DATABASE; -- change me

SELECT
TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME
FROM information_schema.columns
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA';
        
        """
    )

if __name__ == "__main__":
    cli()