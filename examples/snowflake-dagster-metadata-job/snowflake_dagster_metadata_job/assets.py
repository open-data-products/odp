from dagster import asset


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "sandbox.toys.orders",
    }
)
def example_asset_1(): ...


@asset(
    metadata={
        "dagster/storage_kind": "snowflake",
        "dagster/relation_identifier": "snowflake.account_usage.query_history",
    }
)
def example_asset_2(): ...


@asset(
    metadata={
        "dagster/storage_kind": "bigquery",  # should be ignored as we only support Snowflake for now
        "dagster/relation_identifier": "my_database.my_schema.my_table3",
    }
)
def example_asset_3_bq(): ...


all_assets = [example_asset_1, example_asset_2, example_asset_3_bq]
