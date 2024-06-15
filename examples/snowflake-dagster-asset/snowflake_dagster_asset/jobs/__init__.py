from dagster import AssetSelection, define_asset_job

unused_tables_asset = AssetSelection.assets(["unused_tables"])

unused_tables_job = define_asset_job(
    name="unused_tables_job",
    selection=unused_tables_asset,
)
