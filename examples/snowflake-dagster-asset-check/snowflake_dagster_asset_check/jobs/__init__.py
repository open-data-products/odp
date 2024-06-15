from dagster import AssetSelection, define_asset_job

issues_assets = AssetSelection.assets(["issues_history"])

issues_job = define_asset_job(
    name="issues_job",
    selection=issues_assets,
)
