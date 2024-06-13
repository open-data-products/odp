from setuptools import find_packages, setup

setup(
    name="snowflake_dagster_asset",
    packages=find_packages(exclude=["snowflake_dagster_asset_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-snowflake",
        "odp",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
