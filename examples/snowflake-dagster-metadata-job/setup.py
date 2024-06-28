from setuptools import find_packages, setup

setup(
    name="snowflake_dagster_metadata_job",
    packages=find_packages(exclude=["snowflake_dagster_metadata_job_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-snowflake",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
