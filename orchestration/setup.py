from setuptools import find_packages, setup

setup(
    name="wikipedia_dagster",
    version="0.1.0",
    description="Dagster orchestration for Wikipedia data analysis pipeline",
    author="Mary Welsh",
    author_email="maryrosewelsh42@gmail.com",
    packages=find_packages(exclude=["wikipedia_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dbt-core",
        "dbt-snowflake",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
    package_data={
        "dagster": [
            "dagster_dbt_translator.py",
        ]
    },
) 