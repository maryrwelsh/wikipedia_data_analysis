from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import raw_wikipedia_pageviews, dbt_assets
from .project import wikipedia_dbt_project
from .schedules import schedules

defs = Definitions(
    assets=[raw_wikipedia_pageviews, dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=wikipedia_dbt_project),
    },
) 