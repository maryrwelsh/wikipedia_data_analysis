import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import data_ingest
sys.path.append(str(Path(__file__).parent.parent.parent))

from dagster import AssetExecutionContext, asset, define_asset_job, ScheduleDefinition, RunRequest
from dagster_dbt import DbtCliResource, dbt_assets
from .project import wikipedia_dbt_project
from data_ingest.wikipedia_data_ingest import run_ingestion_workflow



@asset(compute_kind="python")
def raw_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """Ingest raw Wikipedia pageview data from Wikimedia dumps for the current hour."""
    context.log.info("Starting Wikipedia pageview ingestion for current hour")
    
    # The ingestion workflow now automatically handles current hour logic
    run_ingestion_workflow()

@dbt_assets(manifest=wikipedia_dbt_project.manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for transforming Wikipedia pageview data."""
    # This runs after raw_wikipedia_pageviews completes (dependency inferred from dbt sources)
    yield from dbt.cli(["build"], context=context).stream()

hourly_refresh_job = define_asset_job(
    "hourly_refresh_job", selection=[raw_wikipedia_pageviews, dbt_assets]
)

def hourly_wikipedia_schedule_function(context):
    """Schedule function that runs every hour for current hour data."""
    scheduled_time = context.scheduled_execution_time
    
    # The ingestion workflow now automatically handles current hour logic,
    # so we just need to trigger the job
    return RunRequest(
        run_key=f"wikipedia_hourly_{scheduled_time.strftime('%Y%m%d_%H%M%S')}",
        tags={
            "schedule_type": "hourly_wikipedia"
        }
    )

hourly_schedule = ScheduleDefinition(
    name="hourly_wikipedia_ingestion",
    cron_schedule="15 * * * *",  # Runs 15 minutes past every hour
    job=hourly_refresh_job,
    execution_fn=hourly_wikipedia_schedule_function,
) 