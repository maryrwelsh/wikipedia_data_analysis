from datetime import datetime, timedelta
from dagster import AssetExecutionContext, asset, define_asset_job, ScheduleDefinition, RunRequest, SkipReason
from dagster_dbt import DbtCliResource, dbt_assets
from .project import wikipedia_dbt_project
from data_ingest.wikipedia_data_ingest import run_ingestion_workflow

@asset(compute_kind="python")
def raw_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """Ingest raw Wikipedia pageview data from Wikimedia dumps."""
    # Get date range from run config, or default to last hour
    run_config = context.run.run_config or {}
    ops_config = run_config.get("ops", {}).get("raw_wikipedia_pageviews", {}).get("config", {})
    
    if "start_date" in ops_config and "end_date" in ops_config:
        start_date = ops_config["start_date"]
        end_date = ops_config["end_date"]
        context.log.info(f"Using provided date range: {start_date} to {end_date}")
    else:
        # Default: get the last complete hour of data
        now = datetime.utcnow()
        # Round down to the previous hour
        end_hour = now.replace(minute=0, second=0, microsecond=0)
        start_hour = end_hour - timedelta(hours=1)
        
        start_date = start_hour.strftime("%Y-%m-%d %H:%M:%S")
        end_date = end_hour.strftime("%Y-%m-%d %H:%M:%S")
        context.log.info(f"Using last hour: {start_date} to {end_date}")
    
    run_ingestion_workflow(start_date=start_date, end_date=end_date)

@dbt_assets(manifest=wikipedia_dbt_project.manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for transforming Wikipedia pageview data."""
    yield from dbt.cli(["build"], context=context).stream()

hourly_refresh_job = define_asset_job(
    "hourly_refresh_job", selection=[raw_wikipedia_pageviews, dbt_assets]
)

def hourly_wikipedia_schedule_function(context):
    """Schedule function that runs every hour for the previous hour's data."""
    scheduled_time = context.scheduled_execution_time
    
    # Calculate the hour we want to process (the hour before the scheduled time)
    target_end_hour = scheduled_time.replace(minute=0, second=0, microsecond=0)
    target_start_hour = target_end_hour - timedelta(hours=1)
    
    start_date = target_start_hour.strftime("%Y-%m-%d %H:%M:%S")
    end_date = target_end_hour.strftime("%Y-%m-%d %H:%M:%S")
    
    # Add some delay to ensure Wikipedia data is available
    # Wikipedia data is typically available ~10 minutes after the hour
    current_time = datetime.utcnow()
    data_availability_time = target_end_hour + timedelta(minutes=10)
    
    if current_time < data_availability_time:
        return SkipReason(
            f"Skipping run - Wikipedia data for {start_date} to {end_date} "
            f"likely not available yet. Will retry at next schedule."
        )
    
    return RunRequest(
        run_key=f"wikipedia_hourly_{target_start_hour.strftime('%Y%m%d_%H%M%S')}",
        run_config={
            "ops": {
                "raw_wikipedia_pageviews": {
                    "config": {
                        "start_date": start_date,
                        "end_date": end_date
                    }
                }
            }
        },
        tags={
            "data_hour": target_start_hour.strftime("%Y-%m-%d_%H"),
            "schedule_type": "hourly_wikipedia"
        }
    )

hourly_schedule = ScheduleDefinition(
    name="hourly_wikipedia_ingestion",
    cron_schedule="15 * * * *",  # Runs 15 minutes past every hour
    job=hourly_refresh_job,
    execution_fn=hourly_wikipedia_schedule_function,
) 