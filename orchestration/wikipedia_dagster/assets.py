from datetime import datetime, timedelta
import requests
import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import data_ingest
sys.path.append(str(Path(__file__).parent.parent.parent))

from dagster import AssetExecutionContext, asset, define_asset_job, ScheduleDefinition, RunRequest, SkipReason
from dagster_dbt import DbtCliResource, dbt_assets
from .project import wikipedia_dbt_project
from data_ingest.wikipedia_data_ingest import run_ingestion_workflow

def check_wikipedia_data_availability(target_datetime):
    """Check if Wikipedia pageview data is available for a given hour."""
    # Format: pageviews-YYYYMMDD-HHMMSS.gz
    filename = target_datetime.strftime("pageviews-%Y%m%d-%H0000.gz")
    year = target_datetime.strftime("%Y")
    month = target_datetime.strftime("%Y-%m")
    
    url = f"https://dumps.wikimedia.org/other/pageviews/{year}/{month}/{filename}"
    
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200, url
    except requests.RequestException:
        return False, url

def find_latest_available_hour(start_time, max_lookback_hours=6):
    """Find the latest available hour of Wikipedia data, looking back from start_time."""
    current_check = start_time
    
    for i in range(max_lookback_hours):
        is_available, url = check_wikipedia_data_availability(current_check)
        if is_available:
            return current_check, url
        current_check = current_check - timedelta(hours=1)
    
    return None, None

@asset(compute_kind="python")
def raw_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """Ingest raw Wikipedia pageview data from Wikimedia dumps."""
    # Get date range from run config, or default based on execution context
    run_config = context.run.run_config or {}
    ops_config = run_config.get("ops", {}).get("raw_wikipedia_pageviews", {}).get("config", {})
    
    if "start_date" in ops_config and "end_date" in ops_config:
        start_date = ops_config["start_date"]
        end_date = ops_config["end_date"]
        context.log.info(f"Using provided date range: {start_date} to {end_date}")
    else:
        now = datetime.utcnow()
        
        # Check if this is a scheduled run (has specific tags) vs manual run
        run_tags = context.run.tags or {}
        is_scheduled_run = "schedule_type" in run_tags and run_tags["schedule_type"] == "hourly_wikipedia"
        
        if is_scheduled_run:
            # Scheduled run: process the previous complete hour
            start_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            end_hour = start_hour  # Same hour for single hour processing
            context.log.info(f"Scheduled run: processing previous hour {start_hour}")
        else:
            # Manual run: find the latest available hour with data
            current_hour_start = now.replace(minute=0, second=0, microsecond=0)
            minutes_into_current_hour = now.minute
            
            # Start checking from current hour if 15+ minutes have passed, otherwise previous hour
            if minutes_into_current_hour >= 15:
                preferred_start = current_hour_start
                context.log.info(f"Manual run: checking current hour first (15+ min elapsed)")
            else:
                preferred_start = current_hour_start - timedelta(hours=1)
                context.log.info(f"Manual run: checking previous hour first (<15 min in current)")
            
            # Find the latest available data
            available_hour, check_url = find_latest_available_hour(preferred_start, max_lookback_hours=6)
            
            if available_hour:
                start_hour = available_hour
                end_hour = available_hour  # Same hour for start and end to process only one hour
                context.log.info(f"Found available data at {start_hour} - processing single hour {start_hour}")
                context.log.info(f"Verified data availability at: {check_url}")
            else:
                # Fallback to known working demo data if no recent data is available
                start_hour = datetime(2025, 5, 1, 10, 0, 0)
                end_hour = datetime(2025, 5, 1, 10, 0, 0)  # Same hour for single hour processing
                context.log.warning(f"No recent data available - falling back to demo data: single hour {start_hour}")
            
            context.log.info(f"Data availability check: {minutes_into_current_hour} minutes into current hour")
        
        start_date = start_hour.strftime("%Y-%m-%d %H:%M:%S")
        end_date = end_hour.strftime("%Y-%m-%d %H:%M:%S")
        context.log.info(f"Using calculated date range: {start_date} to {end_date}")
    
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
    target_start_hour = scheduled_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    target_end_hour = target_start_hour  # Same hour for single hour processing
    
    start_date = target_start_hour.strftime("%Y-%m-%d %H:%M:%S")
    end_date = target_end_hour.strftime("%Y-%m-%d %H:%M:%S")
    
    # Check if the target data is actually available
    is_available, check_url = check_wikipedia_data_availability(target_start_hour)
    
    if not is_available:
        # Try to find alternative available data within the last 6 hours
        available_hour, _ = find_latest_available_hour(target_start_hour, max_lookback_hours=6)
        
        if available_hour and available_hour != target_start_hour:
            # Use the latest available data instead
            target_start_hour = available_hour
            target_end_hour = available_hour  # Same hour for single hour processing
            start_date = target_start_hour.strftime("%Y-%m-%d %H:%M:%S")
            end_date = target_end_hour.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return SkipReason(
                f"Skipping run - No Wikipedia data available for {start_date} to {end_date}. "
                f"Checked URL: {check_url}. Will retry at next schedule."
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