# Wikipedia Data Pipeline Orchestration (Dagster)

This directory contains the Dagster orchestration layer for the Wikipedia data analysis pipeline. Dagster coordinates the entire data workflow from ingestion through transformation, providing monitoring, scheduling, and asset management capabilities. This orchestration layer provides a robust, scalable foundation for the Wikipedia data analysis pipeline with comprehensive monitoring and automated execution capabilities.

## Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Assets](#assets)
* [Schedules](#schedules)
* [Setup](#setup)
* [Usage](#usage)
* [Configuration](#configuration)
* [Monitoring](#monitoring)
* [Development](#development)

## Overview

The Dagster orchestration layer provides:
- **Asset-based pipeline management** with clear data lineage
- **Automated hourly scheduling** for real-time data processing
- **Web-based monitoring** and control interface
- **Integration between data ingestion and dbt transformations**
- **Comprehensive logging** and error tracking

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ raw_wikipedia_  │───▶│ dbt_assets      │───▶│ Analytics Ready│
│ pageviews       │    │ (All Models)    │    │ Data Models     │
│ (Ingestion)     │    │ - Staging       │    │ - Fact Tables   │
│                 │    │ - Marts         │    │ - Dimensions    │
│                 │    │ - Analytics     │    │ - Views         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Assets

### Data Ingestion Asset

**`raw_wikipedia_pageviews`**
- **Purpose**: Downloads and ingests current hour Wikipedia pageview data
- **Compute Kind**: Python
- **Output**: Raw pageview data in Snowflake (`WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`)
- **Behavior**: 
  - Automatically determines current hour to process
  - Downloads exactly one file per execution
  - Uploads only the latest processed file to Snowflake
  - Includes comprehensive error handling and logging

### dbt Transformation Assets

**`dbt_assets`**
- **Purpose**: Transforms raw data into analytics-ready models using dbt
- **Compute Kind**: dbt
- **Dependencies**: Requires `raw_wikipedia_pageviews` to complete first
- **Models Included**:
  - **Staging**: `stg_wikipedia_pageviews` (Incremental)
  - **Dimensions**: `dim_wikipedia_page` (Incremental), `dim_date`, `dim_hour`
  - **Facts**: `fct_wikipedia_pageviews` (Incremental)
  - **Analytics**: `wikipedia_pageviews` (View)

## Schedules

### Hourly Wikipedia Ingestion

**Schedule Name**: `hourly_wikipedia_ingestion`
- **Cron Expression**: `15 * * * *` (15 minutes past every hour)
- **Target**: Full pipeline (`raw_wikipedia_pageviews` + `dbt_assets`)
- **Logic**: 
  - Automatically processes current hour data
  - No manual date configuration required
  - Incremental processing for optimal performance

## Setup

### Prerequisites

- **Python 3.11+** with virtual environment
- **Dagster CLI** installed (`pip install dagster`)
- **Environment Variables** configured (see `.env` file)
- **Snowflake Access** with appropriate permissions
- **dbt Project** configured and tested

### Installation

```bash
# Navigate to orchestration directory
cd orchestration/

# Install the orchestration package
pip install -e .

# Verify installation
dagster --version
```

### Environment Configuration

Ensure your `.env` file in the project root contains:

```ini
# Snowflake Connection
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_USER="your_username"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE="your_warehouse"
SNOWFLAKE_DATABASE="your_database"
SNOWFLAKE_SCHEMA="WIKIPEDIA"
SNOWFLAKE_ROLE="your_role"

# Optional: Custom object names
SNOWFLAKE_TABLE_NAME="RAW_WIKIPEDIA_PAGEVIEWS"
SNOWFLAKE_STAGE_NAME="WIKIPEDIA_PAGEVIEWS_STAGE"

# Data Ingestion Settings
LOCAL_DATA_DIR="wikipedia_pageviews_files"
MAX_DOWNLOAD_WORKERS=5
```

## Usage

### Development Mode

```bash
# Start Dagster development server (IMPORTANT: Run from orchestration directory)
cd orchestration/
dagster dev

# Access the UI
open http://localhost:3000
```

### Manual Pipeline Execution

1. **Navigate to Assets Tab** in the Dagster UI
2. **Select All Assets** or specific asset groups
3. **Click "Materialize"** to run the pipeline
4. **Monitor Progress** in real-time through the UI

### Schedule Management

1. **Navigate to Schedules Tab** in the Dagster UI
2. **Enable/Disable** the `hourly_wikipedia_ingestion` schedule
3. **View Run History** and schedule status
4. **Monitor Automatic Executions** every hour

## Configuration

### Asset Configuration

The orchestration is configured in `wikipedia_dagster/definitions.py`:

```python
defs = Definitions(
    assets=[raw_wikipedia_pageviews, dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=wikipedia_dbt_project),
    },
)
```

### dbt Integration

- **dbt Project Path**: Automatically detected from project structure
- **dbt Profiles**: Uses environment variables for Snowflake connection
- **Asset Dependencies**: Inferred from dbt source/ref relationships
- **Model Selection**: Processes all models with `--select fqn:*`

### Resource Management

- **DbtCliResource**: Manages dbt command execution
- **Environment Variables**: Loaded from project root `.env` file
- **Connection Pooling**: Optimized for concurrent operations

## Monitoring

### Dagster UI Features

- **Asset Lineage**: Visual representation of data dependencies
- **Run History**: Complete execution logs and timing
- **Asset Status**: Real-time status of all data assets
- **Error Tracking**: Detailed error messages and stack traces
- **Performance Metrics**: Execution times and resource usage

### Logging

- **Structured Logging**: JSON-formatted logs for easy parsing
- **Multiple Log Levels**: DEBUG, INFO, WARN, ERROR
- **Asset-Specific Logs**: Separate log streams for each asset
- **Integration Logs**: Combined logs from Python and dbt executions

### Alerting

- **Failed Run Detection**: Automatic failure detection
- **Schedule Monitoring**: Tracks schedule execution success
- **Asset Freshness**: Monitors data freshness and staleness
- **Custom Alerts**: Configurable alert conditions

## Development

### Project Structure

```
orchestration/
├── wikipedia_dagster/           # Main Dagster package
│   ├── __init__.py             # Package initialization
│   ├── assets.py               # Asset definitions
│   ├── definitions.py          # Main Dagster definitions
│   ├── project.py              # dbt project configuration
│   └── schedules.py            # Schedule definitions
├── pyproject.toml              # Package configuration
├── setup.py                    # Package setup
└── README.md                   # This file
```

### Adding New Assets

1. **Define Asset Function** in `assets.py`:
```python
@asset(compute_kind="python")
def new_asset(context: AssetExecutionContext) -> None:
    # Asset logic here
    pass
```

2. **Add to Definitions** in `definitions.py`:
```python
defs = Definitions(
    assets=[raw_wikipedia_pageviews, dbt_assets, new_asset],
    # ... other configuration
)
```

### Extending Schedules

1. **Create Schedule Function** in `schedules.py`
2. **Define Schedule** with cron expression
3. **Add to Definitions** in `definitions.py`

### Testing

```bash
# Validate Dagster definitions
dagster definitions validate

# Test specific assets
dagster asset materialize --select raw_wikipedia_pageviews

# Dry run schedules
dagster schedule list
```

## Best Practices

### Asset Development

- **Clear Descriptions**: Document asset purposes and outputs
- **Proper Dependencies**: Define explicit asset dependencies
- **Error Handling**: Implement comprehensive error handling
- **Logging**: Add meaningful log messages for debugging

### Schedule Management

- **Reasonable Frequency**: Balance freshness with resource usage
- **Failure Handling**: Implement retry logic for transient failures
- **Monitoring**: Regularly check schedule execution status

### Resource Optimization

- **Efficient Queries**: Optimize SQL and data processing logic
- **Incremental Processing**: Use incremental models where appropriate
- **Connection Management**: Properly manage database connections
- **Memory Usage**: Monitor and optimize memory consumption

## Troubleshooting

### Common Issues

1. **Module Import Errors**
   ```bash
   # Ensure you're in the orchestration directory
   cd orchestration/
   dagster dev
   ```

2. **Environment Variable Issues**
   ```bash
   # Verify .env file is in project root
   cat ../.env
   # Restart Dagster after env changes
   ```

3. **dbt Connection Errors**
   ```bash
   # Test dbt connection separately
   cd ../dbt/
   dbt debug
   ```

4. **Asset Dependency Errors**
   - Check asset dependency chain in UI
   - Verify source data availability
   - Review error logs for specific issues

### Performance Issues

- **Long-running Assets**: Check data volume and query efficiency
- **Memory Usage**: Monitor system resources during execution
- **Database Connections**: Verify connection pool settings
- **Incremental Logic**: Ensure incremental models are working correctly

## Support

For issues and questions:
1. **Check Dagster UI**: Review run logs and error messages
2. **Review Documentation**: Check dbt and ingestion README files
3. **Validate Configuration**: Ensure all environment variables are set
4. **Test Components**: Test ingestion and dbt separately
