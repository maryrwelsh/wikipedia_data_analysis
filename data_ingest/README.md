# Wikipedia Pageview Data Ingestion

This Python module facilitates the automated download of Wikipedia hourly pageview data for the current hour and ingests it into a Snowflake data warehouse. It is designed with Clean Code principles, emphasizing modularity, clear responsibilities, and robust configuration management.

## Table of Contents

* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
    * [Environment Variables](#environment-variables)
* [Usage](#usage)
    * [Manual Execution](#manual-execution)
    * [Orchestrated Execution](#orchestrated-execution)
* [Architecture](#architecture)
* [Clean Code Principles Applied](#clean-code-principles-applied)
* [Future Improvements](#future-improvements)

## Features

* **Automated Current Hour Processing**: Downloads Wikipedia pageview data for the current hour only
* **Intelligent Hour Detection**: Automatically determines the current hour and downloads exactly one file
* **Parallel Processing**: Utilizes thread pools for concurrent downloads and unzipping, significantly improving performance
* **Automatic Unzipping**: Extracts downloaded `.gz` files into `.txt` format for processing
* **Idempotent Operations**: Skips downloading/unzipping files that already exist locally
* **Snowflake Integration**:
    * Connects to Snowflake with robust error handling
    * Creates internal stage and target table in the WIKIPEDIA schema
    * Uploads local data files to Snowflake stage
    * Copies data from stage into the target table with proper formatting
* **Schema Consistency**: All objects created in dedicated WIKIPEDIA schema
* **Robust Error Handling**: Comprehensive logging and graceful error recovery
* **Environment-Driven Configuration**: All parameters managed via environment variables
* **Data Validation**: Built-in checks for data availability and file integrity

## Prerequisites

Before running the ingestion script, ensure you have:

* **Python 3.11+**
* **Required Python packages**:
  ```bash
  pip install requests snowflake-connector-python python-dotenv
  ```
* **Snowflake Account**: Access with permissions to create stages, tables, and load data
* **Internet Access**: Required for downloading Wikipedia pageview data

## Setup

### Environment Variables

Create a `.env` file in the project root with your configuration:

```ini
# Snowflake Connection Details (Required)
SNOWFLAKE_ACCOUNT="your_account_identifier"
SNOWFLAKE_USER="your_username"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE="your_warehouse"
SNOWFLAKE_DATABASE="your_database"
SNOWFLAKE_SCHEMA="WIKIPEDIA"  # All objects created here

# Data Ingestion Settings
LOCAL_DATA_DIR="wikipedia_pageviews_files"
MAX_DOWNLOAD_WORKERS=5

# Optional: Custom object names
SNOWFLAKE_STAGE_NAME="WIKIPEDIA_PAGEVIEWS_STAGE"
SNOWFLAKE_TABLE_NAME="RAW_WIKIPEDIA_PAGEVIEWS"
```

**Security Note**: For production environments, use dedicated secrets management services instead of `.env` files.

## Usage

### Manual Execution

Run the ingestion script directly:

```bash
cd data_ingest/
python wikipedia_data_ingest.py
```

The script will:
1. Automatically determine the current hour to process
2. Download exactly one Wikipedia pageview file for that hour
3. Extract and process the file locally
4. Connect to Snowflake and create necessary objects
5. Upload data to Snowflake stage
6. Copy data into the `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS` table

### Orchestrated Execution

The ingestion script is integrated with Dagster for automated orchestration:

```bash
cd orchestration/
dagster dev -m wikipedia_dagster.definitions
```

This provides:
- **Automatic scheduling** every hour
- **Web-based monitoring** and control
- **Integration with dbt transformations**
- **Comprehensive logging** and error tracking

## Architecture

### Data Flow

```
Wikipedia Dumps → Download → Extract → Snowflake Stage → Raw Table
     (.gz)           ↓           ↓            ↓            ↓
                Local Files → Process → Upload → WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS
```

### Snowflake Objects Created

The ingestion process automatically creates:

1. **Schema**: `WIKIPEDIA`
   - Creates the schema if it doesn't exist
   - Ensures consistent organization across the pipeline

2. **Stage**: `WIKIPEDIA.WIKIPEDIA_PAGEVIEWS_STAGE`
   - Internal Snowflake stage for file uploads
   - Temporary storage for data loading

3. **Table**: `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`
   ```sql
   CREATE TABLE WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS (
       PROJECT_CODE VARCHAR,        -- e.g., 'en.wikipedia'
       PAGE_TITLE VARCHAR,          -- Wikipedia page title
       VIEW_COUNT NUMBER,           -- Hourly view count
       BYTE_SIZE NUMBER,            -- Page size in bytes  
       FILE_NAME VARCHAR,           -- Source file name
       LOAD_TIMESTAMP TIMESTAMP_NTZ -- When data was loaded
   );
   ```

### Class Structure

* **`Config`**: Centralized configuration management with environment variable loading
* **`WikipediaDownloader`**: Handles data download and local file management
* **`SnowflakeLoader`**: Manages all Snowflake interactions (connection, setup, data loading)
* **`run_ingestion_workflow`**: Main orchestration function

## Clean Code Principles Applied

### Single Responsibility Principle (SRP)
- **`Config`**: Manages all configuration and environment variables
- **`WikipediaDownloader`**: Handles data acquisition and local processing
- **`SnowflakeLoader`**: Manages database operations exclusively
- **`run_ingestion_workflow`**: Orchestrates the high-level workflow

### Other Principles
* **Meaningful Names**: Clear, descriptive variable and function names
* **Small Functions**: Complex logic broken into focused, testable methods
* **Error Handling**: Explicit error handling with informative logging
* **Configuration Management**: Externalized configuration via environment variables
* **Reduced Dependencies**: Minimal coupling between components
* **Self-Documenting Code**: Clear structure reduces need for extensive comments

### Error Handling Features
* **Graceful Degradation**: Continues processing when possible after errors
* **Comprehensive Logging**: Detailed logging at all stages for debugging
* **Connection Management**: Robust Snowflake connection handling with cleanup
* **File Validation**: Checks for data integrity and availability

## Integration with dbt

The ingestion script creates the raw table that serves as the source for dbt transformations:

1. **Raw Data**: Loaded into `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`
2. **dbt Processing**: Transforms raw data into analytics-ready models
3. **Schema Consistency**: All objects use the same WIKIPEDIA schema
4. **Dependency Management**: dbt automatically waits for raw data availability

## Future Improvements

### Performance Enhancements
* **Parallel Snowflake Loading**: Multi-threaded uploads for large datasets
* **Incremental Loading**: Only process new/changed data
* **Compression Optimization**: Better file handling and compression strategies

### Monitoring and Observability
* **Metrics Collection**: Track download speeds, load times, error rates
* **Health Checks**: Monitor data freshness and quality
* **Alerting Integration**: Notify on failures or data issues

### Data Quality
* **Validation Rules**: Enhanced data integrity checks before loading
* **Duplicate Detection**: Prevent duplicate data ingestion
* **Schema Evolution**: Handle changes in Wikipedia data format

### Cloud Integration
* **Direct Cloud Storage**: Upload to S3/GCS instead of local files
* **Serverless Deployment**: AWS Lambda/Google Cloud Functions support
* **Auto-scaling**: Dynamic resource allocation based on data volume

### Configuration Management
* **CLI Interface**: Command-line arguments for flexible execution
* **Configuration Validation**: Strict validation of all parameters
* **Multiple Environments**: Support for dev/staging/production configurations

