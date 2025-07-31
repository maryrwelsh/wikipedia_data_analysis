# Wikipedia Data Analysis Pipeline

A comprehensive data pipeline for analyzing Wikipedia pageview data using modern data engineering practices. This project combines data ingestion, transformation, and orchestration to provide insights into trending topics and content categorization.

## 📑 Table of Contents

* [🏗️ Architecture Overview](#%EF%B8%8F-architecture-overview)
* [✨ Key Features](#-key-features)
* [📁 Project Structure](#-project-structure)
* [🚀 Quick Start](#-quick-start)
  * [Prerequisites](#prerequisites)
  * [1. Setup Environment](#1-setup-environment)
  * [2. Configure Environment Variables](#2-configure-environment-variables)
  * [3. Run the Pipeline](#3-run-the-pipeline)
    * [Option A: Manual Execution](#option-a-manual-execution)
    * [Option B: Orchestrated Execution (Recommended)](#option-b-orchestrated-execution-recommended)
* [📊 Data Models](#-data-models)
  * [Raw Data Schema](#raw-data-schema)
  * [Transformed Models](#transformed-models)
* [🤖 AI-Powered Categorization](#-ai-powered-categorization)
* [🔧 Configuration](#-configuration)
  * [dbt Configuration](#dbt-configuration)
  * [Dagster Configuration](#dagster-configuration)
  * [Schema Management](#schema-management)
* [📈 Usage Examples](#-usage-examples)
  * [Query Trending Topics](#query-trending-topics)
  * [Analyze Page Performance](#analyze-page-performance)
* [🧪 Testing](#-testing)
  * [dbt Tests](#dbt-tests)
  * [Data Quality Checks](#data-quality-checks)
* [🔄 Scheduling](#-scheduling)
* [⏰ Automated Scheduling](#-automated-scheduling)
  * [Schedule Configuration](#schedule-configuration)
  * [How It Works](#how-it-works)
  * [Managing Schedules](#managing-schedules)
* [🐛 Troubleshooting](#-troubleshooting)
  * [Common Issues](#common-issues)
  * [Logs](#logs)
* [📚 Documentation](#-documentation)
* [🙏 Acknowledgments](#-acknowledgments)

## 🏗️ Architecture Overview

The pipeline consists of three main components:

1. **Data Ingestion Layer (Python)**: Downloads and processes raw Wikipedia pageview data
2. **Transformation Layer (dbt)**: Transforms raw data into analytics-ready models with AI-powered categorization
3. **Orchestration Layer (Dagster)**: Coordinates the entire pipeline with scheduling and monitoring

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Wikimedia Dumps │───▶│ Data Ingestion  │───▶│ Snowflake Raw   │───▶│ dbt Transform   │
│ (.gz files)     │    │ (Python Script) │    │ Table           │    │ (Models, UDFs)  │
│                 │    │ (Parallel DL)   │    │ (WIKIPEDIA.     │    │                 │
└─────────────────┘    └─────────────────┘    │ RAW_WIKIPEDIA_  │    └─────────────────┘
                                              │ PAGEVIEWS)      │
                                              └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │ Snowflake Cortex│
                                              │ (AI Categorization)│
                                              └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │ Transformed     │
                                              │ Tables & Views  │
                                              │ (WIKIPEDIA.     │
                                              │ dim_*, fct_*)   │
                                              └─────────────────┘
```

## ✨ Key Features

- **🔄 Automated Hourly Processing**: Intelligent processing that automatically ingests the current hour's Wikipedia pageview data
- **⚡ Incremental Processing**: Efficient incremental data loading that only processes new data for optimal performance
- **🤖 AI-Powered Categorization**: Uses Snowflake Cortex to automatically categorize Wikipedia pages into meaningful topics
- **📊 Modern Data Stack**: Built with dbt, Dagster, and Snowflake for scalable data engineering
- **⏱️ Real-time Monitoring**: Comprehensive logging and monitoring through Dagster's web interface
- **🔧 Flexible Configuration**: Easy configuration through environment variables and parameterized functions
- **📈 Analytics-Ready Models**: Produces clean, documented data models with incremental updates ready for analysis
- **🚀 Optimized Processing**: Single-file ingestion and incremental transformations for maximum efficiency
- **✅ Data Quality**: Built-in testing and validation with dbt's testing framework
- **🏛️ Schema Consistency**: All objects created in dedicated WIKIPEDIA schema for organization

## 📁 Project Structure

```
wikipedia_data_analysis/
├── orchestration/              # Dagster orchestration
│   └── wikipedia_dagster/      # Dagster project files (renamed from 'dagster')
│       ├── assets.py          # Asset definitions
│       ├── definitions.py     # Dagster definitions
│       ├── project.py         # dbt project configuration
│       ├── schedules.py       # Schedule definitions
│       ├── __init__.py        # Python package init
│       ├── pyproject.toml     # Python package config
│       └── setup.py           # Package setup
├── data_ingest/               # Data ingestion scripts
│   ├── wikipedia_data_ingest.py  # Main ingestion script
│   ├── README.md              # Ingestion documentation
│   └── wikipedia_pageviews_files/ # Downloaded data files
├── dbt/                       # dbt transformation project
│   ├── models/                # dbt models
│   │   ├── staging/           # Staging models
│   │   ├── marts/             # Mart models (dim, fct)
│   │   ├── analytics/         # Analytics models
│   │   └── sources.yml        # Source definitions
│   ├── macros/                # dbt macros and UDFs
│   │   ├── create_cortex_udf.sql  # Cortex AI UDF
│   │   └── get_custom_schema.sql  # Custom schema macro
│   ├── tests/                 # dbt tests
│   ├── dbt_project.yml        # dbt project config
│   └── profiles.yml           # dbt profiles (with defaults)
├── README.md                  # This file
├── .env                       # Environment variables (not in repo)
├── .env_example               # Environment variables template
└── logs/                      # Application logs
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **Snowflake Account** with appropriate permissions
- **dbt CLI** (`pip install dbt-snowflake`)

### 1. Setup Environment

```bash
# Clone the repository
git clone https://github.com/maryrwelsh/wikipedia_data_analysis
cd wikipedia_data_analysis

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install orchestration package
pip install -e orchestration/
```

### 2. Configure Environment Variables

Copy the example environment file and configure your settings:

```bash
cp .env_example .env
```

Edit `.env` with your Snowflake credentials and configuration:

```ini
# Snowflake Connection
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_USER="your_username"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE="your_warehouse"
SNOWFLAKE_DATABASE="your_database"
SNOWFLAKE_SCHEMA="WIKIPEDIA"  # All objects will be created here

# Data Ingestion Settings
LOCAL_DATA_DIR="wikipedia_pageviews_files"
MAX_DOWNLOAD_WORKERS=5

# Optional: Custom table names
SNOWFLAKE_TABLE_NAME="RAW_WIKIPEDIA_PAGEVIEWS"
SNOWFLAKE_STAGE_NAME="WIKIPEDIA_PAGEVIEWS_STAGE"
```

### 3. Run the Pipeline

#### Option A: Manual Execution

```bash
# 1. Ingest raw data
cd data_ingest/
python wikipedia_data_ingest.py
cd ..

# 2. Transform data with dbt
cd dbt/
dbt debug  # Test connection
dbt run    # Execute transformations
dbt test   # Run tests
cd ..
```

#### Option B: Orchestrated Execution (Recommended)

```bash
# Start Dagster development server (IMPORTANT: Run from orchestration directory)
cd orchestration
dagster dev -m wikipedia_dagster.definitions
```

Then open http://localhost:3000 to:
- View and trigger pipeline runs manually
- Monitor automatic hourly execution
- Access real-time logs and metrics
- Manage schedules and assets

**🕐 Automatic Hourly Processing**: The pipeline is configured to automatically process the current hour's Wikipedia pageview data with incremental efficiency. The ingestion script intelligently determines the current hour, downloads exactly one file, and the dbt models use incremental processing to only transform new data, ensuring optimal performance and up-to-date trending topics.

## 📊 Data Models

### Raw Data Schema

The ingestion process creates a raw table in the WIKIPEDIA schema with the following structure:

```sql
CREATE TABLE WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS (
    PROJECT_CODE VARCHAR,      -- e.g., 'en.wikipedia'
    PAGE_TITLE VARCHAR,        -- Wikipedia page title
    VIEW_COUNT NUMBER,         -- Hourly view count
    BYTE_SIZE NUMBER,          -- Page size in bytes
    FILE_NAME VARCHAR,         -- Source file name
    LOAD_TIMESTAMP TIMESTAMP_NTZ -- When data was loaded
);
```

### Transformed Models

The dbt project creates several analytical models in the WIKIPEDIA schema with incremental processing:

- **`stg_wikipedia_pageviews`**: Incremental staging model with AI categorization and timestamp tracking
- **`dim_wikipedia_page`**: Incremental dimension table capturing new page combinations
- **`dim_date`**: Date dimension for time-based analysis
- **`dim_hour`**: Hour dimension for hourly analysis
- **`fct_wikipedia_pageviews`**: Incremental fact table with optimized performance
- **`wikipedia_pageviews`**: Analytics-ready **view** for trending analysis with real-time data

## 🤖 AI-Powered Categorization
The pipeline includes Snowflake Cortex integration for automatic page categorization. **Note**: AI categorization is currently limited by default for performance and cost optimization.

Available categories include:
- **Technology**
- **History**
- **Science**
- **Sports**
- **Arts and Culture**
- **Geography**
- **Politics**
- **Current Events**
- **Biography**
- **Health**
- **Nature**
- **Entertainment**
- **Miscellaneous**

To enable AI categorization, modify the `stg_wikipedia_pageviews.sql` model and remove the LIMIT clause.

## 🔧 Configuration

### dbt Configuration

The dbt project is configured in `dbt/dbt_project.yml`:

```yaml
name: 'wikipedia_dbt'
version: '1.0.0'
config-version: 2

profile: 'wikipedia_dbt'

models:
  wikipedia_dbt:
    +schema: wikipedia
    +materialized: table
```

### Dagster Configuration

The orchestration is configured in `orchestration/wikipedia_dagster/`:

- **Assets**: Define data pipeline components with proper dependencies
- **Schedules**: Configure automated execution
- **Resources**: Manage external connections (dbt CLI Resource)

### Schema Management

All database objects are created in the **WIKIPEDIA** schema by default:
- Raw tables: `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`
- Staging models: `WIKIPEDIA.stg_wikipedia_pageviews`
- Dimension tables: `WIKIPEDIA.dim_*`
- Fact tables: `WIKIPEDIA.fct_*`

**Note**: The WIKIPEDIA schema is automatically created if it doesn't exist during both data ingestion and dbt execution.

## 📈 Usage Examples

### Query Trending Topics

```sql
-- Get top trending categories for a specific date
SELECT 
    page_category,
    SUM(view_count) as total_views,
    COUNT(DISTINCT page_title) as unique_pages
FROM WIKIPEDIA.wikipedia_pageviews
WHERE date_day = '2025-01-01'
GROUP BY page_category
ORDER BY total_views DESC;
```

### Analyze Page Performance

```sql
-- Find most viewed pages by category
SELECT 
    page_title,
    page_category,
    SUM(view_count) as total_views
FROM WIKIPEDIA.wikipedia_pageviews
WHERE page_category = 'Technology'
GROUP BY page_title, page_category
ORDER BY total_views DESC
LIMIT 10;
```

## 🧪 Testing

### dbt Tests

```bash
cd dbt/
dbt test  # Run all tests
dbt test --select test_type:generic  # Run generic tests only
dbt test --select test_type:singular  # Run singular tests only
```

### Data Quality Checks

The project includes basic data quality tests:
- Not null checks


## 🔄 Scheduling

The pipeline can be scheduled using Dagster:

- **Hourly Refresh**: Automatically runs at 15 minutes past each hour
- **Manual Triggers**: On-demand execution via UI
- **Conditional Execution**: Based on data availability


## ⏰ Automated Scheduling

The pipeline includes an intelligent hourly schedule that automatically processes the latest Wikipedia pageview data:

### Schedule Configuration

- **Frequency**: Every hour at 15 minutes past the hour (`:15`)
- **Cron Schedule**: `15 * * * *`
- **Target Data**: Current hour's Wikipedia pageview data
- **Smart Processing**: Automatically determines the current hour to process

### How It Works

1. **Current Hour Processing**: Automatically processes data for the current hour
2. **Single File Download**: Downloads exactly one file per execution
3. **Intelligent Logic**: The ingestion script handles all hour calculation internally
4. **Unique Run Keys**: Prevents duplicate processing with timestamped run identifiers
5. **Full Pipeline**: Runs both data ingestion and dbt transformations

### Managing Schedules

In the Dagster UI (http://localhost:3000):

1. **View Schedules**: Navigate to "Schedules" tab
2. **Enable/Disable**: Toggle the `hourly_wikipedia_ingestion` schedule
3. **Monitor Runs**: View scheduled run history and status
4. **Manual Triggers**: Run the pipeline manually for specific time ranges

## 🐛 Troubleshooting

### Common Issues

1. **Snowflake Connection Errors**
   - Verify credentials in `.env`
   - Check network connectivity
   - Ensure proper role permissions

2. **dbt Model Failures**
   - Check `dbt/logs/` for detailed error messages
   - Verify source data availability
   - Run `dbt debug` to test connections

3. **Dagster Asset Failures**
   - Check asset logs in Dagster UI
   - Verify Python dependencies
   - Ensure proper file paths

4. **Module Import Errors**
   - **CRITICAL**: Always run `dagster dev` from the `orchestration/` directory
   - Use: `cd orchestration && dagster dev -m wikipedia_dagster.definitions`

5. **Schema Issues**
   - Verify `SNOWFLAKE_SCHEMA=WIKIPEDIA` in `.env`
   - Check that your Snowflake role has permissions on WIKIPEDIA schema

### Logs

- **Application Logs**: `logs/`
- **dbt Logs**: `dbt/logs/`
- **Dagster Logs**: Available in Dagster UI

## 📚 Documentation

- **Data Ingestion**: See `data_ingest/README.md`
- **dbt Models**: See `dbt/README.md`
- **Orchestration**: See `orchestration/README.md`

## 🙏 Acknowledgments

- Wikimedia Foundation for providing pageview data
- Snowflake for the data platform and Cortex AI capabilities
- dbt for the transformation framework
- Dagster for the orchestration platform

---

**Note**: This pipeline is designed for educational and research purposes. Please ensure compliance with Wikimedia's terms of service and data usage policies.





