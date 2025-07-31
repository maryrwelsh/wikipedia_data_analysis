# Wikipedia Pageview Data Transformation (dbt Project)

This dbt (data build tool) project transforms raw Wikipedia hourly pageview data from Snowflake, focusing on cleaning, enriching, and optionally categorizing data using Snowflake Cortex's AI capabilities for analytics on trending topics.

## Table of Contents

* [Project Overview](#project-overview)
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
    * [Environment Variables](#environment-variables)
    * [Snowflake Database Objects](#snowflake-database-objects)
* [Project Structure](#project-structure)
* [Models](#models)
* [Usage](#usage)
* [Schema Management](#schema-management)
* [AI Categorization](#ai-categorization)
* [Clean Code Principles Applied](#clean-code-principles-applied)
* [Future Improvements](#future-improvements)

## Project Overview

This dbt project takes raw Wikipedia pageview data (from `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`) and transforms it into clean, categorized, and analytics-ready models. It leverages Snowflake Cortex's AI functions for optional page categorization and creates a comprehensive data model for trending content analysis.

## Features

* **Incremental Processing**: Efficient incremental materialization that only processes new data for optimal performance
* **Data Cleaning & Standardization**: Processes raw pageview data with consistent data types and validation
* **AI-Powered Categorization**: Snowflake Cortex integration for automatic page categorization with intelligent processing
* **Automatic UDF Management**: The Cortex categorization User-Defined Function is automatically created/updated on each dbt run
* **Schema Consistency**: All objects are created in the dedicated WIKIPEDIA schema
* **Trending Topic Analysis**: Analytical models for aggregating pageviews by category and time with real-time updates
* **Modular & Testable**: Follows dbt best practices for modularity, reusability, and testability
* **Performance Optimized**: Incremental models with smart filtering for maximum efficiency and cost control

## Prerequisites

Before running this dbt project, ensure you have the following:

* **Snowflake Account**: Active account with permissions to create databases, schemas, tables, and functions (including Cortex functions)
* **dbt CLI**: The dbt command-line interface installed and configured
    ```bash
    pip install dbt-snowflake
    ```
* **Python 3.11+**: Required for dbt and dependencies
* **Raw Wikipedia Data**: `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS` table populated by the ingestion process

## Setup

### Environment Variables

The project uses a `profiles.yml` file with default values and environment variable overrides:

```yaml
wikipedia_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT', 'placeholder-account') }}"
      user: "{{ env_var('SNOWFLAKE_USER', 'placeholder-user') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD', 'placeholder-password') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'PUBLIC') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'placeholder-database') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'placeholder-warehouse') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'WIKIPEDIA') }}"
      threads: 4
      keepalives_idle: 240
      search_path: "{{ env_var('SNOWFLAKE_SCHEMA', 'WIKIPEDIA') }}"
```

Create a `.env` file in the project root with your Snowflake credentials:

```ini
# Snowflake Connection Details
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_USER="your_username"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE="your_warehouse"
SNOWFLAKE_DATABASE="your_database"
SNOWFLAKE_SCHEMA="WIKIPEDIA"
SNOWFLAKE_ROLE="your_role"
```

### Snowflake Database Objects

The project expects a raw table with this structure:

```sql
CREATE TABLE WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS (
    PROJECT_CODE VARCHAR,
    PAGE_TITLE VARCHAR,
    VIEW_COUNT NUMBER,
    BYTE_SIZE NUMBER,
    FILE_NAME VARCHAR,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
);
```

The Cortex UDF is automatically created via the `on-run-start` hook.

## Project Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_wikipedia_pageviews.sql     # Staging layer with data cleaning
│   │   └── stg_wikipedia_pageviews.yml     # Tests and documentation
│   ├── marts/
│   │   ├── dim_date.sql                    # Date dimension
│   │   ├── dim_hour.sql                    # Hour dimension  
│   │   ├── dim_wikipedia_page.sql          # Page dimension with categories
│   │   ├── dim_wikipedia_page.yml          # Tests and documentation
│   │   ├── fct_wikipedia_pageviews.sql     # Pageview fact table
│   │   └── fct_wikipedia_pageviews.yml     # Tests and documentation
│   ├── analytics/
│   │   └── wikipedia_pageviews.sql         # Analytics-ready view
│   └── sources.yml                         # Source definitions
├── macros/
│   ├── create_cortex_udf.sql               # Cortex AI UDF macro
│   └── get_custom_schema.sql               # Custom schema naming
├── tests/                                  # Custom dbt tests
├── dbt_project.yml                         # Project configuration
├── profiles.yml                            # Connection profiles
└── README.md                               # This file
```

## Models

### Staging Layer

* **`stg_wikipedia_pageviews`** (Incremental): 
  * Incrementally processes raw pageview data with 2-hour window filtering
  * Cleans and standardizes data with AI categorization
  * Extracts language and project information with timestamp tracking
  * Uses unique surrogate key for efficient incremental updates

### Marts Layer

* **`dim_wikipedia_page`** (Incremental): 
  * Incrementally captures new unique Wikipedia page combinations
  * AI-generated category classifications for new pages only
  * Page-level attributes and keys with efficient deduplication

* **`dim_date`** (Table): 
  * Date dimension with calendar attributes
  * Supports time-based analysis and filtering

* **`dim_hour`** (Table): 
  * Hour dimension (0-23) for hourly analysis
  * Useful for time-of-day trending patterns

* **`fct_wikipedia_pageviews`** (Incremental): 
  * Incremental fact table with optimized performance
  * Links all dimensions with foreign key relationships
  * Contains pageview metrics with timestamp-based filtering
  * Uses composite unique key for precise incremental updates

### Analytics Layer

* **`wikipedia_pageviews`** (View): 
  * Analytics-ready **view** combining all dimensions with real-time data
  * Dynamic aggregation for common trending analysis
  * Includes category, time, and view metrics from incremental sources
  * Always reflects the latest incremental updates

## Usage

1. **Test Connection:**
   ```bash
   dbt debug
   ```

2. **Run Transformations:**
   ```bash
   dbt run
   ```
   This automatically:
   - Creates the Cortex UDF via `on-run-start` hook
   - Builds all models in dependency order
   - Creates tables in the WIKIPEDIA schema

3. **Run Tests:**
   ```bash
   dbt test
   ```

4. **Build Everything:**
   ```bash
   dbt build  # Runs + tests models
   ```

## Schema Management

All dbt models are created in the **WIKIPEDIA** schema by default:

* Raw tables: `WIKIPEDIA.RAW_WIKIPEDIA_PAGEVIEWS`
* Staging: `WIKIPEDIA.STG_WIKIPEDIA_PAGEVIEWS`
* Dimensions: `WIKIPEDIA.DIM_*`
* Facts: `WIKIPEDIA.FCT_*`
* Analytics: `WIKIPEDIA.WIKIPEDIA_PAGEVIEWS` (view)

This is controlled by the `+schema: wikipedia` configuration in `dbt_project.yml` and the custom schema macro.

## AI Categorization

### Automatic Setup

The project includes `on-run-start` hooks that automatically set up the environment:

```yaml
on-run-start:
  - "{{ create_schema_if_not_exists() }}"
  - "{{ create_cortex_udf() }}"
```

This ensures:
1. **Schema Creation**: The WIKIPEDIA schema is created if it doesn't exist
2. **UDF Creation**: The `CLASSIFY_WIKIPEDIA_PAGE` UDF is created/updated

### Performance Considerations

* AI categorization is **limited to 1,000 rows** by default to control costs/speed
* To enable full categorization, modify `stg_wikipedia_pageviews.sql` and remove the `LIMIT` clause
* The UDF categorizes pages into predefined topics using Snowflake Cortex

### Available Categories

- Technology
- History
- Science
- Sports
- Arts_and_Culture
- Geography
- Politics
- Current_Events
- Biography
- Health
- Nature
- Entertainment
- Miscellaneous

## Clean Code Principles Applied

* **Modularity**: Clear separation between staging, marts, and analytics layers
* **Single Responsibility**: Each model has a focused purpose
* **Readability**: Well-documented SQL with consistent formatting
* **Idempotency**: Models can be run multiple times safely
* **Testability**: Basic test coverage with dbt's testing framework
* **Configuration Management**: Environment-driven configuration with sensible defaults

## Future Improvements

* **Dynamic Category Management**: Store categories in a configuration table
* **Performance Optimization**: 
  * The AI calls can be expensive. Performance gains can be made by fine tuning Snowflake's warehouse settings (scale up/out) to handle processing larger row counts.
  * Add clustering keys for frequently queried tables
* **Advanced Analytics**: 
  * Implement trending algorithms and anomaly detection
  * Add time-series analysis capabilities
* **Data Quality**: Enhanced testing and data validation
* **Cost Optimization**: Smart sampling strategies for AI categorization
* **CI/CD Integration**: Automated testing and deployment pipelines