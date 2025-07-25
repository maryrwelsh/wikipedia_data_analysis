# Wikipedia Pageview Data Pipeline: Ingestion, Transformation & Analytics

This project provides a complete end-to-end data pipeline for ingesting Wikipedia hourly pageview data, transforming it using dbt, and enriching it with AI-powered categorization via Snowflake Cortex for analytics.

## Table of Contents

* [Project Overview](#project-overview)
* [Features](#features)
* [Architecture](#architecture)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
    * [Repository Structure](#repository-structure)
    * [Installation](#installation)
    * [Configuration (`.env`)](#configuration-env)
    * [Snowflake Database Objects](#snowflake-database-objects)
* [Usage](#usage)
    * [Step 1: Ingest Raw Data (Python)](#step-1-ingest-raw-data-python)
    * [Step 2: Transform and Categorize Data (dbt)](#step-2-transform-and-categorize-data-dbt)
* [Key Components](#key-components)
* [Clean Code Principles Applied](#clean-code-principles-applied)
* [Future Improvements](#future-improvements)

## Project Overview

This project automates the process of acquiring public Wikipedia pageview data, loading it into Snowflake, and then transforming and enriching it to enable analytical insights into trending topics. It combines a Python ingestion script with a dbt (data build tool) project for robust data transformation and AI-driven categorization.

## Features

* **Automated Data Download:** Downloads hourly Wikipedia pageview `.gz` files for a specified date range.
* **Parallel Processing for Downloads:** Utilizes a Python thread pool to download and unzip files concurrently, speeding up data acquisition.
* **Automatic Unzipping:** Unzips downloaded `.gz` files into `.txt` format.
* **Idempotent Ingestion:** Skips downloading/unzipping files that already exist locally.
* **Snowflake Data Loading:** Uploads local `.txt` files to a Snowflake internal stage and copies them into a raw table.
* **Data Cleaning & Standardization (dbt):** Processes raw pageview data, handling parsing, and ensuring consistent data types.
* **AI-Powered Categorization (dbt & Snowflake Cortex):** Leverages Snowflake Cortex's `SNOWFLAKE.CORTEX.COMPLETE` function to dynamically categorize Wikipedia page titles into predefined topics (e.g., Technology, History, Sports).
* **Idempotent UDF Creation (dbt):** The Cortex categorization User-Defined Function (UDF) is automatically created/updated at the start of each dbt run.
* **Date and Time Dimensions (dbt):** Generates `dim_date` and `dim_hour` tables for rich time-based analysis.
* **Trending Topic Analysis (dbt):** Provides analytical models to aggregate pageviews by category, enabling the identification of trending topics.
* **Environment-Driven Configuration:** All sensitive and environment-specific parameters are managed via environment variables (with `.env` support for local development).
* **Modular & Testable:** Follows Clean Code principles and dbt best practices for modularity, reusability, and testability.

## Architecture

The pipeline consists of two main stages:

1.  **Ingestion Layer (Python Script):**
    * Downloads compressed Wikipedia pageview files from Wikimedia dumps.
    * Unzips files locally.
    * Uploads `.txt` files to a Snowflake internal stage.
    * Copies data from the stage into a raw Snowflake table (`WIKIPEDIA_PAGEVIEWS_RAW`).

2.  **Transformation & Analytics Layer (dbt Project):**
    * Connects to Snowflake.
    * Creates a Snowflake Cortex UDF for AI categorization.
    * Transforms raw data into clean staging models.
    * Categorizes unique page titles using the Cortex UDF, storing results in `dim_wikipedia_page`.
    * Generates `dim_date` and `dim_hour` dimensions.
    * Builds fact tables (`fct_wikipedia_pageviews`) by joining pageview data with categories and date/time dimensions.
    * Creates analytical models (e.g., `wikipedia_pageviews`) for reporting.

```ini
+-------------------+       +-------------------+       +-------------------+       +-------------------+
| Wikimedia Dumps   |       | Python Ingestion  |       | Snowflake Raw     |       | dbt Transformation|
| (.gz files)       |------>| Script            |------>| Table             |------>| Project           |
|                   |       | (Parallel Download)|       | (WIKIPEDIA_PAGEVIEWS_RAW) | (Models, UDFs)    |
+-------------------+       +-------------------+       +-------------------+       +-------------------+
                                                                   |
                                                                   v
                                                         +-------------------+
                                                         | Snowflake Cortex  |
                                                         | (AI Categorization)|
                                                         +-------------------+
                                                                   |
                                                                   v
                                                         +-------------------+
                                                         | Snowflake Transformed |
                                                         | Tables & Views    |
                                                         | (e.g., dim_*, fct_*)  |
                                                         +-------------------+






```
## Prerequisites

Before setting up and running the pipeline, ensure you have the following:

* **Python 3.8+**
* **`pip`** (Python package installer)
* **Snowflake Account:** An active Snowflake account with appropriate roles and permissions to:
    * Create databases, schemas, tables, and stages.
    * Create and execute User-Defined Functions (UDFs), including Cortex functions (`SNOWFLAKE.CORTEX.COMPLETE`).
    * Load data from stages.
* **dbt CLI:** The dbt command-line interface installed and configured.
    ```bash
    pip install dbt-snowflake
    ```

## Setup

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd your_project_root
    ```
2.  **Install Python Dependencies:**
    Navigate to the `wikipedia_data_ingest/` directory and install Python packages:
    ```bash
    cd wikipedia_data_ingest/
    pip install requests snowflake-connector-python python-dotenv concurrent.futures
    cd .. # Go back to project root
    ```
3.  **Install dbt Dependencies:**
    Navigate to the `wikipedia_dbt/` directory and install dbt packages:
    ```bash
    cd wikipedia_dbt/
    dbt deps
    cd .. # Go back to project root
    ```

### Configuration (`.env`)

Create a file named `.env` in your `your_project_root/` directory. This file will store all environment variables required by both the Python script and dbt. **Do NOT commit this file to version control (e.g., Git).**

```ini
# .env file for the overall project configuration

# --- General Configuration ---
# Optional: Local directory for downloaded pageview files (for Python script)
# If not set, defaults to "wikipedia_pageviews" inside ingest_script/
# LOCAL_DATA_DIR=my_pageviews_data

# Optional: Number of parallel workers for downloading files (for Python script)
# If not set, defaults to 5. Adjust based on your network and CPU.
# MAX_DOWNLOAD_WORKERS=5

# --- Date Range for Data Ingestion (for Python script) ---
# Required: Start and End dates for data ingestion
# Format: YYYY-MM-DD HH:MM:SS
START_DATE="2024-01-01 00:00:00"
END_DATE="2024-01-01 02:00:00"

# --- Snowflake Connection Details (Required by both Python and dbt) ---
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT_IDENTIFIER"
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USERNAME"
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"
SNOWFLAKE_DATABASE="YOUR_SNOWFLAKE_DATABASE"
SNOWFLAKE_SCHEMA="YOUR_SNOWFLAKE_SCHEMA"
# Optional: If you use a specific role for dbt or Python connection
# SNOWFLAKE_ROLE="YOUR_DBT_ROLE"
```

**Important Security Note:** For production environments, it is strongly recommended to use a dedicated secrets management service (e.g., AWS Secrets Manager, Azure Key Vault, Google Secret Manager) instead of `.env` files for sensitive credentials.

### Snowflake Database Objects

The Python ingestion script expects a raw table in Snowflake. The dbt project will then transform this.

**Raw Data Table DDL:**
```sql
CREATE TABLE WIKIPEDIA_PAGEVIEWS_RAW (
    PROJECT_CODE VARCHAR,
    PAGE_TITLE VARCHAR,
    VIEW_COUNT NUMBER,
    BYTE_SIZE NUMBER,
    FILE_NAME VARCHAR,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
);
```
*(This table will be populated by the Python ingestion script.)*

The dbt project will automatically create the `CLASSIFY_WIKIPEDIA_PAGE` UDF using Snowflake Cortex and manage the creation/updates of other transformed tables (`dim_wikipedia_page`, `dim_date`, `dim_hour`, `fct_wikipedia_pageviews`, etc.).

## Usage

Follow these steps to run the entire data pipeline:

### Step 1: Ingest Raw Data (Python)

Navigate to the `wikipedia_data_ingest/` directory and run the Python script:

```bash
cd wikipedia_data_ingest/
python wikipedia_data_ingest.py
cd .. # Go back to project root
```

This will download the specified Wikipedia pageview data and load it into your `WIKIPEDIA_PAGEVIEWS_RAW` table in Snowflake.

### Step 2: Transform and Categorize Data (dbt)

Navigate to the `wikipedia_dbt/` directory and run your dbt project:

```bash
cd wikipedia_dbt/
dbt debug # Optional: Test your Snowflake connection
dbt run   # Executes all models, including UDF creation and data transformations
dbt test  # Optional: Run data tests
cd .. # Go back to project root
```

After a successful `dbt run`, your Snowflake database will contain the transformed and categorized Wikipedia pageview data, ready for analytical queries.

## Key Components

* **`wikipedia_data_ingest/wikipedia_data_ingest.py`**: The Python script responsible for data download, unzipping, and initial loading into Snowflake.
* **`wikipedia_dbt/`**: The dbt project containing all SQL models for data transformation, categorization, and analytics.
    * **`wikipedia_dbt/macros/create_cortex_udf.sql`**: Defines the Snowflake Cortex UDF for AI categorization.
    * **`wikipedia_dbt/models/staging/stg_wikipedia_pageviews.sql`**: Cleans and prepares raw ingested data.
    * **`wikipedia_dbt/models/marts/dim_wikipedia_page.sql`**: Dimension table for AI-categorized Wikipedia page titles (uses Cortex UDF).
    * **`wikipedia_dbt/models/marts/dim_date.sql`**: Generates a comprehensive date dimension table.
    * **`wikipedia_dbt/models/marts/dim_hour.sql`**: Generates an hourly time dimension table.
    * **`wikipedia_dbt/models/marts/fct_wikipedia_pageviews.sql`**: Fact table combining pageviews with categories and date/time dimensions.
    * **`wikipedia_dbt/models/analytics/wikipedia_pageviews.sql`**: Analytical model for identifying trending topics.

## Clean Code Principles Applied

This project consistently applies Clean Code principles across both the Python script and the dbt project:

* **Single Responsibility Principle (SRP):** Each component (Python script, dbt models, functions, classes) has a clear, defined purpose.
* **Modularity:** The pipeline is broken down into distinct, manageable parts (ingestion, staging, dimensions, facts, analytics).
* **Meaningful Names:** Clear and descriptive naming conventions are used for files, variables, functions, and models.
* **Small Functions/Models:** Complex logic is decomposed into smaller, focused units.
* **Error Handling:** Robust error handling and logging are implemented to ensure graceful failure and traceability.
* **Configuration over Hardcoding:** All dynamic and sensitive parameters are externalized via environment variables.
* **Idempotency:** Operations are designed to be repeatable without causing unintended side effects (e.g., `CREATE OR REPLACE`, skipping existing files).
* **Testability:** The modular design facilitates easier testing of individual components and data transformations.
* **Self-Documenting Code:** Code structure and naming reduce the need for excessive comments, supplemented by docstrings and dbt model descriptions.

## Future Improvements

* **Enhanced Logging & Monitoring:** Implement more granular logging, integrate with external monitoring tools, and set up alerts for pipeline failures or anomalies.
* **Dynamic Category Management:** Store and manage the list of categories for the Cortex UDF more dynamically (e.g., in a Snowflake configuration table) to allow for easier updates without code changes.
* **Performance Optimization:**
    * Explore further parallelization for Snowflake loading (e.g., using Snowflake's multi-cluster warehouses).
    * Optimize dbt model strategies and investigate Snowflake clustering keys.
* **Advanced Analytics:** Develop more sophisticated trending algorithms (e.g., weighted averages, anomaly detection, seasonality analysis).
* **Data Quality Framework:** Implement a comprehensive data quality framework with dbt tests and custom checks.
* **CI/CD Integration:** Automate the entire pipeline (ingestion, dbt run, tests) within a CI/CD pipeline for continuous delivery.
* **Cloud Storage Integration:** Consider direct ingestion to cloud storage (e.g., S3, GCS) if Snowflake is configured to read from there, bypassing local storage.
* **Dashboarding:** Connect a BI tool (Tableau, Power BI, Looker) to the dbt analytics models for interactive data exploration.