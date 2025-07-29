# Wikipedia Pageview Data Transformation (dbt Project)

This dbt (data build tool) project is designed to transform raw Wikipedia hourly pageview data, which is assumed to be ingested into Snowflake by an upstream process (e.g., the Python ingestion script). The project focuses on cleaning, enriching, and categorizing this data using Snowflake Cortex's AI capabilities, enabling powerful analytics on trending topics.

## Table of Contents

* [Project Overview](#project-overview)
* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
    * [Environment Variables (`.env`)](#environment-variables-env)
    * [Snowflake Database Objects](#snowflake-database-objects)
* [Project Structure](#project-structure)
* [Models](#models)
* [Usage](#usage)
* [Clean Code Principles Applied (dbt)](#clean-code-principles-applied-dbt)
* [Future Improvements](#future-improvements)

## Project Overview

This dbt project takes raw Wikipedia pageview data (expected in a table named `WIKIPEDIA_PAGEVIEWS_RAW`) and transforms it into a clean, categorized, and analytics-ready format. It leverages Snowflake Cortex's AI functions to automatically classify Wikipedia page titles into relevant topics, allowing for deeper insights into trending content.

## Features

* **Data Cleaning & Standardization:** Processes raw pageview data, handling parsing, and ensuring consistent data types.
* **AI-Powered Categorization:** Utilizes Snowflake Cortex's `SNOWFLAKE.CORTEX.COMPLETE` function to dynamically categorize Wikipedia page titles into predefined topics (e.g., Technology, History, Sports).
* **Idempotent UDF Creation:** The Cortex categorization User-Defined Function (UDF) is automatically created/updated at the start of each dbt run, ensuring its availability.
* **Trending Topic Analysis:** Provides analytical models to aggregate pageviews by category, enabling the identification of trending topics.
* **Modular & Testable:** Follows dbt best practices for modularity, reusability, and testability.

## Prerequisites

Before running this dbt project, ensure you have the following:

* **Snowflake Account:** An active Snowflake account with appropriate roles and permissions to create databases, schemas, tables, and functions (including Cortex functions).
* **dbt CLI:** The dbt command-line interface installed and configured.
    ```bash
    pip install dbt-snowflake
    ```
* **Python 3.8+:** Required for dbt and `python-dotenv`.
* **Raw Wikipedia Pageview Data:** A Snowflake table named `WIKIPEDIA_PAGEVIEWS_RAW` (or as configured in your dbt `sources.yml`) populated with raw Wikipedia hourly pageview data. This is typically loaded by an external ingestion process (e.g., the accompanying Python script).

## Setup

1.  **Clone the repository (or set up dbt project):**
    If you haven't already, set up your dbt project structure.

2.  **Configure your dbt profile:**
    Ensure your `profiles.yml` file (typically located in `~/.dbt/profiles.yml`) is correctly configured to connect to your Snowflake account.

    ```yaml
    your_snowflake_profile: # This name should match the 'profile' in dbt_project.yml
      target: dev
      outputs:
        dev:
          type: snowflake
          account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
          user: "{{ env_var('SNOWFLAKE_USER') }}"
          password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
          role: "{{ env_var('SNOWFLAKE_ROLE', 'ACCOUNTADMIN') }}" # Or a more specific role
          warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
          database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
          schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
          threads: 4
          client_session_keep_alive: False
    ```

3.  **Create a `.env` file:**
    In the root directory of your dbt project, create a file named `.env`. This file will store your Snowflake connection details and other environment-specific configurations. **Do NOT commit this file to version control (e.g., Git).**

### Environment Variables (`.env`)

Populate your `.env` file with the following:

```ini
# .env file for dbt project configuration

# --- Snowflake Connection Details (Required by dbt profile) ---
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT_IDENTIFIER"
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USERNAME"
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"
SNOWFLAKE_DATABASE="YOUR_SNOWFLAKE_DATABASE"
SNOWFLAKE_SCHEMA="YOUR_SNOWFLAKE_SCHEMA"
# Optional: If you use a specific role for dbt
# SNOWFLAKE_ROLE="YOUR_DBT_ROLE"
```
**Important Security Note:** For production environments, it is strongly recommended to use a dedicated secrets management service (e.g., AWS Secrets Manager, Azure Key Vault, Google Secret Manager) instead of `.env` files for sensitive credentials.

### Snowflake Database Objects

Ensure your raw data is available in Snowflake. This dbt project assumes the existence of a table (or view) with the following structure, typically named `WIKIPEDIA_PAGEVIEWS_RAW`:

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
(This table is expected to be populated by an external ingestion process, like the Python script mentioned in the overall solution.)*

This dbt project will automatically create the `CLASSIFY_WIKIPEDIA_PAGE` UDF using Snowflake Cortex.


## Models

Here's a brief description of the key dbt models:

* **`stg_wikipedia_pageviews.sql`**:

  * **Purpose:** Acts as a staging layer for the raw `WIKIPEDIA_PAGEVIEWS_RAW` table.

  * **Transformation:** Performs basic cleaning, type casting, and selects necessary columns. It also extracts the device type (e.g., 'mobile', 'desktop') from the `PROJECT_CODE` as well as utilizes the Snowflake Cortex AI to categorize the page titles.

* **`dim_wikipedia_page.sql`**:

  * **Purpose:** Stores unique Wikipedia page view sources, page titles, and their AI-generated categories.

  * **Transformation:** Selects distinct `PAGEVIEW_SOURCE` and `PAGE_TITLE` from the staging layer and applies the `CLASSIFY_WIKIPEDIA_PAGE` Cortex UDF to categorize them.

  * **Dependencies:** `stg_wikipedia_pageviews`

* **`fct_wikipedia_pageviews.sql`**:

  * **Purpose:** Combines hourly pageview data with the AI-generated categories.

  * **Transformation:** Includes fact data as well as the surrogate key for `dim_wikipedia_page`.

  * **Dependencies:** `stg_wikipedia_pageviews`

* **`wikipedia_pageviews.sql`**:

  * **Purpose:** Provides an aggregated view of trending topics based on pageview counts per category.

  * **Transformation:** Aggregates `fct_wikipedia_pageviews` with the `dim_date`, `dim_hour`, and `dim_wikipedia_page` dimensions.

  * **Materialization:** `view`

  * **Dependencies:** `fct_hourly_pageviews_categorized`, `dim_date`, `dim_hour`, and `dim_wikipedia_page`

## Usage

1. **Ensure Prerequisites are Met:** Confirm Snowflake is set up, raw data is ingested, and `dbt-snowflake` is installed.

2. **Configure `.env`:** Populate your `.env` file with the correct Snowflake credentials.

3. **Test Connection:**

   ```bash
   dbt debug
   ```
4.  **Run the dbt Project:**
 This command will execute all models, including the `on-run-start` hook to create the Cortex UDF, and build your transformed tables/views.

    ```bash
    dbt run
    ```

5.  **Test your models (Optional but Recommended):**

    ```bash
    dbt test
    ```

After a successful `dbt run`, you can query the generated tables/views in your Snowflake database (e.g., `YOUR_DATABASE.YOUR_SCHEMA.WIKIPEDIA_PAGEVIEWS`) to explore the categorized and trending Wikipedia data.

## Clean Code Principles Applied (dbt)

This dbt project embodies several Clean Code principles:

* **Modularity:** Breaking down the transformation logic into small, focused models (`stg_`, `int_`, `dim_`, `fct_`, `analytics_`) makes the pipeline easier to understand, manage, and debug.

* **Single Responsibility:** Each model has a clear, defined purpose (e.g., `stg_` for raw data cleaning, `dim_` for dimensions, `fct_` for facts).

* **Readability:** SQL code is formatted, commented, and follows consistent naming conventions.

* **Idempotency:** The `CREATE OR REPLACE FUNCTION` in the `on-run-start` hook ensures that the UDF creation is idempotent, meaning it can be run multiple times without side effects. Incremental models also contribute to idempotency and efficiency.

* **Testability:** dbt's built-in testing framework allows for easy validation of data quality and transformation logic.

* **Configuration over Hardcoding:** Leveraging environment variables for sensitive credentials and dbt's `vars` for project-specific settings reduces hardcoding.

## Future Improvements

* **More Robust Error Handling:** Implement dbt-specific error handling or alerting for failed model runs.

* **Dynamic Category Management:** Explore ways to manage the list of categories for the Cortex UDF more dynamically (e.g., from a configuration table in Snowflake) rather than hardcoding them in the SQL.

* **Performance Optimization:**

    * Further optimize incremental model strategies.

    * Investigate Snowflake clustering keys for frequently queried tables.

* **Advanced Analytics:**

    * Implement more sophisticated trending algorithms (e.g., weighted averages, anomaly detection).

* **Data Quality Checks:** Add more comprehensive dbt tests for data integrity, uniqueness, and freshness.

* **CI/CD Integration:** Automate dbt runs and tests within a CI/CD pipeline.