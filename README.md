# Wikipedia Pageview Ingestion to Snowflake

This Python script facilitates the automated download of Wikipedia hourly pageview data, unzips it, and then ingests it into a Snowflake data warehouse. It is designed with Clean Code principles in mind, emphasizing modularity, clear responsibilities, and configuration through environment variables.

## Table of Contents

* [Features](#features)
* [Prerequisites](#prerequisites)
* [Setup](#setup)
    * [Environment Variables (`.env`)](#environment-variables-env)
* [Usage](#usage)
* [Clean Code Principles Applied](#clean-code-principles-applied)
* [Future Improvements](#future-improvements)
* [License](#license)

## Features

* **Automated Data Download:** Downloads hourly Wikipedia pageview `.gz` files for a specified date range.
* **Automatic Unzipping:** Unzips downloaded `.gz` files into `.txt` format.
* **Idempotent Operations:** Skips downloading/unzipping files that already exist locally.
* **Snowflake Integration:**
    * Connects to a Snowflake instance.
    * Ensures the existence of a Snowflake internal stage and a target raw table.
    * Uploads local `.txt` data files to the Snowflake stage.
    * Copies data from the stage into the Snowflake table.
* **Robust Error Handling:** Logs errors gracefully and continues processing where possible.
* **Environment-Driven Configuration:** All sensitive and environment-specific parameters are managed via environment variables (with `.env` support for local development).

## Prerequisites

Before running the script, ensure you have the following installed:

* **Python 3.8+**
* **`pip`** (Python package installer)

You will also need:

* **Snowflake Account:** Access to a Snowflake account with necessary permissions to create stages, tables, and load data.
* **Wikipedia Pageview Data:** The script fetches this automatically, but internet access is required.

## Setup

1.  **Clone the repository (or save the script):**
    Save the provided Python code as `app.py` (or any other `.py` filename).

2.  **Install Python Dependencies:**
    Navigate to the directory containing `app.py` in your terminal and run:

    ```bash
    pip install requests snowflake-connector-python python-dotenv
    ```

3.  **Create a `.env` file:**
    In the same directory as `app.py`, create a file named `.env`. This file will store your configuration, especially sensitive Snowflake credentials. **Do NOT commit this file to version control (e.g., Git).**

### Environment Variables (`.env`)

The `.env` file is crucial for configuring the script. Populate it with your specific details.

```ini
# .env file for local development configuration

# --- General Configuration ---
# Optional: Local directory to store downloaded pageview files
# If not set, defaults to "wikipedia_pageviews"
# LOCAL_DATA_DIR=my_pageviews_data

# --- Date Range for Data Download ---
# Required: Start and End dates for data ingestion
# Format: YYYY-MM-DD HH:MM:SS
START_DATE="2024-01-01 00:00:00"
END_DATE="2024-01-01 02:00:00"

# --- Snowflake Connection Details ---
# Required: Your Snowflake account identifier (e.g., 'your_org-your_account')
SNOWFLAKE_ACCOUNT="YOUR_SNOWFLAKE_ACCOUNT_IDENTIFIER"

# Required: Your Snowflake user name
SNOWFLAKE_USER="YOUR_SNOWFLAKE_USERNAME"

# Required: Your Snowflake password
# WARNING: Storing sensitive credentials directly in .env is for local dev ONLY.
# For production, use secure secret management services (e.g., AWS Secrets Manager, Azure Key Vault).
SNOWFLAKE_PASSWORD="YOUR_SNOWFLAKE_PASSWORD"

# Required: Your default Snowflake warehouse
SNOWFLAKE_WAREHOUSE="YOUR_SNOWFLAKE_WAREHOUSE"

# Required: Your default Snowflake database
SNOWFLAKE_DATABASE="YOUR_SNOWFLAKE_DATABASE"

# Required: Your default Snowflake schema
SNOWFLAKE_SCHEMA="YOUR_SNOWFLAKE_SCHEMA"

# --- Snowflake Object Names (Optional, defaults provided in code) ---
# If not set, defaults to 'WIKIPEDIA_PAGEVIEWS_STAGE'
# SNOWFLAKE_STAGE_NAME=MY_CUSTOM_STAGE

# If not set, defaults to 'WIKIPEDIA_PAGEVIEWS_RAW'
# SNOWFLAKE_TABLE_NAME=MY_CUSTOM_TABLE
```

**Important Security Note:** For production environments, it is strongly recommended to use a dedicated secrets management service (e.g., AWS Secrets Manager, Azure Key Vault, Google Secret Manager) instead of `.env` files for sensitive credentials.

## Usage

Once the setup is complete and your `.env` file is configured, simply run the script from your terminal:

```bash
python app.py
```

The script will:

1. Read the configuration from your environment variables (or `.env` file).

2. Download and unzip the specified Wikipedia pageview data files into the `wikipedia_pageviews` directory (or your `LOCAL_DATA_DIR`).

3. Connect to your Snowflake account.

4. Create the necessary stage and table in Snowflake if they don't already exist.

5. Upload the unzipped data files to the Snowflake stage.

6. Copy the data from the stage into the `WIKIPEDIA_PAGEVIEWS_RAW` table (or your `SNOWFLAKE_TABLE_NAME`).

7. Log its progress and any errors encountered.

## Clean Code Principles Applied

This refactored codebase strives to adhere to the following Clean Code principles:

* **Single Responsibility Principle (SRP):**

  * `Config` class centralizes all configuration.

  * `WikipediaDownloader` class is solely responsible for downloading and unzipping data.

  * `SnowflakeLoader` class manages all interactions with Snowflake (connection, setup, data loading).

  * The main `run_ingestion_workflow` function orchestrates the high-level flow.

* **Meaningful Names:** Variables, functions, and classes are named to clearly convey their purpose (e.g., `_get_file_metadata`, `_upload_file_to_stage`).

* **Small Functions:** Complex logic is broken down into smaller, focused methods, each doing "one thing well." This improves readability and testability.

* **Error Handling:** Explicit `try-except` blocks are used, and errors are logged informatively, allowing the application to fail gracefully or continue where appropriate.

* **Configuration over Hardcoding:** All dynamic parameters are externalized to environment variables, making the code more flexible and portable across different environments.

* **Reduced Global State:** Dependencies are passed explicitly to classes and methods, reducing reliance on global variables.

* **Self-Documenting Code:** The clarity of the code structure and naming reduces the need for excessive comments. Docstrings explain the purpose of classes and public methods.

## Future Improvements

* **Logging Enhancements:** Implement more granular logging levels or structured logging for easier analysis.

* **Configuration Validation:** Add more robust validation for all environment variables (e.g., type checking for numbers, stricter date format validation).

* **Error Reporting:** Integrate with an error reporting service (e.g., Sentry, Rollbar) for production environments.

* **Parallel Processing:** For larger date ranges, consider using multiprocessing or threading to download and process files concurrently.

* **Data Quality Checks:** Implement checks for data integrity before loading into Snowflake.

* **Monitoring:** Add metrics collection for download progress, load times, and error rates.

* **Command-Line Interface (CLI):** Use a library like `argparse` or `Click` to allow overriding environment variables via command-line arguments for more flexibility.

* **Cloud Storage Integration:** Instead of local files, consider downloading directly to cloud storage (e.g., S3, GCS) if Snowflake is configured to read from there.

