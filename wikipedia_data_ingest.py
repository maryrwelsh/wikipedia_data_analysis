"""Ingests Wikipedia page view .csv files for the given time period and loads the data into Snowflake"""

import os
import gzip
from datetime import datetime, timedelta
import logging
import getpass
import requests
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

LOCAL_DATA_DIR = "wikipedia_pageviews"
BASE_URL = "https://dumps.wikimedia.org/other/pageviews"

# --- Data Loading Configuration ---
SNOWFLAKE_STAGE_NAME = 'WIKIPEDIA_PAGEVIEWS_STAGE' # Name for the internal stage in Snowflake
SNOWFLAKE_TABLE_NAME = 'WIKIPEDIA_PAGEVIEWS_RAW' # Name for the target table in Snowflake

def download_wikipedia_pageview_data(start_date, end_date, output_dir="wikipedia_pageviews"):
    """
    Downloads Wikipedia pageview data for a specified date range.

    Args:
        start_date (datetime): The start date in 'YYYY-MM-DD HH:MM:SS' format.
        end_date (datetime): The end date in 'YYYY-MM-DD HH:MM:SS' format.
        output_dir (str): The directory to save the downloaded files.
    """

    date_format = "%Y-%m-%d %H:%M:%S"

    # Convert the string to a datetime object
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    current_date = start_date
    end_dt = end_date
    # Include any extra time between the dates, hourly
    if (end_dt - current_date).total_seconds()/60 > 0:
        end_dt += timedelta(hours=1)

    log.info(f"Starting download from {start_date} to {end_date}...")

    while current_date <= end_dt:
        year = current_date.year
        month = current_date.strftime('%m')
        dt = current_date.strftime('%Y%m%d')
        hour = current_date.strftime('%H')

        # Wikipedia pageview data is hourly.
        file_name = f"pageviews-{dt}-{hour}0000.gz"
        url = f"{BASE_URL}/{year}/{year}-{month}/{file_name}"
        output_path_gz = os.path.join(output_dir, file_name)
        output_path_txt = os.path.join(output_dir, file_name.replace('.gz', '.txt'))
        if os.path.exists(output_path_txt):
            log.info(f"Skipping {file_name}, already downloaded and unzipped.")
            current_date += timedelta(hours=1)
            continue
        elif os.path.exists(output_path_gz):
            log.info(f"Skipping {file_name}, already downloaded.")
            current_date += timedelta(hours=1)
            continue
        else:
            try:
                log.info(f"Downloading {file_name}...")
                response = requests.get(url, stream=True)
                response.raise_for_status() # Raise an exception for HTTP errors
                with open(output_path_gz, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                log.info(f"Successfully downloaded {file_name}")
            except requests.exceptions.RequestException as e:
                log.info(f"Error downloading {file_name}: {e}")
                # You might want to log this error and continue or break
                continue # Continue to next hour/day if one file fails
        # Unzip the file
        try:
            log.info(f"Unzipping {file_name}...")
            with gzip.open(output_path_gz, 'rb') as f_in:
                with open(output_path_txt, 'wb') as f_out:
                    f_out.write(f_in.read())
            log.info(f"Successfully unzipped {file_name} to {file_name.replace('.gz', '.txt')}")
            # Optionally remove the .gz file after unzipping to save space
            # os.remove(output_path_gz)
        except Exception as e:
            log.info(f"Error unzipping {file_name}: {e}")
        current_date += timedelta(hours=1)

    log.info("Download process completed.")

def get_snowflake_connection(pw):
    """
    Establishes and returns a connection to Snowflake.
    Args:
        pw (str): The password given after the prompt.
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=pw,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        log.info("Successfully connected to Snowflake!")
        return conn
    except Exception as e:
        log.info(f"Error connecting to Snowflake: {e}")
        return None

def setup_snowflake_objects(conn):
    """
    Creates the necessary stage and table in Snowflake if they don't exist.
    """
    cur = conn.cursor()
    try:
        # Create Stage
        log.info(f"Ensuring stage '{SNOWFLAKE_STAGE_NAME}' exists...")
        cur.execute(f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE_NAME}")
        log.info(f"Stage '{SNOWFLAKE_STAGE_NAME}' is ready.")

        # Create Table
        log.info(f"Ensuring table '{SNOWFLAKE_TABLE_NAME}' exists...")
        # Schema matches: project_code page_title view_count byte_size
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE_NAME} (
                PROJECT_CODE VARCHAR,
                PAGE_TITLE VARCHAR,
                VIEW_COUNT NUMBER,
                BYTE_SIZE NUMBER,
                FILE_NAME VARCHAR, -- To track which file the data came from
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        log.info(f"Table '{SNOWFLAKE_TABLE_NAME}' is ready.")
    except Exception as e:
        log.info(f"Error setting up Snowflake objects: {e}")
        raise # Re-raise to stop execution if setup fails
    finally:
        cur.close()

def load_data_to_snowflake(conn, local_data_dir, stage_name, table_name):
    """
    Uploads .txt files to a Snowflake stage and loads them into a table.
    """
    cur = conn.cursor()

    # Get list of .txt files to process
    txt_files = [f for f in os.listdir(local_data_dir) if f.endswith('.txt')]
    if not txt_files:
        log.info(f"No .txt files found in '{local_data_dir}'. Please ensure data is downloaded and unzipped.")
        return

    log.info(f"Found {len(txt_files)} .txt files to process.")

    for file_name in sorted(txt_files): # Sort to process in a consistent order
        local_file_path = os.path.join(local_data_dir, file_name)

        # 1. Upload file to Snowflake internal stage
        log.info(f"\nUploading '{file_name}' to stage '{stage_name}'...")
        try:
            # PUT command to upload file from local to stage
            cur.execute(f"PUT file://{local_file_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            log.info(f"Successfully uploaded '{file_name}'.")
        except Exception as e:
            log.error(f"Error uploading '{file_name}' to stage: {e}")
            continue # Skip to the next file if upload fails

        # 2. Copy data from stage into the table
        log.info(f"Copying data from '{file_name}' (in stage) into table '{table_name}'...")
        try:
            # COPY INTO command
            # FILE_FORMAT: Defines how Snowflake should interpret the file.
            #   TYPE = CSV (even for TSV, as CSV can handle custom delimiters)
            #   FIELD_DELIMITER = '\t' (tab-separated)
            #   SKIP_HEADER = 0 (no header row)
            #   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE (more robust if some lines are malformed)
            #   ON_ERROR = 'CONTINUE' (continue loading even if some rows fail, log errors)
            #   TRUNCATECOLUMNS = TRUE (truncate strings that are too long for VARCHAR columns)
            #   VALIDATION_MODE = RETURN_ERRORS (for debugging if issues arise)
            cur.execute(f"""
                COPY INTO {table_name} (PROJECT_CODE, PAGE_TITLE, VIEW_COUNT, BYTE_SIZE, FILE_NAME)
                FROM (SELECT $1, $2, $3, $4, '{file_name}' FROM @{stage_name}/{file_name})
                FILE_FORMAT = (
                    TYPE = CSV
                    FIELD_DELIMITER = ' '
                    SKIP_HEADER = 0
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                    NULL_IF = ('')
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                ON_ERROR = 'CONTINUE'
            """)
            log.info(f"Successfully copied data from '{file_name}' to '{table_name}'.")

            # Optional: Remove the file from the stage after successful load
            # print(f"Removing '{file_name}' from stage '{stage_name}'...")
            # cur.execute(f"REMOVE @{stage_name}/{file_name}")
            # print(f"Removed '{file_name}' from stage.")

        except Exception as e:
            log.error(f"Error copying data from '{file_name}' to table: {e}")
            continue # Skip to the next file if copy fails

    log.info("\nData loading process completed.")
    cur.close()

if __name__ == "__main__":
    START_DATE = input("Enter start date (datetime): ") # "2025-05-01 10:30:00"
    END_DATE = input("Enter end date (datetime): ") # "2025-05-01 11:31:00"
    download_wikipedia_pageview_data(START_DATE, END_DATE)
    SNOWFLAKE_ACCOUNT =  input("Enter Snowflake account: ") # 'NQKHNNX-NVB21540'
    SNOWFLAKE_USER = input("Enter Snowflake user: ") # 'MARYRWELSH'
    conn = get_snowflake_connection(getpass.getpass('Enter Snowflake password: '))
    SNOWFLAKE_WAREHOUSE = input("Enter Snowflake warehouse: ") # 'SNOWFLAKE_LEARNING_WH'
    SNOWFLAKE_DATABASE = input("Enter Snowflake database: ") # 'SNOWFLAKE_LEARNING_DB'
    SNOWFLAKE_SCHEMA = input("Enter Snowflake schema: ") # 'PUBLIC'

    setup_snowflake_objects(conn)
    load_data_to_snowflake(conn, LOCAL_DATA_DIR, SNOWFLAKE_STAGE_NAME, SNOWFLAKE_TABLE_NAME)
