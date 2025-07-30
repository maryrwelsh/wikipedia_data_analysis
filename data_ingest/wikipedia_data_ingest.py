import os
import gzip
import logging
import getpass
from datetime import datetime, timedelta
import requests
import snowflake.connector
from dotenv import load_dotenv
import concurrent.futures

# Load environment variables from .env file at the very beginning
load_dotenv()

# --- Configuration ---
class Config:
    """Configuration settings for the Wikipedia Pageview Ingestion."""
    # Local paths and static configurations
    LOCAL_DATA_DIR = os.getenv('LOCAL_DATA_DIR', "wikipedia_pageviews_files") # Can be overridden by env var
    BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    CHUNK_SIZE = 8192
    MAX_DOWNLOAD_WORKERS = int(os.getenv('MAX_DOWNLOAD_WORKERS', 5))

    # Snowflake Configuration (primarily from environment variables)
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'WIKIPEDIA')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
    SNOWFLAKE_STAGE_NAME = os.getenv('SNOWFLAKE_STAGE_NAME', 'WIKIPEDIA_PAGEVIEWS_STAGE') # Default if not set
    SNOWFLAKE_TABLE_NAME = os.getenv('SNOWFLAKE_TABLE_NAME', 'RAW_WIKIPEDIA_PAGEVIEWS') # Default if not set

# --- Logging Setup ---
def setup_logging():
    """Configures the logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

log = setup_logging()

# --- Data Download Module ---
class WikipediaDownloader:
    """Handles the downloading and unzipping of Wikipedia pageview data."""

    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir
        self._ensure_output_directory_exists()

    def _ensure_output_directory_exists(self):
        """Ensures the local output directory for downloads exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            log.info(f"Created output directory: {self.output_dir}")

    def _get_file_metadata(self, current_date):
        """Constructs and returns file metadata (URL, paths, names) for a given date and hour."""
        year = current_date.year
        month = current_date.strftime('%m')
        dt_str = current_date.strftime('%Y%m%d')
        hour_str = current_date.strftime('%H')

        file_name_gz = f"pageviews-{dt_str}-{hour_str}0000.gz"
        file_name_txt = file_name_gz.replace('.gz', '.txt')
        url = f"{self.base_url}/{year}/{year}-{month}/{file_name_gz}"
        output_path_gz = os.path.join(self.output_dir, file_name_gz)
        output_path_txt = os.path.join(self.output_dir, file_name_txt)
        return url, output_path_gz, output_path_txt, file_name_gz, file_name_txt

    def _is_already_processed(self, output_path_txt, file_name_txt):
        """Checks if the unzipped file already exists."""
        if os.path.exists(output_path_txt):
            log.info(f"Skipping {file_name_txt}, already downloaded and unzipped.")
            return True
        return False

    def _is_gz_downloaded(self, output_path_gz, file_name_gz):
        """Checks if the gzipped file already exists."""
        if os.path.exists(output_path_gz):
            log.info(f"Skipping download of {file_name_gz}, already downloaded. Proceeding to unzip.")
            return True
        return False

    def _download_file(self, url, output_path_gz, file_name_gz):
        """Attempts to download a single gzipped file."""
        log.info(f"Downloading {file_name_gz}...")
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status() # Raise an exception for HTTP errors
            with open(output_path_gz, 'wb') as f:
                for chunk in response.iter_content(chunk_size=Config.CHUNK_SIZE):
                    f.write(chunk)
            log.info(f"Successfully downloaded {file_name_gz}")
            return True
        except requests.exceptions.RequestException as e:
            log.error(f"Error downloading {file_name_gz}: {e}")
            return False

    def _unzip_file(self, output_path_gz, output_path_txt, file_name_gz, file_name_txt):
        """Attempts to unzip a single gzipped file."""
        log.info(f"Unzipping {file_name_gz}...")
        try:
            with gzip.open(output_path_gz, 'rb') as f_in:
                with open(output_path_txt, 'wb') as f_out:
                    f_out.write(f_in.read())
            log.info(f"Successfully unzipped {file_name_gz} to {file_name_txt}")
            # Optionally remove the .gz file after unzipping to save space
            # os.remove(output_path_gz)
            return True
        except Exception as e:
            log.error(f"Error unzipping {file_name_gz}: {e}")
            return False

    def process_hour_data(self, current_date):
        """Downloads and unzips an hourly Wikipedia pageview data file, if not already present."""
        url, output_path_gz, output_path_txt, file_name_gz, file_name_txt = \
            self._get_file_metadata(current_date)

        if self._is_already_processed(output_path_txt, file_name_txt):
            return True

        if not self._is_gz_downloaded(output_path_gz, file_name_gz):
            if not self._download_file(url, output_path_gz, file_name_gz):
                return False # Failed to download

        return self._unzip_file(output_path_gz, output_path_txt, file_name_gz, file_name_txt)

    def download_data_for_range(self, start_dt, end_dt):
        """
        Downloads Wikipedia pageview data for a specified date and time range.

        Args:
            start_dt (datetime): The start datetime.
            end_dt (datetime): The end datetime.
        """
        log.info(f"Starting parallel download from {start_dt} to {end_dt}...")

        # Adjust end_dt to include the full last hour for hourly processing
        # Include any extra time between the dates, hourly
        current_date = start_dt.replace(minute=0, second=0, microsecond=0)

        effective_end_dt = end_dt.replace(minute=0, second=0, microsecond=0)
        if end_dt.minute > 0 or end_dt.second > 0:
            effective_end_dt += timedelta(hours=1)

        # Generate a list of all hours to process
        hours_to_process = []
        while current_date <= effective_end_dt:
            hours_to_process.append(current_date)
            current_date += timedelta(hours=1)

        # Use ThreadPoolExecutor for parallel processing of download and unzip
        with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_DOWNLOAD_WORKERS) as executor:
            # Map the process_hour_data method to each hour
            # The results (True/False for success) are collected, though not explicitly used here beyond logging
            list(executor.map(self.process_hour_data, hours_to_process))

        log.info("Parallel download process completed.")

# --- Snowflake Module ---
class SnowflakeLoader:
    """Handles connection, setup, and data loading to Snowflake."""

    def __init__(self, account, user, password, warehouse, database, schema, role):
        # Validate that all required connection parameters are provided
        required_params = {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "role": role
        }
        # Role is optional, so we check it differently
        if not role:
            del required_params["role"]

        for param, value in required_params.items():
            if not value:
                raise ValueError(f"Missing required Snowflake connection parameter: {param}. "
                                 f"Please ensure '{param.upper()}' environment variable is set.")

        self.connection_params = {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "role": role
        }
        self.conn = None

    def connect(self):
        """Establishes a connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(**self.connection_params)
            log.info("Successfully connected to Snowflake!")
            return True
        except Exception as e:
            log.error(f"Error connecting to Snowflake: {e}")
            self.conn = None
            return False

    def close_connection(self):
        """Closes the Snowflake connection."""
        if self.conn:
            self.conn.close()
            log.info("Snowflake connection closed.")

    def _execute_sql(self, cursor, sql_command, error_msg):
        """Helper to execute a single SQL command with error handling."""
        try:
            cursor.execute(sql_command)
            return True
        except Exception as e:
            log.error(f"{error_msg}: {e}")
            return False

    def setup_snowflake_objects(self, stage_name, table_name):
        """
        Creates the necessary stage and table in Snowflake if they don't exist.
        """
        if not self.conn:
            log.error("No Snowflake connection available. Cannot set up objects.")
            return False

        cur = self.conn.cursor()
        try:
            # Force use of WIKIPEDIA schema (consistent across pipeline)
            schema = 'WIKIPEDIA'
            
            # First, ensure the schema exists
            log.info(f"Ensuring schema '{schema}' exists...")
            schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
            if not self._execute_sql(cur, schema_sql,
                                   f"Error creating schema '{schema}'"):
                return False
            log.info(f"Schema '{schema}' is ready.")
            
            log.info(f"Ensuring stage '{stage_name}' exists in schema '{schema}'...")
            stage_sql = f"CREATE STAGE IF NOT EXISTS {schema}.{stage_name}"
            if not self._execute_sql(cur, stage_sql,
                                     f"Error creating stage '{schema}.{stage_name}'"):
                return False
            log.info(f"Stage '{schema}.{stage_name}' is ready.")

            log.info(f"Ensuring table '{table_name}' exists in schema '{schema}'...")
            table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    PROJECT_CODE VARCHAR,
                    PAGE_TITLE VARCHAR,
                    VIEW_COUNT NUMBER,
                    BYTE_SIZE NUMBER,
                    FILE_NAME VARCHAR,
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """
            if not self._execute_sql(cur, table_sql, f"Error creating table '{schema}.{table_name}'"):
                return False
            log.info(f"Table '{schema}.{table_name}' is ready.")
            return True
        finally:
            cur.close()

    def _upload_file_to_stage(self, cursor, local_path, file_name, stage_name):
        """Uploads a single file to the Snowflake internal stage."""
        schema = self.connection_params.get('schema', 'WIKIPEDIA')
        full_stage_name = f"{schema}.{stage_name}"
        log.info(f"Uploading '{file_name}' to stage '{full_stage_name}'...")
        sql_command = f"PUT file://{local_path} @{full_stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        if self._execute_sql(cursor, sql_command, f"Error uploading '{file_name}' to stage"):
            log.info(f"Successfully uploaded '{file_name}'.")
            return True
        return False

    def _copy_data_into_table(self, cursor, file_name, stage_name, table_name):
        """Copies data from a staged file into the target table."""
        schema = self.connection_params.get('schema', 'WIKIPEDIA')
        full_table_name = f"{schema}.{table_name}"
        full_stage_name = f"{schema}.{stage_name}"
        log.info(f"Copying data from '{file_name}' (in stage) into table '{full_table_name}'...")
        copy_sql = f"""
            COPY INTO {full_table_name} (PROJECT_CODE, PAGE_TITLE, VIEW_COUNT, BYTE_SIZE, FILE_NAME)
            FROM (SELECT $1, $2, $3, $4, '{file_name}' FROM @{full_stage_name}/{file_name})
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = ' '
                SKIP_HEADER = 0
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                NULL_IF = ('')
                EMPTY_FIELD_AS_NULL = TRUE
            )
            ON_ERROR = 'CONTINUE'
        """
        if self._execute_sql(cursor, copy_sql, f"Error copying data from '{file_name}' to table"):
            log.info(f"Successfully copied data from '{file_name}' to '{full_table_name}'.")
            # Optional: Remove file from stage after successful load
            # self._execute_sql(cursor, f"REMOVE @{full_stage_name}/{file_name}", f"Error removing '{file_name}' from stage")
            return True
        return False

    def load_data_from_local_to_snowflake(self, local_data_dir, stage_name, table_name):
        """
        Uploads .txt files from a local directory to a Snowflake stage and loads them into a table.
        """
        if not self.conn:
            log.error("No Snowflake connection available. Cannot load data.")
            return

        txt_files = [f for f in os.listdir(local_data_dir) if f.endswith('.txt')]
        if not txt_files:
            log.info(f"No .txt files found in '{local_data_dir}'. Please ensure data is downloaded and unzipped.")
            return

        log.info(f"Found {len(txt_files)} .txt files to process for Snowflake loading.")
        cur = self.conn.cursor()
        try:
            for file_name in sorted(txt_files):
                local_file_path = os.path.join(local_data_dir, file_name)

                if not self._upload_file_to_stage(cur, local_file_path, file_name, stage_name):
                    continue # Skip to the next file if upload fails

                self._copy_data_into_table(cur, file_name, stage_name, table_name)
        finally:
            cur.close()
        log.info("\nData loading process completed.")


# --- Main Application Logic ---
def get_current_hour_datetime():
    """Returns the current datetime rounded down to the hour."""
    current_datetime = datetime.now()
    # Round down to the current hour (set minutes, seconds, microseconds to 0)
    current_hour = current_datetime.replace(minute=0, second=0, microsecond=0)
    return current_hour

def run_ingestion_workflow():
    """Orchestrates the entire data ingestion workflow for the current hour only."""

    # 1. Get the current hour to process
    current_hour = get_current_hour_datetime()
    log.info(f"Processing Wikipedia pageview data for current hour: {current_hour}")

    # Prepare Snowflake credentials from Config (which reads env vars)
    sf_creds = {
        "account": Config.SNOWFLAKE_ACCOUNT,
        "user": Config.SNOWFLAKE_USER,
        "password": Config.SNOWFLAKE_PASSWORD,
        "warehouse": Config.SNOWFLAKE_WAREHOUSE,
        "database": Config.SNOWFLAKE_DATABASE,
        "schema": Config.SNOWFLAKE_SCHEMA,
        "role": Config.SNOWFLAKE_ROLE
    }

    # 2. Download data for the current hour only
    downloader = WikipediaDownloader(Config.BASE_URL, Config.LOCAL_DATA_DIR)
    success = downloader.process_hour_data(current_hour)
    
    if not success:
        log.error(f"Failed to download/process data for {current_hour}. Aborting.")
        return

    # 3. Load data to Snowflake
    try:
        snowflake_loader = SnowflakeLoader(**sf_creds)
    except ValueError as e:
        log.error(f"Snowflake initialization error: {e}")
        return

    if not snowflake_loader.connect():
        log.error("Failed to connect to Snowflake. Aborting data load.")
        return

    try:
        if not snowflake_loader.setup_snowflake_objects(Config.SNOWFLAKE_STAGE_NAME, Config.SNOWFLAKE_TABLE_NAME):
            log.error("Failed to set up Snowflake objects. Aborting data load.")
            return

        snowflake_loader.load_data_from_local_to_snowflake(
            Config.LOCAL_DATA_DIR,
            Config.SNOWFLAKE_STAGE_NAME,
            Config.SNOWFLAKE_TABLE_NAME
        )
    finally:
        snowflake_loader.close_connection()

if __name__ == "__main__":
    run_ingestion_workflow()