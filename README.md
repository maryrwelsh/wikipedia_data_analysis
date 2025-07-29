# Wikipedia Data Analysis Pipeline

A comprehensive data pipeline for analyzing Wikipedia pageview data using modern data engineering practices. This project combines data ingestion, transformation, and orchestration to provide insights into trending topics and content categorization.

## 🏗️ Architecture Overview

The pipeline consists of three main components:

1. **Data Ingestion Layer (Python)**: Downloads and processes raw Wikipedia pageview data
2. **Transformation Layer (dbt)**: Transforms raw data into analytics-ready models with AI-powered categorization
3. **Orchestration Layer (Dagster)**: Coordinates the entire pipeline with scheduling and monitoring

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Wikimedia Dumps │───▶│ Data Ingestion  │───▶│ Snowflake Raw   │───▶│ dbt Transform   │
│ (.gz files)     │    │ (Python Script) │    │ Table           │    │ (Models, UDFs)  │
│                 │    │ (Parallel DL)   │    │ (RAW_WIKIPEDIA_ │    │                 │
└─────────────────┘    └─────────────────┘    │ PAGEVIEWS)      │    └─────────────────┘
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
                                              │ (dim_*, fct_*)  │
                                              └─────────────────┘
```

## ✨ Key Features

- **🔄 Automated Hourly Processing**: Intelligent scheduling that automatically processes the latest Wikipedia pageview data every hour
- **🤖 AI-Powered Categorization**: Uses Snowflake Cortex to automatically categorize Wikipedia pages into meaningful topics
- **📊 Modern Data Stack**: Built with dbt, Dagster, and Snowflake for scalable data engineering
- **⏱️ Real-time Monitoring**: Comprehensive logging and monitoring through Dagster's web interface
- **🔧 Flexible Configuration**: Easy configuration through environment variables and parameterized functions
- **📈 Analytics-Ready Models**: Produces clean, documented data models ready for analysis and visualization
- **🚀 Parallel Processing**: Concurrent data downloads and processing for optimal performance
- **✅ Data Quality**: Built-in testing and validation with dbt's testing framework

## 📁 Project Structure

```
wikipedia_data_analysis/
├── orchestration/              # Dagster orchestration
│   └── dagster/               # Dagster project files
│       ├── assets.py          # Asset definitions
│       ├── definitions.py     # Dagster definitions
│       ├── project.py         # dbt project configuration
│       ├── schedules.py       # Schedule definitions
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
│   │   └── analytics/         # Analytics models
│   ├── macros/                # dbt macros
│   ├── tests/                 # dbt tests
│   ├── dbt_project.yml        # dbt project config
│   └── profiles.yml           # dbt profiles
├── README.md                  # This file
├── .env_example               # Environment variables template
└── logs/                      # Application logs
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.12+**
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
SNOWFLAKE_SCHEMA="your_schema"

# Data Ingestion Settings
START_DATE="2025-01-01 00:00:00"
END_DATE="2025-01-01 02:00:00"
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
# Start Dagster development server
source .venv/bin/activate
dagster dev -m orchestration.dagster.definitions
```

Then open http://localhost:3000 to:
- View and trigger pipeline runs manually
- Monitor automatic hourly execution
- Access real-time logs and metrics
- Manage schedules and assets

**🕐 Automatic Hourly Processing**: The pipeline is configured to automatically run every hour at 15 minutes past the hour (e.g., 1:15, 2:15, 3:15) to process the previous hour's Wikipedia pageview data. This ensures your data is always up-to-date with the latest trending topics.

## 📊 Data Models

### Raw Data Schema

The ingestion process creates a raw table with the following structure:

```sql
CREATE TABLE RAW_WIKIPEDIA_PAGEVIEWS (
    PROJECT_CODE VARCHAR,      -- e.g., 'en.wikipedia'
    PAGE_TITLE VARCHAR,        -- Wikipedia page title
    VIEW_COUNT NUMBER,         -- Hourly view count
    BYTE_SIZE NUMBER,          -- Page size in bytes
    FILE_NAME VARCHAR,         -- Source file name
    LOAD_TIMESTAMP TIMESTAMP_NTZ -- When data was loaded
);
```

### Transformed Models

The dbt project creates several analytical models:

- **`stg_wikipedia_pageviews`**: Cleaned staging data with AI categorization
- **`dim_wikipedia_page`**: Dimension table with page metadata and categories
- **`dim_date`**: Date dimension for time-based analysis
- **`dim_hour`**: Hour dimension for hourly analysis
- **`fct_wikipedia_pageviews`**: Fact table combining all dimensions
- **`wikipedia_pageviews`**: Analytics-ready view for trending analysis

## 🤖 AI-Powered Categorization

The pipeline uses Snowflake Cortex to automatically categorize Wikipedia pages into topics:

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

## 🔧 Configuration

### dbt Configuration

The dbt project is configured in `dbt/dbt_project.yml`:

```yaml
name: 'wikipedia_analytics'
version: '1.0.0'
config-version: 2

profile: 'snowflake_profile'

models:
  wikipedia_analytics:
    staging:
      +materialized: table
    marts:
      +materialized: table
    analytics:
      +materialized: view
```

### Dagster Configuration

The orchestration is configured in `orchestration/dagster/`:

- **Assets**: Define data pipeline components
- **Schedules**: Configure automated execution
- **Resources**: Manage external connections

## 📈 Usage Examples

### Query Trending Topics

```sql
-- Get top trending categories for a specific date
SELECT 
    page_category,
    SUM(view_count) as total_views,
    COUNT(DISTINCT page_title) as unique_pages
FROM wikipedia_pageviews
WHERE pageview_date = '2025-01-01'
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
FROM fct_wikipedia_pageviews f
JOIN dim_wikipedia_page d ON f.page_key = d.page_key
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

The project includes comprehensive data quality tests:
- Uniqueness constraints
- Not null checks
- Referential integrity
- Custom business logic tests

## 🔄 Scheduling

The pipeline can be scheduled using Dagster:

- **Daily Refresh**: Automatically runs at midnight
- **Manual Triggers**: On-demand execution via UI
- **Conditional Execution**: Based on data availability

## 🛠️ Development

### Adding New Models

1. Create new model in `dbt/models/`
2. Add tests in `dbt/tests/`
3. Update documentation in model YAML files
4. Run `dbt run --select model_name` to test

### Extending the Pipeline

1. **New Data Sources**: Add ingestion scripts in `data_ingest/`
2. **New Transformations**: Create dbt models in `dbt/models/`
3. **New Assets**: Define in `orchestration/dagster/assets.py`

## ⏰ Automated Scheduling

The pipeline includes an intelligent hourly schedule that automatically processes the latest Wikipedia pageview data:

### Schedule Configuration

- **Frequency**: Every hour at 15 minutes past the hour (`:15`)
- **Cron Schedule**: `15 * * * *`
- **Target Data**: Previous hour's Wikipedia pageview data
- **Data Availability Check**: Waits for Wikipedia to publish data (~10 minutes after each hour)

### How It Works

1. **Smart Timing**: Runs 15 minutes after each hour to ensure data availability
2. **Dynamic Date Calculation**: Automatically determines the correct hour to process
3. **Skip Logic**: Intelligently skips runs if data isn't available yet
4. **Unique Run Keys**: Prevents duplicate processing with timestamped run identifiers
5. **Full Pipeline**: Runs both data ingestion and dbt transformations

### Example Schedule

```
12:15 PM → Process 11:00-12:00 data
1:15 PM  → Process 12:00-1:00 data  
2:15 PM  → Process 1:00-2:00 data
...and so on every hour
```

### Managing Schedules

In the Dagster UI (http://localhost:3000):

1. **View Schedules**: Navigate to "Schedules" tab
2. **Enable/Disable**: Toggle the `hourly_wikipedia_ingestion` schedule
3. **Monitor Runs**: View scheduled run history and status
4. **Manual Triggers**: Run the pipeline manually for specific time ranges

### Manual vs Scheduled Execution

The pipeline automatically detects execution context and chooses appropriate date ranges:

- **📅 Scheduled Runs**: Process the **previous complete hour** (e.g., at 2:15 PM, processes 1:00-2:00 PM data)
- **🖱️ Manual Runs**: Process the **current hour** (e.g., at 2:30 PM, processes 2:00-3:00 PM data, even if incomplete)

### Custom Date Ranges

For specific historical data or custom ranges, provide explicit configuration:

```python
# In Dagster UI, use the "Materialize" button with custom config:
{
  "ops": {
    "raw_wikipedia_pageviews": {
      "config": {
        "start_date": "2025-01-01 10:00:00",
        "end_date": "2025-01-01 12:00:00"
      }
    }
  }
}
```

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

### Logs

- **Application Logs**: `logs/`
- **dbt Logs**: `dbt/logs/`
- **Dagster Logs**: Available in Dagster UI

## 📚 Documentation

- **Data Ingestion**: See `data_ingest/README.md`
- **dbt Models**: See `dbt/README.md`
- **Orchestration**: See `orchestration/dagster/`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Wikimedia Foundation for providing pageview data
- Snowflake for the data platform and Cortex AI capabilities
- dbt for the transformation framework
- Dagster for the orchestration platform

---

**Note**: This pipeline is designed for educational and research purposes. Please ensure compliance with Wikimedia's terms of service and data usage policies.





