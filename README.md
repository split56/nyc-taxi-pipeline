# NYC Taxi Analytics Pipeline

End-to-end data engineering pipeline processing NYC Yellow Taxi trip data using distributed compute, incremental loading, data quality validation, and CI/CD.

![dbt CI](https://github.com/split56/nyc-taxi-pipeline/actions/workflows/dbt_ci.yml/badge.svg)

## Tech Stack

| Layer | Tool |
|---|---|
| Distributed processing | PySpark |
| Storage | Parquet (partitioned by year/month) |
| Data quality | Great Expectations |
| Transformation | dbt + dbt-expectations |
| Warehouse | DuckDB |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |

## Architecture

```
NYC TLC API
    │
    ▼
Incremental Download (watermark-based)
    │
    ▼
Great Expectations (validate raw data)
    │
    ▼
PySpark Bronze → Silver → Gold  (Parquet, partitioned by year/month)
                                │
                                ▼
                            DuckDB
                                │
                                ▼
                    dbt (staging → intermediate → mart)
                                │
                                ▼
                        Streamlit Dashboard
```

## Key Concepts Demonstrated

- **Incremental loading** — watermark tracks last processed month; only new data is processed on each run
- **Distributed compute** — PySpark with partition pruning, window functions, and aggregations at scale
- **Medallion architecture** — bronze (raw), silver (clean + enriched), gold (fact/dimension tables)
- **Data quality** — Great Expectations validates schema, ranges, and completeness before processing
- **dbt-expectations** — column-level assertions on transformed data
- **CI/CD** — GitHub Actions runs `dbt run` + `dbt test` on every push to main

## Project Structure

```
nyc-taxi-pipeline/
├── .github/workflows/dbt_ci.yml   # CI/CD pipeline
├── ingestion/
│   ├── download_data.py           # Downloads NYC TLC Parquet files
│   ├── watermark.py               # Tracks last processed month
│   └── load_to_duckdb.py          # Loads gold Parquet into DuckDB
├── expectations/
│   └── validate_bronze.py         # Great Expectations validation suite
├── spark_jobs/
│   ├── bronze.py                  # Raw Parquet → partitioned bronze
│   ├── silver.py                  # Clean, enrich, window functions (incremental)
│   └── gold.py                    # Fact/dimension tables
├── dbt_project/
│   ├── models/
│   │   ├── staging/               # Type casting, renaming
│   │   ├── intermediate/          # Hourly aggregations
│   │   └── mart/                  # fct_daily_summary, fct_peak_hours
│   └── packages.yml               # dbt-expectations
├── dashboard/app.py               # Streamlit dashboard
└── requirements.txt
```

## How to Run

**1. Install dependencies**
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**2. Download data** (starts from Jan 2023, incremental)
```bash
python ingestion/download_data.py
```

**3. Validate raw data**
```bash
python expectations/validate_bronze.py
```

**4. Run PySpark pipeline**
```bash
python spark_jobs/bronze.py
python spark_jobs/silver.py
python spark_jobs/gold.py
```

**5. Load into DuckDB**
```bash
python ingestion/load_to_duckdb.py
```

**6. Run dbt**
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

**7. Launch dashboard**
```bash
streamlit run dashboard/app.py
```

## Incremental Loading

The watermark file (`watermarks.json`) tracks the last processed month. Re-running `silver.py` only processes new months:

```bash
# First run: processes Jan, Feb, Mar 2023
python spark_jobs/silver.py

# Download April, then re-run — only April is processed
python spark_jobs/silver.py
```

## Data Source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — public dataset, no authentication required.
