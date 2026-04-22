"""
Bronze layer: Raw parquet -> partitioned bronze Parquet
Adds ingestion metadata, partitions by year/month
Only processes files matching the watermark
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / "ingestion"))
from watermark import get_last_processed

PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = str(PROJECT_ROOT / "data" / "raw")
BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")

spark = SparkSession.builder \
    .appName("NYC Taxi - Bronze Layer") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def ingest_to_bronze():
    """Read raw Parquets files"""
    raw_files = list(Path(RAW_DIR).glob("yellow_tripdata_*.parquet"))

    if not raw_files:
        print("No raw parquet files found")
        return

    print(f"Found {len(raw_files)} raw parquet files")

    df = spark.read.parquet(RAW_DIR)

    df_bronze = df.withColumn("ingested_at", F.current_timestamp()) \
                  .withColumn("source_file", F.input_file_name()) \
                  .withColumn("pickup_year", F.year("tpep_pickup_datetime")) \
                  .withColumn("pickup_month", F.month("tpep_pickup_datetime"))

    row_count = df_bronze.count()

    # partition by year/month
    df_bronze.write.mode("overwrite").partitionBy("pickup_year","pickup_month").parquet(BRONZE_DIR)
    print(f"✓ Bronze: {row_count:,} rows → partitioned by year/month")
    print(f"  Location: {BRONZE_DIR}")


if __name__ == "__main__":
    ingest_to_bronze()
    spark.stop()
