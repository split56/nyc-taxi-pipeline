"""
Silver Layer: Incremental processing using watermark
only processes new months, skips already-processed data
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / "ingestion"))
from watermark import get_last_processed, update_watermark, get_months_to_process

PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")

spark = SparkSession.builder \
    .appName("NYC Taxi - Silver Layer (Incremental)") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def get_new_months():
    """Determine which months need processing based on watermark"""
    bronze_path = Path(BRONZE_DIR)
    years = [int(p.name.split("=")[1]) for p in bronze_path.glob("pickup_year=*")]
    if not years:
        print("No new months")
        return []
    max_year = max(years)
    months = [int(p.name.split("=")[1])
              for p in (bronze_path / f"pickup_year={max_year}").glob("pickup_month=*")]
    max_month = max(months)
    new_months = get_months_to_process(max_year, max_month)
    return new_months

def process_month(year: int, month: int):
    """process one month of data from bronze to silver
    """
    print(f"\nProcessing {year}-{month:02d}...")
    df = spark.read.parquet(BRONZE_DIR).filter(
        (F.col("pickup_year") == year) & (F.col("pickup_month") == month)
    )
    row_count = df.count()
    print(f"  Raw rows: {row_count:,}")

    # Cleaning
    df_clean = df \
        .filter(F.col("fare_amount") > 0) \
        .filter(F.col("trip_distance") > 0) \
        .filter(F.col("passenger_count").between(1, 9)) \
        .filter(F.col("tpep_pickup_datetime").isNotNull()) \
        .filter(F.col("tpep_dropoff_datetime").isNotNull())

    # Enrichment
    df_enriched = df_clean \
        .withColumn("trip_duration_minutes",
                    (F.unix_timestamp("tpep_dropoff_datetime") -
                     F.unix_timestamp("tpep_pickup_datetime")) / 60) \
        .withColumn("speed_mph",
                    F.when(
                        F.col("trip_duration_minutes") > 0,
                        F.col("trip_distance") /
                        (F.col("trip_duration_minutes") / 60)
                    ).otherwise(None)) \
        .withColumn("total_amount_per_mile",
                    F.when(
                        F.col("trip_distance") > 0,
                        F.col("total_amount") / F.col("trip_distance")
                    ).otherwise(None)) \
        .withColumn("pickup_hour",
                    F.hour("tpep_pickup_datetime")) \
        .withColumn("pickup_day_of_week",
                    F.dayofweek("tpep_pickup_datetime")) \
        .withColumn("is_weekend",
                    F.col("pickup_day_of_week").isin([1, 7])) \
        .filter(
        (F.col("trip_duration_minutes") > 1) &
        (F.col("trip_duration_minutes") < 300)
    )

    # Window functions
    # Hourly demand window
    hourly_window = Window.partitionBy("pickup_hour")

    df_final = df_enriched \
        .withColumn("avg_fare_this_hour",
                    F.avg("fare_amount").over(hourly_window)) \
        .withColumn("trip_count_this_hour",
                    F.count("*").over(hourly_window))

    clean_count = df_final.count()
    print(f"  Clean rows: {clean_count:,} "
          f"({100 * clean_count / row_count:.1f}% retained)")

    # Write this month's silver data
    df_final.write \
        .mode("overwrite") \
        .partitionBy("pickup_year", "pickup_month") \
        .parquet(SILVER_DIR)

    return clean_count

def run_incremental_silver():
    """Main entry point — only process new months."""
    new_months = get_new_months()

    if not new_months:
        print("Watermark is up to date. No new months to process.")
        return

    print(f"New months to process: {[f'{y}-{m:02d}' for y, m in new_months]}")

    for year, month in new_months:
        rows = process_month(year, month)
        if rows > 0:
            # Update watermark AFTER successful processing
            update_watermark(year, month)
            print(f"  ✓ Watermark updated to {year}-{month:02d}")


if __name__ == "__main__":
    run_incremental_silver()
    spark.stop()
