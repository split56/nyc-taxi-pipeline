"""
Gold Layer: Fact and dimension tables for the warehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
GOLD_DIR = str(PROJECT_ROOT / "data" / "gold")

spark = SparkSession.builder \
    .appName("NYC Taxi - Gold Layer") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# NYC TLC zone lookup (embedded — avoids extra download)
PAYMENT_TYPES = {
    0: "Credit card",
    1: "Cash",
    2: "No charge",
    3: "Dispute",
    4: "Unknown",
    5: "Voided trip"
}


def build_fct_trips():
    """Main fact table — one row per trip."""
    print("Building fct_trips...")

    df = spark.read.parquet(SILVER_DIR)

    fct = df.select(
        F.monotonically_increasing_id().alias("trip_id"),
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_duration_minutes",
        "speed_mph",
        "total_amount_per_mile",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "pickup_year",
        "pickup_month",
    )

    fct.write \
        .mode("overwrite") \
        .partitionBy("pickup_year", "pickup_month") \
        .parquet(f"{GOLD_DIR}/fct_trips")

    print(f"  ✓ fct_trips: {fct.count():,} rows")


def build_dim_time():
    """Time dimension — hour of day analytics."""
    print("Building dim_time...")

    time_data = [(h,
                  "Morning" if 6 <= h < 12
                  else "Afternoon" if 12 <= h < 18
                  else "Evening" if 18 <= h < 22
                  else "Night",
                  h in [7, 8, 9, 17, 18, 19])
                 for h in range(24)]

    dim = spark.createDataFrame(
        time_data,
        ["hour", "time_of_day", "is_rush_hour"]
    )

    dim.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_time")
    print(f"  ✓ dim_time: {dim.count()} rows")


def build_dim_payment():
    """Payment type dimension."""
    print("Building dim_payment...")

    payment_data = list(PAYMENT_TYPES.items())
    dim = spark.createDataFrame(
        payment_data,
        ["payment_type_id", "payment_type_name"]
    )

    dim.write.mode("overwrite").parquet(f"{GOLD_DIR}/dim_payment")
    print(f"  ✓ dim_payment: {dim.count()} rows")


def build_fct_hourly_summary():
    """
    Pre-aggregated hourly summary — fast dashboard queries.
    Demonstrate aggregation at scale.
    """
    print("Building fct_hourly_summary...")

    df = spark.read.parquet(SILVER_DIR)

    summary = df.groupBy(
        "pickup_year", "pickup_month",
        "pickup_day_of_week", "pickup_hour"
    ).agg(
        F.count("*").alias("trip_count"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("tip_amount").alias("avg_tip"),
    ).orderBy("pickup_year", "pickup_month",
              "pickup_day_of_week", "pickup_hour")

    summary.write \
        .mode("overwrite") \
        .partitionBy("pickup_year", "pickup_month") \
        .parquet(f"{GOLD_DIR}/fct_hourly_summary")

    print(f"  ✓ fct_hourly_summary: {summary.count():,} rows")


if __name__ == "__main__":
    build_fct_trips()
    build_dim_time()
    build_dim_payment()
    build_fct_hourly_summary()
    print("\n Gold layer complete!")
    spark.stop()