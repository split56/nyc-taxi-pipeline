"""
Great Expectations validation suite for raw NYC taxi data.
Runs BEFORE PySpark processing — fail fast on bad data.
"""

import great_expectations as gx
from pathlib import Path
import sys
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def build_expectation_suite(validator):
    """
    Define good data
    """
    # Completeness
    validator.expect_column_values_to_not_be_null(
        "tpep_pickup_datetime",
        mostly=0.99  # allow 1% nulls
    )
    validator.expect_column_values_to_not_be_null(
        "tpep_dropoff_datetime",
        mostly=0.99
    )
    validator.expect_column_values_to_not_be_null(
        "fare_amount"
    )

    # Validity
    validator.expect_column_values_to_be_between(
        "passenger_count",
        min_value=1,
        max_value=9,
        mostly=0.95
    )
    validator.expect_column_values_to_be_between(
        "fare_amount",
        min_value=0,
        max_value=1000,
        mostly=0.99
    )
    validator.expect_column_values_to_be_between(
        "trip_distance",
        min_value=0,
        max_value=200,
        mostly=0.99
    )

    # Referential integrity
    validator.expect_column_values_to_be_in_set(
        "payment_type",
        value_set=[0,1, 2, 3, 4, 5],
        mostly = 0.99
    )

    # Freshness
    validator.expect_column_values_to_be_between(
        "tpep_pickup_datetime",
        min_value=datetime(2022, 1, 1),
        max_value=datetime(2025, 1, 1),
        mostly=0.99,
        parse_strings_as_datetimes=True
    )

    return validator


def validate_file(parquet_path: str) -> bool:
    """Validate a single month's data file."""
    print(f"Validating: {parquet_path}")

    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name="taxi_source")
    asset = datasource.add_parquet_asset(
        name="taxi_asset",
        path=parquet_path
    )

    batch_request = asset.build_batch_request()
    validator = context.get_validator(batch_request=batch_request)

    build_expectation_suite(validator)
    results = validator.validate()

    success_pct = results.statistics["success_percent"]
    passed = results.statistics["successful_expectations"]
    total = results.statistics["evaluated_expectations"]

    print(f"  Results: {passed}/{total} expectations passed ({success_pct:.1f}%)")

    if not results.success:
        failed = [r for r in results.results if not r.success]
        print(f"  FAILED expectations:")
        for r in failed:
            print(f"    ✗ {r.expectation_config.expectation_type} "
                  f"on column '{r.expectation_config.kwargs.get('column')}'")
        return False

    print("  ✓ All expectations passed")
    return True


def validate_all():
    """Validate all downloaded raw files."""
    raw_files = sorted(RAW_DIR.glob("yellow_tripdata_*.parquet"))

    if not raw_files:
        print("No raw files found. Run download_data.py first.")
        sys.exit(1)

    all_passed = True
    for f in raw_files:
        passed = validate_file(str(f))
        if not passed:
            all_passed = False

    if not all_passed:
        print("\n DATA QUALITY CHECK FAILED — fix issues before processing")
        sys.exit(1)

    print(f"\n All {len(raw_files)} files passed validation")


if __name__ == "__main__":
    validate_all()