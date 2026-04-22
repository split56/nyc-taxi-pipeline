"""load gold parquet into duckdb for dbt and dashboard"""
import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
DB_PATH = str(PROJECT_ROOT / "taxi.db")

def load():
    con = duckdb.connect(DB_PATH)

    schemas = [r[0] for r in con.execute(
        "select schema_name from information_schema.schemata"
    ).fetchall()]

    for schema in ["raw","gold"]:
        if schema not in schemas:
            con.execute(f"create schema {schema};")

    tables = {
        "fct_trips":"fct_trips",
        "fct_hourly_summary":"fct_hourly_summary",
        "dim_time": "dim_time",
        "dim_payment":"dim_payment",
    }

    for folder, table in tables.items():
        path = GOLD_DIR / folder
        if path.exists():
            con.execute(f"""
                create or replace table gold.{table} as
                select * from read_parquet(
                        '{path}/**/*.parquet',
                        union_by_name = true
                )
            """)
            count = con.execute(
                f"select count(*) from gold.{table};"
            ).fetchone()[0]
            print(f"  ✓ gold.{table}: {count:,} rows")

    con.close()
    print(f"\nLoaded into {DB_PATH}")

if __name__ == "__main__":
    load()