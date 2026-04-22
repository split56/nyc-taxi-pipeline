# diagnostic.py
import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

for file in RAW_DIR.glob("yellow_tripdata_*.parquet"):
    print(f"\nChecking: {file.name}")
    df = pd.read_parquet(file)
    print(f"Unique payment_type values: {sorted(df['payment_type'].unique())}")
    print(f"Value counts:\n{df['payment_type'].value_counts().sort_index()}")