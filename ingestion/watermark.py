"""
Watermark manager - tracks which months have been processed, prevent reprocessing data that have already been seen
"""

import json
from pathlib import Path
from datetime import datetime

WATERMARK_FILE = Path(__file__).parent.parent / "watermarks.json"

def get_last_processed() -> dict:
    """Return the last processed year/month"""
    if not WATERMARK_FILE.exists():
        return {"year":2023, "month":1}
    with open(WATERMARK_FILE) as f:
        return json.load(f)

def update_watermark(year: int, month: int):
    """Update the watermark"""
    with open(WATERMARK_FILE,'w') as f:
        json.dump({"year":year, "month":month,"updated_at": datetime.now().isoformat()}, f, indent=2)
    print(f"Updated watermark for {year}/{month}")

def get_months_to_process(up_to_year: int, up_to_month: int) -> list:
    """Return a list of months that have not been processed yet"""
    last = get_last_processed()
    last_year, last_month = last["year"], last["month"]
    months = []
    year,month = last_year, last_month
    while (year, month) <= (up_to_year, up_to_month):
        months.append((year,month))
        month += 1
        if month > 12:
            month = 1
            year += 1

    return months[1:] if len(months) > 1 else months
