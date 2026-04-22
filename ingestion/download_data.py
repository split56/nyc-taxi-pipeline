"""
Download data from NYC TLC Yellow Taxi
"""

import urllib.request
from pathlib import Path
from watermark import get_months_to_process, update_watermark

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def download_month(year: int, month: int) -> Path:
    """Download one month of yellow taxi data"""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    dest = RAW_DIR / filename

    if dest.exists():
        print(f"File {dest} downloaded")
        return dest
    print(f"Downloading {filename} from {url}")
    try:
        urllib.request.urlretrieve(url, dest)
        size_mb = dest.stat().st_size / 1024 / 1024
        print(f"  ✓ {filename} ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        if dest.exists():
            dest.unlink()
        return None

    return dest

def run_incremental_download(target_year: int = 2023, target_month: int = 3):
    """
    Incrementally download months not yet processed.
    Change target_year/month to expand the dataset.
    """
    months = get_months_to_process(target_year, target_month)
    if not months:
        print("No months to process, watermark is up to date")
        return []
    print(f"Months to download: {[f'{y}-{m:02d}' for y, m in months]}")

    downloaded = []
    for year, month in months:
        path = download_month(year, month)
        if path:
            downloaded.append({year,month, path})
    print(f"Downloaded {len(downloaded)} months")
    return downloaded

if __name__ == "__main__":
    # Start small — download 3 months for development
    run_incremental_download(target_year=2023, target_month=3)
