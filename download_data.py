"""Download the three official NYC TLC Parquet files (about 1.44 GB)."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import urlopen
import shutil


DATA = Path(__file__).resolve().parent / "data"
BASE = "https://d37ci6vzurychx.cloudfront.net"
FILES = {
    **{f"fhvhv_tripdata_2024-{month:02d}.parquet": f"trip-data/fhvhv_tripdata_2024-{month:02d}.parquet"
       for month in range(1, 4)},
    "taxi_zone_lookup.csv": "misc/taxi_zone_lookup.csv",
}


def download(item):
    name, remote = item
    target = DATA / name
    with urlopen(f"{BASE}/{remote}", timeout=120) as response:
        expected = int(response.headers["Content-Length"])
        if target.exists() and target.stat().st_size == expected:
            print(f"Already downloaded: {name}", flush=True)
            return
        temporary = target.with_suffix(target.suffix + ".part")
        with temporary.open("wb") as destination:
            shutil.copyfileobj(response, destination)
        if temporary.stat().st_size != expected:
            raise RuntimeError(f"Incomplete download: {name}; rerun this script.")
        temporary.replace(target)
    print(f"Downloaded: {name} ({expected:,} bytes)", flush=True)


if __name__ == "__main__":
    DATA.mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(download, FILES.items()))
    size = sum(p.stat().st_size for p in DATA.glob("*.parquet"))
    if size < 1_000_000_000:
        raise RuntimeError("The assignment requires at least 1 GB of input data.")
    print(f"Ready: {size:,} bytes of Parquet input ({size / 1e9:.3f} GB).")
