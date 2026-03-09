import requests
import datetime as dt
import pandas as pd
from datetime import timezone
import json
from pathlib import Path

def get_top_marketcap(n=200):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": n,
        "page": 1,
    }
    return requests.get(url, params=params).json()

def store_market_cap(top):
    ts = dt.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path = MKT_CAP_DIR / f"{ts}.csv"
    pd.DataFrame(top).to_csv(path)
#TODO: Fix store_meta to work for mkt cap
MKT_CAP_DIR = Path("data/mkt_cap") 
def get_latest_market_cap():
    latest_csv = max(
        (f for f in MKT_CAP_DIR.glob("*.csv")),
        key=lambda f: f.stat().st_mtime,
    )
    top = pd.read_csv(latest_csv, index_col = 0)
    return top
MKT_CAP_DIR.mkdir(exist_ok=True)
if __name__ == "__main__":
    top = get_top_marketcap(200)
    store_market_cap(top)