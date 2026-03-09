
# TODO: Abstract this as it's the same flow as user_state_data
# fetch_and_store_meta.py
import json
import requests
from datetime import datetime,timezone
from pathlib import Path

META_DIR = Path("data/hyperliquid_meta")
META_DIR.mkdir(exist_ok=True)

def fetch_meta():
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "meta"}
    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def store_meta(data):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = META_DIR / f"meta_{ts}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path
# read_latest_meta.py
import json
from pathlib import Path

# META_DIR = Path("hyperliquid_meta")

def read_latest_meta():
    files = sorted(META_DIR.glob("meta_*.json"))
    if not files:
        raise FileNotFoundError("No meta snapshots found in hyperliquid_meta/")
    latest_path = files[-1]
    with open(latest_path) as f:
        data = json.load(f)
    return data

def get_hl_coins():
    data = read_latest_meta()
    return {x["name"].upper() for x in data["universe"]}

if __name__ == "__main__":
    meta = fetch_meta()
    path = store_meta(meta)
    print(f"Saved meta snapshot → {path}")
