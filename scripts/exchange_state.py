# TODO: Abstract this as it's the same flow as meta_data
# fetch_and_store_exchange_state.py
import json
import requests
from datetime import datetime,timezone
from pathlib import Path
import os

from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL

from dotenv import load_dotenv
load_dotenv()

WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")

exchange_state_DIR = Path("data/hyperliquid_exchange_state")
exchange_state_DIR.mkdir(exist_ok=True)
address = WALLET_ADDRESS

def fetch_exchange_state():
    info = Info(MAINNET_API_URL, skip_ws=True)
    exchange_state = info.user_state(address)
    return exchange_state

def store_exchange_state(data):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = exchange_state_DIR / f"exchange_state_{ts}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path

import json
from pathlib import Path

def read_latest_exchange_state():
    files = sorted(exchange_state_DIR.glob("exchange_state_*.json"))
    if not files:
        raise FileNotFoundError("No exchange_state snapshots found in hyperliquid_exchange_state/")
    latest_path = files[-1]
    with open(latest_path) as f:
        data = json.load(f)
    return data

def get_hl_coins():
    data = read_latest_exchange_state()
    return {x["name"].upper() for x in data["universe"]}
def run_exchange_state():
    exchange_state = fetch_exchange_state()
    path = store_exchange_state(exchange_state)
    print(f"Saved exchange_state snapshot → {path}")
    return exchange_state

if __name__ == "__main__":
    run_exchange_state()