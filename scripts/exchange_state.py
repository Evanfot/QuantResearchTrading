# TODO: Abstract this as it's the same flow as meta_data
import json
import logging
import requests
from datetime import datetime,timezone
from pathlib import Path

logger = logging.getLogger(__name__)

from src.config import make_info, WALLET_ADDRESS, TRADING_ENV

exchange_state_DIR = Path(f"data/hyperliquid_exchange_state_{TRADING_ENV}")
exchange_state_DIR.mkdir(exist_ok=True)
address = WALLET_ADDRESS

def fetch_exchange_state():
    info = make_info()
    exchange_state = info.user_state(address)
    # Under HL unified margin the bulk of collateral lives in the spot wallet, so
    # the perp marginSummary.accountValue only reflects collateral held against
    # open positions. Embed spot state so the true account equity survives in the
    # snapshot (used by read_latest_exchange_state for the offline fallback).
    try:
        exchange_state["spotState"] = info.spot_user_state(address)
    except Exception:
        logger.warning("could not fetch spot_user_state; equity will fall back to perp accountValue")
    return exchange_state


def get_account_equity(exchange_state):
    """Unified-margin account value used for sizing: the USDC balance in the spot
    wallet. Falls back to the perp marginSummary.accountValue for snapshots taken
    before spot state was captured."""
    spot = exchange_state.get("spotState") or {}
    for bal in spot.get("balances", []):
        if bal.get("coin") == "USDC":
            return float(bal["total"])
    return float(exchange_state["marginSummary"]["accountValue"])

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
    logger.info(f"Saved exchange_state snapshot → {path}")
    return exchange_state

if __name__ == "__main__":
    run_exchange_state()