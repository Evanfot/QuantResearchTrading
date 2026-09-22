# TODO: Abstract this as it's the same flow as meta_data
import json
import logging
import time
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

from src.config import make_info, WALLET_ADDRESS, TRADING_ENV

exchange_state_DIR = Path(f"data/hyperliquid_exchange_state_{TRADING_ENV}")
exchange_state_DIR.mkdir(exist_ok=True)
address = WALLET_ADDRESS

_SPOT_FETCH_RETRIES = 3
_SPOT_FETCH_BACKOFF_S = 2  # doubles each attempt: 2s, 4s, 8s
_STALE_EQUITY_THRESHOLD = timedelta(hours=1)


def fetch_exchange_state():
    info = make_info()
    exchange_state = info.user_state(address)
    # Under HL unified margin the bulk of collateral lives in the spot wallet, so
    # the perp marginSummary.accountValue only reflects collateral held against
    # open positions -- NOT a safe stand-in for true account equity (confirmed
    # 2026-09-21: marginSummary.accountValue ran 40-70% below the real spot USDC
    # balance on this account at every checked point, not just during the
    # incident). info.spot_user_state has shown transient single-request gaps
    # that self-correct within ~2 minutes, so retry a few times before treating
    # it as a real failure.
    for attempt in range(_SPOT_FETCH_RETRIES):
        try:
            exchange_state["spotState"] = info.spot_user_state(address)
            break
        except Exception:
            if attempt == _SPOT_FETCH_RETRIES - 1:
                logger.warning(
                    "spot_user_state failed after %d attempts; snapshot will lack spotState "
                    "-- get_account_equity() will fall back to the last known-good reading",
                    _SPOT_FETCH_RETRIES, exc_info=True,
                )
            else:
                time.sleep(_SPOT_FETCH_BACKOFF_S * (2 ** attempt))
    return exchange_state


def _last_known_good_equity():
    """Most recent stored snapshot with a valid spot USDC balance, and its age.

    Returns (equity, age) or (None, None) if no such snapshot exists at all.
    """
    files = sorted(exchange_state_DIR.glob("exchange_state_*.json"), reverse=True)
    now = datetime.now(timezone.utc)
    for f in files:
        try:
            with open(f) as fh:
                data = json.load(fh)
        except Exception:
            continue
        spot = data.get("spotState") or {}
        for bal in spot.get("balances", []):
            if bal.get("coin") == "USDC":
                ts = datetime.strptime(
                    f.stem.replace("exchange_state_", ""), "%Y%m%d_%H%M%S"
                ).replace(tzinfo=timezone.utc)
                return float(bal["total"]), now - ts
    return None, None


def get_account_equity(exchange_state):
    """Unified-margin account value used for sizing: the USDC balance in the spot
    wallet. If the current snapshot's spotState is missing/incomplete (a fetch
    gap, not a real account event -- see fetch_exchange_state), falls back to the
    last snapshot that DID have a valid spot balance, rather than
    marginSummary.accountValue, which measures a structurally different (and
    much smaller) quantity on this account and must never be used as a live
    equity substitute -- it silently sized real positions off a phantom ~42%
    "equity drop" on 2026-09-21 before this fix.
    """
    spot = exchange_state.get("spotState") or {}
    for bal in spot.get("balances", []):
        if bal.get("coin") == "USDC":
            return float(bal["total"])

    equity, age = _last_known_good_equity()
    if equity is None:
        raise RuntimeError(
            "no exchange_state snapshot with a valid spot USDC balance found "
            "(current fetch has no spotState, and none of the stored snapshots do either)"
        )
    if age > _STALE_EQUITY_THRESHOLD:
        logger.error(
            "!!! ACCOUNT EQUITY STALE: current fetch is missing spotState and the last "
            "known-good equity reading ($%.2f) is %s old (> %s threshold) -- sizing/dashboard "
            "may be acting on out-of-date equity. Investigate the spot_user_state fetch. !!!",
            equity, age, _STALE_EQUITY_THRESHOLD,
        )
    else:
        logger.warning(
            "spotState missing from current fetch; using last known-good equity "
            "$%.2f (%s old)", equity, age,
        )
    return equity

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