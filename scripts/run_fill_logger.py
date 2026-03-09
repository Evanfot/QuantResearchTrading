# %%
import time
import os
from pathlib import Path
import json
import datetime as dt
from src.loggers.fill_logger import FillLogger
from dotenv import load_dotenv
load_dotenv()
WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")

STATE_PATH = Path(f"state/hyperliquid_{WALLET_ADDRESS}_state.json")

def load_state():
    if not STATE_PATH.exists():
        return {
            "last_fill_timestamp_ms": 0,
            "recent_seen_fill_ids": [],
            'exchange':'',
            'schema_version':1,
            'address':''
        }
    return json.loads(STATE_PATH.read_text())

def save_state(state):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(STATE_PATH)

# from src.exchange.hyperliquid import get_info_client

from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
POLL_INTERVAL_SECONDS = 1.0
from src.positions.position_rebuilder import PositionRebuilder

def get_hyperliquid_positions(user_state):
    try:
        if "assetPositions" in user_state and user_state["assetPositions"]:
            positions = {
                p['position']["coin"]: float(p["position"]["szi"])
                for p in user_state["assetPositions"]}
        elif len(user_state['assetPositions']) == 0:
            positions = {}
    except requests.RequestException as e:
        print(f"Error fetching balances: {e}")
    return positions

def main():
    exchange = "hyperliquid"

    info = Info(MAINNET_API_URL, skip_ws=True)
    fill_logger = FillLogger("logs/fills.jsonl")

    fill_run_id = dt.datetime.now(dt.timezone.utc).isoformat()
    state = load_state()
    last_ts = state["last_fill_timestamp_ms"]
    state_positions = state["positions"]
    assert state["exchange"] == 'hyperliquid'
    assert state["address"] == WALLET_ADDRESS
    assert state["schema_version"] == 1
    print(f"[fill_logger] starting at cursor={last_ts}, run_id={fill_run_id}")
    open_orders = [1]
    user_state = info.user_state(WALLET_ADDRESS)
    while (len(open_orders) > 0):
        try:
            raw_fills = info.user_fills_by_time(WALLET_ADDRESS, last_ts)

            fills_to_commit = []
            max_seen_ts = last_ts

            for fill in raw_fills:
                ts = int(fill["time"])

                if ts > last_ts:
                    fills_to_commit.append(fill)
                    max_seen_ts = max(max_seen_ts, ts)

            if fills_to_commit:
                fill_logger.log_fills(
                    run_id=fill_run_id,
                    exchange=exchange,
                    account=WALLET_ADDRESS,
                    fills=fills_to_commit,
                    source="polling",
                )
#                 # advance cursor by 1 ms to avoid duplicates
                last_ts = max_seen_ts + 1

                MAX_IDS = 20_000
                # state["recent_seen_fill_ids"] = list(seen_ids)[-MAX_IDS:]
                state["last_fill_timestamp_ms"] = last_ts
                # state["exchange"] = 'hyperliquid'
                # state["address"] = WALLET_ADDRESS
                state["schema_version"] = 1
                state["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()

                rebuilder = PositionRebuilder(state_positions)

                fills = fill_logger.parse_fills(
                    run_id=fill_run_id,
                    exchange=exchange,
                    account=WALLET_ADDRESS,
                    fills=fills_to_commit,
                    source="polling",
                )
                
                # --- Sort fills chronologically ---
                fills.sort(key=lambda x: x["fill_timestamp_ms"])
                positions= rebuilder.rebuild_from_fills(fills)
                state["positions"] = positions
                save_state(state)
        except Exception as e:
#             # never crash the poller
            print(f"[fill_logger] error: {e}")

        positions = get_hyperliquid_positions(user_state)
        open_orders = info.open_orders(WALLET_ADDRESS)
        time.sleep(POLL_INTERVAL_SECONDS)


if 0:
    from src.positions.position_rebuilder import PositionRebuilder
    import json

    # --- Load fills ---
    fills = []
    with open("logs/fills.jsonl", "r") as f:
        for line in f:
            fills.append(json.loads(line))

    # --- Sort fills chronologically ---
    fills.sort(key=lambda x: x["fill_timestamp_ms"])

    # --- Rebuild positions from scratch ---
    state = load_state()
    rebuilder = PositionRebuilder()
    positions = rebuilder.rebuild_from_fills(fills)
    state["positions"] = positions
    save_state(state)
    # --- Result ---

if __name__ == "__main__":
    main()
