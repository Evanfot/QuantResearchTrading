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
from src.state.strategy_state import load_state, save_state

from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
from src.positions.position_rebuilder import PositionRebuilder

def main():
    """Run one fill-logger poll cycle. Returns open_orders so the caller can loop."""
    exchange = "hyperliquid"

    info = Info(MAINNET_API_URL, skip_ws=True)
    fill_logger = FillLogger("logs/fills.jsonl")

    fill_run_id = dt.datetime.now(dt.timezone.utc).isoformat()
    state = load_state(STATE_PATH)
    last_ts = state["last_fill_timestamp_ms"]
    state_positions = state["positions"]
    assert state["exchange"] == 'hyperliquid'
    assert state["address"] == WALLET_ADDRESS
    assert state["schema_version"] == 1.2
    print(f"[fill_logger] cursor={last_ts}, run_id={fill_run_id}")

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
            last_ts = max_seen_ts + 1
            state["last_fill_timestamp_ms"] = last_ts
            state["schema_version"] = 1.2
            state["fills_logged_at_ms"] = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)

            rebuilder = PositionRebuilder(state_positions)
            fills = fill_logger.parse_fills(
                run_id=fill_run_id,
                exchange=exchange,
                account=WALLET_ADDRESS,
                fills=fills_to_commit,
                source="polling",
            )
            fills.sort(key=lambda x: x["fill_timestamp_ms"])
            positions = rebuilder.rebuild_from_fills(fills)
            state["positions"] = positions
            save_state(state,STATE_PATH)
    except Exception as e:
        print(f"[fill_logger] error: {e}")

    return info.open_orders(WALLET_ADDRESS)


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
    state = load_state(STATE_PATH)
    rebuilder = PositionRebuilder()
    positions = rebuilder.rebuild_from_fills(fills)
    state["positions"] = positions
    save_state(state,STATE_PATH)
    # --- Result ---

if __name__ == "__main__":
    main()
