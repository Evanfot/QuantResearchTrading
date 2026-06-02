# %%
import logging
from pathlib import Path
import datetime as dt
from src.loggers.fill_logger import FillLogger
from src.config import make_info, open_orders as hl_open_orders, WALLET_ADDRESS, TRADING_ENV
from src.state.strategy_state import load_state, save_state

from src.positions.position_rebuilder import PositionRebuilder

STATE_PATH = Path(f"state/hyperliquid_{TRADING_ENV}_{WALLET_ADDRESS}_state.json")

def _setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    err_handler = logging.FileHandler(f"logs/errors_{TRADING_ENV}.log")
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(err_handler)


def main():
    """Run one fill-logger poll cycle. Returns open_orders so the caller can loop."""
    exchange = "hyperliquid"

    try:
        info = make_info()
        fill_logger = FillLogger(f"logs/fills_{TRADING_ENV}.jsonl")

        fill_run_id = dt.datetime.now(dt.timezone.utc).isoformat()
        state = load_state(STATE_PATH)
        last_ts = state["last_fill_timestamp_ms"]
        state_positions = state["positions"]
        assert state["exchange"] == 'hyperliquid'
        assert state["schema_version"] == 1.2
        if state.get("address") and state["address"] != WALLET_ADDRESS:
            raise RuntimeError(f"State address mismatch: file has {state['address']}, expected {WALLET_ADDRESS}")
        state["address"] = WALLET_ADDRESS

        raw_fills = info.user_fills_by_time(WALLET_ADDRESS, last_ts)

        fills_to_commit = []
        max_seen_ts = last_ts

        for fill in raw_fills:
            ts = int(fill["time"])
            if ts > last_ts:
                fills_to_commit.append(fill)
                max_seen_ts = max(max_seen_ts, ts)

        if fills_to_commit:
            coins = ", ".join(sorted({f["coin"] for f in fills_to_commit}))
            logging.info(f"[fill_logger] {len(fills_to_commit)} new fill(s) detected — {coins}")
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

        return hl_open_orders(info, WALLET_ADDRESS)
    except Exception as e:
        logging.exception(f"[fill_logger] error: {e}")
        return []


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
    _setup_logging()
    main()
