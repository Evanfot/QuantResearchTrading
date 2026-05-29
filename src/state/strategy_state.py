import json
from pathlib import Path

DEFAULT_STATE = {
    "schema_version": 1.2,
    "exchange": "hyperliquid",
    "address": "",
    "last_data_run_ms": 0,
    "last_trading_intent_run_id": "",
    "last_trading_exec_ms": 0,
    "last_fill_timestamp_ms": 0,
    "has_open_orders": False,
    "last_position_check_ms": 0,
    "fills_logged_at_ms": 0,
    "last_meta_run_ms": 0,
    "positions": {},
}

def load_state(path: Path) -> dict:
    if not path.exists():
        return DEFAULT_STATE.copy()
    return json.loads(path.read_text())

def save_state(state: dict, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)

def get_state_positions(state: dict) -> dict[str, float]:
    return {k: v["qty"] for k, v in state.get("positions", {}).items()}
