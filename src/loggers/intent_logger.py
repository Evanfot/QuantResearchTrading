from pathlib import Path
import json
import datetime as dt
import uuid

def init_intent(mode, strategy_name, run_id):
    return {
        "meta": {
            "run_id": run_id,
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "mode": mode,                 #"live" / "backtest"
            "strategy": strategy_name,
        },

        "portfolio": {
            "equity_usd": None,
            "equity_used_for_sizing": None,
            "gross_exposure_pre_rebal": None,
            "net_exposure_pre_rebal": None,
            "maintenance_margin":None,
        },

        "risk_inputs": {
            "correlation_matrix": None,
        },

        "universe": {
            "tradable": [],
            "non_tradable": [],
            "holdings_outside_universe": {},
        },

        "assets": {}
    }

# intent = init_intent(
#     run_id="2026-01-01T12:00_live",
#     mode="live",
#     strategy_name="trend_v4",
# )

def init_asset() -> dict:
    return {
        "market": {
            "ltp": None,
            "data_timestamp": None,
            "best_bid": None,
            "best_ask": None,
        },
        "model": {
            "signal": None,
            "vol_1d": None,
            "risk_position":None,
            "target_weight": None,
        },
        "current": {
            "qty": None
        },
        "target": {
            "qty": None
        },
        
        "order_intent": {
            "coin": None,
            "side": None,
            "target": 0.0,
            "current": 0.0,
            "delta": 0.0,
        }
    }


def generate_run_id() -> str:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_id = uuid.uuid4().hex[:6]
    return f"{ts}_{short_id}"

class IntentLogger:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, intent: dict):
        with self.path.open("a") as f:
            f.write(json.dumps(intent) + "\n")

    def read_latest(self):
        with open("logs/intent.jsonl", "r") as f:
            latest_intent = json.loads(f.readlines()[-1])
        return latest_intent
