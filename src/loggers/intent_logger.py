from pathlib import Path
import json
import datetime as dt
import uuid

def init_intent(mode, strategy_name, run_id, provenance=None):
    meta = {
        "run_id": run_id,
        "schema_version": 2,          # 2 = MVO sizing block + model fields + provenance
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": mode,                 #"live" / "backtest"
        "strategy": strategy_name,
        "strategy_components": None,  # {alpha_model, risk_model, allocator} from registry
        "git_commit": None,
        "git_dirty": None,
        "config_hash": None,
    }
    if provenance:
        # Denormalise the registry manifest + run provenance into the log line so
        # each record stays self-contained and reproducible.
        meta["strategy"] = provenance.get("strategy", strategy_name)
        meta["strategy_components"] = provenance.get("strategy_components")
        meta["git_commit"] = provenance.get("git_commit")
        meta["git_dirty"] = provenance.get("git_dirty")
        meta["config_hash"] = provenance.get("config_hash")
    return {
        "meta": meta,

        "portfolio": {
            "equity_usd": None,
            "equity_used_for_sizing": None,
            "gross_exposure_pre_rebal": None,
            "net_exposure_pre_rebal": None,
            "maintenance_margin":None,
        },

        # Portfolio-level sizing decision (populated by the active sizing model).
        "sizing": {
            "model": None,                # "mvo_max_sharpe" | "risk_parity"
            "sizing_rule": None,          # e.g. "vol_target"
            "target_vol_daily": None,
            "target_vol_annual": None,
            "trading_days": None,
            "max_position_weight": None,
            "gamma": None,
            "applied_leverage": None,     # leverage applied to the tangency book
            "expected_vol_annual": None,  # model's expected annualised book vol
            "gross_leverage": None,       # sum(|target_weight|)
            "portfolio_mu": None,         # tangency expected excess return
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
            "risk_position": None,        # risk-parity only (None under MVO)
            "tangency_weight": None,      # MVO max-Sharpe direction (None under risk-parity)
            "capped": None,               # MVO: did per-asset cap bind for this asset?
            "target_weight": None,        # final weight (fraction of equity) — both models
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
        if not Path(self.path).exists():
            return None
        with open(self.path, "r") as f:
            latest_intent = json.loads(f.readlines()[-1])
        return latest_intent
