import numpy as np
import pandas as pd

import src.main as m
from src.backtester.full_backtest import StrategyConfig


def _synthetic_inputs(symbols, n_days=120):
    dates = pd.date_range("2025-01-01", periods=n_days)
    rets = np.random.RandomState(0).normal(0.001, 0.03, (n_days, len(symbols)))
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=symbols)
    returns_adj = prices.pct_change()
    mu = np.random.RandomState(1).normal(0.0, 0.02, (n_days, len(symbols)))
    vo = np.abs(np.random.RandomState(2).normal(0.3, 0.05, (n_days, len(symbols))))
    cor = returns_adj.ewm(com=30, min_periods=30).corr()
    return prices, mu, vo, cor


class _FakeLogger:
    def __init__(self):
        self.logged = None

    def log(self, x):
        self.logged = x


def test_holdings_outside_universe_excludes_zero_qty_coins():
    """positions.keys() can carry stale zero-qty entries for coins that dropped
    out of the universe a while ago; holdings_outside_universe must only report
    coins with an ACTUAL open position left to close, not every stale key."""
    tradable = [f"A{i}" for i in range(6)]
    prices, mu, vo, cor = _synthetic_inputs(tradable)

    positions = {
        "A0": 0.0, "A1": 0.0,          # tradable, flat -- irrelevant either way
        "OLD_ZERO": 0.0,               # outside universe, but NO open position
        "OLD_NONZERO": 5.0,            # outside universe, WITH an open position
    }
    ltps = {s: float(prices[s].iloc[-1]) for s in tradable}
    ltps.update({"OLD_ZERO": 1.0, "OLD_NONZERO": 1.0})
    latest_view = pd.DataFrame(
        {"mid": ltps, "downloaded_at": ["2025-01-01"] * len(ltps)}, index=list(ltps)
    )

    config = StrategyConfig()
    intent = m.init_intent(mode="live", strategy_name="test", run_id="test_run")
    for s in tradable:
        intent["assets"][s] = m.init_asset()
    intent["portfolio"]["equity_used_for_sizing"] = 10000.0

    logger_capture = _FakeLogger()
    m.run_live(prices, mu, vo, cor, positions, ltps, intent, config, latest_view, m.logger, logger_capture)

    holdings_outside = logger_capture.logged["universe"]["holdings_outside_universe"]
    assert "OLD_NONZERO" in holdings_outside
    assert "OLD_ZERO" not in holdings_outside
