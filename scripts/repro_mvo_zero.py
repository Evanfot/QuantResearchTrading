"""Replay the 2026-11-15 all-zero MVO intent against the copied production DB.

Uses the tradable universe and per-asset mu actually logged that day
(logs/intent_mainnet.jsonl) so the only unknown is the price window feeding
the covariance estimate -- which now comes from the real
data/pricing/ohlcv_data.duckdb snapshot. compute_strategy_mvo now logs the
reason for any flatten (see src/backtester/full_backtest_mvo.py), so this
just needs to call it and watch stderr.
"""
import json
import logging

import duckdb
import numpy as np
import pandas as pd

from src.backtester.full_backtest_mvo import compute_strategy_mvo
from src.data import db_path, drop_incomplete_bars, get_ohlcv
from src.strategy.config import StrategyConfig

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

RUN_ID = "20261115T000100Z_477f14"


def load_todays_intent():
    with open("logs/intent_mainnet.jsonl") as f:
        for line in f:
            rec = json.loads(line)
            if rec["meta"]["run_id"] == RUN_ID:
                return rec
    raise ValueError(f"run_id {RUN_ID} not found")


def main():
    intent = load_todays_intent()
    tradable = intent["universe"]["tradable"]
    run_ts = pd.Timestamp(intent["meta"]["timestamp"])
    print(f"run_id={RUN_ID} timestamp={run_ts} n_tradable={len(tradable)}")

    conn = duckdb.connect(db_path, read_only=True)
    hyperliquid_prices = get_ohlcv(conn)
    conn.close()

    hype_cols = [f"{s}/USDC:USDC" for s in tradable]
    missing = [s for s, c in zip(tradable, hype_cols) if c not in hyperliquid_prices.columns]
    if missing:
        print(f"WARNING: {len(missing)} tradable symbols have no column in the DB: {missing}")
    present_cols = [c for c in hype_cols if c in hyperliquid_prices.columns]
    prices = hyperliquid_prices[present_cols].copy()
    prices.columns = prices.columns.str.replace("/USDC:USDC", "", regex=False)

    prices = drop_incomplete_bars(prices, now=run_ts)
    print(f"price window: {prices.index[0]} .. {prices.index[-1]} ({len(prices)} rows)")

    t = prices.index[-1]
    mask = ~prices.loc[t].isna().values
    print(f"mask: {mask.sum()} / {len(mask)} assets have a valid price on {t}")
    nan_assets = list(prices.columns[~mask])
    if nan_assets:
        print(f"NaN-priced today: {nan_assets}")

    mu_by_symbol = {s: intent["assets"][s]["model"]["signal"]["mu"] for s in tradable if s in present_cols or True}
    mu_row = np.array([mu_by_symbol.get(s, 0.0) for s in prices.columns])

    config = StrategyConfig()
    print(f"mvo config: {config.mvo}")

    out = compute_strategy_mvo(price_window=prices, mu_row=mu_row, mask=mask, config=config.mvo)
    print("---- MVOIntent ----")
    print(f"leverage={out.leverage} gross={out.gross} portfolio_mu={out.portfolio_mu} expected_vol={out.expected_vol}")
    print(f"target_weight nonzero: {int(np.count_nonzero(out.target_weight))} / {len(out.target_weight)}")


if __name__ == "__main__":
    main()
