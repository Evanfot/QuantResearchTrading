
# %%
import sys
from pathlib import Path
import os

root = Path().resolve()
while not (root / "src").exists():
    root = root.parent
os.chdir(root)
print("Working directory:", root)
# %%

import logging
import pandas as pd
import datetime as dt
import numpy as np
import duckdb
from dotenv import load_dotenv

from scripts.meta_data import get_hl_coins
from scripts.mkt_cap_data import get_latest_market_cap
from src.backtester.full_backtest import run_backtest, StrategyConfig
from src.signal import ewmac, breakout, scaled_bollinger, alpha006, alpha014, alpha020
from src.data import get_hyperliquid_trading_universe, db_path, get_ohlcv, get_final_pricing, load_ohlcv_for_alphas
# %%
START_DATE = "2025-01-01"  # set to None to use all available data

# %%
top = get_latest_market_cap()
hl = get_hl_coins()
universe, symbol_index = get_hyperliquid_trading_universe(top, hl)
# Get pricing and add to intent
conn = duckdb.connect(db_path)
hyperliquid_prices = get_ohlcv(conn)
latest_view = pd.read_csv('data/snapshots/mids.csv', index_col=0)
prices, returns_adj = get_final_pricing(hyperliquid_prices, universe, latest_view)
config = StrategyConfig()
ewmac_forecast = ewmac(returns_adj, config.ewmac_fast)
breakout_forecast = breakout(prices, config.breakout_window)
bollinger_forecast = scaled_bollinger(prices, param=config.bollinger_window, scalar=1)

o, h, l, c_alpha, v = load_ohlcv_for_alphas(universe)
o        = o.reindex(index=prices.index, columns=prices.columns)
h        = h.reindex(index=prices.index, columns=prices.columns)
l        = l.reindex(index=prices.index, columns=prices.columns)
v        = v.reindex(index=prices.index, columns=prices.columns)
c_alpha  = c_alpha.reindex(index=prices.index, columns=prices.columns)
r_alpha  = np.log(c_alpha).diff()

alpha006_forecast = alpha006(o, v)
alpha014_forecast = alpha014(o, v, r_alpha)
alpha020_forecast = alpha020(o, h, l, c_alpha)

mu = np.mean([bollinger_forecast, ewmac_forecast, breakout_forecast,
              alpha006_forecast, alpha014_forecast, alpha020_forecast], axis=0)
vo = prices.pct_change().ewm(com=config.vo_window, min_periods=20).std().values
cor = returns_adj.ewm(com=config.correlation, min_periods=config.correlation).corr()

if START_DATE:
    mask = prices.index >= pd.Timestamp(START_DATE)
    prices = prices.loc[mask]
    mu = mu[mask]
    vo = vo[mask]
    cor = cor.loc[prices.index]

portfolio = run_backtest(prices, mu, vo, cor, config)
portfolio.snapshot()

import polars as pl
from jquantstats import Portfolio, CostModel

def _to_polars_with_date(df: pd.DataFrame) -> pl.DataFrame:
    """Convert a pandas DataFrame with DatetimeIndex to polars with a 'Date' column."""
    return pl.from_pandas(df.reset_index().rename(columns={df.index.name or "index": "date"}))

prices_pl = _to_polars_with_date(portfolio.prices)
positions_pl = _to_polars_with_date(portfolio.cashposition)

pf = Portfolio.from_cash_position(prices=prices_pl, cash_position=positions_pl, aum=float(portfolio.aum.iloc[0]))

sharpe = pf.stats.sharpe()
print(f"Sharpe: {sharpe}")

fig = pf.plots.snapshot()
fig.show()
# %%

# Model B: turnover-bps cost (macro, fund-of-funds)
pf_bps = pf.from_cash_position(
    prices=prices_pl, cash_position=positions_pl, aum=float(portfolio.aum.iloc[0]),
    cost_model=CostModel.turnover_bps(5.0),
)
# Sweep Sharpe across 0 → 20 bps in a single call
impact = pf_bps.trading_cost_impact(max_bps=20)
# %%
html = pf.report.to_html()
with open("report.html", "w") as f:
    f.write(html)