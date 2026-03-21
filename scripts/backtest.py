
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
from src.signal import ewmac, breakout, scaled_bollinger
from src.main import get_hyperliquid_trading_universe, db_path, get_ohlcv, get_final_pricing
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
mu = np.mean([bollinger_forecast, ewmac_forecast, breakout_forecast], axis=0)
vo = prices.pct_change().ewm(com=config.vo_window, min_periods=20).std().values
cor = returns_adj.ewm(com=config.correlation, min_periods=config.correlation).corr()
portfolio = run_backtest(prices, mu, vo, cor, config)
portfolio.snapshot()
