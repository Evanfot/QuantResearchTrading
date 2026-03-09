
# %%
import os
import json
import logging
import pandas as pd
from pathlib import Path
import importlib
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
import warnings
import datetime as dt
import numpy as np
from cvx.simulator import Builder
from cvx.simulator import interpolate
from tinycta.linalg import solve, inv_a_norm
from tinycta.signal import returns_adjust, osc, shrink2id
import requests
import duckdb
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from typing import List, Dict
import json
from pathlib import Path
from src.state.strategy_state import load_state, save_state, get_state_positions
from src.loggers.intent_logger import (
    init_intent,
    init_asset,  
    IntentLogger,
    generate_run_id
)
from src.loggers.order_logger import OrderLogger
from src.helpers.dict_diff import dict_diff
from scripts.exchange_state import run_exchange_state, read_latest_exchange_state
from scripts.meta_data import read_latest_meta
# TODO: set meta_data, latest_market_caps to run every day
from scripts.meta_data import get_hl_coins 
from scripts.mkt_cap_data import get_latest_market_cap
from src.ingestion.update_mids import run_update_mids
# %%

    # Get universe and initialise relevant rows i nintent
    from src.main import get_hyperliquid_trading_universe, get_ohlcv, get_final_pricing

    db_path = 'data/pricing/ohlcv_data.duckdb'
    top = get_latest_market_cap()
    hl = get_hl_coins()
    universe = get_hyperliquid_trading_universe(top, hl)
    # Get pricing and add to intent
    conn = duckdb.connect(db_path)
    hyperliquid_prices = get_ohlcv(conn)

    latest_view = pd.read_csv('data/snapshots/mids.csv')
    prices, returns_adj = get_final_pricing(hyperliquid_prices,universe,latest_view)

        ewmac_forecast = ewmac(returns_adj, ewmac_fast)
        breakout_forecast = breakout(prices, breakout_window)
        bollinger_forecast = scaled_bollinger(prices, param=bollinger_window, scalar=1)
        mu = np.mean([bollinger_forecast, ewmac_forecast, breakout_forecast], axis=0)
        vo = prices.pct_change().ewm(com=vo_window, min_periods=20).std().values
        cor = returns_adj.ewm(com=correlation, min_periods=correlation).corr()


        target_weights, portfolio = run_trading_logic(state)
        portfolio.snapshot()