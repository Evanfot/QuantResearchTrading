"""Lightweight data-access utilities shared by analysis scripts and the live loop."""

import datetime as dt

import duckdb
import numpy as np
import pandas as pd

from src.universe import get_universe

db_path = "data/pricing/ohlcv_data.duckdb"

_VOLA_COM = 36
_WINSOR = 4.2


def get_ohlcv(conn):
    df = conn.execute(
        """
        SELECT datetime, symbol, close, volume
        FROM hyperliquid_1d
        """
    ).df()
    conn.close()
    return df.pivot(index="datetime", columns="symbol", values="close")


def get_final_pricing(hyperliquid_prices, universe, latest_view):
    hype_universe = [k + "/USDC:USDC" for k in universe]
    hype_universe = [s for s in hype_universe if s in hyperliquid_prices.columns]
    prices = hyperliquid_prices[hype_universe].copy(deep=True)
    prices.columns = prices.columns.str.replace("/USDC:USDC", "")
    available = [c for c in prices.columns if c in latest_view.index]
    prices = prices[available]
    prices.loc[
        dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    ] = latest_view.loc[available, "mid"].astype("float")
    returns = np.log(prices).diff()
    returns_adj = (returns / returns.ewm(com=_VOLA_COM, min_periods=120).std()).clip(
        -_WINSOR, +_WINSOR
    )
    return prices, returns_adj


def load_ohlcv_for_alphas(universe):
    """Load daily OHLCV for the universe from DuckDB, pivoted to wide format."""
    symbols_sql = ", ".join(f"'{s}/USDC:USDC'" for s in universe)
    conn = duckdb.connect(db_path)
    df = conn.execute(f"""
        SELECT datetime AS date, symbol, open, high, low, close, volume
        FROM hyperliquid_1d
        WHERE symbol IN ({symbols_sql})
        ORDER BY datetime
    """).df()
    conn.close()
    df["symbol"] = df["symbol"].str.replace("/USDC:USDC", "", regex=False)

    def pivot(col):
        return df.pivot(index="date", columns="symbol", values=col)

    return tuple(pivot(x) for x in ("open", "high", "low", "close", "volume"))


def get_hyperliquid_trading_universe(top_market_cap, hl_universe):
    """Shim for analysis scripts — always returns the top-N initial universe."""
    return get_universe(top_market_cap, hl_universe, current_universe=None)
