"""Lightweight data-access utilities shared by analysis scripts and the live loop."""

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


def _infer_bar_interval(index: pd.DatetimeIndex) -> pd.Timedelta:
    """The resample cadence, taken as the most common spacing between consecutive bars.

    Works for daily bars today and 1h/30m bars later without a code change.
    """
    diffs = pd.Series(index).diff().dropna()
    if diffs.empty:
        raise ValueError("need >= 2 timestamps to infer the bar interval")
    return pd.Timedelta(diffs.mode().iloc[0])


def drop_incomplete_bars(prices: pd.DataFrame, now=None, interval=None) -> pd.DataFrame:
    """Keep only bars whose full period has already elapsed.

    A resampled bar stamped ``t`` covers ``[t, t + interval)`` and is a settled, usable
    observation only once ``now >= t + interval``. The current period's bar is still
    forming — e.g. the intent runs at 00:01, so the "daily" bar holds ~1 minute of data —
    and trading on it treats a sliver of the period as a whole bar. We therefore decide on
    the last COMPLETE bar rather than the in-progress one.

    ``interval`` is inferred from the index spacing rather than hardcoded to a day, so the
    same rule holds when the strategy moves to a 30-minute (or any) cadence. Pass
    ``interval`` explicitly to override inference. Only the forming tail is ever excluded;
    complete history is always retained (a stale feed drops nothing).
    """
    if len(prices) == 0:
        return prices
    if now is None:
        now = pd.Timestamp.now(tz="UTC")
    now = pd.Timestamp(now)
    if now.tzinfo is not None:
        now = now.tz_convert("UTC").tz_localize(None)
    if interval is None:
        interval = _infer_bar_interval(prices.index)
    complete = (prices.index + interval) <= now
    return prices.loc[complete]


def get_final_pricing(hyperliquid_prices, universe, latest_view):
    hype_universe = [k + "/USDC:USDC" for k in universe]
    hype_universe = [s for s in hype_universe if s in hyperliquid_prices.columns]
    prices = hyperliquid_prices[hype_universe].copy(deep=True)
    prices.columns = prices.columns.str.replace("/USDC:USDC", "")
    # Gate the universe to coins with a live mid — a price we can actually execute against —
    # even though the mid itself is not used in the signal series.
    available = [c for c in prices.columns if c in latest_view.index]
    prices = prices[available]
    # Decide on the last COMPLETE bar. The forming current-period bar (~1 minute of data at
    # 00:01) is dropped so a sliver of a period cannot masquerade as a full bar. The live mid
    # is deliberately NOT appended into the signal series — it is execution-only (ltps /
    # latest_view); injecting a partial price into the signal is the bug this fixes.
    prices = drop_incomplete_bars(prices)
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
