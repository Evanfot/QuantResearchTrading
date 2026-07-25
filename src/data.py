"""Lightweight data-access utilities shared by analysis scripts and the live loop.

OHLCV data comes from data/cache/daily_ohlcv.parquet, rebuilt daily by
scripts/build_daily_cache.build():
  - Primary:  Binance daily closes from $BINANCE_KLINES_DIR/klines/processed/daily_closes.parquet
  - Fallback: Hyperliquid DuckDB for coins not covered by Binance (XMR, HYPE, …)
  - Today:    live HL mid prices appended via append_today_mids()
"""

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from src.universe import get_universe

_VOLA_COM = 36
_WINSOR   = 4.2

_CACHE_PATH         = Path(__file__).parent.parent / "data" / "cache" / "daily_ohlcv.parquet"
_CACHE_MAX_AGE_HOURS = 26


def _check_cache() -> None:
    if not _CACHE_PATH.exists():
        raise FileNotFoundError(
            f"Daily cache missing: {_CACHE_PATH}\n"
            "Run:  python scripts/build_daily_cache.py"
        )
    age_h = (dt.datetime.now().timestamp() - _CACHE_PATH.stat().st_mtime) / 3600
    if age_h > _CACHE_MAX_AGE_HOURS:
        raise RuntimeError(
            f"Daily cache is {age_h:.1f}h old (limit {_CACHE_MAX_AGE_HOURS}h). "
            "Run:  python scripts/build_daily_cache.py"
        )


def get_ohlcv() -> pd.DataFrame:
    """Daily close prices for all HL-mapped symbols.

    Returns a DataFrame with DatetimeIndex and bare HL name columns (BTC, ETH …).
    """
    _check_cache()
    df = pd.read_parquet(_CACHE_PATH, columns=["date", "symbol", "close"])
    result = df.pivot(index="date", columns="symbol", values="close")
    result.index = pd.to_datetime(result.index)
    result.columns.name = None
    return result


def latest_is_provisional() -> bool | None:
    """Whether the most recent cached daily bar is a provisional (live-buffer) close.

    The binance-klines producer flags `provisional=True` for a close synthesised from
    the live 1m buffer before Binance publishes the official daily file. Returns None
    if the cache predates the flag (no `provisional` column). Best-effort: never raises.
    """
    try:
        import pyarrow.parquet as _pq
        if "provisional" not in _pq.read_schema(_CACHE_PATH).names:
            return None
        df = pd.read_parquet(_CACHE_PATH, columns=["date", "provisional"])
        latest = pd.to_datetime(df["date"]).max()
        return bool(df.loc[pd.to_datetime(df["date"]) == latest, "provisional"].fillna(False).any())
    except Exception:
        return None


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
    forming — e.g. the intent runs at 00:12, so today's "daily" bar holds only a few
    minutes of data — and trading on it treats a sliver of the period as a whole bar. We
    therefore decide on the last COMPLETE bar rather than the in-progress one.

    Crucial for the merged Binance+HL cache: HL-only coins get a same-day (forming) bar
    from the HL DuckDB while Binance-sourced coins lag a day, so the trailing row is ragged
    — present for a handful of coins, NaN for the rest. Dropping that incomplete row lands
    every coin on the last COMPLETE day (Binance provisional bars carry full OHLCV there),
    so the formulaic alphas compute for all coins instead of NaN→0 for the Binance ones.

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


def get_final_pricing(
    prices_all: pd.DataFrame,
    universe: list[str],
    latest_view: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Slice universe to the last COMPLETE bar, compute vol-normalised returns.

    Decides on the last settled daily bar, NOT the forming current-day one. Today's
    live mid is deliberately NOT appended to the signal series — it is execution-only
    (used via ``latest_view`` for mark price / ltps). Appending it, or keeping the
    HL-only coins' partial same-day bar, would (a) trade on a few-minute sliver as if
    it were a full day and (b) leave Binance-sourced coins NaN on that ragged trailing
    row, which the formulaic alphas turn into a fabricated 0 signal.

    prices_all:  wide DataFrame with bare HL name columns (BTC, ETH …).
    universe:    list of bare HL names.
    latest_view: DataFrame indexed by bare HL name with a 'mid' column (execution-only).
    """
    prices = prices_all[universe].copy(deep=True)
    prices = drop_incomplete_bars(prices)
    returns = np.log(prices).diff()
    returns_adj = (returns / returns.ewm(com=_VOLA_COM, min_periods=120).std()).clip(
        -_WINSOR, +_WINSOR
    )
    return prices, returns_adj


def load_ohlcv_for_alphas(
    universe: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Daily OHLCV for the universe from the cache, pivoted wide.

    universe: bare HL names (BTC, ETH …).
    Returns: (open, high, low, close, volume) DataFrames.
    volume is quote_volume (dollar volume = sum of price×qty per bar).
    """
    _check_cache()
    df = pd.read_parquet(_CACHE_PATH)
    df = df[df["symbol"].isin(universe)].copy()
    df["date"] = pd.to_datetime(df["date"])

    def _pivot(col: str) -> pd.DataFrame:
        p = df.pivot(index="date", columns="symbol", values=col)
        p.columns.name = None
        return p

    return _pivot("open"), _pivot("high"), _pivot("low"), _pivot("close"), _pivot("quote_volume")


def get_hyperliquid_trading_universe(
    top_market_cap: pd.DataFrame,
    hl_universe: set,
) -> tuple[list[str], dict[str, int]]:
    """Shim for analysis scripts — returns the first-run Tradable Universe.

    The Tradable Universe is the top-N-by-market-cap Eligible Universe intersected
    with the tradability filters; it may contain fewer than UNIVERSE_SIZE coins and
    never includes assets ranked beyond the top N. See universe.get_universe.
    """
    return get_universe(top_market_cap, hl_universe, current_universe=None)
