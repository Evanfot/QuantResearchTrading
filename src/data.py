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


def get_final_pricing(
    prices_all: pd.DataFrame,
    universe: list[str],
    latest_view: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Slice universe, append today's live mid, compute vol-normalised returns.

    prices_all:  wide DataFrame with bare HL name columns (BTC, ETH …).
    universe:    list of bare HL names.
    latest_view: DataFrame indexed by bare HL name with a 'mid' column.
    """
    prices = prices_all[universe].copy(deep=True)
    prices.loc[
        dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    ] = latest_view.loc[prices.columns.values, "mid"].astype("float")
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
