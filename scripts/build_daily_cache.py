"""
Build the daily OHLCV cache in two phases:

  build_binance() — run by the cache-builder service on a nightly cron loop.
    Resamples Binance 1m klines → daily and writes data/cache/binance_daily.parquet.

  build() — run by the trader data task after HL prices are refreshed.
    Reads binance_daily.parquet and appends HL prices for symbols not covered
    by Binance (XMR, HYPE, …). Writes data/cache/daily_ohlcv.parquet.
"""

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

import duckdb
import pandas as pd

from config import HISTORICAL_DIR, INTERVAL
from storage import HistoricalStore
from symbol_map import _binance_to_hl, _hl_to_binance, to_hl

CACHE_PATH         = ROOT / "data" / "cache" / "daily_ohlcv.parquet"
BINANCE_CACHE_PATH = ROOT / "data" / "cache" / "binance_daily.parquet"
HL_DB_PATH         = ROOT / "data" / "pricing" / "ohlcv_data.duckdb"

_SYM_RE = r"/([A-Z0-9]+)/1m/"


def _binance_globs() -> str:
    return ", ".join(
        f"'{HISTORICAL_DIR / b / INTERVAL / '*.parquet'}'" for b in sorted(_binance_to_hl)
    )


def _hl_universe() -> set[str]:
    meta_dir = ROOT / "data" / "hyperliquid_meta"
    latest = sorted(meta_dir.glob("meta_*.json"))[-1]
    with open(latest) as f:
        return {x["name"].upper() for x in json.load(f)["universe"]}


def _build_binance(store: HistoricalStore) -> pd.DataFrame:
    return store.query(f"""
        SELECT
            date_trunc('day', open_time)::DATE       AS date,
            regexp_extract(filename, '{_SYM_RE}', 1) AS symbol,
            arg_min(open,  open_time)                AS open,
            max(high)                                AS high,
            min(low)                                 AS low,
            arg_max(close, open_time)                AS close,
            sum(volume)                              AS volume,
            sum(quote_volume)                        AS quote_volume
        FROM read_parquet([{_binance_globs()}], filename=true)
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).assign(symbol=lambda d: d["symbol"].map(to_hl)).dropna(subset=["symbol"])


def _build_hl(hl_only: set[str]) -> pd.DataFrame:
    if not hl_only:
        return pd.DataFrame()
    if not HL_DB_PATH.exists():
        return pd.DataFrame()
    symbols_sql = ", ".join(f"'{s}/USDC:USDC'" for s in sorted(hl_only))
    conn = duckdb.connect(str(HL_DB_PATH), read_only=True)
    try:
        df = conn.execute(f"""
            SELECT
                datetime::DATE                     AS date,
                REPLACE(symbol, '/USDC:USDC', '')  AS symbol,
                open, high, low, close, volume,
                close * volume                     AS quote_volume
            FROM hyperliquid_1d
            WHERE symbol IN ({symbols_sql})
            ORDER BY date, symbol
        """).df()
    except Exception:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df


def _write_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    try:
        df.to_parquet(tmp, index=False)
        tmp.rename(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def build_binance() -> None:
    """Resample Binance 1m klines → daily. Runs in the cache-builder service."""
    t = time.time()
    store = HistoricalStore()
    print(f"Binance: aggregating {len(_binance_to_hl)} symbols  1m → daily …")
    df = _build_binance(store)
    print(f"  {len(df):,} rows, {df['symbol'].nunique()} symbols")
    _write_atomic(df, BINANCE_CACHE_PATH)
    print(f"Written → {BINANCE_CACHE_PATH}  ({time.time() - t:.1f}s)", flush=True)


def build() -> None:
    """Combine Binance daily + HL fallback → daily_ohlcv.parquet. Runs in the trader."""
    t = time.time()

    # ── Binance ────────────────────────────────────────────────────────────────
    if BINANCE_CACHE_PATH.exists():
        binance_df = pd.read_parquet(BINANCE_CACHE_PATH)
        print(f"Binance: {binance_df['symbol'].nunique()} symbols (from cache)")
    else:
        store = HistoricalStore()
        print(f"Binance: aggregating {len(_binance_to_hl)} symbols  1m → daily …")
        binance_df = _build_binance(store)
        print(f"  {len(binance_df):,} rows, {binance_df['symbol'].nunique()} symbols")

    # ── HL (fallback for uncovered symbols) ────────────────────────────────────
    hl_only = _hl_universe() - set(_hl_to_binance.keys())
    print(f"HL:      fetching {len(hl_only)} symbols from DuckDB …")
    hl_df = _build_hl(hl_only)
    n_hl = hl_df["symbol"].nunique() if not hl_df.empty else 0
    print(f"  {len(hl_df):,} rows, {n_hl} symbols")

    # ── Merge & write ──────────────────────────────────────────────────────────
    df = (
        pd.concat([binance_df, hl_df], ignore_index=True)
        .assign(date=lambda d: pd.to_datetime(d["date"]))
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    _write_atomic(df, CACHE_PATH)

    print(f"\nWritten → {CACHE_PATH}")
    print(f"  total rows:    {len(df):,}")
    print(f"  total symbols: {df['symbol'].nunique()}  "
          f"({binance_df['symbol'].nunique()} Binance + {n_hl} HL-only)")
    print(f"  date range:    {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"  elapsed:       {time.time() - t:.1f}s", flush=True)


if __name__ == "__main__":
    import time as _time
    import datetime as _dt

    def _seconds_until_next_run() -> float:
        now = _dt.datetime.now(_dt.timezone.utc)
        target = now.replace(hour=0, minute=1, second=0, microsecond=0)
        if now >= target:
            target += _dt.timedelta(days=1)
        return (target - now).total_seconds()

    build_binance()  # always build on startup so the cache is fresh from first launch
    while True:
        delay = _seconds_until_next_run()
        print(f"[cache-builder] next build in {delay / 3600:.1f}h  (00:01 UTC)", flush=True)
        _time.sleep(delay)
        build_binance()
