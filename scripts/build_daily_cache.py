"""
Build the daily OHLCV cache from two sources:

  1. Binance 1m kline store  — 157 symbols that have Binance data
  2. Hyperliquid DuckDB       — remaining HL-universe symbols (XMR, HYPE, CC …)
                                quote_volume approximated as close × volume

Scheduled nightly at 23:45 UTC. Writes data/cache/daily_ohlcv.parquet.
The live loop reads this cache at 00:00 (~10ms) instead of scanning raw files.
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

CACHE_PATH = ROOT / "data" / "cache" / "daily_ohlcv.parquet"
HL_DB_PATH = ROOT / "data" / "pricing" / "ohlcv_data.duckdb"

_SYM_RE = r"/([A-Z0-9]+)/1m/"
_GLOBS  = ", ".join(
    f"'{HISTORICAL_DIR / b / INTERVAL / '*.parquet'}'" for b in sorted(_binance_to_hl)
)


def _hl_universe() -> set[str]:
    """Current HL universe from the latest cached meta snapshot."""
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
        FROM read_parquet([{_GLOBS}], filename=true)
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).assign(symbol=lambda d: d["symbol"].map(to_hl)).dropna(subset=["symbol"])


def _build_hl(hl_only: set[str]) -> pd.DataFrame:
    """Pull daily OHLCV from HL DuckDB for symbols not covered by Binance."""
    if not hl_only:
        return pd.DataFrame()
    symbols_sql = ", ".join(f"'{s}/USDC:USDC'" for s in sorted(hl_only))
    conn = duckdb.connect(str(HL_DB_PATH), read_only=True)
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
    conn.close()
    return df


def build() -> None:
    t = time.time()
    store = HistoricalStore()

    # ── Binance ────────────────────────────────────────────────────────────────
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

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".tmp.parquet")
    try:
        df.to_parquet(tmp, index=False)
        tmp.rename(CACHE_PATH)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    print(f"\nWritten → {CACHE_PATH}")
    print(f"  total rows:    {len(df):,}")
    print(f"  total symbols: {df['symbol'].nunique()}  "
          f"({binance_df['symbol'].nunique()} Binance + {n_hl} HL-only)")
    print(f"  date range:    {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"  elapsed:       {time.time() - t:.1f}s")


if __name__ == "__main__":
    build()
