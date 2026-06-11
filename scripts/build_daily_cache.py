"""
Build and maintain the daily OHLCV cache.

Binance data is provided by the external binance-klines project and written to
  $BINANCE_KLINES_DIR/klines/processed/daily_closes.parquet  (available ~03:00 UTC).

Nightly at 23:45 UTC (trader data task):
  build() — downloads HL OHLCV, then reads daily_closes.parquet (Binance) +
    HL DuckDB → daily_ohlcv.parquet.

Today's live prices are NOT written to the parquet. They are appended in-memory
at intent time via get_final_pricing() in src/main.py.
"""

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

import duckdb
import pandas as pd

CACHE_PATH = ROOT / "data" / "cache" / "daily_ohlcv.parquet"
HL_DB_PATH = ROOT / "data" / "pricing" / "ohlcv_data.duckdb"

_BINANCE_KLINES_DIR = Path(
    os.environ.get("BINANCE_KLINES_DIR", "~/projects/binance-klines")
).expanduser()
_DAILY_CLOSES_PATH = _BINANCE_KLINES_DIR / "klines" / "processed" / "daily_closes.parquet"


def _hl_universe() -> set[str]:
    meta_dir = ROOT / "data" / "hyperliquid_meta"
    latest = sorted(meta_dir.glob("meta_*.json"))[-1]
    with open(latest) as f:
        return {x["name"].upper() for x in json.load(f)["universe"]}


def _load_binance_closes() -> pd.DataFrame:
    """Read daily_closes.parquet → long DataFrame with full OHLCV, HL-mapped symbol names."""
    df = pd.read_parquet(_DAILY_CLOSES_PATH)

    # Wide format: DatetimeIndex × symbol columns (no 'symbol' column)
    if "symbol" not in df.columns:
        df.index = pd.to_datetime(df.index)
        df = (
            df.rename_axis("date")
              .reset_index()
              .melt(id_vars="date", var_name="symbol", value_name="close")
        )
        for col in ("open", "high", "low"):
            df[col] = df["close"]
        df["volume"] = float("nan")
        df["quote_volume"] = float("nan")
    else:
        df["date"] = pd.to_datetime(df["date"])

    # Map Binance names (BTCUSDT) → HL names (BTC)
    df["symbol"] = df["symbol"].str.replace(r"(USDT|BUSD)$", "", regex=True)
    return df.dropna(subset=["symbol", "close"])


def _build_hl(hl_only: set[str]) -> pd.DataFrame:
    if not hl_only:
        return pd.DataFrame()
    if not HL_DB_PATH.exists():
        return pd.DataFrame()
    symbols_sql = ", ".join(f"'{s}/USDC:USDC'" for s in sorted(hl_only))
    try:
        conn = duckdb.connect(str(HL_DB_PATH), read_only=True)
    except Exception as e:
        print(f"HL: could not open DuckDB ({e}), skipping HL-only symbols", flush=True)
        return pd.DataFrame()
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


def _download_hl() -> None:
    """Download HL 1h data via CCXT and resample to daily in ohlcv_data.duckdb."""
    from src.ingestion.hyperliquid import run_ohlcv_dl, update_daily
    print("HL: downloading 1h data via CCXT …", flush=True)
    run_ohlcv_dl()
    update_daily()
    print("HL: done", flush=True)


def build() -> None:
    """Read Binance daily_closes.parquet + HL-only symbols → daily_ohlcv.parquet."""
    t = time.time()

    # ── Binance ────────────────────────────────────────────────────────────────
    print(f"Binance: reading from {_DAILY_CLOSES_PATH} …")
    binance_df = _load_binance_closes()
    binance_covered = set(binance_df["symbol"].unique())
    print(f"  {len(binance_df):,} rows, {len(binance_covered)} symbols")

    # ── HL fallback for coins not in Binance data ──────────────────────────────
    hl_only = _hl_universe() - binance_covered
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
          f"({len(binance_covered)} Binance + {n_hl} HL-only)")
    print(f"  date range:    {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"  elapsed:       {time.time() - t:.1f}s", flush=True)


if __name__ == "__main__":
    import datetime as _dt

    def _seconds_until_next_run() -> float:
        now = _dt.datetime.now(_dt.timezone.utc)
        target = now.replace(hour=23, minute=45, second=0, microsecond=0)
        if now >= target:
            target += _dt.timedelta(days=1)
        return (target - now).total_seconds()

    _download_hl()
    build()

    while True:
        delay = _seconds_until_next_run()
        print(f"[cache-builder] next build in {delay / 3600:.1f}h  (23:45 UTC)", flush=True)
        time.sleep(delay)
        _download_hl()
        build()
