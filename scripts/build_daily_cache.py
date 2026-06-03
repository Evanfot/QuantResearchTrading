"""
Build and maintain the daily OHLCV cache.

cache-builder service (nightly at 23:45 UTC):
  build_nightly() — runs Binance incremental resample and HL download in
    parallel, then combines both into daily_ohlcv.parquet.
  On first launch a full Binance history build is done before entering the loop.

trader data task (daily at 00:01 UTC):
  append_today_mids() — upserts today's live mid prices into daily_ohlcv.parquet.
"""

import json
import sys
import threading
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


def _query_binance(store: HistoricalStore, since: pd.Timestamp | None = None) -> pd.DataFrame:
    where = f"WHERE open_time >= TIMESTAMP '{since.strftime('%Y-%m-%d')}'" if since else ""
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
        {where}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).assign(
        symbol=lambda d: d["symbol"].map(to_hl),
        date=lambda d: pd.to_datetime(d["date"]),
    ).dropna(subset=["symbol"])


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
    """Resample Binance 1m klines → binance_daily.parquet.

    If a cache already exists, resamples from the last cached date onward and
    upserts — filling however many days are missing. Falls back to a full build
    if no cache exists yet.
    """
    t = time.time()
    store = HistoricalStore()

    since = None
    if BINANCE_CACHE_PATH.exists():
        try:
            existing = pd.read_parquet(BINANCE_CACHE_PATH)
            existing["date"] = pd.to_datetime(existing["date"])
            since = existing["date"].max()
            if not (2020 <= since.year <= 2100):
                raise ValueError(f"date out of expected range: {since}")
        except Exception as e:
            print(f"Binance: cache unreadable ({e}), falling back to full build …")
            since = None

    if since is not None:
        print(f"Binance: incremental from {since.date()} ({len(_binance_to_hl)} symbols) …")
        recent = _query_binance(store, since=since)
        df = (
            pd.concat([existing[existing["date"] < since], recent], ignore_index=True)
            .sort_values(["date", "symbol"])
            .reset_index(drop=True)
        )
        print(f"  {recent['symbol'].nunique()} symbols, {len(recent):,} rows added/updated")
    else:
        print(f"Binance: full build ({len(_binance_to_hl)} symbols) …")
        df = _query_binance(store)
        print(f"  {len(df):,} rows, {df['symbol'].nunique()} symbols")

    _write_atomic(df, BINANCE_CACHE_PATH)
    print(f"Written → {BINANCE_CACHE_PATH}  ({time.time() - t:.1f}s)", flush=True)


def _download_hl() -> None:
    """Download HL 1h data via CCXT and resample to daily in ohlcv_data.duckdb."""
    from src.ingestion.hyperliquid import run_ohlcv_dl, update_daily
    print("HL: downloading 1h data via CCXT …", flush=True)
    run_ohlcv_dl()
    update_daily()
    print("HL: done", flush=True)


def build() -> None:
    """Combine binance_daily.parquet + HL-only symbols → daily_ohlcv.parquet."""
    t = time.time()

    if BINANCE_CACHE_PATH.exists():
        binance_df = pd.read_parquet(BINANCE_CACHE_PATH)
        print(f"Binance: {binance_df['symbol'].nunique()} symbols (from cache)")
    else:
        store = HistoricalStore()
        print(f"Binance: full resample ({len(_binance_to_hl)} symbols) …")
        binance_df = _query_binance(store)
        print(f"  {len(binance_df):,} rows, {binance_df['symbol'].nunique()} symbols")

    hl_only = _hl_universe() - set(_hl_to_binance.keys())
    print(f"HL:      fetching {len(hl_only)} symbols from DuckDB …")
    hl_df = _build_hl(hl_only)
    n_hl = hl_df["symbol"].nunique() if not hl_df.empty else 0
    print(f"  {len(hl_df):,} rows, {n_hl} symbols")

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


def build_nightly() -> None:
    """Run Binance incremental resample and HL download in parallel, then combine."""
    errors: list[Exception] = []

    def _run(fn):
        try:
            fn()
        except Exception as e:
            errors.append(e)

    t_binance = threading.Thread(target=_run, args=(build_binance,))
    t_hl      = threading.Thread(target=_run, args=(_download_hl,))
    t_binance.start()
    t_hl.start()
    t_binance.join()
    t_hl.join()

    if errors:
        raise errors[0]

    build()


def append_today_mids(mids_csv: Path) -> None:
    """Upsert today's live mid prices as the latest row in daily_ohlcv.parquet."""
    if not CACHE_PATH.exists():
        print("append_today_mids: cache not found, skipping", flush=True)
        return
    if not mids_csv.exists():
        print("append_today_mids: mids.csv not found, skipping", flush=True)
        return

    mids = pd.read_csv(mids_csv, index_col="symbol")["mid"].astype(float)
    today = pd.Timestamp.utcnow().normalize()

    df = pd.read_parquet(CACHE_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] < today]

    rows = [
        {
            "date": today, "symbol": s,
            "open": mids[s], "high": mids[s], "low": mids[s], "close": mids[s],
            "volume": float("nan"), "quote_volume": float("nan"),
        }
        for s in df["symbol"].unique() if s in mids.index
    ]
    if rows:
        df = (
            pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
            .sort_values(["date", "symbol"])
            .reset_index(drop=True)
        )
    _write_atomic(df, CACHE_PATH)
    print(f"append_today_mids: {len(rows)} symbols → {today.date()}", flush=True)


if __name__ == "__main__":
    import time as _time
    import datetime as _dt

    def _seconds_until_next_run() -> float:
        now = _dt.datetime.now(_dt.timezone.utc)
        target = now.replace(hour=23, minute=45, second=0, microsecond=0)
        if now >= target:
            target += _dt.timedelta(days=1)
        return (target - now).total_seconds()

    # Full Binance build on first launch (no existing cache), then HL, then combine
    build_binance()
    _download_hl()
    build()

    while True:
        delay = _seconds_until_next_run()
        print(f"[cache-builder] next build in {delay / 3600:.1f}h  (23:45 UTC)", flush=True)
        _time.sleep(delay)
        build_nightly()
