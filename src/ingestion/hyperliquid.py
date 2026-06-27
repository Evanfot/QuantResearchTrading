# %%
import asyncio
import concurrent.futures
import logging

import ccxt
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
import datetime as dt

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

DB_PATH = 'data/pricing/ohlcv_data.duckdb'
LATEST_VIEW = "ohlcv_hyperliquid_latest"
TIMEFRAME = "1h"
TABLE_NAME = f"hyperliquid_{TIMEFRAME}"
LIMIT = 500

exchange = None


def _init_exchange():
    global exchange
    exchange = ccxt.hyperliquid({'enableRateLimit': True})
    exchange.load_markets()


def get_symbol_start_time_dict(symbols: list[str], con=None) -> dict[str, int]:
    """Query DuckDB for the resume timestamp (ms) per symbol.

    Opens its own read-only connection by default; pass ``con`` to reuse an
    existing connection (e.g. for tests against a temp DB).
    """
    three_months_ago = datetime.now(timezone.utc) - timedelta(days=730)
    default_ms = int(three_months_ago.timestamp() * 1000)

    owns_con = con is None
    try:
        if owns_con:
            con = duckdb.connect(DB_PATH, read_only=True)
        try:
            rows = con.execute(f"""
                SELECT symbol, MAX(datetime)
                FROM {TABLE_NAME}
                WHERE symbol IN ({', '.join(f"'{s}'" for s in symbols)})
                GROUP BY symbol
            """).fetchall()
        finally:
            if owns_con:
                con.close()
        known = {sym: int(pd.Timestamp(ts).timestamp() * 1000) + 1
                 for sym, ts in rows if ts is not None}
    except Exception:
        known = {}

    return {sym: known.get(sym, default_ms) for sym in symbols}


def update_daily():
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS hyperliquid_1d (
            symbol VARCHAR,
            datetime TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            downloaded_at TIMESTAMP,
            PRIMARY KEY(symbol, datetime)
        )
        """)
        latest_day = dt.date.today()
        con.execute(f"""INSERT OR REPLACE INTO hyperliquid_1d
        SELECT
            symbol,
            DATE_TRUNC('day', datetime) AS datetime,
            arg_min(open, datetime)  AS open,
            max(high)                AS high,
            min(low)                 AS low,
            arg_max(close, datetime) AS close,
            sum(volume)              AS volume,
            max(downloaded_at)       AS downloaded_at
        FROM hyperliquid_1h
        WHERE datetime >= DATE_TRUNC(
            'day',
            TIMESTAMP '{latest_day}' - INTERVAL 5 DAY
        )
        GROUP BY symbol, DATE_TRUNC('day', datetime)
        HAVING COUNT(open) > 0;""")
    finally:
        con.close()


def update_latest_view():
    con = duckdb.connect(DB_PATH)
    try:
        con.execute(f"""
        CREATE OR REPLACE VIEW {LATEST_VIEW} AS
        SELECT t.*
        FROM {TABLE_NAME} t
        JOIN (
            SELECT symbol, MAX(datetime) AS max_dt
            FROM {TABLE_NAME}
            GROUP BY symbol
        ) latest
        ON t.symbol = latest.symbol
        AND t.datetime = latest.max_dt
        """)
    finally:
        con.close()


freq_ms = {
    '1m': 60_000,
    '5m': 300_000,
    '1h': 3_600_000,
    '1d': 86_400_000
}[TIMEFRAME]


async def fetch_symbol(symbol: str, since: int, con: duckdb.DuckDBPyConnection):
    """Fetch OHLCV for a single symbol and insert into DuckDB incrementally."""
    all_ohlcv = []
    end_ms = exchange.milliseconds()
    while since <= end_ms:
        backoff = 5
        for attempt in range(5):
            try:
                ohlcv = await asyncio.to_thread(
                    exchange.fetch_ohlcv,
                    symbol,
                    timeframe=TIMEFRAME,
                    since=since,
                    limit=LIMIT
                )
                break
            except Exception as e:
                logger.warning(f"[data] {symbol} fetch error (attempt {attempt + 1}/5): {e}")
                if attempt == 4:
                    return f"[{symbol}] Giving up after 5 failed attempts"
                await asyncio.sleep(backoff)
                backoff *= 2
        else:
            break

        if not ohlcv:
            break

        all_ohlcv += ohlcv
        since = ohlcv[-1][0] + 1

    if all_ohlcv:
        df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol
        df["downloaded_at"] = pd.Timestamp.utcnow()
        df = df[["symbol", "datetime", "open", "high", "low", "close", "volume", "downloaded_at"]]
        con.execute(f"INSERT OR IGNORE INTO {TABLE_NAME} SELECT * FROM df")
        return f"[{symbol}] Inserted {len(df)} rows"
    else:
        return f"[{symbol}] No new data"


async def dl(start_times: dict[str, int], con: duckdb.DuckDBPyConnection):
    semaphore = asyncio.Semaphore(2)

    async def sem_task(symbol, since):
        async with semaphore:
            return await fetch_symbol(symbol, since, con)

    results = await asyncio.gather(*[sem_task(s, t) for s, t in start_times.items()])
    updated = sum(1 for r in results if r and "Inserted" in r)
    logger.info(f"[data] updated for {updated} symbols")


def _run_async(coro):
    """Run a coroutine from sync code, even when an event loop is already running."""
    try:
        asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        asyncio.run(coro)


def run_ohlcv_dl(symbols: list[str] | None = None, con=None):
    """Download OHLCV for ``symbols`` into DuckDB.

    Opens its own connection to ``DB_PATH`` by default; pass ``con`` to inject an
    existing connection (e.g. a temp DB in tests). An injected connection is left
    open for the caller to close.
    """
    _init_exchange()

    if symbols is None:
        symbols = [s for s in exchange.symbols if s.endswith("/USDC:USDC")]

    owns_con = con is None
    if owns_con:
        con = duckdb.connect(DB_PATH)
    try:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            symbol TEXT,
            datetime TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            downloaded_at TIMESTAMP,
            PRIMARY KEY(symbol, datetime)
        )""")
        start_times = get_symbol_start_time_dict(symbols, con=con)
        _run_async(dl(start_times, con))
    finally:
        if owns_con:
            con.close()


if __name__ == "__main__":
    run_ohlcv_dl()
    update_daily()
    update_latest_view()
# %%
