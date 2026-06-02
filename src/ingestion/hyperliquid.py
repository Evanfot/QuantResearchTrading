# %%
import asyncio
import concurrent.futures
import logging
import os

import ccxt
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
import datetime as dt

logger = logging.getLogger(__name__)

# === CONFIGURATION ===
from dotenv import load_dotenv
load_dotenv()

DB_PATH = 'data/pricing/ohlcv_data.duckdb'
LATEST_VIEW = "ohlcv_hyperliquid_latest"

TIMEFRAME = "1h"
TABLE_NAME = f"hyperliquid_{TIMEFRAME}"
LIMIT = 500

# === INITIALIZATION ===
exchange = None
con = None

def _init():
    global exchange, con
    exchange = ccxt.hyperliquid({'enableRateLimit': True})
    exchange.load_markets()

    con = duckdb.connect(DB_PATH)
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
)  """)

# Create or replace a view with the latest candle per symbol & timeframe
def update_latest_view():
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

def update_daily():
    # Ensure hyperliquid_1d exists with PK
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


def get_start_time(symbol: str) -> int:
    """Get the timestamp (ms) to start fetching from DuckDB contents."""
    try:
        result = con.execute(f""" SELECT MAX(datetime) FROM {TABLE_NAME} WHERE symbol = '{symbol}'""").fetchone()[0]

        if result is not None:
            # Resume from last candle + 1 ms
            return int(pd.Timestamp(result).timestamp() * 1000) + 1
    except Exception:
        pass

    three_months_ago = datetime.now(timezone.utc) - timedelta(days=730)
    return int(three_months_ago.timestamp() * 1000)

freq_ms = {
    '1m': 60_000,
    '5m': 300_000,
    '1h': 3_600_000,
    '1d': 86_400_000
}[TIMEFRAME]

async def fetch_symbol(symbol: str):
    """Fetch OHLCV for a single symbol and insert into DuckDB incrementally."""
    all_ohlcv = []
    since = get_start_time(symbol)
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
        df = pd.DataFrame(all_ohlcv, columns=["timestamp","open","high","low","close","volume"])
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol
        df["downloaded_at"] = pd.Timestamp.utcnow()
        df = df[["symbol","datetime","open","high","low","close","volume","downloaded_at"]]

        con.execute(f"INSERT OR IGNORE INTO {TABLE_NAME} SELECT * FROM df")
        return f"[{symbol}] Inserted {len(df)} rows"
    else:
        return f"[{symbol}] No new data"

async def dl(symbols=None):
    if symbols is None:
        symbols = [s for s in exchange.symbols if s.endswith("/USDC:USDC")]

    semaphore = asyncio.Semaphore(2)

    async def sem_task(symbol):
        async with semaphore:
            return await fetch_symbol(symbol)

    results = await asyncio.gather(*[sem_task(s) for s in symbols])
    updated = sum(1 for r in results if r and "Inserted" in r)
    logger.info(f"[data] updated for {updated} symbols")


def _run_async(coro):
    """Run a coroutine from sync code, even when an event loop is already running (e.g. Jupyter)."""
    try:
        asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(asyncio.run, coro).result()
    except RuntimeError:
        asyncio.run(coro)


def run_ohlcv_dl(symbols=None):
    _init()
    _run_async(dl(symbols))

if __name__ == "__main__":
    run_ohlcv_dl()
    update_daily()
    update_latest_view()
# %%
