# %%
import ccxt
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
import datetime as dt
import duckdb
from tqdm.asyncio import tqdm_asyncio
import os

# === CONFIGURATION ===
from dotenv import load_dotenv
load_dotenv()

DB_PATH = 'data/pricing/ohlcv_data.duckdb'
LATEST_VIEW = "ohlcv_hyperliquid_latest"

TIMEFRAME = "1h"
TABLE_NAME = f"hyperliquid_{TIMEFRAME}"
LIMIT = 500

# === INITIALIZATION ===
exchange = ccxt.hyperliquid({'enableRateLimit': True})
exchange.load_markets()

# === DUCKDB CONNECTION ===
con = duckdb.connect(DB_PATH)

# Create table if it doesn't exist
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
        try:
            ohlcv = await asyncio.to_thread(
                exchange.fetch_ohlcv,
                symbol,
                timeframe=TIMEFRAME,
                since=since,
                limit=LIMIT
            )
        except Exception as e:
            print(f"[{symbol}] Error: {e}")
            await asyncio.sleep(2)
            continue

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

async def dl():
    symbols = exchange.symbols
    print(f"Fetching {len(symbols)} symbols...")

    semaphore = asyncio.Semaphore(5)

    async def sem_task(symbol):
        async with semaphore:
            return await fetch_symbol(symbol)

    results = []
    tasks = [sem_task(s) for s in symbols]

    for coro in tqdm_asyncio.as_completed(
        tasks,
        total=len(tasks),
        desc="Progress"
    ):
        msg = await coro
        results.append(msg)

    print("\n=== Summary ===")
    for r in results:
        print(r)


def run_ohlcv_dl():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop → safe to use asyncio.run
        return asyncio.run(dl())
    else:
        # Already running → schedule task
        return loop.create_task(dl())

if __name__ == "__main__":
    run_ohlcv_dl()
    update_daily()
    update_latest_view()