from datetime import datetime

import duckdb
import pytest
import pandas as pd

import src.ingestion.hyperliquid as hl_module
from src.ingestion.hyperliquid import run_ohlcv_dl, TABLE_NAME


@pytest.fixture
def temp_con(tmp_path):
    """In-memory DuckDB with the same schema as production."""
    con = duckdb.connect(str(tmp_path / "test.duckdb"))
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
        )
    """)
    return con


class _FakeExchange:
    """Minimal stand-in for ccxt.hyperliquid that serves canned candles once.

    Records the `since` passed to each fetch so tests can assert the resume
    timestamp, and returns an empty page on the second call so the paginating
    fetch loop in fetch_symbol terminates deterministically.
    """

    def __init__(self, candles):
        self._candles = candles
        self.symbols = ["BTC/USDC:USDC"]
        self.calls = []          # (symbol, since) per fetch_ohlcv call
        self._served = False

    def milliseconds(self):
        # Far enough ahead that the loop is gated by the empty 2nd page, not by time.
        return self._candles[-1][0] + hl_module.freq_ms * 1000

    def fetch_ohlcv(self, symbol, timeframe=None, since=0, limit=500):
        self.calls.append((symbol, since))
        if self._served:
            return []
        self._served = True
        return [list(c) for c in self._candles]


def _use_fake_exchange(monkeypatch, fake):
    monkeypatch.setattr(hl_module, "exchange", fake)
    monkeypatch.setattr(hl_module, "_init_exchange", lambda: None)


# --- Hermetic unit tests (no network) --------------------------------------

def test_run_ohlcv_dl_inserts_mocked_candles(temp_con, monkeypatch):
    """Canned candles from a mocked exchange are written with the right schema/values."""
    base = int(pd.Timestamp(datetime(2025, 1, 1)).timestamp() * 1000)
    candles = [
        [base,                         100.0, 110.0, 90.0, 105.0, 1000.0],
        [base + hl_module.freq_ms,     105.0, 115.0, 95.0, 108.0, 1200.0],
        [base + 2 * hl_module.freq_ms, 108.0, 118.0, 98.0, 112.0,  900.0],
    ]
    _use_fake_exchange(monkeypatch, _FakeExchange(candles))

    run_ohlcv_dl(symbols=["BTC/USDC:USDC"], con=temp_con)

    rows = temp_con.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE symbol = 'BTC/USDC:USDC' ORDER BY datetime"
    ).df()
    assert len(rows) == 3
    assert list(rows["close"]) == [105.0, 108.0, 112.0]
    assert set(rows.columns) >= {"symbol", "datetime", "open", "high", "low", "close", "volume", "downloaded_at"}
    assert (rows["symbol"] == "BTC/USDC:USDC").all()


def test_run_ohlcv_dl_resumes_from_last_datetime_and_dedups(temp_con, monkeypatch):
    """Resumes fetch from existing max(datetime)+1ms and INSERT OR IGNOREs overlaps."""
    existing_dt = datetime(2025, 1, 1)
    temp_con.execute(
        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ["BTC/USDC:USDC", existing_dt, 1.0, 1.0, 1.0, 1.0, 1.0, datetime(2025, 1, 1)],
    )
    expected_since = int(pd.Timestamp(existing_dt).timestamp() * 1000) + 1

    d0 = int(pd.Timestamp(existing_dt).timestamp() * 1000)   # same PK as existing -> ignored
    d1 = d0 + hl_module.freq_ms                              # new candle -> inserted
    candles = [
        [d0, 9.0, 9.0, 9.0, 9.0, 9.0],
        [d1, 6.0, 7.0, 5.0, 6.5, 50.0],
    ]
    fake = _FakeExchange(candles)
    _use_fake_exchange(monkeypatch, fake)

    run_ohlcv_dl(symbols=["BTC/USDC:USDC"], con=temp_con)

    # Resume timestamp threaded through get_symbol_start_time_dict into fetch_ohlcv.
    assert fake.calls[0] == ("BTC/USDC:USDC", expected_since)

    rows = temp_con.execute(f"SELECT close FROM {TABLE_NAME} ORDER BY datetime").df()
    assert len(rows) == 2
    assert rows["close"].iloc[0] == 1.0   # existing row preserved (not overwritten by d0)
    assert rows["close"].iloc[1] == 6.5   # new candle appended


# --- Live integration test (network; excluded from default run) ------------

@pytest.mark.integration
def test_run_ohlcv_dl_btc_end_to_end(temp_con):
    """End-to-end: fetches BTC/USDC:USDC from the live Hyperliquid API and inserts.

    Canary for ccxt/HL contract breaks (e.g. the spot-market NoneType regression).
    Run with `pytest -m integration`.
    """
    run_ohlcv_dl(symbols=["BTC/USDC:USDC"], con=temp_con)

    rows = temp_con.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE symbol = 'BTC/USDC:USDC'"
    ).df()

    assert len(rows) > 0, "Expected rows for BTC/USDC:USDC but found none"
    assert set(rows.columns) >= {"symbol", "datetime", "open", "high", "low", "close", "volume"}
    assert (rows["symbol"] == "BTC/USDC:USDC").all()
    assert rows["close"].notna().all()
    assert (rows["volume"] >= 0).all()
