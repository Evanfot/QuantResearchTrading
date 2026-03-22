import duckdb
import pytest
import pandas as pd
from unittest.mock import patch

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


def test_run_ohlcv_dl_btc_end_to_end(temp_con):
    """End-to-end: fetches BTC/USDC:USDC from Hyperliquid and inserts into DB."""
    with patch.object(hl_module, "con", temp_con):
        run_ohlcv_dl(symbols=["BTC/USDC:USDC"])

    rows = temp_con.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE symbol = 'BTC/USDC:USDC'"
    ).df()

    assert len(rows) > 0, "Expected rows for BTC/USDC:USDC but found none"
    assert set(rows.columns) >= {"symbol", "datetime", "open", "high", "low", "close", "volume"}
    assert (rows["symbol"] == "BTC/USDC:USDC").all()
    assert rows["close"].notna().all()
    assert (rows["volume"] >= 0).all()
