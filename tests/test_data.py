import pandas as pd

from src.data import load_ohlcv_for_alphas


def test_load_ohlcv_for_alphas_empty_universe_returns_empty_frames():
    """A cold/stale DB whose symbols don't yet match the tradable universe leaves
    ``universe`` empty; this must not build ``WHERE symbol IN ()`` (a DuckDB
    syntax error) and crash the intent task.
    """
    o, h, l, c, v = load_ohlcv_for_alphas([])

    for frame in (o, h, l, c, v):
        assert isinstance(frame, pd.DataFrame)
        assert frame.empty
