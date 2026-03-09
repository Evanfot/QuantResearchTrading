
import pytest
from src.main import get_execution_plan
# Size rounding (apply szDecimals to both target_qty and current_qty)
# Refer to: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/examples/rounding.py

# Assuming max_precision is 6 based on your logic
MAX_PRECISION = 6

# No imports needed for fixtures! 

@pytest.mark.parametrize("coin, ltp, sz_dec, expected_price", [
    # Case: High price (>100k) should round to whole number
    ("BTC", 105250.75, 5, 105251.0), 
    
    # Case: Mid price, 5 sig figs, precision capped by sz_dec (6-3 = 3 decimals)
    ("ETH", 2542.12345, 3, 2542.1), 
    
    # Case: Low price, 5 sig figs (6-2 = 4 decimals)
    # ("SOL", 145.6789, 2, 145.68),
    
    # Case: Very low price asset
    # ("MEME", 0.000123456, 1, 0.00012346), 
])
def test_price_rounding(run_context, coin, ltp, sz_dec, expected_price):
    # run_context is automatically injected from conftest.py
    order_intentions = {
        coin: {"coin": coin, "delta": 100.013546, "target": 100.013546, "current": 0, "side": "BUY"}
    }
    
    # We override the LTP for specific test cases
    custom_ltps = {coin: ltp}
    sz_decimals = {coin: sz_dec}
    orders, _ = get_execution_plan(
        order_intentions, [], custom_ltps, sz_decimals, run_context["logger"]
    )
    # assert orders[0]["sz"] == expected_size
    assert orders[0]["limit_px"] == expected_price