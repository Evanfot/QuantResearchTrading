import pytest
from src.main import get_execution_plan

# Size rounding (apply szDecimals to both target_qty and current_qty)
# Refer to: https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/examples/rounding.py

MAX_PRECISION = 6


# ---------------------------------------------------------------------------
# Price rounding — slippage_bps=0 to isolate the :.5g rounding logic
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("coin, ltp, sz_dec, expected_price", [
    # High price (>100k): rounds to whole number
    ("BTC", 105250.75, 5, 105250.0),

    # Mid price: 5 sig figs, precision capped by sz_dec (6-3 = 3 decimals)
    ("ETH", 2542.12345, 3, 2542.1),

    # Low price: 5 sig figs (6-2 = 4 decimals)
    ("SOL", 145.6789, 2, 145.68),

    # Very low price: :.5g gives 0.00012346 but round(..., 5) truncates to 0.00012
    ("MEME", 0.000123456, 1, 0.00012),
])
def test_price_rounding(run_context, coin, ltp, sz_dec, expected_price):
    # Use a delta large enough that all cases clear the $10 notional floor
    notional_safe_delta = max(100.0, 11.0 / ltp)
    order_intentions = {
        coin: {
            "coin": coin,
            "delta": notional_safe_delta,
            "target": notional_safe_delta,
            "current": 0,
            "side": "BUY",
        }
    }
    orders = get_execution_plan(
        order_intentions, {coin: ltp}, {coin: sz_dec},
        run_context["logger"], slippage_bps=0
    )
    assert len(orders) == 1, "Expected one order to be generated"
    assert orders[0]["limit_px"] == expected_price


# ---------------------------------------------------------------------------
# Slippage — verify BUY adds bps, SELL subtracts bps, before 5g rounding
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("side, ltp, slippage_bps, expected_price", [
    # BUY: price nudged up by 1 bps; 2500×1.0001=2500.25 → :.5g → 2500.2
    ("BUY",  2500.0, 1, 2500.2),
    # SELL: price nudged down by 1 bps
    ("SELL", 2500.0, 1, 2499.8),
    # Higher slippage
    ("BUY",  2500.0, 10, 2502.5),
])
def test_slippage(run_context, side, ltp, slippage_bps, expected_price):
    coin = "ETH"
    sz_dec = 3  # precision = max(0, 6-3) = 3
    # Drive side via target/current so get_execution_plan derives it correctly
    if side == "BUY":
        target, current = 6.0, 5.0   # delta = +1.0
    else:
        target, current = 5.0, 6.0   # delta = -1.0
    order_intentions = {
        coin: {
            "coin": coin,
            "target": target,
            "current": current,
        }
    }
    orders = get_execution_plan(
        order_intentions, {coin: ltp}, {coin: sz_dec},
        run_context["logger"], slippage_bps=slippage_bps
    )
    assert len(orders) == 1, "Expected one order to be generated"
    assert orders[0]["limit_px"] == expected_price

