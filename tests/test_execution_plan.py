import pytest
from unittest.mock import MagicMock

# Assuming your function is in execution_logic.py
# from execution_logic import get_execution_plan
from src.main import get_execution_plan

@pytest.fixture
def mock_context():
    """Provides common metadata for execution tests."""
    return {
        "logger": MagicMock(),
        "sz_decimals": {"BTC": 5, "ETH": 3, "SOL": 2},
        "ltps": {"BTC": 60000.0, "ETH": 2500.0, "SOL": 100.0}
    }

def test_basic_buy_sell_above_10_dollars(mock_context):
    """Test basic buy and sell orders > $10 are processed correctly."""
    order_intentions = {
        "BTC": {"delta": 0.001, "target": 0.001, "current": 0, "side": "buy"}, # ~$60
        "ETH": {"delta": -0.1, "target": 0, "current": 0.1, "side": "SELL"}    # ~$250
    }
    
    orders, cancels = get_execution_plan(
        order_intentions, [], mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    # Assert BTC Buy
    btc = next(o for o in orders if o["coin"] == "BTC")
    assert btc["is_buy"] is True
    assert btc["sz"] == 0.001
    
    # Assert ETH Sell
    eth = next(o for o in orders if o["coin"] == "ETH")
    assert eth["is_buy"] is False
    assert eth["sz"] == 0.1
    assert len(cancels) == 0

def test_case_1_delta_less_than_10_no_cancel(mock_context):
    """Case 1: Delta < $10. Should result in no new order and NO cancellation."""
    coin = "SOL"
    # Delta is 0.05 SOL = $5.00
    order_intentions = {
        coin: {"delta": 0.05, "target": 1.05, "current": 1.0, "side": "buy"}
    }
    open_orders = [{"coin": coin, "oid": 999, "limit_px": 95.0}]

    orders, cancels = get_execution_plan(
        order_intentions, open_orders, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 0
    assert len(cancels) == 0

def test_case_2_delta_greater_than_10_with_cancel(mock_context):
    """Case 2: Delta > $10. Should result in a new order and the existing order in cancel_list."""
    coin = "SOL"
    # Delta is 0.2 SOL = $20.00
    order_intentions = {
        coin: {"delta": 0.2, "target": 1.2, "current": 1.0, "side": "buy"}
    }
    open_orders = [{"coin": coin, "oid": 888, "limit_px": 90.0}]

    orders, cancels = get_execution_plan(
        order_intentions, open_orders, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 1
    assert orders[0]["coin"] == coin
    assert len(cancels) == 1
    assert cancels[0]["oid"] == 888

def test_force_close_small_position(mock_context):
    """Test that target=0 overrides the $10 minimum (dust closing)."""
    coin = "ETH"
    # Current pos is $5.00. Target 0 means we should close it.
    order_intentions = {
        coin: {"delta": -0.002, "target": 0, "current": 0.002, "side": "SELL"}
    }

    orders, cancels = get_execution_plan(
        order_intentions, [], mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 1
    assert orders[0]["sz"] == 0.002
    assert orders[0]["is_buy"] is False
