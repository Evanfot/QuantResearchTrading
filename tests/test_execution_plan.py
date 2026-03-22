import pytest
from unittest.mock import MagicMock

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
        "BTC": {"target": 0.001, "current": 0},   # ~$60 buy
        "ETH": {"target": 0,     "current": 0.1},  # ~$250 sell
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    btc = next(o for o in orders if o["coin"] == "BTC")
    assert btc["is_buy"] is True
    assert btc["sz"] == 0.001

    eth = next(o for o in orders if o["coin"] == "ETH")
    assert eth["is_buy"] is False
    assert eth["sz"] == 0.1


def test_delta_less_than_10_no_order(mock_context):
    """Delta < $10 should produce no order."""
    coin = "SOL"
    # Delta is 0.05 SOL = $5.00
    order_intentions = {
        coin: {"target": 1.05, "current": 1.0}
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 0


def test_delta_greater_than_10_generates_order(mock_context):
    """Delta > $10 should produce an order."""
    coin = "SOL"
    # Delta is 0.2 SOL = $20.00
    order_intentions = {
        coin: {"target": 1.2, "current": 1.0}
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 1
    assert orders[0]["coin"] == coin


def test_force_close_small_position(mock_context):
    """Target=0 with an existing position should generate a close order regardless of $10 minimum."""
    coin = "ETH"
    order_intentions = {
        coin: {"target": 0, "current": 0.002}  # $5 position — below floor but must close
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 1
    assert orders[0]["sz"] == 0.002
    assert orders[0]["is_buy"] is False
