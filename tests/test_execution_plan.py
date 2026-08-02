import pytest
from unittest.mock import MagicMock

from src.execution import get_execution_plan

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


def test_reduce_only_classification(mock_context):
    """reduce_only is True only for trades that shrink |position| without flipping
    sign; opens, increases and flips (which need fresh margin) stay reduce_only=False."""
    order_intentions = {
        "BTC": {"target": 0.0005, "current": 0.002},   # long trimmed toward 0 → reduce
        "ETH": {"target": 0.02,   "current": 0.005},   # long increased → not reduce
        "SOL": {"target": 0.0,    "current": -1.0},    # short covered → reduce
    }
    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )
    by_coin = {o["coin"]: o for o in orders}
    assert by_coin["BTC"]["reduce_only"] is True
    assert by_coin["ETH"]["reduce_only"] is False
    assert by_coin["SOL"]["reduce_only"] is True


def test_flip_is_not_reduce_only(mock_context):
    """A position that flips long→short overshoots zero and needs margin for the new
    short leg, so it must not be marked reduce_only."""
    order_intentions = {
        "ETH": {"target": -0.02, "current": 0.02}  # +$50 long → -$50 short
    }
    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )
    assert len(orders) == 1
    assert orders[0]["is_buy"] is False
    assert orders[0]["reduce_only"] is False


def test_dust_close_below_min_notional_skipped(mock_context):
    """A sub-$10 residual ('dust') close is skipped: HL rejects orders under $10,
    so such a position is unclosable via a normal order and shouldn't be resubmitted
    as a guaranteed MIN_NOTIONAL rejection every tick."""
    coin = "ETH"
    order_intentions = {
        coin: {"target": 0, "current": 0.002}  # $5 position — below the $10 floor
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert orders == []


def test_close_above_min_notional_is_reduce_only(mock_context):
    """A full close large enough to clear the $10 floor is submitted and flagged
    reduce_only so it never reserves margin during the two-phase submission."""
    coin = "ETH"
    order_intentions = {
        coin: {"target": 0, "current": 0.02}  # $50 position
    }

    orders = get_execution_plan(
        order_intentions, mock_context["ltps"], mock_context["sz_decimals"], mock_context["logger"]
    )

    assert len(orders) == 1
    assert orders[0]["sz"] == 0.02
    assert orders[0]["is_buy"] is False
    assert orders[0]["reduce_only"] is True


def test_sub10_after_rounding_is_dropped(mock_context):
    """MIN_NOTIONAL leak: an order whose value clears $10 on the un-rounded delta but
    falls below $10 once size is rounded to szDecimals must be dropped (HL rejects it).
    """
    ltps = {"ENA": 9.70}
    sz_decimals = {"ENA": 0}                       # whole units → rounding shaves a lot
    # 1.04 * 9.70 = $10.09 passes the pre-rounding floor, but rounds to 1 unit = $9.70
    order_intentions = {"ENA": {"target": 1.04, "current": 0}}

    orders = get_execution_plan(order_intentions, ltps, sz_decimals, mock_context["logger"])

    assert orders == [], f"sub-$10 rounded order should be dropped, got {orders}"


def test_above10_after_rounding_is_kept(mock_context):
    """An order still worth ≥ $10 after rounding is kept."""
    ltps = {"ENA": 9.70}
    sz_decimals = {"ENA": 0}
    order_intentions = {"ENA": {"target": 3.0, "current": 0}}   # 3 × $9.70 = $29.10

    orders = get_execution_plan(order_intentions, ltps, sz_decimals, mock_context["logger"])

    assert len(orders) == 1
    assert orders[0]["coin"] == "ENA"
    assert orders[0]["sz"] == 3
