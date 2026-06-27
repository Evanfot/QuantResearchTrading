"""Regression tests for Hyperliquid bulk_orders response classification.

Fixtures mirror the real status shapes and error strings observed in
logs/orders_mainnet.jsonl: `filled`, `resting`, and `error` with the three
recurring messages (note the `asset=N` suffix, which forces substring matching).
"""

import pytest

from src.execution import (
    ClassifiedOrder,
    ExecutionResult,
    OrderStatus,
    classify_order_responses,
    classify_status,
)

# Real status shapes (oids/values trimmed) ----------------------------------
FILLED = {"filled": {"totalSz": "0.001", "avgPx": "60000.0", "oid": 111}}
RESTING = {"resting": {"oid": 222}}
ERR_HALT = {"error": "Trading is halted."}
ERR_MARGIN = {"error": "Insufficient margin to place order. asset=2"}
ERR_MIN = {"error": "Order must have minimum value of $10. asset=214"}
ERR_OTHER = {"error": "Some brand new error nobody has seen. asset=9"}


def _order(coin, is_buy=True):
    return {"coin": coin, "is_buy": is_buy, "sz": 0.001, "limit_px": 60000.0}


# --- classify_status: single status mapping --------------------------------

@pytest.mark.parametrize("status,expected", [
    (FILLED, OrderStatus.FILLED),
    (RESTING, OrderStatus.OPEN),
    (ERR_HALT, OrderStatus.TRADING_HALTED),
    (ERR_MARGIN, OrderStatus.INSUFFICIENT_MARGIN),
    (ERR_MIN, OrderStatus.MIN_NOTIONAL),
    (ERR_OTHER, OrderStatus.REJECTED),
])
def test_classify_status_known_shapes(status, expected):
    assert classify_status(status)[0] is expected


def test_error_message_preserved_for_errors():
    status, msg = classify_status(ERR_MARGIN)
    assert status is OrderStatus.INSUFFICIENT_MARGIN
    assert msg == "Insufficient margin to place order. asset=2"


def test_filled_and_resting_have_no_error_message():
    assert classify_status(FILLED)[1] is None
    assert classify_status(RESTING)[1] is None


def test_error_matching_is_substring_not_equality():
    # The asset=N suffix must not defeat the match.
    assert classify_status({"error": "Trading is halted. asset=99"})[0] is OrderStatus.TRADING_HALTED


def test_error_matching_is_case_insensitive():
    assert classify_status({"error": "TRADING IS HALTED."})[0] is OrderStatus.TRADING_HALTED


@pytest.mark.parametrize("weird", [None, "ok", 42, [], {}, {"surprise": {"oid": 1}}])
def test_unparseable_status_is_unknown(weird):
    assert classify_status(weird)[0] is OrderStatus.UNKNOWN


# --- classify_order_responses: batch behaviour -----------------------------

def test_mixed_batch_partitions_accepted_and_rejected():
    orders = [_order("BTC"), _order("ETH"), _order("SOL"), _order("DOGE")]
    statuses = [FILLED, RESTING, ERR_HALT, ERR_MIN]
    result = classify_order_responses(orders, statuses)

    assert isinstance(result, ExecutionResult)
    assert {c.coin for c in result.accepted} == {"BTC", "ETH"}
    assert {c.coin for c in result.rejected} == {"SOL", "DOGE"}
    assert result.halted is True
    assert result.count_mismatch is False


def test_resting_counts_as_accepted():
    result = classify_order_responses([_order("BTC")], [RESTING])
    assert len(result.accepted) == 1
    assert result.accepted[0].status is OrderStatus.OPEN


def test_halted_flag_only_true_when_halt_present():
    result = classify_order_responses([_order("BTC")], [ERR_MARGIN])
    assert result.rejected and result.halted is False


def test_the_incident_top_level_ok_with_per_order_error_not_accepted():
    """The bug from 2026-06-16: HL returns ok with per-order {'error': ...}.

    Every order must be classified rejected, none accepted, halted=True.
    """
    orders = [_order("BTC"), _order("ETH"), _order("SOL")]
    statuses = [ERR_HALT, ERR_HALT, ERR_HALT]
    result = classify_order_responses(orders, statuses)
    assert result.accepted == []
    assert len(result.rejected) == 3
    assert result.halted is True


def test_unknown_status_is_not_accepted():
    # A status shape we don't recognise must not be silently treated as submitted.
    result = classify_order_responses([_order("BTC")], [{"mystery": 1}])
    assert result.accepted == []
    assert result.rejected[0].status is OrderStatus.UNKNOWN


def test_count_mismatch_flagged_and_overlap_still_classified():
    orders = [_order("BTC"), _order("ETH")]
    statuses = [FILLED]  # exchange returned fewer statuses than orders
    result = classify_order_responses(orders, statuses)
    assert result.count_mismatch is True
    assert len(result.classified) == 1
    assert result.classified[0].coin == "BTC"


def test_classified_order_carries_through_submitted_order():
    co = classify_order_responses([_order("BTC", is_buy=False)], [FILLED]).classified[0]
    assert isinstance(co, ClassifiedOrder)
    assert co.order["is_buy"] is False
    assert co.raw_status is FILLED
