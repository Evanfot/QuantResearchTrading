"""Tests for the pre-flight circuit breaker (preflight_check).

Caps below are illustrative; main.py derives the notional ones from live equity.
"""

import pytest

from src.execution import PreflightResult, preflight_check

CAPS = dict(
    max_order_notional_usd=5_000.0,
    max_gross_notional_usd=20_000.0,
    max_order_count=120,
    max_price_deviation=0.05,
)


def _order(coin, sz, limit_px):
    return {"coin": coin, "is_buy": sz > 0, "sz": abs(sz), "limit_px": limit_px}


def test_clean_batch_passes():
    orders = [_order("BTC", 0.01, 60000.0), _order("ETH", 0.1, 2500.0)]
    ltps = {"BTC": 60000.0, "ETH": 2500.0}
    res = preflight_check(orders, ltps, **CAPS)
    assert isinstance(res, PreflightResult)
    assert res.ok is True
    assert res.violations == []


def test_empty_batch_passes():
    res = preflight_check([], {}, **CAPS)
    assert res.ok is True


def test_per_order_notional_cap_trips():
    # 1 BTC @ 60k = $60k, over the $5k per-order cap (fat-finger size bug).
    orders = [_order("BTC", 1.0, 60000.0)]
    ltps = {"BTC": 60000.0}
    res = preflight_check(orders, ltps, **CAPS)
    assert res.ok is False
    assert any("per-order cap" in v for v in res.violations)


def test_gross_notional_cap_trips():
    # Many mid-size orders that individually pass but together exceed the batch cap.
    orders = [_order(f"C{i}", 1.0, 4000.0) for i in range(6)]  # 6 x $4k = $24k > $20k
    ltps = {f"C{i}": 4000.0 for i in range(6)}
    res = preflight_check(orders, ltps, **CAPS)
    assert res.ok is False
    assert any("batch cap" in v for v in res.violations)


def test_order_count_cap_trips():
    orders = [_order(f"C{i}", 0.001, 100.0) for i in range(121)]
    ltps = {f"C{i}": 100.0 for i in range(121)}
    res = preflight_check(orders, ltps, **CAPS)
    assert res.ok is False
    assert any("order count" in v for v in res.violations)


def test_price_deviation_cap_trips():
    # limit_px 10% away from the mid it should have been derived from.
    orders = [_order("BTC", 0.01, 66000.0)]
    ltps = {"BTC": 60000.0}
    res = preflight_check(orders, ltps, **CAPS)
    assert res.ok is False
    assert any("deviates" in v for v in res.violations)


def test_small_price_deviation_within_cap_passes():
    # 1 bp slippage (what get_execution_plan actually applies) is fine.
    orders = [_order("BTC", 0.01, 60006.0)]
    ltps = {"BTC": 60000.0}
    assert preflight_check(orders, ltps, **CAPS).ok is True


def test_missing_reference_price_trips():
    orders = [_order("WEIRD", 1.0, 5.0)]
    res = preflight_check(orders, {}, **CAPS)
    assert res.ok is False
    assert any("no reference price" in v for v in res.violations)


def test_multiple_violations_all_reported():
    orders = [
        _order("BTC", 1.0, 60000.0),     # per-order + contributes to gross
        _order("ETH", 0.01, 3000.0),     # price deviation vs 2500 mid (20%)
    ]
    ltps = {"BTC": 60000.0, "ETH": 2500.0}
    res = preflight_check(orders, ltps, **CAPS)
    assert res.ok is False
    assert len(res.violations) >= 2


def test_realistic_rebalance_passes_equity_scaled_caps():
    # ~$1.8k equity, caps as main.py derives them (3x / 12x). A batch like the
    # observed max (per-order ~$1.2k, gross ~$8.4k) must NOT trip.
    equity = 1_800.0
    orders = [_order("BTC", 0.02, 60000.0)]          # $1,200 single order
    orders += [_order(f"C{i}", 1.0, 600.0) for i in range(12)]  # +$7,200 gross
    ltps = {"BTC": 60000.0, **{f"C{i}": 600.0 for i in range(12)}}
    res = preflight_check(
        orders, ltps,
        max_order_notional_usd=3.0 * equity,
        max_gross_notional_usd=12.0 * equity,
        max_order_count=120,
        max_price_deviation=0.05,
    )
    assert res.ok is True
