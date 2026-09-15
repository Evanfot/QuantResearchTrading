"""Tests for cap_gross_leverage — the hard ceiling on vol-target sizing.

Vol-target sizing (weight ~ signal / expected_vol) has no natural ceiling: as
realized vol compresses, the same signal conviction produces ever-larger
positions. See docs/decisions for the incident and the 3.7x threshold.
"""

import math

from src.execution import cap_gross_leverage


class _Logger:
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args, **kwargs):
        self.warnings.append(msg % args if args else msg)


def test_under_cap_passes_through_unchanged():
    weights = {"BTC": 0.5, "ETH": -0.4, "SOL": 0.3}  # gross = 1.2x
    out = cap_gross_leverage(weights, max_gross_leverage=3.7)
    assert out == weights


def test_over_cap_scales_book_down_proportionally():
    # Mirrors the incident: many small-to-medium offsetting weights summing
    # to ~6.25x gross, well over the 3.7x ceiling.
    weights = {f"C{i}": 0.12 if i % 2 == 0 else -0.13 for i in range(50)}
    gross_before = sum(abs(w) for w in weights.values())
    assert gross_before > 3.7

    out = cap_gross_leverage(weights, max_gross_leverage=3.7)
    gross_after = sum(abs(w) for w in out.values())
    assert math.isclose(gross_after, 3.7, rel_tol=1e-9)

    # Scaling is uniform: relative sizing between assets is preserved.
    ratio = out["C0"] / weights["C0"]
    for coin in weights:
        assert math.isclose(out[coin] / weights[coin], ratio, rel_tol=1e-9)


def test_exactly_at_cap_passes_through_unchanged():
    weights = {"BTC": 1.85, "ETH": -1.85}  # gross = 3.7x exactly
    out = cap_gross_leverage(weights, max_gross_leverage=3.7)
    assert out == weights


def test_empty_book_is_a_noop():
    assert cap_gross_leverage({}, max_gross_leverage=3.7) == {}


def test_nan_weights_ignored_in_gross_and_left_untouched():
    # Unpriced/unmasked assets log as NaN target_weight upstream (get_order_intention
    # skips them); the cap must neither blow up on them nor count them toward gross.
    weights = {"BTC": 3.0, "ETH": -2.0, "GRAM": float("nan")}
    out = cap_gross_leverage(weights, max_gross_leverage=3.7)
    assert math.isnan(out["GRAM"])
    gross_after = sum(abs(w) for c, w in out.items() if c != "GRAM")
    assert math.isclose(gross_after, 3.7, rel_tol=1e-9)


def test_warns_only_when_scaling_applied():
    logger = _Logger()
    cap_gross_leverage({"BTC": 0.5}, max_gross_leverage=3.7, logger=logger)
    assert logger.warnings == []

    logger = _Logger()
    cap_gross_leverage({"BTC": 5.0, "ETH": -3.0}, max_gross_leverage=3.7, logger=logger)
    assert len(logger.warnings) == 1
    assert "3.70x" in logger.warnings[0] or "3.7" in logger.warnings[0]
