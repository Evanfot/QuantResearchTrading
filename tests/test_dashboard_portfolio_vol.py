import math

from src.dashboard.app import compute_portfolio_vol


def _exchange_state(positions):
    return {
        "marginSummary": {"accountValue": "10000"},
        "spotState": {"balances": [{"coin": "USDC", "total": 10000.0}]},
        "assetPositions": [
            {"position": {"coin": c, "szi": str(qty), "positionValue": str(abs(qty) * 100)}}
            for c, qty in positions.items()
        ],
    }


def _intent(vol_per_coin, corr):
    return {
        "assets": {c: {"model": {"vol_1d": v}} for c, v in vol_per_coin.items()},
        "risk_inputs": {"correlation_matrix": corr},
    }


def test_normal_case_returns_a_finite_vol():
    positions = {"BTC": 1.0, "ETH": -2.0}
    vol_per_coin = {"BTC": 0.02, "ETH": 0.03}
    corr = {"BTC": {"BTC": 1.0, "ETH": 0.5}, "ETH": {"BTC": 0.5, "ETH": 1.0}}
    result = compute_portfolio_vol(_exchange_state(positions), _intent(vol_per_coin, corr))
    assert result is not None
    assert math.isfinite(result)
    assert result > 0


def test_nan_vol_1d_for_one_coin_does_not_poison_the_result():
    """A data-gappy coin (e.g. too little history for vol_1d) must be dropped,
    not silently NaN the whole portfolio figure."""
    positions = {"BTC": 1.0, "ETH": -2.0, "GRAM": 0.5}
    vol_per_coin = {"BTC": 0.02, "ETH": 0.03, "GRAM": float("nan")}
    corr = {
        "BTC": {"BTC": 1.0, "ETH": 0.5, "GRAM": 0.1},
        "ETH": {"BTC": 0.5, "ETH": 1.0, "GRAM": 0.2},
        "GRAM": {"BTC": 0.1, "ETH": 0.2, "GRAM": 1.0},
    }
    result = compute_portfolio_vol(_exchange_state(positions), _intent(vol_per_coin, corr))
    assert result is not None
    assert math.isfinite(result)


def test_nan_correlation_entry_for_one_coin_does_not_poison_the_result():
    """A coin with too little history for the (longer-window) correlation
    estimate specifically, even with a valid vol_1d, must be dropped rather
    than NaN-poisoning every other held coin's contribution too."""
    positions = {"BTC": 1.0, "ETH": -2.0, "GRAM": 0.5}
    vol_per_coin = {"BTC": 0.02, "ETH": 0.03, "GRAM": 0.025}  # GRAM's vol IS valid
    corr = {
        "BTC": {"BTC": 1.0, "ETH": 0.5, "GRAM": float("nan")},
        "ETH": {"BTC": 0.5, "ETH": 1.0, "GRAM": float("nan")},
        "GRAM": {"BTC": float("nan"), "ETH": float("nan"), "GRAM": 1.0},
    }
    result = compute_portfolio_vol(_exchange_state(positions), _intent(vol_per_coin, corr))
    assert result is not None
    assert math.isfinite(result)


def test_returns_none_not_nan_when_too_few_clean_coins_remain():
    positions = {"BTC": 1.0, "GRAM": 0.5}
    vol_per_coin = {"BTC": 0.02, "GRAM": float("nan")}
    corr = {"BTC": {"BTC": 1.0, "GRAM": 0.1}, "GRAM": {"BTC": 0.1, "GRAM": 1.0}}
    result = compute_portfolio_vol(_exchange_state(positions), _intent(vol_per_coin, corr))
    assert result is None
