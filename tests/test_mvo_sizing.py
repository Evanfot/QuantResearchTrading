"""Unit tests for the MVO (max-Sharpe + vol-target) sizing core.

Synthetic data only — no data pipeline / network dependencies.
"""
import numpy as np
import pandas as pd
import pytest

from src.backtester.full_backtest_mvo import (
    MVOConfig,
    MVOIntent,
    compute_strategy_mvo,
)


def _synthetic(n_periods=300, n_assets=6, seed=0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2024-01-01", periods=n_periods, freq="D")
    cols = [f"A{i}" for i in range(n_assets)]
    rets = rng.normal(0.001, 0.03, (n_periods, n_assets))
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=idx, columns=cols)
    # momentum-style alpha, scaled into a forecast-like range
    mu = (
        pd.DataFrame(rets, index=idx, columns=cols)
        .rolling(10).mean().fillna(0.0) * 100
    ).clip(-10, 10).values
    return prices, mu, cols


def test_returns_mvo_intent_aligned_to_mask():
    prices, mu, _ = _synthetic()
    mask = np.ones(prices.shape[1], dtype=bool)
    out = compute_strategy_mvo(prices, mu[-1], mask, MVOConfig())
    assert isinstance(out, MVOIntent)
    n_valid = int(mask.sum())
    assert out.target_weight.shape == (n_valid,)
    assert out.tangency_weight.shape == (n_valid,)
    assert out.capped.shape == (n_valid,)
    assert np.isfinite(out.target_weight).all()
    assert out.gross > 0  # took some exposure


def test_vol_target_leverage_hits_target():
    """With no caps binding, expected book vol == the annualised target."""
    prices, mu, _ = _synthetic()
    mask = np.ones(prices.shape[1], dtype=bool)
    cfg = MVOConfig(sizing="vol_target", target_vol_daily=0.02, max_gross_leverage=1e9)
    out = compute_strategy_mvo(prices, mu[-1], mask, cfg)
    assert np.isclose(out.expected_vol, cfg.target_vol_annual, rtol=1e-6)
    # annual target uses the 365 basis
    assert np.isclose(cfg.target_vol_annual, 0.02 * np.sqrt(365))


def test_position_cap_is_respected():
    prices, mu, _ = _synthetic()
    mask = np.ones(prices.shape[1], dtype=bool)
    cap = 0.10
    cfg = MVOConfig(target_vol_daily=0.05, max_position_weight=cap)  # hot target to force cap
    out = compute_strategy_mvo(prices, mu[-1], mask, cfg)
    assert np.all(np.abs(out.target_weight) <= cap + 1e-9)
    assert out.capped.any()  # cap actually bound somewhere


def test_masked_assets_excluded_and_zero():
    prices, mu, _ = _synthetic()
    mask = np.ones(prices.shape[1], dtype=bool)
    mask[0] = False  # drop first asset from the tradable set
    out = compute_strategy_mvo(prices, mu[-1], mask, MVOConfig())
    assert out.target_weight.shape == (int(mask.sum()),)  # masked-length output


def test_degenerate_universe_stays_flat():
    prices, mu, _ = _synthetic(n_assets=6)
    mask = np.zeros(prices.shape[1], dtype=bool)
    mask[0] = True  # only one valid asset -> cannot form a covariance
    out = compute_strategy_mvo(prices, mu[-1], mask, MVOConfig())
    assert np.allclose(out.target_weight, 0.0)
    assert out.leverage == 0.0


def test_insufficient_history_stays_flat():
    prices, mu, _ = _synthetic(n_periods=300)
    prices.iloc[:, 1:] = np.nan  # only one asset has history
    mask = np.ones(prices.shape[1], dtype=bool)
    out = compute_strategy_mvo(prices, mu[-1], mask, MVOConfig(min_periods=60))
    assert np.allclose(out.target_weight, 0.0)
