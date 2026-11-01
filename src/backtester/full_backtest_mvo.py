"""MVO (max-Sharpe) weighting — production sizing + backtest harness.

Alternative to the risk-parity weighting in ``full_backtest.py``. Positions are
sized each period with PyPortfolioOpt's max-Sharpe efficient frontier (the
tangency direction), then levered either by fractional Kelly or to an explicit
annualised vol target, with an optional per-asset cap.

Weighting logic mirrors ``analysis/mvo_backtest.py`` lines 83-103:

    S  = risk_models.CovarianceShrinkage(prices).ledoit_wolf()
    ef = EfficientFrontier(mu, S, weight_bounds=(None, None))
    ef.add_objective(objective_functions.L2_reg, gamma=0.1)
    ef.max_sharpe(); w = clean_weights()

then sized by either
    kelly:      lev = kelly_fraction * (w·(μ−rf)) / (w·S·w)
    vol_target: lev = target_vol / sqrt(w·S·w)

The core returns *weights as a fraction of equity* (:class:`MVOIntent`), so the
same function drives both the backtest (multiply by AUM) and live sizing
(``run_live`` consumes the weights directly).
"""

import numpy as np
import pandas as pd
from cvx.simulator import Builder
from pypfopt import EfficientFrontier, objective_functions, risk_models

from src.strategy.config import MVOConfig, MVOIntent


def _flat_intent(mask: np.ndarray, n_valid: int) -> MVOIntent:
    """Stay-flat result for degenerate / failed periods."""
    z = np.zeros(n_valid)
    return MVOIntent(
        target_weight=z, tangency_weight=z.copy(), capped=np.zeros(n_valid, dtype=bool),
        mask=mask, leverage=0.0, expected_vol=0.0, gross=0.0, portfolio_mu=0.0,
    )


def compute_strategy_mvo(
    price_window: pd.DataFrame,
    mu_row: np.ndarray,
    mask: np.ndarray,
    config: MVOConfig,
) -> MVOIntent:
    """Compute per-asset target weights for one period (max-Sharpe + sizing).

    Parameters
    ----------
    price_window:
        Price *levels* up to and including the current period, columns aligned to
        the full asset universe (same order as ``mask``).
    mu_row:
        Expected-return (alpha) vector for the full universe this period.
    mask:
        Boolean array of assets with a valid current price.
    config:
        :class:`MVOConfig`.
    """
    valid_assets = price_window.columns[mask]
    n_valid = len(valid_assets)
    if n_valid < 2:
        return _flat_intent(mask, n_valid)

    # Trailing window of prices for the currently valid assets.
    window = price_window.loc[:, valid_assets]
    if config.lookback:
        window = window.tail(config.lookback)

    # Keep only assets with enough history in the window to estimate covariance.
    enough = window.notna().sum() >= config.min_periods
    usable = valid_assets[enough.values]
    if len(usable) < 2:
        return _flat_intent(mask, n_valid)

    window = window.loc[:, usable].dropna(how="any")
    mu_valid = pd.Series(np.nan_to_num(mu_row[mask]), index=valid_assets)

    try:
        S = risk_models.CovarianceShrinkage(window, frequency=config.trading_days).ledoit_wolf()
        mu_aligned = mu_valid.loc[usable].reindex(S.columns)

        ef = EfficientFrontier(mu_aligned, S, weight_bounds=config.weight_bounds)
        ef.add_objective(objective_functions.L2_reg, gamma=config.gamma)
        ef.max_sharpe(risk_free_rate=config.rf)
        cw = ef.clean_weights()
        w = np.array([cw[t] for t in S.columns])

        mu_excess = mu_aligned.values - config.rf
        mu_p = float(w @ mu_excess)
        var_p = float(w @ S.values @ w)
        if var_p <= 0 or not np.isfinite(mu_p):
            raise ValueError("degenerate tangency portfolio")
        sigma_p = np.sqrt(var_p)

        if config.sizing == "vol_target":
            lev = config.target_vol_annual / sigma_p
        elif config.sizing == "kelly":
            lev = config.kelly_fraction * (mu_p / var_p)
        else:
            raise ValueError(f"unknown sizing mode: {config.sizing!r}")

        positions = lev * w  # fraction of equity per usable asset

        # Per-asset cap (hard risk limit, fraction of equity).
        capped_usable = np.zeros(len(positions), dtype=bool)
        if config.max_position_weight is not None:
            mpw = config.max_position_weight
            capped_usable = np.abs(positions) > mpw + 1e-12
            positions = np.clip(positions, -mpw, mpw)

        # Cap gross leverage to avoid pathological single-period blow-ups.
        gross = float(np.abs(positions).sum())
        if gross > config.max_gross_leverage:
            positions *= config.max_gross_leverage / gross
    except Exception:
        return _flat_intent(mask, n_valid)

    # Scatter usable-asset results back onto the masked-asset vectors.
    def scatter(values, fill=0.0, dtype=float):
        s = pd.Series(values, index=S.columns).reindex(valid_assets).fillna(fill)
        return s.values.astype(dtype)

    target_weight = scatter(positions)
    return MVOIntent(
        target_weight=target_weight,
        tangency_weight=scatter(w),
        capped=scatter(capped_usable, fill=False, dtype=bool),
        mask=mask,
        leverage=float(lev),
        expected_vol=float(abs(lev) * sigma_p),
        gross=float(np.abs(target_weight).sum()),
        portfolio_mu=mu_p,
    )


def run_backtest_mvo(prices: pd.DataFrame, mu: np.ndarray, config: MVOConfig):
    """Run the MVO backtest (per-period optimisation).

    ``mu`` is the period-by-period alpha matrix (rows = periods, cols = assets),
    identical to what the risk-parity baseline consumes, so results are directly
    comparable. Weights are converted to cash positions via the running AUM.
    """
    builder = Builder(prices=prices, initial_aum=1e3)

    for n, (t, state) in enumerate(builder):
        mvo_intent = compute_strategy_mvo(
            price_window=prices.loc[t],
            mu_row=mu[n],
            mask=state.mask,
            config=config,
        )
        builder.cashposition = mvo_intent.target_weight * state.aum

        if config.add_commission:
            # state.trades (units) was just computed by the cashposition
            # setter above as new_position - old_position; NaN on day 0 only
            # (no prior position to diff against) -> treat as flat-start, 0 cost.
            traded_dollars = np.abs(state.trades.fillna(0.0).to_numpy() * builder.current_prices).sum()
            commission = traded_dollars * (config.commission_bps / 1e4)
        else:
            commission = 0

        builder.aum = state.aum - commission

    return builder.build()
