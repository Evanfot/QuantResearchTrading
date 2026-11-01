"""Strategy sizing configs + per-period intent results.

Shared by both allocators (risk-parity in ``full_backtest.py``, MVO max-Sharpe
in ``full_backtest_mvo.py``) and by live trading (``main.py``), so it lives
here rather than inside either backtest engine module.
"""
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


@dataclass
class MVOConfig:
    """Parameters for the MVO (max-Sharpe) weighting scheme.

    ``sizing`` selects how the tangency (max-Sharpe) portfolio is *scaled* — both
    choices share the same direction ``w`` and only differ in leverage:

      * ``"kelly"``      — fractional-Kelly leverage ``kelly_fraction * μ_p/σ_p²``
      * ``"vol_target"`` — lever the tangency book to hit ``target_vol`` each period
                            (``lev = target_vol / sqrt(wᵀSw)``)
    """

    sizing: str = "vol_target"  # "kelly" | "vol_target"
    gamma: float = 0.1          # L2 regularisation on weights (PyPortfolioOpt)
    rf: float = 0.15            # risk-free rate used for Kelly excess return
    kelly_fraction: float = 0.25  # fraction of full-Kelly leverage (sizing="kelly")
    target_vol_daily: float = 0.022  # daily vol target (sizing="vol_target")
    trading_days: int = 365     # annualisation basis — crypto trades 24/7/365
    max_position_weight: Optional[float] = None  # cap on |weight| per asset (frac of equity)
    max_gross_leverage: float = 4.5  # cap on sum(|w|); matches StrategyConfig's live cap (dcb00c2/f50dfc5)
    lookback: int = 252         # trailing window (rows) for covariance estimation
    min_periods: int = 60       # min non-NaN rows an asset needs to be included
    weight_bounds: tuple = (None, None)  # (None, None) => long/short allowed
    add_commission: bool = False
    # Blended taker/maker fee, calibrated from this account's real Hyperliquid
    # fills (logs/fills_mainnet.jsonl, 12k fills): taker 4.50bps, maker 1.50bps,
    # ~46% taker historically -> blended mean 2.88bps of traded notional.
    commission_bps: float = 2.88

    @property
    def target_vol_annual(self) -> float:
        """Target vol on the same annualisation basis as the covariance."""
        return self.target_vol_daily * (self.trading_days ** 0.5)


@dataclass
class MVOIntent:
    """Per-period MVO sizing result.

    All per-asset arrays are aligned to the *masked* (valid-price) assets, in the
    same order as ``price_window.columns[mask]``. ``target_weight`` is a fraction
    of equity (post-cap) — multiply by AUM for cash positions, or use directly as
    a live target weight.
    """

    target_weight: np.ndarray    # final weight per masked asset (fraction of equity)
    tangency_weight: np.ndarray  # max-Sharpe direction w (sums to 1 over usable assets)
    capped: np.ndarray           # bool: did the per-asset cap bind for this asset?
    mask: np.ndarray
    leverage: float              # leverage applied to the tangency book
    expected_vol: float          # model's expected annualised vol of the book
    gross: float                 # sum(|target_weight|)
    portfolio_mu: float          # tangency expected excess return (w·(μ−rf))


@dataclass
class StrategyConfig:
    """Centralizes all tunable parameters to avoid global variables and magic numbers."""

    shrinkage_value: float = 0.5
    ewmac_fast = 4
    breakout_window = 14
    bollinger_window = 14
    vo_window = 20
    correlation = 64
    ignore_small: bool = False
    threshold_trade: bool = False
    add_commission: bool = False
    # Blended taker/maker fee, calibrated from this account's real Hyperliquid
    # fills (logs/fills_mainnet.jsonl, 12k fills): taker 4.50bps, maker 1.50bps,
    # ~46% taker historically -> blended mean 2.88bps of traded notional.
    commission_bps: float = 2.88
    position_multiplier: float = 10.0
    weight_multiplier: float = 0.0225
    small_threshold: float = 10.0
    max_gross_leverage: float = 4.5

    # ── Sizing model selection ──────────────────────────────────────────────
    # Promoted 2026-09-22 (docs/decisions/202609-mvo-transition-investigation.md)
    # after holdout + walk-forward + as-of-2025-12-31 time-robustness validation,
    # SHADOW_SIZING stays on so risk-parity automatically becomes the shadow,
    # giving a same-day rollback comparison. Revert = flip this
    # back to "risk_parity"; no other code change needed.
    sizing_model: str = "mvo"              # "mvo" | "risk_parity"
    # MVO (max-Sharpe + vol-target) params. Values are the holdout-validated
    # candidate, not feature/mvo's original defaults (gamma=0.1, lookback=252,
    # target_vol_daily=0.022): the original beat risk-parity in-sample but
    # lost money on a genuine holdout; tighter L2 regularisation + a shorter
    # covariance window fixed that and generalised across a 12-block
    # walk-forward and an independent as-of-2025-12-31 time-robustness check.
    #
    # target_vol_daily kept at the fully-validated 0.0172 (32.8% annualised,
    # matches prod's actual risk level) -- deliberately not run at reduced size;
    # this is the exact config the validation work covers, at the exact risk
    # level it was validated at.
    mvo: MVOConfig = field(default_factory=lambda: MVOConfig(
        target_vol_daily=0.0172,   # 32.8% annualised -- matches prod's actual risk level
        trading_days=365,          # annualisation basis (crypto 24/7/365)
        max_position_weight=0.30,  # cap |weight| per asset (frac of equity)
        max_gross_leverage=4.5,    # matches risk-parity's execution-level cap above
        gamma=1.0,
        rf=0.15,
        kelly_fraction=0.25,
        lookback=90,
        min_periods=60,
    ))


@dataclass
class StrategyIntent:
    """Holds the computed state for a specific period."""

    risk_position: np.ndarray
    target_position: np.ndarray
    expected_vo: np.ndarray
    mask: np.ndarray
