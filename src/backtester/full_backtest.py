from dataclasses import dataclass
from typing import Optional

import numpy as np
from cvx.simulator import Builder
from tinycta.linalg import solve, inv_a_norm
from tinycta.signal import shrink2id


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
    position_multiplier: float = 10.0
    weight_multiplier: float = 0.02
    small_threshold: float = 10.0


@dataclass
class StrategyIntent:
    """Holds the computed state for a specific period."""

    risk_position: np.ndarray
    target_position: np.ndarray
    expected_vo: np.ndarray
    mask: np.ndarray


def compute_strategy(
    mu: np.ndarray,
    vo: np.ndarray,
    cor_matrix: np.ndarray,
    mask: np.ndarray,
    config: StrategyConfig,
    yesterday_pos: Optional[np.ndarray] = None,
) -> StrategyIntent:

    matrix = shrink2id(cor_matrix, lamb=config.shrinkage_value)[mask][:, mask]

    expected_mu = np.nan_to_num(mu[mask])
    expected_vo = np.nan_to_num(vo[mask])

    risk_position = solve(matrix, expected_mu) / inv_a_norm(expected_mu, matrix)
    target_pos = config.position_multiplier * risk_position / expected_vo

    if config.ignore_small:
        target_pos[np.abs(target_pos) < config.small_threshold] = 0

    # Restored threshold logic: Only applies if we provide yesterday's position
    if config.threshold_trade and yesterday_pos is not None:
        below_threshold = np.abs(target_pos - yesterday_pos) < config.small_threshold
        target_pos[below_threshold] = yesterday_pos[below_threshold]

    return StrategyIntent(
        risk_position=risk_position,
        target_position=target_pos,
        expected_vo=expected_vo,
        mask=mask,
    )


def run_backtest(prices, mu, vo, cor, config: StrategyConfig):
    builder = Builder(prices=prices, initial_aum=1e3)

    for n, (t, state) in enumerate(builder):
        mask = state.mask

        # Determine yesterday's position for the threshold logic
        if config.threshold_trade and n > 0:
            yesterday_pos_full = builder.position * builder.current_prices
            yesterday_pos = yesterday_pos_full[mask]
        else:
            yesterday_pos = None

        strategy_intent = compute_strategy(
            mu=mu[n],
            vo=vo[n],
            cor_matrix=cor.loc[t[-1]].values,
            mask=mask,
            config=config,
            yesterday_pos=yesterday_pos,
        )

        builder.cashposition = strategy_intent.target_position

        if config.add_commission:
            # TODO: DO commission calcs here based on turnover
            commission = 0
        else:
            commission = 0

        builder.aum = state.aum - commission

    return builder.build()
