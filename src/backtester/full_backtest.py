import numpy as np
from cvx.simulator import Builder
from tinycta.linalg import solve, inv_a_norm
from tinycta.signal import shrink2id

from src.strategy.config import StrategyConfig, StrategyIntent


def compute_strategy(
    mu: np.ndarray,
    vo: np.ndarray,
    cor_matrix: np.ndarray,
    mask: np.ndarray,
    config: StrategyConfig,
) -> StrategyIntent:

    matrix = shrink2id(cor_matrix, lamb=config.shrinkage_value)[mask][:, mask]

    expected_mu = np.nan_to_num(mu[mask])
    expected_vo = np.nan_to_num(vo[mask])

    risk_position = solve(matrix, expected_mu) / inv_a_norm(expected_mu, matrix)
    #     If vo/corr is NaN, target_pos is NaN, but `gross = np.abs(weight).sum()` downstream returns NaN and
    #    `NaN > max_gross_leverage` is always False --
    #     silently disabling the gross-leverage cap, so explicitly set target pos to zero
    valid = (expected_vo > 0) & np.isfinite(risk_position)
    target_pos = np.zeros_like(risk_position)
    target_pos[valid] = config.position_multiplier * risk_position[valid] / expected_vo[valid]

    if config.ignore_small:
        target_pos[np.abs(target_pos) < config.small_threshold] = 0

    return StrategyIntent(
        risk_position=risk_position,
        target_position=target_pos,
        expected_vo=expected_vo,
        mask=mask,
    )


def run_backtest(prices, mu, vo, cor, config: StrategyConfig):
    builder = Builder(prices=prices, initial_aum=1e3)
    # Same conversion run_live uses (src/main.py): position_multiplier cancels
    # out of this ratio, so it's a no-op in live -- the real risk level is set
    # by weight_multiplier alone.
    conversion_factor = config.weight_multiplier / config.position_multiplier

    for n, (t, state) in enumerate(builder):
        mask = state.mask

        strategy_intent = compute_strategy(
            mu=mu[n],
            vo=vo[n],
            cor_matrix=cor.loc[t[-1]].values,
            mask=mask,
            config=config,
        )

        # Rescale target_position  into a
        # fraction-of-equity weight via conversion_factor, then apply the same
        # Without this, the backtest priced the book as a fixed-dollar
        # gross-leverage backstop as prod enforces (cap_gross_leverage in src/main.py / src/execution.py).
        # position against the $1,000 starting AUM -- uncapped, and scaled by
        # position_multiplier instead of weight_multiplier -- which doesn't
        # track prod's actual risk level at all.
        weight = strategy_intent.target_position * conversion_factor
        gross = np.abs(weight).sum()
        if gross > config.max_gross_leverage:
            weight = weight * (config.max_gross_leverage / gross)

        target_cashposition = weight * state.aum

        # threshold_trade's deadband has to compare like with like -- it was
        # previously applied inside compute_strategy, against target_pos's own
        # pre-conversion units, which broke the moment this function started
        # converting target_position into a real dollar cashposition (that
        # comparison was pre-conversion target_pos vs. post-conversion actual
        # dollar position: different scales, so the deadband either never
        # fired or fired on everything depending on config). Applying it here,
        # against last bar's actual dollar position (builder.position marked
        # at this bar's prices), keeps both sides in the same units.
        if config.threshold_trade and n > 0:
            # builder.position is full-width (all of prices.columns); current_prices
            # is already masked down to this bar's valid assets (state.assets)
            yesterday_cashposition = builder.position[mask].to_numpy() * builder.current_prices
            below_threshold = np.abs(target_cashposition - yesterday_cashposition) < config.small_threshold
            target_cashposition[below_threshold] = yesterday_cashposition[below_threshold]

        builder.cashposition = target_cashposition

        if config.add_commission:
            # state.trades (units) was just computed by the cashposition
            traded_dollars = np.abs(state.trades.fillna(0.0).to_numpy() * builder.current_prices).sum()
            commission = traded_dollars * (config.commission_bps / 1e4)
        else:
            commission = 0

        builder.aum = state.aum - commission

    return builder.build()
