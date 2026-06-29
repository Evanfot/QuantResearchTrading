import copy
import datetime as dt
import logging
import os
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from src.backtester.full_backtest import StrategyConfig, StrategyIntent, compute_strategy
from src.backtester.full_backtest_mvo import MVOConfig, MVOIntent, compute_strategy_mvo
from src.strategy import registry as strategy_registry
from src.data import get_final_pricing, get_hyperliquid_trading_universe, get_ohlcv, latest_is_provisional, load_ohlcv_for_alphas
from src.execution import classify_order_responses, generate_readable_summary, get_execution_plan, get_order_intention, preflight_check
from src.helpers.dict_diff import dict_diff
from src.loggers.intent_logger import IntentLogger, generate_run_id, init_asset, init_intent
from src.loggers.order_logger import OrderLogger
from src.signal import alpha006, alpha014, alpha020, breakout, ewmac, scaled_bollinger
from src.state.strategy_state import get_state_positions, load_state, save_state
from src.universe import UNIVERSE_SIZE, get_latest_market_cap, get_top_marketcap, get_universe, store_market_cap

# ── Scheduling constants ───────────────────────────────────────────────────────
DATA_HOUR_UTC = 23
DATA_MINUTE_UTC = 45
MKT_CAP_HOUR_UTC = 0
MKT_CAP_MINUTE_UTC = 5
META_HOUR_UTC = 23
META_MINUTE_UTC = 40
TRADING_EXEC_HOUR_UTC = 2
TRADING_EXEC_INTERVAL_MINUTES = 30
HALT_RETRY_MINUTES = 5  # retry cadence after a transient "Trading is halted." rejection

# ── Pre-flight circuit-breaker caps ─────────────────────────────────────────────
# Calibrated against real intent history on this account (~$1.8k equity): per-order
# notional maxed ~0.6x equity, gross batch ~4.6x, count ~49. Caps sit well above a
# legitimate rebalance/flip but trip on ~10x fat-finger / stale-price / runaway bugs.
# On a trip the whole batch is blocked and retried next cycle (nothing is submitted).
PREFLIGHT_MAX_ORDER_NOTIONAL_MULT = 3.0    # x equity — single-order ceiling
PREFLIGHT_MAX_GROSS_NOTIONAL_MULT = 12.0   # x equity — total-batch ceiling
PREFLIGHT_MAX_ORDER_COUNT = 120
PREFLIGHT_MAX_PRICE_DEVIATION = 0.05       # limit_px vs mid sanity (price-construction bugs)
TRADING_INTENT_HOUR_UTC = 0
TRADING_INTENT_MINUTE_UTC = 1
POSITION_CHECK_INTERVAL_HOURS = 1

def _env_flag(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# Compute orders but never cancel/submit to the exchange (intents still logged).
DRY_RUN = _env_flag("DRY_RUN", False)
# Also compute + log the *other* sizing model to a side file for parallel
# validation, without ever feeding it to the executor.
SHADOW_SIZING = _env_flag("SHADOW_SIZING", False)

logger = logging.getLogger(__name__)


# ── Scheduling predicates ──────────────────────────────────────────────────────



def is_day_open_due(now, state):
    last_ms = state.get("last_day_open_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return now.date() > last.date()


def is_position_check_due(now, state):
    last_ms = state.get("last_position_check_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= POSITION_CHECK_INTERVAL_HOURS * 3600


def is_data_due(now, state):
    if state.get("last_data_run_ms") is None:
        return True
    last_run = dt.datetime.fromtimestamp(state["last_data_run_ms"] / 1000, tz=dt.timezone.utc)
    return now.date() > last_run.date() and now.hour >= DATA_HOUR_UTC and now.minute >= DATA_MINUTE_UTC


def is_meta_due(now, state):
    last_ms = state.get("last_meta_run_ms")
    if not last_ms:
        return True
    last_run = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return now.date() > last_run.date() and now.hour >= META_HOUR_UTC and now.minute >= META_MINUTE_UTC


def is_mkt_cap_due(now, state):
    last_ms = state.get("last_mkt_cap_run_ms")
    if last_ms is None:
        return True
    last_run = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return now.date() > last_run.date() and now.hour >= MKT_CAP_HOUR_UTC and now.minute >= MKT_CAP_MINUTE_UTC


def is_trading_intent_due(now, state):
    last_run_id = state.get("last_trading_intent_run_id")
    if not last_run_id:
        return True
    ts_str = last_run_id.split("_")[0]
    last_run = dt.datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
    return now.date() > last_run.date() and now.hour >= TRADING_INTENT_HOUR_UTC and now.minute >= TRADING_INTENT_MINUTE_UTC


def is_trading_exec_due(now, state):
    if now.hour < TRADING_EXEC_HOUR_UTC:
        return False
    last_ms = state.get("last_trading_exec_ms")
    if not last_ms:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= TRADING_EXEC_INTERVAL_MINUTES * 60


def sleep_until_next_tick(state):
    import time
    time.sleep(1)


def _watchdog(threshold_s=1800):
    import time
    while True:
        time.sleep(60)
        try:
            ts_ms = float(Path("state/heartbeat.ms").read_text())
            age = time.time() - ts_ms / 1000
            if age > threshold_s:
                logger.error(f"[watchdog] heartbeat stale for {age:.0f}s — forcing exit")
                os._exit(1)
        except Exception:
            pass


# ── Live-trading helpers ───────────────────────────────────────────────────────

def update_ltps():
    from src.ingestion.update_mids import run_update_mids
    run_update_mids()
    latest_view = pd.read_csv("data/snapshots/mids.csv")
    latest_view.set_index("symbol", inplace=True)
    return {key: float(val) for key, val in latest_view.to_dict()["mid"].items()}


def initialise_asset_intent(intent, universe):
    for symbol in universe:
        intent["assets"][symbol] = init_asset()
    return intent


def add_ltp_to_intent(intent, latest_view):
    for symbol in intent["assets"].keys():
        if symbol not in latest_view.index:
            continue
        intent["assets"][symbol]["market"]["mark_price"] = float(latest_view.at[symbol, "mid"])
        intent["assets"][symbol]["market"]["data_timestamp"] = latest_view.loc[symbol, "downloaded_at"]
    return intent


def log_strategy_intent(
    intent_log: Dict[str, Any],
    prices: pd.DataFrame,
    strategy_intent: StrategyIntent,
    config: StrategyConfig,
):
    mask = strategy_intent.mask
    intent_log["universe"]["tradable"] = list(prices.columns.values[mask])
    intent_log["universe"]["non_tradable"] = list(prices.columns.values[~mask])

    weights = config.weight_multiplier * strategy_intent.risk_position / strategy_intent.expected_vo
    for i, symbol in enumerate(prices.columns.values[mask]):
        intent_log["assets"][symbol]["model"]["risk_position"] = float(strategy_intent.risk_position[i])
        intent_log["assets"][symbol]["model"]["target_weight"] = float(weights[i])


def mvo_config_from_strategy(config: StrategyConfig) -> MVOConfig:
    """Build the MVO core config from the production StrategyConfig."""
    return MVOConfig(
        sizing="vol_target",
        gamma=config.mvo_gamma,
        rf=config.mvo_rf,
        kelly_fraction=config.mvo_kelly_fraction,
        target_vol_daily=config.mvo_target_vol_daily,
        trading_days=config.mvo_trading_days,
        max_position_weight=config.mvo_max_position_weight,
        lookback=config.mvo_lookback,
        min_periods=config.mvo_min_periods,
    )


def log_strategy_intent_mvo(
    intent_log: Dict[str, Any],
    prices: pd.DataFrame,
    mvo_intent: MVOIntent,
    mvo_config: MVOConfig,
):
    mask = mvo_intent.mask
    tradable = list(prices.columns.values[mask])
    intent_log["universe"]["tradable"] = tradable
    intent_log["universe"]["non_tradable"] = list(prices.columns.values[~mask])

    for i, symbol in enumerate(tradable):
        model = intent_log["assets"][symbol]["model"]
        model["target_weight"] = float(mvo_intent.target_weight[i])
        model["tangency_weight"] = float(mvo_intent.tangency_weight[i])
        model["capped"] = bool(mvo_intent.capped[i])
        model["risk_position"] = None  # risk-parity only

    intent_log["sizing"].update({
        "model": "mvo_max_sharpe",
        "sizing_rule": "vol_target",
        "target_vol_daily": mvo_config.target_vol_daily,
        "target_vol_annual": mvo_config.target_vol_annual,
        "trading_days": mvo_config.trading_days,
        "max_position_weight": mvo_config.max_position_weight,
        "gamma": mvo_config.gamma,
        "applied_leverage": float(mvo_intent.leverage),
        "expected_vol_annual": float(mvo_intent.expected_vol),
        "gross_leverage": float(mvo_intent.gross),
        "portfolio_mu": float(mvo_intent.portfolio_mu),
    })


def run_live(prices, mu, vo, cor, positions, ltps, intent_log, config, latest_view, logger, intent_logger):
    t = prices.index[-1]
    mask = ~prices.loc[t].isna().values
    tradable_symbols = prices.columns[mask]

    if config.sizing_model == "mvo":
        mvo_config = mvo_config_from_strategy(config)
        mvo_intent = compute_strategy_mvo(
            price_window=prices.loc[:t], mu_row=mu[-1], mask=mask, config=mvo_config
        )
        log_strategy_intent_mvo(intent_log, prices, mvo_intent, mvo_config)
        target_weights = {
            symbol: float(w)
            for symbol, w in zip(tradable_symbols, mvo_intent.target_weight)
        }
    else:
        strategy_intent = compute_strategy(
            mu=mu[-1], vo=vo[-1], cor_matrix=cor.loc[t].values, mask=mask, config=config
        )
        log_strategy_intent(intent_log, prices, strategy_intent, config)
        intent_log["sizing"]["model"] = "risk_parity"
        conversion_factor = config.weight_multiplier / config.position_multiplier
        target_weights = {
            symbol: pos * conversion_factor
            for symbol, pos in zip(tradable_symbols, strategy_intent.target_position)
        }

    target_zeroes = {coin: 0 for coin in set(positions.keys()) - set(target_weights.keys())}
    intent_log["universe"]["holdings_outside_universe"] = list(target_zeroes.keys())

    for symbol in target_zeroes:
        if symbol not in intent_log["assets"]:
            intent_log["assets"][symbol] = init_asset()
        intent_log["assets"][symbol]["market"]["mark_price"] = float(latest_view.loc[symbol, "mid"])
        intent_log["assets"][symbol]["market"]["data_timestamp"] = latest_view.loc[symbol, "downloaded_at"]

    all_target_weights = {**target_weights, **target_zeroes}
    account_val = intent_log["portfolio"]["equity_used_for_sizing"]

    for symbol, weight in all_target_weights.items():
        current_qty = positions.get(symbol, 0)
        intent_log["assets"][symbol]["current"]["qty"] = float(current_qty)
        target_qty = weight * account_val / ltps[symbol]
        intent_log["assets"][symbol]["target"]["qty"] = float(target_qty)

    target_qtys_dict = {s: intent_log["assets"][s]["target"]["qty"] for s in all_target_weights}
    order_intentions = get_order_intention(target_qtys=target_qtys_dict, positions_input=positions, logger=logger)

    for coin, order in order_intentions.items():
        intent_log["assets"][coin]["order_intent"].update({
            "coin": order["coin"],
            "side": order["side"],
            "delta": order["delta"],
            "target": order["target"],
            "current": order["current"],
        })

    intent_logger.log(intent_log)
    return order_intentions


def run_shadow_sizing(prices, mu, vo, cor, positions, ltps, base_intent, live_config,
                      latest_view, logger, shadow_logger):
    """Compute + log the *other* sizing model on the same inputs, to a side file.

    Reuses run_live so the shadow record is a full intent (weights + would-be
    order intentions vs the real book), but its return value is discarded and the
    executor never reads ``shadow_logger``'s file — so no real-money path touches it.
    """
    shadow_model = "mvo" if live_config.sizing_model != "mvo" else "risk_parity"
    shadow_config = replace(live_config, sizing_model=shadow_model)

    # Deep-copy the pre-sizing base (same market/signal/risk/portfolio inputs);
    # re-stamp meta provenance for the shadow strategy (keep run_id/git to join).
    shadow_intent = copy.deepcopy(base_intent)
    prov = strategy_registry.provenance(shadow_config)
    shadow_intent["meta"]["strategy"] = prov["strategy"]
    shadow_intent["meta"]["strategy_components"] = prov["strategy_components"]
    shadow_intent["meta"]["config_hash"] = prov["config_hash"]

    try:
        run_live(prices, mu, vo, cor, positions, ltps, shadow_intent, shadow_config,
                 latest_view, logger, shadow_logger)
        logger.info(f"[shadow] logged {prov['strategy']} intent to side file")
    except Exception:
        logger.warning("[shadow] sizing failed — live path unaffected", exc_info=True)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    import time

    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from src.config import make_info, open_orders as hl_open_orders, HL_API_URL, PRIVATE_KEY, WALLET_ADDRESS, API_ADDRESS
    from scripts.exchange_state import read_latest_exchange_state, run_exchange_state, get_account_equity
    from scripts.meta_data import get_hl_coins, read_latest_meta
    from scripts.run_fill_logger import main as run_fill_logger
    from src.ingestion.hyperliquid import run_ohlcv_dl, update_daily

    # ── Setup ──────────────────────────────────────────────────────────────────
    root = Path().resolve()
    while not (root / "src").exists():
        root = root.parent
    os.chdir(root)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    _err_handler = logging.FileHandler("logs/errors.log")
    _err_handler.setLevel(logging.ERROR)
    _err_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(_err_handler)

    from src.config import TRADING_ENV
    STATE_PATH = Path(f"state/hyperliquid_{TRADING_ENV}_{WALLET_ADDRESS}_state.json")

    intent_logger = IntentLogger(f"logs/intent_{TRADING_ENV}.jsonl")
    order_logger = OrderLogger(f"logs/orders_{TRADING_ENV}.jsonl")
    # Parallel-validation sink: the executor never reads this file.
    shadow_intent_logger = IntentLogger(f"logs/intent_{TRADING_ENV}_shadow.jsonl")

    state = load_state(STATE_PATH)

    # Bootstrap meta on first run so PositionRebuilder can load sz_decimals
    if not state.get("last_meta_run_ms"):
        try:
            from scripts.meta_data import fetch_meta, store_meta
            store_meta(fetch_meta())
            state["last_meta_run_ms"] = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.info("[meta] bootstrapped on first run")
        except Exception:
            logger.warning("[meta] bootstrap failed — fill logger may error until meta is available", exc_info=True)

    threading.Thread(target=_watchdog, daemon=True).start()

    first_run = True

    while True:
        now = dt.datetime.now(dt.timezone.utc)
        run_id = generate_run_id()
        Path("state/heartbeat.ms").write_text(str(int(now.timestamp() * 1000)))

        if first_run:
            logger.info(f"[startup] env={TRADING_ENV.upper()} | account={WALLET_ADDRESS}")
            logger.info(f"[startup] loop started at {now.isoformat()}")
            logger.info(
                f"[startup] state loaded: last_data_run_ms={state.get('last_data_run_ms')}, "
                f"last_trading_intent_run_id={state.get('last_trading_intent_run_id')}, "
                f"last_trading_exec_ms={state.get('last_trading_exec_ms')}, "
                f"has_open_orders={state.get('has_open_orders')}, "
                f"fills_logged_at_ms={state.get('fills_logged_at_ms')}"
            )

        # ── Day-open snapshot (once per calendar day, first tick after midnight) ─
        if is_day_open_due(now, state):
            try:
                from src.ingestion.update_mids import get_all_ltps
                _info = make_info()
                _day_open = get_all_ltps(_info)
                _day_open.to_csv("data/snapshots/day_open.csv", index=False)
                state["last_day_open_ms"] = int(now.timestamp() * 1000)
                save_state(state, STATE_PATH)
                logger.info("[day_open] snapshot saved")
            except Exception:
                logger.warning("[day_open] failed to save snapshot", exc_info=True)

        # ── Fill logger ────────────────────────────────────────────────────────
        if state.get("has_open_orders", False):
            open_orders = run_fill_logger()
            state = load_state(STATE_PATH)
            state["has_open_orders"] = bool(open_orders)
            state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
        elif first_run:
            logger.info("[fill_logger] no open orders, skipping tick poll")

        # ── Position reconciliation (hourly) ───────────────────────────────────
        if is_position_check_due(now, state):
            open_orders = run_fill_logger()
            state = load_state(STATE_PATH)
            state["has_open_orders"] = bool(open_orders)
            state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
            try:
                exchange_state = run_exchange_state()
                exchange_positions = {
                    row["position"]["coin"]: float(row["position"]["szi"])
                    for row in exchange_state["assetPositions"]
                }
                state_positions = get_state_positions(state)
                diff = dict_diff(exchange_positions, state_positions)
                if diff["changed"]:
                    logger.warning(f"Position mismatch — state vs exchange: {diff['changed']}")
                    state["positions_match_exchange"] = False
                else:
                    state["positions_match_exchange"] = True
            except Exception:
                logger.warning("Position check failed: could not reach exchange")
                state["positions_match_exchange"] = None
            state["last_position_check_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
        elif first_run:
            last_ms = state.get("last_position_check_ms", 0)
            next_due = dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc) + dt.timedelta(hours=POSITION_CHECK_INTERVAL_HOURS)
            logger.info(f"[position_check] not due, next at {next_due.isoformat()}")

        # ── Data task (nightly at 23:45 UTC) ──────────────────────────────────
        # 1. Download HL OHLCV (for coins not in Binance data); skipped on testnet.
        # 2. build() reads daily_closes.parquet (Binance) + HL DuckDB → daily_ohlcv.parquet.
        # Today's live prices are appended in-memory at intent time via get_final_pricing().
        if is_data_due(now, state):
            logger.info("[data] rebuilding OHLCV cache from Binance + HL data")
            if TRADING_ENV != "testnet":
                run_ohlcv_dl()
                update_daily()
            from scripts.build_daily_cache import build as _build_daily_cache
            _build_daily_cache()
            state["last_data_run_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.info("[data] complete")
        elif first_run:
            last_ms = state.get("last_data_run_ms", 0)
            logger.info(f"[data] not due (last run: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, scheduled: {DATA_HOUR_UTC:02d}:{DATA_MINUTE_UTC:02d} UTC)")

        # ── Market cap task (daily at 00:05 UTC) ───────────────────────────────
        if is_mkt_cap_due(now, state):
            logger.debug("[mkt_cap] fetching latest market cap data")
            store_market_cap(get_top_marketcap(200))
            state["last_mkt_cap_run_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.debug("[mkt_cap] complete")
        elif first_run:
            last_ms = state.get("last_mkt_cap_run_ms", 0)
            logger.debug(f"[mkt_cap] not due (last run: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, scheduled: {MKT_CAP_HOUR_UTC:02d}:{MKT_CAP_MINUTE_UTC:02d} UTC)")

        # ── Exchange meta task (daily at 23:45 UTC) ───────────────────────────
        if is_meta_due(now, state):
            try:
                from scripts.meta_data import fetch_meta, store_meta
                store_meta(fetch_meta())
                state["last_meta_run_ms"] = int(now.timestamp() * 1000)
                save_state(state, STATE_PATH)
                logger.info("[meta] snapshot saved")
            except Exception:
                logger.warning("[meta] failed to fetch exchange meta", exc_info=True)
        elif first_run:
            last_ms = state.get("last_meta_run_ms", 0)
            logger.info(f"[meta] not due (last run: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, scheduled: {META_HOUR_UTC:02d}:{META_MINUTE_UTC:02d} UTC)")

        # ── Trading intent task (daily) ────────────────────────────────────────
        if is_trading_intent_due(now, state):
            logger.info("[intent] computing trading intent")
            config = StrategyConfig()
            prov = strategy_registry.provenance(config)
            intent = init_intent(mode="live", strategy_name=prov["strategy"], run_id=run_id, provenance=prov)
            logger.info(f"[intent] strategy={prov['strategy']} commit={prov['git_commit']} "
                        f"dirty={prov['git_dirty']} config_hash={prov['config_hash']}")
            state = load_state(STATE_PATH)
            positions = get_state_positions(state)
            try:
                exchange_state = run_exchange_state()
            except Exception:
                logger.warning("can't fetch exchange state for intent, using latest cached")
                exchange_state = read_latest_exchange_state()

            meta = read_latest_meta()
            top = get_latest_market_cap()
            hl = get_hl_coins()
            universe, symbol_index = get_universe(top, hl, state.get("universe"))
            logger.info(
                f"[intent] tradable universe: {len(universe)}/{UNIVERSE_SIZE} "
                f"(top-{UNIVERSE_SIZE} by market cap, narrowed by exchange listing; "
                f"size varies as listings change)"
            )

            # ── Data availability + freshness guard ──────────────────────────
            # The cache may be missing (cold start before the nightly build) or
            # stale (dead producer / unfinished rebuild). In every case skip and
            # retry on the next tick rather than crash the process or size on a
            # stale/gapped series. The provisional close (binance-klines live
            # buffer, ~23:40 UTC) + the later official bar both land before this
            # decision, so a current cache should hold the expected most-recent day.
            _intent_skip_reason = None
            try:
                prices_all = get_ohlcv()
                latest_cache_date = prices_all.index.max().date()
                expected_date = (now - dt.timedelta(days=1)).date()
                if latest_cache_date < expected_date:
                    _intent_skip_reason = (
                        f"data stale: latest cached close {latest_cache_date} < "
                        f"expected {expected_date}"
                    )
            except (FileNotFoundError, RuntimeError) as e:
                _intent_skip_reason = f"cache not ready ({e})"
            if _intent_skip_reason is not None:
                _stale_key = now.strftime("%Y%m%dT%H%M")
                if state.get("intent_stale_logged_min") != _stale_key:
                    logger.warning(f"[intent] {_intent_skip_reason} — skipping, will retry next tick")
                    state["intent_stale_logged_min"] = _stale_key
                    save_state(state, STATE_PATH)
                # Don't mark the intent done; take the normal tick sleep (not a bare
                # `continue`, which would busy-loop) and retry.
                if first_run:
                    first_run = False
                sleep_until_next_tick(state)
                continue
            _is_prov = latest_is_provisional()
            intent["meta"]["provisional_close"] = _is_prov
            if _is_prov:
                logger.info(
                    f"[intent] using PROVISIONAL close for {latest_cache_date} "
                    f"(official Binance bar not yet published)"
                )

            _missing = [c for c in universe if c not in prices_all.columns]
            if _missing:
                logger.warning(f"[intent] {len(_missing)} coins absent from price cache, dropping from universe: {_missing}")
                universe = [c for c in universe if c in prices_all.columns]
                symbol_index = {sym: i for i, sym in enumerate(universe)}
            ltps = update_ltps()
            latest_view = pd.read_csv("data/snapshots/mids.csv", index_col=0)
            prices, returns_adj = get_final_pricing(prices_all, universe, latest_view)

            intent["universe"]["tradable"] = universe
            intent = initialise_asset_intent(intent, universe)
            # Source sizing equity from the unified-margin USDC balance (not
            # marginSummary.accountValue) — keeps HEAD's fix over dev's version.
            account_equity = get_account_equity(exchange_state)
            intent["portfolio"]["equity_usd"] = account_equity
            intent["portfolio"]["equity_used_for_sizing"] = account_equity
            intent["portfolio"]["maintenance_margin"] = exchange_state["crossMaintenanceMarginUsed"]
            intent["portfolio"]["gross_exposure_pre_rebal"] = exchange_state["marginSummary"]["totalNtlPos"]
            intent = add_ltp_to_intent(intent, latest_view)

            ewmac_forecast = ewmac(returns_adj, config.ewmac_fast)
            breakout_forecast = breakout(prices, config.breakout_window)
            bollinger_forecast = scaled_bollinger(prices, param=config.bollinger_window, scalar=1)

            o, h, l, c_alpha, v = load_ohlcv_for_alphas(universe)
            o        = o.reindex(index=prices.index, columns=prices.columns)
            h        = h.reindex(index=prices.index, columns=prices.columns)
            l        = l.reindex(index=prices.index, columns=prices.columns)
            v        = v.reindex(index=prices.index, columns=prices.columns)
            c_alpha  = c_alpha.reindex(index=prices.index, columns=prices.columns)
            r_alpha  = np.log(c_alpha).diff()

            alpha006_forecast = alpha006(o, v)
            alpha014_forecast = alpha014(o, v, r_alpha)
            alpha020_forecast = alpha020(o, h, l, c_alpha)

            mu = np.mean([bollinger_forecast, ewmac_forecast, breakout_forecast, alpha006_forecast, alpha014_forecast, alpha020_forecast], axis=0)
            vo = prices.pct_change().ewm(com=config.vo_window, min_periods=20).std().values
            cor = returns_adj.ewm(com=config.correlation, min_periods=config.correlation).corr()

            for symbol in universe:
                intent["assets"][symbol]["model"]["vol_1d"] = float(vo[-1, symbol_index[symbol]])
                intent["assets"][symbol]["model"]["signal"] = {
                    "mu": float(mu[-1, symbol_index[symbol]]),
                    "sub_signals": {
                        "ewmac": float(ewmac_forecast[-1, symbol_index[symbol]]),
                        "breakout": float(breakout_forecast[-1, symbol_index[symbol]]),
                        "bollinger": float(bollinger_forecast[-1, symbol_index[symbol]]),
                        "alpha014": float(alpha014_forecast[-1, symbol_index[symbol]]),
                        "alpha020": float(alpha020_forecast[-1, symbol_index[symbol]]),
                    },
                }
            intent["risk_inputs"]["correlation_matrix"] = cor.loc[prices.index[-1]].to_dict()

            # Snapshot the pre-sizing intent so the shadow runs off identical inputs.
            shadow_base = copy.deepcopy(intent) if SHADOW_SIZING else None

            run_live(prices, mu, vo, cor, positions, ltps, intent, config, latest_view, logger, intent_logger)

            if SHADOW_SIZING:
                run_shadow_sizing(prices, mu, vo, cor, positions, ltps, shadow_base, config,
                                  latest_view, logger, shadow_intent_logger)

            state["last_trading_intent_run_id"] = run_id
            state["universe"] = universe
            save_state(state, STATE_PATH)
            logger.info(f"[intent] complete, run_id={run_id}, universe size={len(universe)}")
        elif first_run:
            last_id = state.get("last_trading_intent_run_id", "never")
            logger.info(f"[intent] not due (last: {last_id}, scheduled: {TRADING_INTENT_HOUR_UTC:02d}:{TRADING_INTENT_MINUTE_UTC:02d} UTC)")

        # ── Execution task ─────────────────────────────────────────────────────
        if is_trading_exec_due(now, state):
            try:
                logger.info("[exec] running execution plan")
                meta = read_latest_meta()
                sz_decimals = {coin["name"]: coin["szDecimals"] for coin in meta["universe"]}
                info = make_info()
                wallet = Account.from_key(PRIVATE_KEY)
                spot_meta = {"universe": [], "tokens": []} if TRADING_ENV == "testnet" else None
                ex = Exchange(wallet=wallet, base_url=HL_API_URL, account_address=API_ADDRESS, spot_meta=spot_meta)

                intent_from_file = intent_logger.read_latest()
                if not intent_from_file:
                    logger.warning("[exec] no intent file found, skipping execution until next intent run")
                    state["last_trading_exec_ms"] = int(now.timestamp() * 1000)
                    save_state(state, STATE_PATH)
                    continue
                order_intentions = {
                    asset: intent_from_file["assets"][asset]["order_intent"]
                    for asset in intent_from_file["assets"]
                }

                ltps = update_ltps()
                try:
                    exchange_state = run_exchange_state()
                    positions = {
                        row["position"]["coin"]: float(row["position"]["szi"])
                        for row in exchange_state["assetPositions"]
                    }
                except Exception:
                    logger.warning("can't fetch live positions for execution plan, falling back to state file")
                    positions = get_state_positions(state)

                if not DRY_RUN:
                    open_orders = hl_open_orders(info, WALLET_ADDRESS)
                    if open_orders:
                        cancels = [{"coin": o["coin"], "oid": o["oid"]} for o in open_orders]
                        logger.debug(f"Cancel Requests: {cancels}")
                        resp = ex.bulk_cancel(cancels)
                        logger.debug(f"Cancel Response: {resp}")

                orders = get_execution_plan(order_intentions, ltps, sz_decimals, logger, positions=positions)

                # ── Pre-flight circuit breaker ───────────────────────────────
                # Sanity-gate the batch before it reaches the exchange. On any
                # violation, block the whole submission and retry next cycle.
                equity = float(intent_from_file["portfolio"].get("equity_used_for_sizing") or 0)
                if orders and equity > 0:
                    preflight = preflight_check(
                        orders, ltps,
                        max_order_notional_usd=PREFLIGHT_MAX_ORDER_NOTIONAL_MULT * equity,
                        max_gross_notional_usd=PREFLIGHT_MAX_GROSS_NOTIONAL_MULT * equity,
                        max_order_count=PREFLIGHT_MAX_ORDER_COUNT,
                        max_price_deviation=PREFLIGHT_MAX_PRICE_DEVIATION,
                    )
                    if not preflight.ok:
                        for v in preflight.violations:
                            logger.error(f"[exec] preflight BLOCKED: {v}")
                        logger.error(
                            f"[exec] circuit breaker tripped — skipping submission of "
                            f"{len(orders)} orders this tick"
                        )
                        orders = []  # block all; existing flow records the tick and retries next cycle
                elif orders:
                    logger.warning("[exec] preflight skipped — equity unknown, can't size notional caps")

                halted = False
                if not DRY_RUN and orders:
                    print(generate_readable_summary(orders, ltps))
                    # Strip to the fields the exchange expects; keep the full dict for logging.
                    wire_keys = ("coin", "is_buy", "sz", "limit_px", "order_type", "reduce_only")
                    wire_orders = [{k: o[k] for k in wire_keys} for o in orders]
                    response = ex.bulk_orders(wire_orders)
                    if response.get("status") == "ok":
                        all_statuses = response["response"]["data"]["statuses"]
                        # HL returns top-level "ok" even when individual orders are
                        # rejected ({"error": ...}); classify each status so rejections
                        # (and their reason) are surfaced instead of counted as submitted.
                        result = classify_order_responses(orders, all_statuses)
                        if result.count_mismatch:
                            logger.error(
                                f"[exec] status count {len(all_statuses)} != order count "
                                f"{len(orders)} — response may be misaligned: {response}"
                            )
                        for co in result.classified:
                            order_logger.log_order_submission(
                                run_id=run_id,
                                exchange="hyperliquid",
                                account=WALLET_ADDRESS,
                                symbol=co.coin,
                                side="buy" if co.order["is_buy"] else "sell",
                                order_type="LIMIT",
                                price=co.order["limit_px"],
                                qty=co.order["sz"],
                                target_qty=co.order.get("target_qty"),
                                current_qty=co.order.get("current_qty"),
                                delta=co.order.get("delta"),
                                response={"response": {"data": {"statuses": [co.raw_status]}}},
                            )
                        for co in result.rejected:
                            logger.error(f"[exec] order REJECTED [{co.status.value}] — {co.coin}: {co.error}")
                        if result.rejected:
                            logger.warning(
                                f"[exec] {len(result.rejected)}/{len(orders)} orders rejected by exchange "
                                f"(accepted {len(result.accepted)})"
                            )
                            # "Trading is halted." is a transient exchange-side condition;
                            # flag it so we retry soon instead of waiting the full interval.
                            halted = result.halted
                        if result.accepted:
                            logging.info(f"[exec] Rebalance triggered — {len(result.accepted)}/{len(orders)} orders accepted")
                    else:
                        logger.error(f"[exec] bulk submission failed (top-level): {response}")

                open_orders = run_fill_logger()
                state = load_state(STATE_PATH)
                state["has_open_orders"] = bool(open_orders)
                state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
                if halted:
                    # Backdate the exec gate so the next tick is due in ~HALT_RETRY_MINUTES
                    # rather than the full interval — resume promptly when the halt lifts.
                    retry_offset = dt.timedelta(minutes=TRADING_EXEC_INTERVAL_MINUTES - HALT_RETRY_MINUTES)
                    state["last_trading_exec_ms"] = int((now - retry_offset).timestamp() * 1000)
                    logger.warning(f"[exec] exchange halted — retrying in ~{HALT_RETRY_MINUTES} min")
                else:
                    state["last_trading_exec_ms"] = int(now.timestamp() * 1000)
                save_state(state, STATE_PATH)
            except Exception:
                logger.warning("[exec] failed", exc_info=True)
                state["last_trading_exec_ms"] = int(now.timestamp() * 1000)
                save_state(state, STATE_PATH)
        elif first_run:
            last_ms = state.get("last_trading_exec_ms", 0)
            next_due = dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc) + dt.timedelta(minutes=TRADING_EXEC_INTERVAL_MINUTES)
            logger.debug(f"[exec] not due (last: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, next: {next_due.isoformat()}, gate: after {TRADING_EXEC_HOUR_UTC:02d}:00 UTC)")

        if first_run:
            first_run = False

        sleep_until_next_tick(state)


if __name__ == "__main__":
    main()
