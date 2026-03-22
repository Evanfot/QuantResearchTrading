# %% %%
import sys
from pathlib import Path
root = Path().resolve()
while not (root / "src").exists():
    root = root.parent
import os

os.chdir(root)
print("Working directory:", root)
import time
import json
import logging
import pandas as pd
import importlib
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
import warnings
import datetime as dt
import numpy as np
import requests
import duckdb
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from typing import List, Dict, Any
from src.state.strategy_state import load_state, save_state, get_state_positions
from src.signal import ewmac, breakout, scaled_bollinger, carry_signal
from src.backtester.full_backtest import StrategyConfig, StrategyIntent, compute_strategy, run_backtest
from src.loggers.intent_logger import (
    init_intent,
    init_asset,
    IntentLogger,
    generate_run_id,
)
from src.loggers.order_logger import OrderLogger
from src.helpers.dict_diff import dict_diff
from scripts.exchange_state import run_exchange_state, read_latest_exchange_state
from scripts.meta_data import read_latest_meta

# TODO: set meta_data, latest_market_caps to run every day
from scripts.meta_data import get_hl_coins
from scripts.mkt_cap_data import get_latest_market_cap
from src.ingestion.update_mids import run_update_mids
from src.ingestion.hyperliquid import run_ohlcv_dl, update_daily, update_latest_view
from scripts.run_fill_logger import main as run_fill_logger

# LOGGING CONFIG
# --------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

intent_logger = IntentLogger("logs/intent.jsonl")
order_logger = OrderLogger("logs/orders.jsonl")
# --------------------------------------------
# SETTINGS
# --------------------------------------------
load_dotenv()
PRIVATE_KEY = os.getenv("HYPERLIQUID_PRIVATE_KEY")
WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")
API_ADDRESS = os.getenv("HYPERLIQUID_API_WALLET_ADDRESS")
db_path = "data/pricing/ohlcv_data.duckdb"
DRY_RUN = False  # %% set to False to enable live trading

STATE_PATH = Path(f"state/hyperliquid_{WALLET_ADDRESS}_state.json")

# %%
# SCHEDULING RULES
# --------------------------------------------
DATA_HOUR_UTC = 0
DATA_MINUTE_UTC = 1
TRADING_EXEC_HOUR_UTC = 7  # 07:00 UTC
TRADING_EXEC_INTERVAL_MINUTES = 30
TRADING_INTENT_HOUR_UTC = 0  # 00:00 UTC
TRADING_INTENT_MINUTE_UTC = 1
POSITION_CHECK_INTERVAL_HOURS = 1

def is_position_check_due(now, state):
    last_ms = state.get("last_position_check_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= POSITION_CHECK_INTERVAL_HOURS * 3600

def is_data_due(now, state):
    if state.get("last_data_run_ms") is None:
        return True

    last_run_ms = state["last_data_run_ms"]
    last_run = dt.datetime.fromtimestamp(last_run_ms / 1000, tz=dt.timezone.utc)
    last = last_run.date()
    today = now.date()

    return today > last and now.hour >= DATA_HOUR_UTC and now.minute >= DATA_MINUTE_UTC

def is_trading_intent_due(now, state):
    last_run_id = state.get("last_trading_intent_run_id")
    if last_run_id is None:
        return True

    ts_str = last_run_id.split("_")[0]  # e.g. "20240321T120000Z"
    last_run = dt.datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
    last = last_run.date()
    today = now.date()
    return today > last and now.hour >= TRADING_INTENT_HOUR_UTC and now.minute >= TRADING_INTENT_MINUTE_UTC

def is_trading_exec_due(now, state):
    if now.hour < TRADING_EXEC_HOUR_UTC:
        return False
    last_ms = state.get("last_trading_exec_ms")
    if not last_ms:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= TRADING_EXEC_INTERVAL_MINUTES * 60

def sleep_until_next_tick(state):
    time.sleep(1)

def main():
    state = load_state(STATE_PATH)
    first_run = True

    while True:
        now = dt.datetime.now(dt.timezone.utc)
        run_id = generate_run_id()

        if first_run:
            logger.setLevel(logging.INFO)
            logger.info(f"[startup] loop started at {now.isoformat()}")
            logger.info(f"[startup] state loaded: last_data_run_ms={state.get('last_data_run_ms')}, "
                        f"last_trading_intent_run_id={state.get('last_trading_intent_run_id')}, "
                        f"last_trading_exec_ms={state.get('last_trading_exec_ms')}, "
                        f"has_open_orders={state.get('has_open_orders')}, "
                        f"fills_logged_at_ms={state.get('fills_logged_at_ms')}")

        # ---- FILL LOGGER (runs each tick while open orders remain) ----
        if state.get("has_open_orders", False):
            logger.info("[fill_logger] open orders detected, polling fills")
            open_orders = run_fill_logger()
            state["has_open_orders"] = bool(open_orders)
            state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.info(f"[fill_logger] done, open_orders remaining: {len(open_orders)}")
        elif first_run:
            logger.info("[fill_logger] no open orders, skipping tick poll")

        # ---- POSITION RECONCILIATION + FILL LOGGER (once per hour) ----
        if is_position_check_due(now, state):
            logger.info("[position_check] running hourly fill log + position reconciliation")
            open_orders = run_fill_logger()
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
                    logger.warning(
                        f"Position mismatch — state vs exchange: {diff['changed']}"
                    )
                else:
                    logger.info(f"[position_check] {len(state_positions)} positions match exchange")
            except:
                logger.warning("Position check failed: could not reach exchange")
            state["last_position_check_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
        elif first_run:
            last_ms = state.get("last_position_check_ms", 0)
            next_due = dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc) + dt.timedelta(hours=POSITION_CHECK_INTERVAL_HOURS)
            logger.info(f"[position_check] not due, next at {next_due.isoformat()}")

        # ---- DATA TASK (daily at 00:01 UTC) ----
        if is_data_due(now, state):
            logger.info("[data] downloading OHLCV data")
            open_orders = run_fill_logger()
            state["has_open_orders"] = bool(open_orders)
            state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            run_ohlcv_dl()
            update_daily()
            update_latest_view()
            state["last_data_run_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.info("[data] complete")
        elif first_run:
            last_ms = state.get("last_data_run_ms", 0)
            logger.info(f"[data] not due (last run: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, scheduled: {DATA_HOUR_UTC:02d}:{DATA_MINUTE_UTC:02d} UTC)")

        # ---- TRADING INTENT TASK (once per day at TRADING_HOUR_UTC) ----
        if is_trading_intent_due(now, state):
            logger.info("[intent] computing trading intent")
            intent = init_intent(mode="live", strategy_name="trend_v1", run_id=run_id)
            config = StrategyConfig()
            state = load_state(STATE_PATH)
            positions = get_state_positions(state)
            try:
                exchange_state = run_exchange_state()
            except:
                logger.warning("can't fetch exchange state for intent, using latest cached")
                exchange_state = read_latest_exchange_state()

            # Get asset metadata
            # TODO: Add try except to download latest meta and read from file in case it doesn't work
            meta = read_latest_meta()

            # Get universe and initialise relevant rows in intent
            top = get_latest_market_cap()
            hl = get_hl_coins()
            universe, symbol_index = get_hyperliquid_trading_universe(top, hl)
            # Get pricing and add to intent
            conn = duckdb.connect(db_path)
            hyperliquid_prices = get_ohlcv(conn)
            ltps = update_ltps()
            # TODO: Change below to use ltps instead of latest_view
            latest_view = pd.read_csv("data/snapshots/mids.csv", index_col=0)
            prices, returns_adj = get_final_pricing(
                hyperliquid_prices, universe, latest_view
            )
            intent["universe"]["tradable"] = universe
            intent = initialise_asset_intent(intent, universe)
            intent["portfolio"]["equity_usd"] = float(
                exchange_state["marginSummary"]["accountValue"]
            )
            # TODO: If exchange_state isn't latest, apply haircut?
            # Add exchange state to intent
            # TODO: add function to log all things from exchange_state
            # TODO: the equity used for sizing should be in execution block, not intent block
            intent["portfolio"]["equity_used_for_sizing"] = float(
                exchange_state["marginSummary"]["accountValue"]
            )
            intent["portfolio"]["maintenance_margin"] = exchange_state[
                "crossMaintenanceMarginUsed"
            ]
            intent["portfolio"]["gross_exposure_pre_rebal"] = exchange_state[
                "marginSummary"
            ]["totalNtlPos"]

            intent = add_ltp_to_intent(intent, latest_view)
            # Calc signals, vols, corr and add to intent
            ewmac_forecast = ewmac(returns_adj, config.ewmac_fast)
            breakout_forecast = breakout(prices, config.breakout_window)
            bollinger_forecast = scaled_bollinger(
                prices, param=config.bollinger_window, scalar=1
            )
            mu = np.mean(
                [bollinger_forecast, ewmac_forecast, breakout_forecast], axis=0
            )
            vo = (
                prices.pct_change()
                .ewm(com=config.vo_window, min_periods=20)
                .std()
                .values
            )
            cor = returns_adj.ewm(
                com=config.correlation, min_periods=config.correlation
            ).corr()
            for symbol in universe:
                intent["assets"][symbol]["model"]["vol_1d"] = float(
                    vo[-1, symbol_index[symbol]]
                )
                intent["assets"][symbol]["model"]["signal"] = {
                    "mu": float(mu[-1, symbol_index[symbol]]),
                    "sub_signals": {
                        "ewmac": float(ewmac_forecast[-1, symbol_index[symbol]]),
                        "breakout": float(
                            breakout_forecast[-1, symbol_index[symbol]]
                        ),
                        "bollinger": float(
                            bollinger_forecast[-1, symbol_index[symbol]]
                        ),
                    },
                }
            intent["risk_inputs"]["correlation_matrix"] = cor.loc[
                prices.index[-1]
            ].to_dict()
            # Get target weights
            order_intentions = run_live(
                prices,
                mu,
                vo,
                cor,
                positions,
                ltps,
                intent,
                config,
                latest_view,
                logger,
                intent_logger,
            )
            state["last_trading_intent_run_id"] = run_id
            save_state(state, STATE_PATH)
            logger.info(f"[intent] complete, run_id={run_id}, universe size={len(universe)}")
        elif first_run:
            last_id = state.get("last_trading_intent_run_id", "never")
            logger.info(f"[intent] not due (last: {last_id}, scheduled: {TRADING_INTENT_HOUR_UTC:02d}:{TRADING_INTENT_MINUTE_UTC:02d} UTC)")

        # ---- EXECUTION TASK ----
        if is_trading_exec_due(now, state):
            logger.info("[exec] running execution plan")
            meta = read_latest_meta()
            sz_decimals = {
                coin["name"]: coin["szDecimals"] for coin in meta["universe"]
            }
            info = Info(MAINNET_API_URL, skip_ws=True)
            wallet = Account.from_key(PRIVATE_KEY)
            ex = Exchange(
                wallet=wallet, base_url=MAINNET_API_URL, account_address=API_ADDRESS
            )

            intent_from_file = intent_logger.read_latest()
            asset_list = [asset for asset in intent_from_file["assets"]]
            order_intentions = {}
            for asset in asset_list:
                order_intentions[asset] = intent_from_file["assets"][asset][
                    "order_intent"
                ]

            ltps = update_ltps()
            try:
                exchange_state = run_exchange_state()
                positions = {
                    row["position"]["coin"]: float(row["position"]["szi"])
                    for row in exchange_state["assetPositions"]
                }
            except:
                logger.warning("can't fetch live positions for execution plan, falling back to state file")
                positions = get_state_positions(state)
            # 1. Cancel all open orders before generating new plan
            if not DRY_RUN:
                open_orders = info.open_orders(WALLET_ADDRESS)
                if open_orders:
                    cancels = [{"coin": o["coin"], "oid": o["oid"]} for o in open_orders]
                    logger.info(f"Cancel Requests: {cancels}")
                    resp = ex.bulk_cancel(cancels)
                    logger.info(f"Cancel Response: {resp}")
            # 2. Generate the plan
            orders = get_execution_plan(
                order_intentions, ltps, sz_decimals, logger, positions=positions
            )
            if not DRY_RUN:

                # 3. Place New Orders
                exchange = "hyperliquid"
                if orders:
                    trading_summary = generate_readable_summary(orders, ltps)
                    print(trading_summary)
                    response = ex.bulk_orders(orders)

                    if response.get("status") == "ok":
                        all_statuses = response["response"]["data"]["statuses"]
                        for i, status in enumerate(all_statuses):
                            mock_res = {"response": {"data": {"statuses": [status]}}}
                            order_info = orders[i]

                            order_logger.log_order_submission(
                                run_id=run_id,
                                exchange=exchange,
                                account=WALLET_ADDRESS,
                                symbol=order_info["coin"],
                                side="buy" if order_info["is_buy"] else "sell",
                                order_type="LIMIT",
                                price=order_info["limit_px"],
                                qty=order_info["sz"],
                                response=mock_res,
                            )
                        logging.info("Rebalancing complete.")
                    else:
                        print(f"Bulk submission failed: {response}")
            open_orders = run_fill_logger()
            state["has_open_orders"] = bool(open_orders)
            state["fills_logged_at_ms"] = int(now.timestamp() * 1000)
            state["last_trading_exec_ms"] = int(now.timestamp() * 1000)
            save_state(state, STATE_PATH)
            logger.info(f"[exec] complete, orders placed: {len(orders) if orders else 0}")
        elif first_run:
            last_ms = state.get("last_trading_exec_ms", 0)
            next_due = dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc) + dt.timedelta(minutes=TRADING_EXEC_INTERVAL_MINUTES)
            logger.info(f"[exec] not due (last: {dt.datetime.fromtimestamp((last_ms or 0) / 1000, tz=dt.timezone.utc).isoformat()}, next: {next_due.isoformat()}, gate: after {TRADING_EXEC_HOUR_UTC:02d}:00 UTC)")

        if first_run:
            logger.info("[startup] first iteration complete, switching to ERROR-only logging")
            logger.setLevel(logging.ERROR)
            first_run = False

        sleep_until_next_tick(state)


# # %% Universe

# # Known stablecoin symbols
stable = {"USDT", "USDC", "DAI", "USDD", "FDUSD", "TUSD", "DEI", "USDP", "GUSD", "USDE"}


def get_hyperliquid_trading_universe(top_market_cap, hl_universe):

    filtered = []
    for index, coin in top_market_cap.iterrows():
        symbol = coin["symbol"].upper()
        if symbol == "PAXG":
            continue
        if symbol in stable:
            continue
        if symbol in [k["symbol"] for k in filtered]:
            continue
        if symbol in hl_universe:
            filtered.append(
                {
                    "name": coin["name"],
                    "symbol": symbol,
                    "market_cap": coin["market_cap"],
                }
            )

    # Top 50 Hyperliquid-listed coins by market cap
    result = filtered[:50]

    universe = list(set([k["symbol"] for k in result]))

    symbol_index = {universe[k]: k for k in range(len(universe))}
    return universe, symbol_index


def initialise_asset_intent(intent, universe):
    for symbol in universe:
        intent["assets"][symbol] = init_asset()
    return intent


# %% PRICES:
def get_ohlcv(conn):
    df = conn.execute(
        """
        SELECT datetime, symbol, close, volume
        FROM hyperliquid_1d
    """
    ).df()
    conn.close()
    hyperliquid_prices = df.pivot(index="datetime", columns="symbol", values="close")
    return hyperliquid_prices


def update_ltps():
    run_update_mids()
    latest_view = pd.read_csv("data/snapshots/mids.csv")
    latest_view.set_index("symbol", inplace=True)
    ltps = latest_view.to_dict()["mid"]
    ltps = {key: float(val) for key, val in ltps.items()}
    return ltps


def add_ltp_to_intent(intent, latest_view):
    for symbol in intent["assets"].keys():
        intent["assets"][symbol]["market"]["mark_price"] = float(
            latest_view.at[f"{symbol}", "mid"]
        )
        intent["assets"][symbol]["market"]["data_timestamp"] = latest_view.loc[
            f"{symbol}", "downloaded_at"
        ]
    return intent


# TODO: Come up with an overwrite for coins which don't have data history
# %%
vola_value = 36
winsor_value = 4.2


def get_final_pricing(hyperliquid_prices, universe, latest_view):
    hype_universe = [k + "/USDC:USDC" for k in universe]
    prices = hyperliquid_prices[hype_universe]
    prices.columns = prices.columns.str.replace("/USDC:USDC", "")
    prices = prices.copy(deep=True)
    prices.loc[
        dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    ] = latest_view.loc[prices.columns.values, ("mid")].astype("float")
    returns = np.log(prices).diff()
    returns_adj = (returns / returns.ewm(com=vola_value, min_periods=120).std()).clip(
        -winsor_value, +winsor_value
    )
    return prices, returns_adj


# %%

# %%
shrinkage_value = 0.5
threshold_trade = False
ignore_small = False
add_commision = False



def log_strategy_intent(
    intent_log: Dict[str, Any],
    prices: pd.DataFrame,
    strategy_intent: StrategyIntent,
    config: StrategyConfig,
):
    mask = strategy_intent.mask
    tradable = list(prices.columns.values[mask])
    non_tradable = list(prices.columns.values[~mask])

    intent_log["universe"]["tradable"] = tradable
    intent_log["universe"]["non_tradable"] = non_tradable

    weights = (
        config.weight_multiplier
        * strategy_intent.risk_position
        / strategy_intent.expected_vo
    )

    for i, symbol in enumerate(tradable):
        intent_log["assets"][symbol]["model"]["risk_position"] = float(
            strategy_intent.risk_position[i]
        )
        intent_log["assets"][symbol]["model"]["target_weight"] = float(weights[i])


def run_live(
    prices,
    mu,
    vo,
    cor,
    positions,  # Current quantities held (Step 2 input)
    ltps,  # Latest Tradable Prices
    intent_log,  # The dictionary to be populated
    config,
    latest_view,  # DataFrame with mid prices and timestamps
    logger,
    intent_logger,
):
    # --- 1. CORE ALPHA LOGIC ---
    t = prices.index[-1]
    mask = ~prices.loc[t].isna().values

    strategy_intent = compute_strategy(
        mu=mu[-1], vo=vo[-1], cor_matrix=cor.loc[t].values, mask=mask, config=config
    )

    # --- 2. LOG BASE METRICS (Step 1) ---
    log_strategy_intent(intent_log, prices, strategy_intent, config)

    # --- 3. HANDLE UNIVERSE DRIFT (Holdings outside current tradable set) ---
    # Convert alpha array to dict for easier merging
    tradable_symbols = prices.columns[mask]
    conversion_factor = config.weight_multiplier / config.position_multiplier

    target_weights = {
        symbol: (pos * conversion_factor)
        for symbol, pos in zip(tradable_symbols, strategy_intent.target_position)
    }

    target_zeroes = {
        coin: 0 for coin in set(positions.keys()) - set(target_weights.keys())
    }
    intent_log["universe"]["holdings_outside_universe"] = list(target_zeroes.keys())

    # Initialize log structure for legacy/dropped assets
    for symbol in target_zeroes.keys():
        if symbol not in intent_log["assets"]:
            intent_log["assets"][symbol] = init_asset()
        intent_log["assets"][symbol]["market"]["mark_price"] = float(
            latest_view.loc[symbol, "mid"]
        )
        intent_log["assets"][symbol]["market"]["data_timestamp"] = latest_view.loc[
            symbol, "downloaded_at"
        ]

    # Merge weights: tradable alpha + assets we need to exit
    all_target_weights = {**target_weights, **target_zeroes}

    # --- 4. CONVERT WEIGHTS TO TARGET QUANTITIES ---
    account_val = intent_log["portfolio"]["equity_used_for_sizing"]

    for symbol, weight in all_target_weights.items():
        # Update current state in log
        current_qty = positions.get(symbol, 0)
        intent_log["assets"][symbol]["current"]["qty"] = float(current_qty)

        # Calculate Target Qty
        target_val = weight * account_val
        target_qty = target_val / ltps[symbol]
        intent_log["assets"][symbol]["target"]["qty"] = float(target_qty)

    # --- 5. EXECUTION LOGIC (Step 2 & 3) ---
    # This calls your existing function to handle small trade logic and rounding
    target_qtys_dict = {
        s: intent_log["assets"][s]["target"]["qty"] for s in all_target_weights.keys()
    }

    order_intentions = get_order_intention(
        target_qtys=target_qtys_dict, positions_input=positions, logger=logger
    )

    # --- 6. FINAL LOGGING & DISPATCH ---
    for coin, order in order_intentions.items():
        intent_log["assets"][coin]["order_intent"].update(
            {
                "coin": order["coin"],
                "side": order["side"],
                "delta": order["delta"],
                "target": order["target"],
                "current": order["current"],
            }
        )

    intent_logger.log(intent_log)
    return order_intentions


# %%
def get_order_intention(
    target_qtys: dict, logger: object, positions_input: dict = None
) -> dict:
    order_intentions = {}

    for coin, target_qty in target_qtys.items():
        if target_qty is None or np.isnan(target_qty):
            logger.warning("%s skipped: target_qty is None/NaN", coin)
            continue

        current_qty = positions_input.get(coin, 0) if positions_input is not None else 0
        delta = target_qty - current_qty

        # Prepare the order intention output for this coin
        order_intentions[coin] = {
            "coin": coin,
            "side": "BUY" if delta > 0 else "SELL",  # Side determined by delta
            "target": target_qty,
            "current": current_qty,
            "delta": delta,  # Only calculate delta, no rounding or checks
        }

    return order_intentions


# %%
def get_execution_plan(
    order_intentions: dict,
    ltps: dict,
    sz_decimals: dict,
    logger: object,
    positions: dict = None,
    slippage_bps: int = 1,  # 1 basis point = 0.01%
) -> list:

    exchange_orders = []
    MAX_PRECISION = 6

    for coin, intention in order_intentions.items():
        target_qty = intention["target"]
        current_qty = positions.get(coin, 0) if positions is not None else intention["current"]
        delta = target_qty - current_qty
        side = "BUY" if delta > 0 else "SELL"

        ltp = ltps.get(coin, 0)
        if ltp == 0:
            logger.error(f"Missing LTP data for {coin}")
            continue

        # 1. Dollar Value Logic ($10 Min)
        dollar_target = abs(target_qty) * ltp
        dollar_delta = abs(delta) * ltp

        if dollar_target < 10:
            if current_qty != 0:  # Force close position
                target_qty = 0
                delta = -current_qty
                dollar_delta = abs(delta) * ltp
            else:
                continue

        if round(dollar_delta, 2) < 10 and target_qty != 0:
            continue

        # 2. Price with Slippage Logic
        slippage_factor = (
            (1 + (slippage_bps / 10000))
            if side.upper() == "BUY"
            else (1 - (slippage_bps / 10000))
        )
        adj_px = ltp * slippage_factor

        sz_dec = sz_decimals.get(coin, 0)
        clean_sz = abs(round(delta, sz_dec))

        if clean_sz == 0:
            continue

        # Rounding to HL significant figure standards
        precision = max(0, MAX_PRECISION - sz_dec)
        clean_px = float(f"{adj_px:.5g}")
        clean_px = round(clean_px, precision)

        # 3. Construct Order object
        exchange_orders.append(
            {
                "coin": coin,
                "is_buy": side.upper() == "BUY",
                "sz": clean_sz,
                "limit_px": clean_px,
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False,
            }
        )

    return exchange_orders


def generate_readable_summary(orders, ltps):
    result = pd.DataFrame(orders).set_index("coin")[["is_buy", "sz", "limit_px"]]
    buy_sell_map = {True: "BUY", False: "SELL"}
    result["dir"] = [buy_sell_map[dir_bool] for dir_bool in result["is_buy"]]
    result["ltp"] = [ltps[position] for position in result.index]
    result["dv"] = result["sz"].multiply(result["limit_px"])
    return result[["dir", "dv", "limit_px", "ltp", "sz"]].sort_values(
        by="dv", ascending=False
    )
# %%
if __name__ == "__main__":
    main()
