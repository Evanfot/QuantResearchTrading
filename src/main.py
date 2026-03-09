# %% %%
import os
import json
import logging
import pandas as pd
from pathlib import Path
import importlib
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
import warnings
import datetime as dt
import numpy as np
from cvx.simulator import Builder
from cvx.simulator import interpolate
from tinycta.linalg import solve, inv_a_norm
from tinycta.signal import returns_adjust, osc, shrink2id
import requests
import duckdb
from dotenv import load_dotenv
from decimal import Decimal, getcontext
from typing import List, Dict
import json
from pathlib import Path
from src.state.strategy_state import load_state, save_state, get_state_positions
from src.loggers.intent_logger import (
    init_intent,
    init_asset,  
    IntentLogger,
    generate_run_id
)
from src.loggers.order_logger import OrderLogger
from src.helpers.dict_diff import dict_diff
from scripts.exchange_state import run_exchange_state, read_latest_exchange_state
from scripts.meta_data import read_latest_meta
# TODO: set meta_data, latest_market_caps to run every day
from scripts.meta_data import get_hl_coins 
from scripts.mkt_cap_data import get_latest_market_cap
from src.ingestion.update_mids import run_update_mids
# LOGGING CONFIG
# --------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

run_id = generate_run_id()
intent = init_intent(
    mode="live",
    strategy_name="trend_v1",
    run_id=run_id
)
intent_logger = IntentLogger("logs/intent.jsonl")
order_logger = OrderLogger("logs/orders.jsonl")
# --------------------------------------------
# SETTINGS
# --------------------------------------------
load_dotenv()
PRIVATE_KEY = os.getenv("HYPERLIQUID_PRIVATE_KEY")
WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")
API_ADDRESS = os.getenv("HYPERLIQUID_API_WALLET_ADDRESS")
db_path = 'data/pricing/ohlcv_data.duckdb'
DRY_RUN = False  # %% set to False to enable live trading

STATE_PATH = Path(f"state/hyperliquid_{WALLET_ADDRESS}_state.json")

# %%
# TODO: Implement block below
# def is_data_due(now, state):
#     if state.last_data_run is None:
#         return True
#     return now - state.last_data_run >= DATA_INTERVAL

# def is_trading_due(now, state):
#     if state.last_trading_run is None:
#         return True

#     last = state.last_trading_run.date()
#     today = now.date()

#     return today > last and now.hour >= TRADING_HOUR_UTC

# def sleep_until_next_tick(state):
#     sleep(1)

# from src.ingestion.hyperliquid_docker import run_ohlcv_dl, update_daily, update_latest_view
# def main():
#     state = load_state()

#     while True:
#         now = datetime.now(timezone.utc)

#         # ---- DATA TASK (every minute) ----
#         if is_data_due(now, state):
#             # run_data_ingestion(state)
#             run_ohlcv_dl()
#             update_daily()
#             update_latest_view()
#             # TODO: Implement below
#             # state.last_data_run = now
#             # save_state(state)

#         # ---- TRADING TASK (once per day) ----
#         if is_trading_due(now, state):
#             #TODO: implement
#             pass
#             # run_trading_logic(state)
#             # state.last_trading_run = now
#             # save_state(state)

#         sleep_until_next_tick(state)
# %% Universe

# Known stablecoin symbols
stable = {"USDT", "USDC", "DAI", "USDD", "FDUSD", "TUSD", "DEI", "USDP", "GUSD","USDE"}

def get_hyperliquid_trading_universe(top_market_cap, hl_universe):

    filtered = []
    for index,coin in top_market_cap.iterrows():
        symbol = coin["symbol"].upper()
        if symbol == 'PAXG':
            continue
        if symbol in stable:
            continue
        if symbol in [k['symbol'] for k in filtered]:
            continue
        if symbol in hl_universe:
            filtered.append({
                "name": coin["name"],
                "symbol": symbol,
                "market_cap": coin["market_cap"]
            })

    # Top 50 Hyperliquid-listed coins by market cap
    result = filtered[:50]

    universe = list(set([k['symbol'] for k in result]))
    return universe


def initialise_asset_intent(intent, universe):
    for symbol in universe:
        intent["assets"][symbol] = init_asset()
    return intent


# %% PRICING:
def get_ohlcv(conn):
    df = conn.execute("""
        SELECT datetime, symbol, close, volume
        FROM hyperliquid_1d
    """).df()
    conn.close()
    hyperliquid_prices = df.pivot(index="datetime", columns="symbol", values="close")
    return hyperliquid_prices
    

def update_ltps():
    run_update_mids()
    latest_view = pd.read_csv('data/snapshots/mids.csv')
    # latest_view = run_update_mids()
    latest_view.set_index('symbol',inplace=True)
    ltps = latest_view.to_dict()['mid']
    ltps = {key:float(val) for key,val in ltps.items()}
    return ltps

def add_ltp_to_intent(intent, latest_view):
    for symbol in intent['assets'].keys():
        intent["assets"][symbol]['market']['mark_price'] = float(latest_view.at[f"{symbol}",'mid'])
        intent["assets"][symbol]['market']['data_timestamp'] = latest_view.loc[f"{symbol}",'downloaded_at']
    return intent


#TODO: Come up with an overwrite for coins which don't have data history
# %%
vola_value = 36
winsor_value = 4.2
def get_final_pricing(hyperliquid_prices, universe, latest_view):
    hype_universe = [k+"/USDC:USDC" for k in universe]
    prices = hyperliquid_prices[hype_universe]
    prices.columns = prices.columns.str.replace('/USDC:USDC','')
    prices = prices.copy(deep=True)
    prices.loc[dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)] = latest_view.loc[prices.columns.values, ('mid')].astype('float')
    returns = np.log(prices).diff()
    returns_adj = (returns / returns.ewm(com=vola_value, min_periods=120).std()).clip(-winsor_value, +winsor_value)
    return prices, returns_adj

# %%
def ewmac(returns_adj, param, vola_span=20):
    fast = param
    slow = fast * 4
    return np.tanh(
        returns_adj.cumsum().apply(osc, args=(fast, slow, True))
    ).values

def breakout(prices, param):
    roll_min = prices.rolling(param).min()
    roll_max = prices.rolling(param).max()
    roll_mean = (roll_min + roll_max) / 2
    return np.tanh((prices - roll_mean) / (roll_max - roll_min)).values

def scaled_bollinger(price, param=20, scalar=2, forecast_cap=10):
    ma = price.rolling(param).mean()
    std = price.rolling(param).std()
    z = (price - ma) / std
    forecast = np.tanh(z / scalar)
    return forecast.values

def carry_signal( funding_daily, returns, funding_span=8, vol_span=32, norm_span=60, eps=1e-8):
    """
    funding_daily: pd.Series (daily funding rate, summed)
    returns: pd.Series (daily log returns)
    """
    # 1) Smooth funding
    smoothed_funding = funding_daily.ewm( span=funding_span, adjust=False).mean()
    # 1.2) invert for carry signal
    carry_economic = -1*smoothed_funding
    # 2) Vol estimate
    vol = returns.ewm(span=vol_span, adjust=False).std()
    raw_carry = carry_economic / (vol + eps)
    # 3) Normalize which means will take more exposure when carry is strong less when weak/noisy
    scale = raw_carry.abs().ewm( span=norm_span, adjust=False).mean()
    # carry = np.tanh(raw_carry / (scale + eps)).values
    carry = (raw_carry / (scale + eps)).clip(-5,5).values
    return carry

# %%
ewmac_fast = 4
breakout_window = 14
bollinger_window = 14
vo_window = 20
correlation = 64
shrinkage_value = 0.5
threshold_trade = False
ignore_small = False
add_commision = False

def run_trading_logic(state):
    builder = Builder(prices=prices, initial_aum=1e3)
    for n, (t, state) in enumerate(builder):
        mask = state.mask
        matrix = shrink2id(cor.loc[t[-1]].values, lamb=shrinkage_value)[mask, :][
            :, mask
        ]
        expected_mu = np.nan_to_num(mu[n][mask])
        expected_vo = np.nan_to_num(vo[n][mask])
        risk_position = solve(matrix, expected_mu) / inv_a_norm(expected_mu, matrix)
        target_pos = 1e1 * risk_position / expected_vo
        if ignore_small:
            target_pos[np.abs(target_pos)<10] = 0
        if threshold_trade and n > 0:
            current_pos = builder.position* builder.current_prices
            yesterday_pos = positions[n-1][mask]
            below_threshold = np.abs(target_pos - yesterday_pos) < 10
            target_pos[below_threshold] = yesterday_pos[below_threshold]
        builder.cashposition = target_pos
        if add_commision:
            # DO commission calcs here
            commission = 0
        else:
            commission = 0
        builder.aum = state.aum - commission
    intent['universe']['tradable'] = list(prices.columns.values[mask])
    if mask.sum() < prices.shape[1]:
        non_tradable = prices.columns.values[~mask]
    else:
        non_tradable = []
    intent['universe']['non_tradable'] = non_tradable
    portfolio = builder.build()

    # strat_rets = portfolio.aum.diff()/1000
    # # Following line is based on inspection
    # strat_rets = strat_rets[strat_rets.index > dt.datetime(2024,8,30)]
    # strat_sharpe = strat_rets.mean()/strat_rets.std()*np.sqrt(365)
    # Save weights DataFrame
    # TODO: get weights_df from portfolio.units
    
    # TODO: check whether this works if we don't have builder.build, i.e. return builder. Then backtest runs .build and live trading extracts stuff from state
    weights_df = pd.DataFrame(state._prices)
    weights_df['weight'] = 0.02*risk_position/expected_vo
    logger_weight = weights_df['weight'].reindex(state._prices.index).values
    for symbol in state._prices.index:
        intent['assets'][symbol]['model']['risk_position'] = float(risk_position[symbol_index[symbol]])
        intent['assets'][symbol]['model']['target_weight'] = float(logger_weight[symbol_index[symbol]])

    weights_df['weight'] = 0.02*risk_position/expected_vo
    target_weights  = weights_df['weight'].to_dict()
    return target_weights, portfolio


# %%
def get_order_intention(target_qtys: dict, sz_decimals: dict, logger: object, positions_input: dict = None) -> dict:
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
    open_orders: list, 
    ltps: dict, 
    sz_decimals: dict, 
    logger: object,
    slippage_bps: int = 1  # 1 basis point = 0.01%
) -> tuple[list, list]:
    
    exchange_orders = []
    cancel_requests = []
    MAX_PRECISION = 6 

    for coin, intention in order_intentions.items():
        delta = intention["delta"]
        target_qty = intention["target"]
        current_qty = intention["current"]
        side = intention["side"]
        
        ltp = ltps.get(coin, 0)
        if ltp == 0:
            logger.error(f"Missing LTP data for {coin}")
            continue

        # 1. Dollar Value Logic ($10 Min)
        dollar_target = abs(target_qty) * ltp
        dollar_delta = abs(delta) * ltp

        if dollar_target < 10:
            if current_qty != 0: # Force close position
                target_qty = 0
                delta = -current_qty
                dollar_delta = abs(delta) * ltp
            else:
                continue

        if round(dollar_delta, 2) < 10 and target_qty != 0:
            continue

        # 2. Price with Slippage Logic
        slippage_factor = (1 + (slippage_bps / 10000)) if side.upper() == "BUY" else (1 - (slippage_bps / 10000))
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
        exchange_orders.append({
            "coin": coin,
            "is_buy": side.upper() == "BUY",
            "sz": clean_sz,
            "limit_px": clean_px,
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False
        })

        # 4. Identify Stale Orders
        open_order = next((o for o in open_orders if o['coin'] == coin), None)
        if open_order:
            cancel_requests.append({"coin": coin, "oid": open_order["oid"]})
    return exchange_orders, cancel_requests

def generate_readable_summary(orders, ltps):
    result = pd.DataFrame(orders).set_index('coin')[['is_buy','sz','limit_px']]
    buy_sell_map = {True: 'BUY', False: 'SELL'}
    result['dir'] = [buy_sell_map[dir_bool] for dir_bool in result['is_buy']]
    result['ltp'] = [ltps[position] for position in result.index]
    result['dv'] = result['sz'].multiply(result['limit_px'])
    return result[['dir','dv','limit_px','ltp','sz']].sort_values(by='dv',ascending=False)

# %%
if __name__ == '__main__':
    state = load_state(STATE_PATH)
    state['last_trading_run_id'] = run_id
    positions = get_state_positions(state)
    try:
        exchange_state = run_exchange_state()
        exchange_state_positions = {row['position']['coin']: float(row['position']['szi']) for row in exchange_state['assetPositions']}
        diff = dict_diff(exchange_state_positions, positions)
        if len(diff['changed'])>0:
            logger.warning(f"local_state positions don't match exchange positions")
        positions = exchange_state_positions
    except:
        logger.warning(f"can't dl positions from exchange, local_state positions used")
        exchange_state = read_latest_exchange_state()
    
    # Get asset metadata
# TODO: Add try except to download latest meta and read from file in case it doesn't work
    meta = read_latest_meta()
    sz_decimals = {coin['name']: coin['szDecimals'] for coin in meta['universe']}
    logger.info("Loaded size decimals for %d coins", len(sz_decimals))
    #  ─── INIT ────────────────────────────────────────────────────────────────────
    info = Info(MAINNET_API_URL, skip_ws=True)
    wallet = Account.from_key(PRIVATE_KEY)
    ex = Exchange(
        wallet = wallet,
        base_url=MAINNET_API_URL,
        account_address=API_ADDRESS)

    # Get universe and initialise relevant rows i nintent
    top = get_latest_market_cap()
    hl = get_hl_coins()
    universe = get_hyperliquid_trading_universe(top, hl)
    # Get pricing and add to intent
    conn = duckdb.connect(db_path)
    hyperliquid_prices = get_ohlcv(conn)
    ltps = update_ltps()
    #TODO: Change below to use ltps instead of latest_view
    latest_view = pd.read_csv('data/snapshots/mids.csv')
    latest_view.set_index('symbol',inplace=True)
    prices, returns_adj = get_final_pricing(hyperliquid_prices,universe,latest_view)
    if 1:
        intent["universe"]["tradable"] =universe
        symbol_index = {universe[k]:k for k in range(len(universe))}
        intent = initialise_asset_intent(intent, universe)
        intent['portfolio']['equity_usd'] = float(exchange_state['marginSummary']['accountValue'])
        #TODO: If exchange_state isn't latest, apply haircut?
        # Add exchange state to intent
        #TODO: add function to log all things from exchange_state
        intent['portfolio']['equity_used_for_sizing'] = float(exchange_state['marginSummary']['accountValue'])
        intent['portfolio']['maintenance_margin']=exchange_state['crossMaintenanceMarginUsed']
        intent['portfolio']['gross_exposure_pre_rebal']=exchange_state['marginSummary']['totalNtlPos']

        intent = add_ltp_to_intent(intent, latest_view)
        # Calc signals, vols, corr and add to intent
        ewmac_forecast = ewmac(returns_adj, ewmac_fast)
        breakout_forecast = breakout(prices, breakout_window)
        bollinger_forecast = scaled_bollinger(prices, param=bollinger_window, scalar=1)
        mu = np.mean([bollinger_forecast, ewmac_forecast, breakout_forecast], axis=0)
        vo = prices.pct_change().ewm(com=vo_window, min_periods=20).std().values
        cor = returns_adj.ewm(com=correlation, min_periods=correlation).corr()
        #TODO: change intent update to a separate function
        for symbol in universe:
            intent['assets'][symbol]['model']['vol_1d'] = float(vo[-1,symbol_index[symbol]])
            intent['assets'][symbol]['model']['signal'] = {'mu':float(mu[-1,symbol_index[symbol]]),
            'sub_signals':{'ewmac':float(ewmac_forecast[-1,symbol_index[symbol]]),
                        'breakout':float(breakout_forecast[-1,symbol_index[symbol]]),
                        'bollinger':float(bollinger_forecast[-1,symbol_index[symbol]])}
        }
        intent['risk_inputs']['correlation_matrix'] = cor.loc[prices.index[-1]].to_dict()
        # Get target weights
        target_weights, portfolio = run_trading_logic(state)
        # If coin in positions not in target weights, set target weight to zero
        target_zeroes = {coin: 0 for coin in set(positions.keys())-set(target_weights.keys())}
        intent['universe']['holdings_outside_universe'] = list(target_zeroes.keys())
        intent = initialise_asset_intent(intent, list(target_zeroes.keys()))
        for key,value in positions.items():
            intent['assets'][key]['current']['qty'] = value

        for symbol in target_zeroes.keys():
            intent["assets"][symbol] = init_asset()
            intent["assets"][symbol]['market']['mark_price'] = float(latest_view.loc[f"{symbol}",'mid'])
            intent["assets"][symbol]['market']['data_timestamp'] = latest_view.loc[f"{symbol}",'downloaded_at']

        target_weights = target_weights|target_zeroes
        #  ─── DETERMINE TARGET POSITIONS ──────────────────────────────────────
        account_val = intent['portfolio']['equity_used_for_sizing']
        target_values = {coin: weight*account_val for coin, weight in target_weights.items()}
        target_qtys = {coin: target_values[coin] / ltps[coin] for coin in target_weights.keys()}

        for key,value in target_qtys.items():
            intent['assets'][key]['target']['qty'] = float(value)
        order_intentions = get_order_intention(
            target_qtys=target_qtys, 
            positions_input=positions, 
            sz_decimals=sz_decimals, 
            logger=logger
        )
        for coin in order_intentions.keys():
            order = order_intentions[coin]
            intent['assets'][coin]['order_intent']['coin'] = order['coin']
            intent['assets'][coin]['order_intent']['side'] = order['side']
            intent['assets'][coin]['order_intent']['delta'] = order['delta']
            intent['assets'][coin]['order_intent']['target'] = order['target']
            intent['assets'][coin]['order_intent']['current'] = order['current']

        intent_logger.log(intent)

    intent_from_file = intent_logger.read_latest()
    asset_list = [asset for asset in intent_from_file['assets']]
    order_intentions = {}
    for asset in asset_list:
        order_intentions[asset] = intent_from_file['assets'][asset]['order_intent']
    # --- EXECUTION BLOCK ---
    open_orders = info.open_orders(WALLET_ADDRESS)
    ltps = update_ltps()
    # 1. Generate the plan, trade towards order intentions as long as target > $10, order size > $10 etc.
    orders, cancels = get_execution_plan(order_intentions, open_orders, ltps, sz_decimals, logger)
    trading_summary = generate_readable_summary(orders, ltps)
    print(trading_summary)
    # 
    # 2. Cancel stale
    if not DRY_RUN:
        if cancels:
            logger.info(f"Cancel Requests: {cancels}")
            resp = ex.bulk_cancel(cancels)
            logger.info(f"Cancel Response: {resp}")

        # 3. Place New Orders
        exchange = 'hyperliquid'
        if orders:
            response = ex.bulk_orders(orders)
            
            if response.get("status") == "ok":
                all_statuses = response["response"]["data"]["statuses"]
                for i, status in enumerate(all_statuses):
                    mock_res = {"response": {"data": {"statuses": [status]}}}
                    order_info = orders[i]
                    
                    order_logger.log_order_submission(
                        run_id=run_id, exchange=exchange, account=WALLET_ADDRESS,
                        symbol=order_info["coin"], side="buy" if order_info["is_buy"] else "sell",
                        order_type="LIMIT", price=order_info["limit_px"], 
                        qty=order_info["sz"], response=mock_res
                    )
            else:
                print(f"Bulk submission failed: {response}")
        logging.info("Rebalancing complete.")
