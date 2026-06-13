"""Order intention and execution plan logic."""

import numpy as np
import pandas as pd


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
        order_intentions[coin] = {
            "coin": coin,
            "side": "BUY" if delta > 0 else "SELL",
            "target": target_qty,
            "current": current_qty,
            "delta": delta,
        }
    return order_intentions


def get_execution_plan(
    order_intentions: dict,
    ltps: dict,
    sz_decimals: dict,
    logger: object,
    positions: dict = None,
    slippage_bps: int = 1,
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

        dollar_target = abs(target_qty) * ltp
        dollar_delta = abs(delta) * ltp

        if dollar_target < 10:
            if current_qty != 0:
                target_qty = 0
                delta = -current_qty
                dollar_delta = abs(delta) * ltp
            else:
                continue

        if round(dollar_delta, 2) < 10 and target_qty != 0:
            continue

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

        precision = max(0, MAX_PRECISION - sz_dec)
        clean_px = round(float(f"{adj_px:.5g}"), precision)

        exchange_orders.append({
            "coin": coin,
            "is_buy": side.upper() == "BUY",
            "sz": clean_sz,
            "limit_px": clean_px,
            "order_type": {"limit": {"tif": "Gtc"}},
            "reduce_only": False,
            # intended-trade context (post dust-adjust, pre-rounding) for logging/attribution
            "target_qty": target_qty,
            "current_qty": current_qty,
            "delta": delta,
        })

    return exchange_orders


def generate_readable_summary(orders, ltps):
    result = pd.DataFrame(orders).set_index("coin")[["is_buy", "sz", "limit_px"]]
    result["dir"] = result["is_buy"].map({True: "BUY", False: "SELL"})
    result["ltp"] = [ltps[pos] for pos in result.index]
    result["dv"] = result["sz"].multiply(result["limit_px"])
    return result[["dir", "dv", "limit_px", "ltp", "sz"]].sort_values(by="dv", ascending=False)
