"""Order intention and execution plan logic."""

import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd


class OrderStatus(Enum):
    """Canonical order outcome vocabulary shared across placement, fills and cancels.

    The placement classifier (``classify_order_responses``) only emits the subset a
    Hyperliquid ``bulk_orders`` response can actually return — FILLED, OPEN, the
    error sub-types, REJECTED and UNKNOWN. PARTIAL and CANCELLED are reserved for
    the fills-reconciliation and cancel-response parsers respectively.
    """

    FILLED = "FILLED"
    OPEN = "OPEN"                          # resting on the book
    PARTIAL = "PARTIAL"                    # reserved: from fills reconciliation
    CANCELLED = "CANCELLED"                # reserved: from bulk_cancel parser
    REJECTED = "REJECTED"                  # error with no recognised signature
    TRADING_HALTED = "TRADING_HALTED"
    INSUFFICIENT_MARGIN = "INSUFFICIENT_MARGIN"
    MIN_NOTIONAL = "MIN_NOTIONAL"
    UNKNOWN = "UNKNOWN"                    # unparseable status shape


# Error-string signatures, matched as case-insensitive substrings because HL appends
# an ``asset=N`` suffix (e.g. "Insufficient margin to place order. asset=2"). Order
# matters only if signatures overlap — these don't.
_ERROR_SIGNATURES: Tuple[Tuple[str, OrderStatus], ...] = (
    ("halt", OrderStatus.TRADING_HALTED),
    ("insufficient margin", OrderStatus.INSUFFICIENT_MARGIN),
    ("minimum value", OrderStatus.MIN_NOTIONAL),
)


def classify_status(status: object) -> Tuple[OrderStatus, Optional[str]]:
    """Classify a single Hyperliquid ``bulk_orders`` status object.

    Returns ``(OrderStatus, error_message_or_None)``. A status is one of
    ``{"filled": {...}}``, ``{"resting": {...}}`` or ``{"error": "..."}``; anything
    else is treated as UNKNOWN rather than silently assumed good.
    """
    if not isinstance(status, dict):
        return OrderStatus.UNKNOWN, str(status)
    if "filled" in status:
        return OrderStatus.FILLED, None
    if "resting" in status:
        return OrderStatus.OPEN, None
    if "error" in status:
        msg = str(status["error"])
        low = msg.lower()
        for needle, mapped in _ERROR_SIGNATURES:
            if needle in low:
                return mapped, msg
        return OrderStatus.REJECTED, msg
    return OrderStatus.UNKNOWN, json.dumps(status)


@dataclass
class ClassifiedOrder:
    """A submitted order paired with its classified exchange response."""

    order: dict             # the submitted order dict (coin, is_buy, sz, limit_px, ...)
    raw_status: object      # the raw HL status object for this order
    status: OrderStatus
    error: Optional[str]

    @property
    def coin(self) -> str:
        return self.order.get("coin")

    @property
    def accepted(self) -> bool:
        """Accepted = reached the book (resting) or filled. UNKNOWN is never accepted."""
        return self.status in (OrderStatus.FILLED, OrderStatus.OPEN)


@dataclass
class ExecutionResult:
    """Classified outcome of one ``bulk_orders`` submission."""

    classified: List[ClassifiedOrder]
    count_mismatch: bool = False  # True if statuses count != orders count

    @property
    def accepted(self) -> List[ClassifiedOrder]:
        return [c for c in self.classified if c.accepted]

    @property
    def rejected(self) -> List[ClassifiedOrder]:
        return [c for c in self.classified if not c.accepted]

    @property
    def halted(self) -> bool:
        return any(c.status is OrderStatus.TRADING_HALTED for c in self.classified)


def classify_order_responses(orders: list, statuses: list) -> ExecutionResult:
    """Pair each submitted order with its HL status and classify the outcome.

    HL returns one status per order in index order. If the counts disagree (a
    structural surprise the old code silently mis-indexed), classify the overlap
    and flag ``count_mismatch`` so the caller can treat it conservatively.
    """
    classified = []
    for order, status in zip(orders, statuses):
        st, err = classify_status(status)
        classified.append(ClassifiedOrder(order=order, raw_status=status, status=st, error=err))
    return ExecutionResult(classified=classified, count_mismatch=len(orders) != len(statuses))


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
        })

    return exchange_orders


def generate_readable_summary(orders, ltps):
    result = pd.DataFrame(orders).set_index("coin")[["is_buy", "sz", "limit_px"]]
    result["dir"] = result["is_buy"].map({True: "BUY", False: "SELL"})
    result["ltp"] = [ltps[pos] for pos in result.index]
    result["dv"] = result["sz"].multiply(result["limit_px"])
    return result[["dir", "dv", "limit_px", "ltp", "sz"]].sort_values(by="dv", ascending=False)
