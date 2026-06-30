# src/loggers/order_logger.py

import json
import time
from pathlib import Path
from typing import Dict, Any


class OrderLogger:
    """
    Append-only logger for order submission events.
    One row = one submitted order (not a fill).
    """

    def __init__(self, log_path: str):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_order_submission(
        self,
        *,
        run_id: str,
        exchange: str,
        account: str,
        symbol: str,
        side: str,
        order_type: str,
        price: float,
        qty: float,
        response: Dict[str, Any],
        target_qty: float = None,
        current_qty: float = None,
        delta: float = None,
    ) -> None:
        """
        Logs an order submission based on the raw exchange response.

        The trading loop cancels-and-replaces every cycle, so a single daily
        intent produces many submission rows. Logging the intended
        target/current/delta context per submission (alongside the rounded `qty`
        actually sent) lets downstream analysis separate rounding/threshold
        effects from fills without trusting summed order quantities. The
        `accepted`/`error` flags make rejected attempts (e.g. sub-$10 orders)
        filterable instead of surfacing only as a null order_id.
        """

        submit_ts_ms = int(time.time() * 1000)

        # --- Extract status info safely ---
        statuses = (
            response
            .get("response", {})
            .get("data", {})
            .get("statuses", [])
        )

        exchange_status = "unknown"
        order_id = None
        error = None
        filled_sz = None
        avg_fill_px = None

        if statuses:
            status_dict = statuses[0]

            # Safely unpack first key-value pair
            exchange_status, status_data = next(iter(status_dict.items()))

            if isinstance(status_data, dict):
                order_id = status_data.get("oid")
                # "filled" statuses carry immediate fill detail
                if status_data.get("totalSz") is not None:
                    filled_sz = float(status_data["totalSz"])
                if status_data.get("avgPx") is not None:
                    avg_fill_px = float(status_data["avgPx"])
            elif exchange_status == "error":
                error = status_data  # the error message string

        accepted = exchange_status in ("resting", "filled")

        record = {
            "event_type": "order_submitted",
            "run_id": run_id,

            "exchange": exchange,
            "account": account,

            "symbol": symbol,
            "side": side,

            "order_id": order_id,

            "order_type": order_type,
            "price": price,
            "qty": qty,                 # rounded size actually submitted

            # intended trade context (pre-rounding) for attribution
            "target_qty": target_qty,
            "current_qty": current_qty,
            "delta": delta,

            "submit_timestamp_ms": submit_ts_ms,

            "exchange_status": exchange_status,
            "accepted": accepted,
            "error": error,
            "filled_sz": filled_sz,
            "avg_fill_px": avg_fill_px,
            "raw_statuses": statuses,
        }

        with self.log_path.open("a") as f:
            f.write(json.dumps(record) + "\n")
