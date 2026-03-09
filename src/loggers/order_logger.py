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
    ) -> None:
        """
        Logs an order submission based on the raw exchange response.
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

        if statuses:
            status_dict = statuses[0]

            # Safely unpack first key-value pair
            exchange_status, status_data = next(iter(status_dict.items()))

            # Only access .get if status_data is a dict
            if isinstance(status_data, dict):
                order_id = status_data.get("oid")
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
            "qty": qty,

            "submit_timestamp_ms": submit_ts_ms,

            "exchange_status": exchange_status,
            "raw_statuses": statuses,
        }

        with self.log_path.open("a") as f:
            f.write(json.dumps(record) + "\n")
