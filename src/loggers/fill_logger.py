# src/loggers/fill_logger.py

import json
from pathlib import Path
from typing import Dict, Any, Iterable
import datetime as dt

class FillLogger:
    """
    Append-only logger for fills.
    One row = one fill event from the exchange.
    """

    def __init__(self, log_path: str):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log_fills(
        self,
        *,
        run_id: str,
        exchange: str,
        account: str,
        fills: Iterable[Dict[str, Any]],
        source: str = "polling",
    ) -> None:
        """
        Log a batch of fills returned from the exchange.
        """

        with self.log_path.open("a") as f:
            for fill in fills:
                record = self._parse_fill(
                    run_id=run_id,
                    exchange=exchange,
                    account=account,
                    fill=fill,
                    source=source,
                )
                f.write(json.dumps(record) + "\n")

    from typing import Iterable, Dict, Any, List

    def parse_fills(
        self,
        *,
        run_id: str,
        exchange: str,
        account: str,
        fills: Iterable[Dict[str, Any]],
        source: str = "polling",
    ) -> List[Dict[str, Any]]:
        """
        Transform raw API fills into schema records.
        """
        records: List[Dict[str, Any]] = []

        for fill in fills:
            record = self._parse_fill(
                run_id=run_id,
                exchange=exchange,
                account=account,
                fill=fill,
                source=source,
            )
            records.append(record)

        return records

    @staticmethod
    def _parse_fill(
        *,
        run_id: str,
        exchange: str,
        account: str,
        fill: Dict[str, Any],
        source: str,
    ) -> Dict[str, Any]:
        """
        Convert raw Hyperliquid fill into canonical fill log schema.
        """

        raw_qty = float(fill["sz"])
        side_code = fill.get("side")

        if side_code == "B":
            signed_qty = raw_qty
            side_str = "buy"
        elif side_code == "A":
            signed_qty = -raw_qty
            side_str = "sell"
        else:
            signed_qty = raw_qty
            side_str = "buy"
        if fill["dir"] == "Open Short":
            assert signed_qty < 0
        if fill["dir"] == "Close Short":
            assert signed_qty > 0

        price = float(fill["px"])
        notional = abs(signed_qty) * price


        record = {
            "event_type": "fill",
            "run_id": run_id,

            "exchange": exchange,
            "account": account,

            "coin": fill["coin"],
            "side": side_str,

            # --- IDs ---
            "fill_id": fill["tid"],          # primary deduplication key
            "order_id": fill["oid"],

            # --- Time ---
            "fill_timestamp_ms": int(fill["time"]),

            # --- Economics ---
            "price": price,
            "qty": signed_qty,          # ← THIS MUST BE SIGNED
            "notional": notional,

            "fee": float(fill["fee"]),
            "fee_currency": fill.get("feeToken", "UNKNOWN"),

            # --- Execution metadata ---
            "liquidity": "taker" if fill.get("crossed") else "maker",
            "direction": fill.get("dir"),
            "closed_pnl": float(fill.get("closedPnl", 0.0)),

            # --- Provenance ---
            "source": source,

            # --- Raw exchange data (always keep) ---
            "raw_fill": fill,
        }

        return record
