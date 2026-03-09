# src/positions/position_rebuilder.py

from collections import defaultdict
from typing import Dict, Any, Iterable


from scripts.meta_data import read_latest_meta
meta = read_latest_meta()
sz_decimals = {coin['name']: coin['szDecimals'] for coin in meta['universe']}
max_decimals = 6

class PositionRebuilder:
    """
    Rebuild positions from signed fills.
    qty > 0 = buy
    qty < 0 = sell
    """

    def __init__(self, open_positions=None):
        if open_positions is None:
            self.positions = defaultdict(self._empty_position)
        else:
            self.positions = open_positions

    @staticmethod
    def _empty_position():
        return {
            "qty": 0.0,
            "avg_price": 0.0,
            "realized_pnl": 0.0,
        }
    def round_price(self, avg_price,sz_decimals_coin):
        if avg_price > 100_000:
                avg_price = round(avg_price)
        else:
            avg_price = round(float(f"{avg_price:.5g}"), max_decimals - sz_decimals_coin)
        return avg_price
        
    def apply_fill(self, fill: Dict[str, Any]) -> None:
        coin = fill["coin"]
        fill_qty = float(fill["qty"])        # signed
        fill_price = float(fill["price"])
        sz_decimals_coin = sz_decimals.get(coin, 0)
        if coin in self.positions.keys():
            pass
        else:
            self.positions[coin]['qty']=0
            self.positions[coin]['avg_price']=0
            self.positions[coin]['realized_pnl']=0
        # --- Case 1: No existing position ---
        pos = self.positions[coin]
        pos_qty = pos["qty"]
        pos_avg = pos["avg_price"]
        if pos_qty == 0:
            pos["qty"] = fill_qty
            pos["avg_price"] = fill_price
            return

        # --- Same direction: increase position ---
        if pos_qty * fill_qty > 0:
            new_qty = pos_qty + fill_qty
            avg_price = (abs(pos_qty) * pos_avg + abs(fill_qty) * fill_price
            ) / abs(new_qty)
            avg_price = self.round_price(avg_price,sz_decimals_coin)
            pos["avg_price"] = avg_price             
            pos["qty"] = round(new_qty,sz_decimals_coin)
            return

        # --- Opposite direction: closing or flipping ---
        closing_qty = min(abs(fill_qty), abs(pos_qty))
        opening_qty = abs(fill_qty) - closing_qty

        # --- Realized PnL ---
        if pos_qty > 0:
            # closing a long
            realized = closing_qty * (fill_price - pos_avg)
        else:
            # closing a short
            realized = closing_qty * (pos_avg - fill_price)

        pos["realized_pnl"] += round(realized,2)

        # --- Remaining position ---
        if opening_qty == 0:
            # fully closed
            pos["qty"] = round(pos_qty + fill_qty,sz_decimals_coin)
            if pos["qty"] == 0:
                pos["avg_price"] = 0.0
        else:
            # flipped
            pos["qty"] = round(opening_qty,sz_decimals_coin) if fill_qty > 0 else -round(opening_qty,sz_decimals_coin)
            pos["avg_price"] = self.round_price(fill_price,sz_decimals_coin)

    def rebuild_from_fills(
        self, fills: Iterable[Dict[str, Any]]
    ) -> Dict[str, Dict[str, float]]:
        for fill in fills:
            self.apply_fill(fill)
        return dict(self.positions)
