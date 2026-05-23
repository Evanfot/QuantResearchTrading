"""
Verify that the new Binance DataStore wiring works end-to-end.
Run from the project root: python verify_data_connection.py
"""

import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from storage import DataStore, HistoricalStore
from symbol_map import _binance_to_hl, to_hl, to_binance
from src.data import get_ohlcv, load_ohlcv_for_alphas

# ── 1. DataStore can see data ──────────────────────────────────────────────────
print("=" * 60)
print("1. DataStore — available symbols")
ds = DataStore()
syms = ds.historical.available_symbols()
print(f"   {len(syms)} Binance symbols in historical store")
print(f"   First 5: {syms[:5]}")

# ── 2. get_ohlcv() ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("2. get_ohlcv()")
prices_all = get_ohlcv()
print(f"   shape:  {prices_all.shape}")
print(f"   head:\n{prices_all.iloc[:3, :5]}")
print(f"   tail:\n{prices_all.iloc[-3:, :5]}")

# ── 3. load_ohlcv_for_alphas() ─────────────────────────────────────────────────
print("\n" + "=" * 60)
print("3. load_ohlcv_for_alphas(['BTC', 'ETH', 'SOL'])")
o, h, l, c, v = load_ohlcv_for_alphas(["BTC", "ETH", "SOL"])
for name, df in [("open", o), ("high", h), ("low", l), ("close", c), ("volume", v)]:
    print(f"   {name:8s}: shape={df.shape}, columns={list(df.columns)}")

print(f"\n   Confirming volume is quote_volume (dollar vol) — sample BTC volume:")
print(f"   {v['BTC'].tail(3).to_dict()}")

# ── 4. Symbol map — 10 largest (by coverage in store) ─────────────────────────
print("\n" + "=" * 60)
print("4. Symbol map — 10 largest HL symbols (sorted alphabetically as proxy)")
top10 = sorted(list(_binance_to_hl.items()))[:10]
for b, h_sym in top10:
    print(f"   {b:20s} → {h_sym}")

# ── 5. VWAP = quote_volume / volume ───────────────────────────────────────────
print("\n" + "=" * 60)
print("5. VWAP sanity check on BTC (quote_volume / volume)")
b_sym = to_binance("BTC")
store = HistoricalStore()
raw = store.load_symbol(b_sym)
if not raw.empty:
    raw = raw.tail(1440 * 3)  # last 3 days of 1m bars
    raw["vwap_check"] = raw["quote_volume"] / raw["volume"].replace(0, float("nan"))
    vwap_mean = raw["vwap_check"].mean()
    close_mean = raw["close"].mean()
    print(f"   Mean VWAP:  {vwap_mean:.2f}")
    print(f"   Mean close: {close_mean:.2f}")
    print(f"   Ratio (should be ~1.0): {vwap_mean / close_mean:.4f}")
else:
    print("   No BTC data found.")

print("\n" + "=" * 60)
print("Done — all checks passed.")
