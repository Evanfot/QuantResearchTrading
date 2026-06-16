"""One-shot limit-order placement test to isolate the 'Trading is halted' error.

Places a SINGLE GTC limit BUY 15% below mid (non-marketable — it rests and will
not fill), prints Hyperliquid's raw response, then cancels it if it rested.

Uses the exact same Exchange / wallet / agent setup as the live bot
(src/main.py execution path) so the result reflects the bot's real order path.

Run from the repo root:
    python -m scripts.test_order            # defaults to ETH
    python -m scripts.test_order BTC

Run it on the SERVER (UK) to reproduce, and from a non-UK origin to compare —
if it rests from one origin but returns 'Trading is halted.' from the other,
the cause is HL's trading-endpoint geofence, not the code.
"""
import sys
import json

from eth_account import Account
from hyperliquid.exchange import Exchange

from src.config import make_info, HL_API_URL, PRIVATE_KEY, WALLET_ADDRESS, API_ADDRESS, TRADING_ENV
from scripts.meta_data import read_latest_meta

COIN = sys.argv[1] if len(sys.argv) > 1 else "ETH"
PRICE_OFFSET = 0.85   # BUY 15% below mid -> non-marketable, will not fill
TARGET_NOTIONAL = 15.0  # USD, comfortably above HL's $10 minimum


def _clean_px(px: float, sz_dec: int) -> float:
    """Match the bot's price rounding (5 sig figs, then tick precision)."""
    precision = max(0, 6 - sz_dec)
    return round(float(f"{px:.5g}"), precision)


def main():
    info = make_info()
    sz_decimals = {c["name"]: c["szDecimals"] for c in read_latest_meta()["universe"]}
    if COIN not in sz_decimals:
        raise SystemExit(f"{COIN} not in perp meta universe")
    sz_dec = sz_decimals[COIN]

    mid = float(info.all_mids()[COIN])
    limit_px = _clean_px(mid * PRICE_OFFSET, sz_dec)
    sz = round(TARGET_NOTIONAL / limit_px, sz_dec)

    wallet = Account.from_key(PRIVATE_KEY)
    spot_meta = {"universe": [], "tokens": []} if TRADING_ENV == "testnet" else None
    ex = Exchange(wallet=wallet, base_url=HL_API_URL, account_address=API_ADDRESS, spot_meta=spot_meta)

    print("── setup ──────────────────────────────────────────────")
    print(f"env={TRADING_ENV}  base_url={HL_API_URL}")
    print(f"signer (wallet.address) = {wallet.address}")
    print(f"account_address (API_ADDRESS) = {API_ADDRESS}")
    print(f"master WALLET_ADDRESS = {WALLET_ADDRESS}")
    print(f"order: BUY {sz} {COIN} @ {limit_px}  "
          f"(mid={mid}, ~${sz * limit_px:.2f}, {int((1 - PRICE_OFFSET) * 100)}% below mid — non-marketable)")
    print("── submitting (same path as bot: bulk_orders) ─────────")

    order = {
        "coin": COIN,
        "is_buy": True,
        "sz": sz,
        "limit_px": limit_px,
        "order_type": {"limit": {"tif": "Gtc"}},
        "reduce_only": False,
    }
    resp = ex.bulk_orders([order])
    print("RAW RESPONSE:", json.dumps(resp))

    # Cancel anything that rested so we leave no live order behind.
    try:
        statuses = resp["response"]["data"]["statuses"]
    except (KeyError, TypeError):
        statuses = []
    for st in statuses:
        if isinstance(st, dict) and "resting" in st:
            oid = st["resting"]["oid"]
            c = ex.bulk_cancel([{"coin": COIN, "oid": oid}])
            print(f"cancelled resting oid={oid}: {json.dumps(c)}")
        elif isinstance(st, dict) and "error" in st:
            print(f"REJECTED: {st['error']}")


if __name__ == "__main__":
    main()
