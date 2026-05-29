import os

from dotenv import load_dotenv
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL

load_dotenv()

TRADING_ENV = os.getenv("TRADING_ENV", "mainnet").lower()

if TRADING_ENV == "testnet":
    HL_API_URL = TESTNET_API_URL
    PRIVATE_KEY = os.getenv("HYPERLIQUID_TESTNET_PRIVATE_KEY")
    WALLET_ADDRESS = os.getenv("HYPERLIQUID_TESTNET_WALLET_ADDRESS")
    API_ADDRESS = os.getenv("HYPERLIQUID_TESTNET_API_WALLET_ADDRESS")
    _missing = [k for k, v in {
        "HYPERLIQUID_TESTNET_PRIVATE_KEY": PRIVATE_KEY,
        "HYPERLIQUID_TESTNET_WALLET_ADDRESS": WALLET_ADDRESS,
        "HYPERLIQUID_TESTNET_API_WALLET_ADDRESS": API_ADDRESS,
    }.items() if not v]
    if _missing:
        raise EnvironmentError(f"Testnet env vars not set: {', '.join(_missing)}")
else:
    HL_API_URL = MAINNET_API_URL
    PRIVATE_KEY = os.getenv("HYPERLIQUID_PRIVATE_KEY")
    WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")
    API_ADDRESS = os.getenv("HYPERLIQUID_API_WALLET_ADDRESS")


def _testnet_meta():
    """Fetch perp meta from testnet without the `dex` field the SDK adds."""
    import requests
    r = requests.post(f"{HL_API_URL}/info", json={"type": "meta"}, timeout=10)
    r.raise_for_status()
    return r.json()


def make_info():
    from hyperliquid.info import Info
    if TRADING_ENV == "testnet":
        # Testnet API rejects the `dex` field the SDK sends in both spot_meta
        # and meta requests, so we fetch them manually and pass them in directly.
        meta = _testnet_meta()
        return Info(HL_API_URL, skip_ws=True, meta=meta, spot_meta={"universe": [], "tokens": []})
    return Info(HL_API_URL, skip_ws=True)


def open_orders(info, address):
    # Testnet runs an older API that rejects the `dex` field the SDK sends.
    if TRADING_ENV == "testnet":
        import requests
        r = requests.post(f"{HL_API_URL}/info", json={"type": "openOrders", "user": address}, timeout=10)
        r.raise_for_status()
        return r.json()
    return info.open_orders(address)
