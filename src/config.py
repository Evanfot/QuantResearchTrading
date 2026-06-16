import logging
import os
import time

from dotenv import load_dotenv
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL

logger = logging.getLogger(__name__)

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


_INFO = None


def _build_info():
    from hyperliquid.info import Info
    if TRADING_ENV == "testnet":
        # Testnet API rejects the `dex` field the SDK sends in both spot_meta
        # and meta requests, so we fetch them manually and pass them in directly.
        meta = _testnet_meta()
        return Info(HL_API_URL, skip_ws=True, meta=meta, spot_meta={"universe": [], "tokens": []})
    return Info(HL_API_URL, skip_ws=True)


def make_info(force: bool = False, max_attempts: int = 5):
    """Return a process-wide cached Info instance.

    Constructing Info fires two metadata POSTs (spotMeta + meta), so we build it
    once and reuse it instead of hammering /info on every poll cycle (which was
    triggering CloudFront 429s). Construction is retried with exponential backoff
    so a transient rate-limit doesn't crash the cycle. Pass force=True to rebuild.
    """
    global _INFO
    if _INFO is not None and not force:
        return _INFO

    from hyperliquid.utils.error import ClientError

    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            _INFO = _build_info()
            return _INFO
        except ClientError as e:
            # Only back off and retry on rate-limit; re-raise anything else.
            if getattr(e, "status_code", None) != 429 or attempt == max_attempts:
                raise
            logger.warning(
                "[make_info] 429 rate-limited building Info "
                f"(attempt {attempt}/{max_attempts}), retrying in {delay:.0f}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def open_orders(info, address):
    # Testnet runs an older API that rejects the `dex` field the SDK sends.
    if TRADING_ENV == "testnet":
        import requests
        r = requests.post(f"{HL_API_URL}/info", json={"type": "openOrders", "user": address}, timeout=10)
        r.raise_for_status()
        return r.json()
    return info.open_orders(address)
