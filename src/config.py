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
else:
    HL_API_URL = MAINNET_API_URL
    PRIVATE_KEY = os.getenv("HYPERLIQUID_PRIVATE_KEY")
    WALLET_ADDRESS = os.getenv("HYPERLIQUID_WALLET_ADDRESS")
    API_ADDRESS = os.getenv("HYPERLIQUID_API_WALLET_ADDRESS")
