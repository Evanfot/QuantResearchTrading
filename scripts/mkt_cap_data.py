"""Re-exports from src.universe for backward compatibility."""
from src.universe import get_top_marketcap, store_market_cap, get_latest_market_cap

if __name__ == "__main__":
    store_market_cap(get_top_marketcap(200))
