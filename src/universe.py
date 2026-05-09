"""
Universe management: market-cap data and coin selection with buffer-zone hysteresis.

Selection rules:
  - Initial / no prior universe: top UNIVERSE_SIZE eligible coins
  - Additions: a coin must reach rank <= ADD_THRESHOLD to enter
  - Removals:  a coin must fall to rank >  REMOVE_THRESHOLD to exit

Eligibility: must be listed on Hyperliquid, not a stablecoin, not excluded.
"""
import datetime as dt
import pandas as pd
import requests
from pathlib import Path
from datetime import timezone

# ── Constants ─────────────────────────────────────────────────────────────────

UNIVERSE_SIZE     = 50
ADD_THRESHOLD     = 47
REMOVE_THRESHOLD  = 53

STABLE = {"USDT", "USDC", "DAI", "USDD", "FDUSD", "TUSD", "DEI", "USDP", "GUSD", "USDE"}
EXCLUDED = {"PAXG"}

MKT_CAP_DIR = Path("data/mkt_cap")
MKT_CAP_DIR.mkdir(exist_ok=True)


# ── Market-cap data ───────────────────────────────────────────────────────────

def get_top_marketcap(n: int = 200) -> list:
    """Fetch top-n coins by market cap from CoinGecko. Returns raw JSON list."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": n,
        "page": 1,
    }
    return requests.get(url, params=params).json()


def store_market_cap(top: list) -> Path:
    """Persist a market-cap snapshot to data/mkt_cap/."""
    ts = dt.datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path = MKT_CAP_DIR / f"{ts}.csv"
    pd.DataFrame(top).to_csv(path)
    return path


def get_latest_market_cap() -> pd.DataFrame:
    """Load the most recently saved market-cap snapshot."""
    latest = max(MKT_CAP_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime)
    return pd.read_csv(latest, index_col=0)


# ── Universe selection ────────────────────────────────────────────────────────

def _eligible_ranked(top_market_cap: pd.DataFrame, hl_universe: set) -> list[str]:
    """Return eligible coins in descending market-cap order (rank 1 = largest).

    Eligibility: listed on Hyperliquid, not a stablecoin, not in EXCLUDED,
    no duplicates (keeps first occurrence by market-cap rank).
    """
    seen: set[str] = set()
    eligible: list[str] = []
    for _, coin in top_market_cap.iterrows():
        symbol = coin["symbol"].upper()
        if symbol in EXCLUDED or symbol in STABLE or symbol in seen:
            continue
        if symbol in hl_universe:
            eligible.append(symbol)
            seen.add(symbol)
    return eligible


def get_universe(
    top_market_cap: pd.DataFrame,
    hl_universe: set,
    current_universe: list | None = None,
) -> tuple[list[str], dict[str, int]]:
    """Return (universe, symbol_index) applying buffer-zone hysteresis.

    Args:
        top_market_cap:   Latest market-cap DataFrame (from get_latest_market_cap).
        hl_universe:      Set of coins listed on Hyperliquid (from get_hl_coins).
        current_universe: Coins currently in the universe (from state), or None
                          for first-run initialisation.

    Returns:
        universe:     Ordered list of coin symbols.
        symbol_index: {symbol: position_in_universe}.
    """
    eligible = _eligible_ranked(top_market_cap, hl_universe)
    rank_of = {sym: i + 1 for i, sym in enumerate(eligible)}  # 1-based rank

    if not current_universe:
        # First run — seed with top UNIVERSE_SIZE
        universe = eligible[:UNIVERSE_SIZE]
    else:
        # Retain coins still within the removal threshold
        retained = [s for s in current_universe
                    if rank_of.get(s, REMOVE_THRESHOLD + 1) <= REMOVE_THRESHOLD]

        # Add coins newly inside the addition threshold
        retained_set = set(retained)
        additions = [s for s in eligible[:ADD_THRESHOLD] if s not in retained_set]

        universe = retained + additions

    symbol_index = {sym: i for i, sym in enumerate(universe)}
    return universe, symbol_index
