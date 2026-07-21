"""
Universe management: market-cap data and coin selection with buffer-zone hysteresis.

Two-stage design (ranks are TRUE market-cap rank, not exchange-relative):
  - Eligible Universe = genuine cryptocurrencies (not a stablecoin, not EXCLUDED
    trackers like PAXG), ranked by TRUE market cap.
  - Tradable Universe = the top UNIVERSE_SIZE eligible coins that are also tradable
    (listed on Hyperliquid). Tradability is applied AFTER ranking, so untradable
    coins consume a rank slot rather than letting lower-cap coins back-fill.

Selection rules (all thresholds are TRUE market-cap rank):
  - Initial / no prior universe: top UNIVERSE_SIZE eligible coins that are tradable
  - Additions: a coin must reach rank <= ADD_THRESHOLD (and be tradable) to enter
  - Removals:  a coin must fall to rank >  REMOVE_THRESHOLD (or become untradable) to exit
"""
import datetime as dt
import pandas as pd
import requests
from pathlib import Path
from datetime import timezone

# ── Constants ─────────────────────────────────────────────────────────────────

# Sized so the *tradable* book (top-N genuine cryptos ∩ Hyperliquid) is ~50: on the
# current snapshot only ~50/80 of the top-80 genuine coins are HL-listed. ADD/REMOVE
# keep a ±3-rank hysteresis buffer around N. (If the kPEPE/kSHIB memecoin mapping is
# later added, more coins count as tradable and N should come back down.)
UNIVERSE_SIZE     = 80
ADD_THRESHOLD     = 77
REMOVE_THRESHOLD  = 83

# Tokens removed from the market-cap ranking because they are not genuine,
# independently-trending cryptocurrencies: fiat-pegged stablecoins, wrapped /
# liquid-staking derivatives (track an already-ranked underlying), commodity
# trackers, and tokenized funds / RWAs. Removing them BEFORE ranking keeps a real
# coin's rank reflecting its true market-cap standing. Curated by hand — extend
# these sets as new such tokens climb into the top ranks. (Exchange/utility tokens
# like LEO/OKB/BGB are deliberately NOT excluded; they are genuine tokens and are
# filtered later only if they aren't tradable on Hyperliquid.)

# Fiat-pegged stablecoins.
STABLE = {
    "USDT", "USDC", "DAI", "USDD", "FDUSD", "TUSD", "DEI", "USDP", "GUSD", "USDE",
    "USDS", "USD1", "USDG", "PYUSD", "USDY", "RLUSD", "USDF", "BFUSD", "USDGO", "U",
    "USD0", "USDX", "CRVUSD", "GHO", "LUSD", "FRAX", "SUSD", "USDL", "USDB",
}

# Wrapped / liquid-staking derivatives (track an underlying already in the ranking).
_WRAPPED = {
    "WBTC", "WETH", "WBETH", "WEETH", "WSTETH", "STETH", "CBETH", "RETH", "METH",
    "WBNB", "WSOL", "LBTC", "SOLVBTC", "BTCB", "EZETH", "RSETH", "SWETH",
}
# Commodity trackers (e.g. tokenized gold).
_COMMODITY = {"PAXG", "XAUT"}
# Tokenized funds / real-world-asset money-market instruments.
_TOKENIZED_RWA = {
    "BUIDL", "USYC", "EUTBL", "BCAP", "FIGR_HELOC", "OUSG", "USTB", "BENJI",
    "JAAA", "JTRSY",
}
EXCLUDED = _WRAPPED | _COMMODITY | _TOKENIZED_RWA

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

def _eligible_ranked(top_market_cap: pd.DataFrame) -> list[str]:
    """Return eligible cryptocurrencies in descending market-cap order.

    Position in the returned list is the coin's TRUE market-cap rank
    (index 0 = rank 1 = largest). 'Eligible' means a genuine trend-tradable
    cryptocurrency: not a stablecoin, not in EXCLUDED (trackers like PAXG),
    de-duplicated (first occurrence by market-cap rank wins).

    Tradability filters (exchange listing, liquidity, history/data-quality) are
    deliberately NOT applied here. Applying them before ranking is what let
    lower-cap assets back-fill the universe: a coin's rank must reflect its real
    market-cap standing, not its position within an already-filtered list.
    """
    seen: set[str] = set()
    eligible: list[str] = []
    for _, coin in top_market_cap.iterrows():
        symbol = coin["symbol"].upper()
        if symbol in EXCLUDED or symbol in STABLE or symbol in seen:
            continue
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
        universe:     Ordered list of tradable coin symbols.
        symbol_index: {symbol: position_in_universe}.

    Two-stage Eligible -> Tradable design:
      * Eligible Universe = top UNIVERSE_SIZE coins by TRUE market-cap rank.
      * Tradable Universe = Eligible AND tradable (listed on Hyperliquid, plus
        any liquidity / history filters applied downstream).
    The universe is never expanded past rank UNIVERSE_SIZE to compensate for
    untradable coins, so its size may be < UNIVERSE_SIZE and may vary over time.
    """
    eligible = _eligible_ranked(top_market_cap)
    rank_of = {sym: i + 1 for i, sym in enumerate(eligible)}  # 1-based TRUE market-cap rank

    def tradable(sym: str) -> bool:
        """Tradability layered on top of the Eligible Universe. Currently the only
        exchange-level filter is being listed on Hyperliquid; liquidity / history
        filters downstream (e.g. dropping coins absent from the price cache) can
        further narrow this set but never widen it."""
        return sym in hl_universe

    if not current_universe:
        # First run — Tradable Universe = (top UNIVERSE_SIZE eligible) AND tradable.
        # The slice happens BEFORE the tradability filter, so coins ranked past
        # UNIVERSE_SIZE can never back-fill; the result may hold < UNIVERSE_SIZE.
        universe = [s for s in eligible[:UNIVERSE_SIZE] if tradable(s)]
    else:
        # Retain coins still within the removal threshold AND still tradable.
        retained = [s for s in current_universe
                    if rank_of.get(s, REMOVE_THRESHOLD + 1) <= REMOVE_THRESHOLD
                    and tradable(s)]

        # Add tradable coins newly inside the addition threshold (by TRUE rank).
        retained_set = set(retained)
        additions = [s for s in eligible[:ADD_THRESHOLD]
                     if tradable(s) and s not in retained_set]

        universe = retained + additions

    symbol_index = {sym: i for i, sym in enumerate(universe)}
    return universe, symbol_index
