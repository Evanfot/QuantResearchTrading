"""
Universe management: market-cap data and coin selection with buffer-zone hysteresis.

Two-stage design:

  1. Eligible Universe — the top UNIVERSE_SIZE cryptocurrencies by market cap.
     A coin's rank is its TRUE market-cap rank (rank 1 = largest), computed
     before any tradability filtering. Stablecoins and EXCLUDED tokens are not
     trend-tradable cryptocurrencies and are removed before ranking, so they
     never consume a rank slot.

  2. Tradable Universe — the Eligible Universe intersected with the tradability
     filters: listed on the exchange (Hyperliquid), plus any liquidity and
     history / data-quality requirements applied downstream.

Selection rules (all rank thresholds refer to TRUE market-cap rank):
  - Initial / no prior universe: top UNIVERSE_SIZE eligible coins that are tradable.
  - Additions: a coin must reach rank <= ADD_THRESHOLD (and be tradable) to enter.
  - Removals:  a coin must fall to rank >  REMOVE_THRESHOLD (or become untradable) to exit.

Crucially, the universe is NEVER backfilled with assets ranked beyond the top N
just because fewer than N are tradable. If only 30 of the top 50 are tradable,
the Tradable Universe has 30 members — assets ranked 51+ cannot enter. The
number of tradable assets therefore varies through time.
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
MKT_CAP_DIR.mkdir(parents=True, exist_ok=True)


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
    cryptocurrency: not a stablecoin, not in EXCLUDED, de-duplicated (first
    occurrence by market-cap rank wins).

    Tradability filters (exchange listing, liquidity, history/data-quality) are
    deliberately NOT applied here. Applying them before ranking is what caused
    lower-cap assets to back-fill the universe: a coin's rank must reflect its
    real market-cap standing, not its position within an already-filtered list.
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
    """Return (tradable_universe, symbol_index) applying buffer-zone hysteresis.

    Implements the two-stage Eligible -> Tradable design:
      * Eligible Universe = top UNIVERSE_SIZE coins by TRUE market-cap rank.
      * Tradable Universe = Eligible AND tradable (listed on the exchange, plus
        any liquidity / history filters applied downstream).

    The universe is never expanded past rank UNIVERSE_SIZE to compensate for
    untradable coins, so its size may be < UNIVERSE_SIZE and may vary over time.

    Args:
        top_market_cap:   Latest market-cap DataFrame (from get_latest_market_cap).
        hl_universe:      Set of coins listed on Hyperliquid (from get_hl_coins).
        current_universe: Coins currently in the universe (from state), or None
                          for first-run initialisation.

    Returns:
        universe:     Ordered list of tradable coin symbols.
        symbol_index: {symbol: position_in_universe}.
    """
    eligible = _eligible_ranked(top_market_cap)
    rank_of = {sym: i + 1 for i, sym in enumerate(eligible)}  # 1-based TRUE market-cap rank

    def tradable(sym: str) -> bool:
        """Tradability filters layered on top of the Eligible Universe.

        Currently the only exchange-level filter is being listed on Hyperliquid.
        Liquidity and history / data-quality filters are applied downstream
        (e.g. dropping coins absent from the price cache); they further narrow
        this set but can never widen it.
        """
        return sym in hl_universe

    if not current_universe:
        # First run — Tradable Universe = (top UNIVERSE_SIZE eligible) AND tradable.
        # Note the slice happens BEFORE the tradability filter, so coins ranked
        # past UNIVERSE_SIZE can never back-fill; the result may hold < N coins.
        universe = [s for s in eligible[:UNIVERSE_SIZE] if tradable(s)]
    else:
        # Retain coins still within the removal threshold AND still tradable.
        retained = [s for s in current_universe
                    if rank_of.get(s, REMOVE_THRESHOLD + 1) <= REMOVE_THRESHOLD
                    and tradable(s)]

        # Add tradable coins newly inside the addition threshold. The slice is on
        # market-cap rank, so additions are drawn only from the top ADD_THRESHOLD.
        retained_set = set(retained)
        additions = [s for s in eligible[:ADD_THRESHOLD]
                     if tradable(s) and s not in retained_set]

        universe = retained + additions

    symbol_index = {sym: i for i, sym in enumerate(universe)}
    return universe, symbol_index
