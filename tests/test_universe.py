import pytest
import pandas as pd
from src.universe import (
    get_universe,
    _eligible_ranked,
    ADD_THRESHOLD,
    REMOVE_THRESHOLD,
    UNIVERSE_SIZE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_market_cap(symbols: list[str]) -> pd.DataFrame:
    """Build a minimal market-cap DataFrame ranked in the order given.

    Rank 1 = index 0 (largest market cap). All symbols are pre-filtered as
    eligible (non-stable, non-excluded) so tests can focus on rank logic.
    """
    return pd.DataFrame({
        "symbol": [s.lower() for s in symbols],  # CoinGecko returns lowercase
        "name": symbols,
        "market_cap": [1_000_000 * (len(symbols) - i) for i in range(len(symbols))],
    })


def hl_set(symbols: list[str]) -> set:
    """All supplied symbols are considered listed on Hyperliquid."""
    return set(symbols)


def coins(n: int, prefix: str = "C") -> list[str]:
    """Generate n distinct coin symbols: C01, C02, ..."""
    return [f"{prefix}{i:02d}" for i in range(1, n + 1)]


# ── Initial universe ──────────────────────────────────────────────────────────

def test_initial_universe_is_top_50():
    syms = coins(100)
    top = make_market_cap(syms)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=None)

    assert len(universe) == UNIVERSE_SIZE
    assert set(universe) == set(syms[:UNIVERSE_SIZE])


def test_initial_universe_fewer_than_50_eligible():
    syms = coins(30)
    top = make_market_cap(syms)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=None)

    assert len(universe) == 30
    assert set(universe) == set(syms)


# ── No-change buffer zone ─────────────────────────────────────────────────────

def test_no_change_when_all_coins_within_buffer():
    """Universe is stable when no coin breaches either threshold."""
    syms = coins(60)
    current = syms[:UNIVERSE_SIZE]          # top 50 currently in universe
    top = make_market_cap(syms)             # same ranking — nothing has moved
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert set(universe) == set(current)


def test_coin_at_exact_remove_threshold_is_retained():
    """A coin ranked exactly REMOVE_THRESHOLD (53) must NOT be removed."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]          # C01–C50 in universe

    # Move C01 (previously rank 1) to rank 53 exactly
    reranked = syms[1:REMOVE_THRESHOLD] + [syms[0]] + syms[REMOVE_THRESHOLD:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert syms[0] in universe, f"{syms[0]} at rank {REMOVE_THRESHOLD} should be retained"


def test_coin_at_exact_add_threshold_is_not_added():
    """A new coin ranked exactly ADD_THRESHOLD (47) must be added (boundary inclusive)."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]          # C01–C50 in universe
    new_coin = "NEW"

    # Insert NEW at rank ADD_THRESHOLD (position index ADD_THRESHOLD - 1)
    reranked = syms[:ADD_THRESHOLD - 1] + [new_coin] + syms[ADD_THRESHOLD - 1:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms + [new_coin])

    universe, _ = get_universe(top, hl, current_universe=current)

    assert new_coin in universe, f"new coin at rank {ADD_THRESHOLD} should be added"


# ── Removal logic ─────────────────────────────────────────────────────────────

def test_coin_removed_when_rank_exceeds_53():
    """A coin that drops to rank 54 must be removed from the universe."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]          # C01–C50

    # Drop C01 to rank 54 (index 53)
    reranked = syms[1:54] + [syms[0]] + syms[54:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert syms[0] not in universe, f"{syms[0]} at rank 54 should be removed"


def test_coin_not_removed_at_rank_53():
    """A coin at rank 53 sits inside the buffer and must be kept."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]

    # Move C01 to rank 53 (index 52)
    reranked = syms[1:52] + [syms[0]] + syms[52:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert syms[0] in universe


def test_coin_not_removed_at_rank_48():
    """A coin at rank 48 is comfortably inside both thresholds — retained."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]

    reranked = syms[1:47] + [syms[0]] + syms[47:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert syms[0] in universe


def test_coin_not_listed_on_hl_is_removed():
    """A coin delisted from Hyperliquid must leave the universe immediately."""
    syms = coins(60)
    current = syms[:UNIVERSE_SIZE]
    delisted = syms[0]

    top = make_market_cap(syms)
    hl  = hl_set(syms[1:])                 # delisted is absent from HL

    universe, _ = get_universe(top, hl, current_universe=current)

    assert delisted not in universe


# ── Addition logic ────────────────────────────────────────────────────────────

def test_new_coin_added_when_rank_below_47():
    """A coin not in the universe that reaches rank 46 must be added."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    # Insert NEW at rank 46 (index 45)
    reranked = syms[:45] + [new_coin] + syms[45:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms + [new_coin])

    universe, _ = get_universe(top, hl, current_universe=current)

    assert new_coin in universe


def test_new_coin_not_added_when_rank_is_48():
    """A coin outside the universe at rank 48 sits in the buffer — must not be added."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    # Insert NEW at rank 48 (index 47)
    reranked = syms[:47] + [new_coin] + syms[47:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms + [new_coin])

    universe, _ = get_universe(top, hl, current_universe=current)

    assert new_coin not in universe


def test_new_coin_not_added_when_rank_is_53():
    """A coin at rank 53 is outside the add threshold and must not be added."""
    syms = coins(70)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    reranked = syms[:52] + [new_coin] + syms[52:]
    top = make_market_cap(reranked)
    hl  = hl_set(syms + [new_coin])

    universe, _ = get_universe(top, hl, current_universe=current)

    assert new_coin not in universe


# ── Symbol index ──────────────────────────────────────────────────────────────

def test_symbol_index_matches_universe():
    syms = coins(60)
    top = make_market_cap(syms)
    hl  = hl_set(syms)

    universe, symbol_index = get_universe(top, hl, current_universe=None)

    assert set(symbol_index.keys()) == set(universe)
    for sym, idx in symbol_index.items():
        assert universe[idx] == sym


# ── Eligible vs Tradable: rank integrity ───────────────────────────────────────

def test_eligible_ranking_is_independent_of_exchange_listing():
    """Ranks are TRUE market-cap ranks — listing must not reorder them.

    _eligible_ranked no longer takes the exchange-listing set; a coin's position
    reflects its market-cap standing, not its position among listed coins.
    """
    syms = coins(60)
    ranked = _eligible_ranked(make_market_cap(syms))
    assert ranked == syms


def test_stablecoins_and_excluded_do_not_consume_rank_slots():
    """A stablecoin (or EXCLUDED token) sitting high by market cap is skipped,
    so the coins below it keep their real, contiguous ranks."""
    ranked = _eligible_ranked(make_market_cap(["BTC", "USDT", "ETH", "PAXG", "SOL"]))
    assert ranked == ["BTC", "ETH", "SOL"]


# ── Requirement: assets outside top N cannot enter ─────────────────────────────

def test_untradable_top_coins_do_not_backfill_with_lower_ranks():
    """The headline bug: if only some of the top N are tradable, the universe is
    NOT padded with assets ranked beyond N.

    100 eligible coins; ranks 31–50 are unlisted while ranks 1–30 and 51–100 are
    listed. The old 'filter-first' logic produced ranks 1–30 + 51–70 (size 50).
    The new logic returns only the tradable subset of the top 50.
    """
    syms = coins(100)
    top = make_market_cap(syms)
    hl = hl_set(syms[:30] + syms[UNIVERSE_SIZE:])   # ranks 1-30 and 51-100 listed

    universe, _ = get_universe(top, hl, current_universe=None)

    assert set(universe) == set(syms[:30])
    assert all(s not in universe for s in syms[UNIVERSE_SIZE:]), "no rank-51+ back-fill"


# ── Requirement: tradable universe size may be < N ─────────────────────────────

def test_tradable_universe_may_be_smaller_than_n():
    """Plenty of eligible coins exist, but few of the top N are tradable."""
    syms = coins(100)
    top = make_market_cap(syms)
    hl = hl_set(syms[:20] + syms[60:])              # only 20 of the top 50 listed

    universe, _ = get_universe(top, hl, current_universe=None)

    assert len(universe) == 20
    assert len(universe) < UNIVERSE_SIZE


# ── Requirement: listing changes cannot promote lower-ranked assets ────────────

def test_removal_does_not_backfill_beyond_top_n():
    """Delisting a top coin frees a slot, but rank-51+ coins cannot fill it."""
    syms = coins(100)
    current = syms[:UNIVERSE_SIZE]
    top = make_market_cap(syms)
    hl = hl_set(syms[1:])                            # delist rank 1; 51+ all listed

    universe, _ = get_universe(top, hl, current_universe=current)

    assert syms[0] not in universe                  # delisted coin removed
    assert syms[UNIVERSE_SIZE] not in universe      # rank 51 did NOT back-fill
    assert set(universe) <= set(syms[:UNIVERSE_SIZE])


def test_unlisted_top_ranks_do_not_inflate_a_coins_effective_rank():
    """A coin at TRUE rank 48 (just outside ADD_THRESHOLD=47) must not be added,
    even when every coin ranked above it is unlisted — which would make it the
    highest-ranked *listed* coin under the old filter-first logic.
    """
    syms = coins(100)
    new_coin = "NEW"
    reranked = syms[:ADD_THRESHOLD] + [new_coin] + syms[ADD_THRESHOLD:]  # NEW at rank 48
    top = make_market_cap(reranked)

    # Only NEW and the two coins just below it are listed; ranks 1–47 are not.
    current = [syms[ADD_THRESHOLD], syms[ADD_THRESHOLD + 1]]  # existing tradable holdings
    hl = hl_set([new_coin] + current)

    universe, _ = get_universe(top, hl, current_universe=current)

    assert new_coin not in universe, "rank-48 coin must not be added despite empty top ranks"
    assert set(universe) == set(current)
