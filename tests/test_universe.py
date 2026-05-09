import pytest
import pandas as pd
from src.universe import get_universe, ADD_THRESHOLD, REMOVE_THRESHOLD, UNIVERSE_SIZE


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
