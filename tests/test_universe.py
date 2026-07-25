import pytest
import pandas as pd
from src.universe import get_universe, ADD_THRESHOLD, REMOVE_THRESHOLD, UNIVERSE_SIZE


# ── Helpers ───────────────────────────────────────────────────────────────────
#
# Rank thresholds are read from src.universe (ADD_THRESHOLD / REMOVE_THRESHOLD /
# UNIVERSE_SIZE) rather than hardcoded, so these tests exercise the real buffer
# zone (ADD_THRESHOLD, REMOVE_THRESHOLD] and keep passing when the sizing changes
# (e.g. the top-50 → top-80 move). POOL is a symbol pool comfortably larger than
# any threshold so every rank under test is actually representable.

POOL = REMOVE_THRESHOLD + 20


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
    """Generate n distinct coin symbols: C001, C002, ..."""
    return [f"{prefix}{i:03d}" for i in range(1, n + 1)]


def rerank(symbols: list[str], coin: str, rank: int) -> list[str]:
    """Return `symbols` reordered so `coin` sits at 1-based market-cap `rank`."""
    others = [s for s in symbols if s != coin]
    return others[:rank - 1] + [coin] + others[rank - 1:]


# ── Initial universe ──────────────────────────────────────────────────────────

def test_initial_universe_is_top_n():
    syms = coins(POOL)
    universe, _ = get_universe(make_market_cap(syms), hl_set(syms), current_universe=None)

    assert len(universe) == UNIVERSE_SIZE
    assert set(universe) == set(syms[:UNIVERSE_SIZE])


def test_initial_universe_fewer_than_n_eligible():
    syms = coins(UNIVERSE_SIZE - 20)
    universe, _ = get_universe(make_market_cap(syms), hl_set(syms), current_universe=None)

    assert len(universe) == len(syms)
    assert set(universe) == set(syms)


# ── No-change buffer zone ─────────────────────────────────────────────────────

def test_no_change_when_all_coins_within_buffer():
    """Universe is stable when no coin breaches either threshold."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]           # top-N currently in universe
    top = make_market_cap(syms)              # same ranking — nothing has moved

    universe, _ = get_universe(top, hl_set(syms), current_universe=current)

    assert set(universe) == set(current)


# ── Removal logic (existing coins vs REMOVE_THRESHOLD) ─────────────────────────

def test_coin_at_exact_remove_threshold_is_retained():
    """A coin ranked exactly REMOVE_THRESHOLD must NOT be removed."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    coin = syms[0]                           # currently rank 1

    top = make_market_cap(rerank(syms, coin, REMOVE_THRESHOLD))
    universe, _ = get_universe(top, hl_set(syms), current_universe=current)

    assert coin in universe, f"{coin} at rank {REMOVE_THRESHOLD} should be retained"


def test_coin_removed_when_rank_exceeds_remove_threshold():
    """A coin that drops just past REMOVE_THRESHOLD must be removed."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    coin = syms[0]

    top = make_market_cap(rerank(syms, coin, REMOVE_THRESHOLD + 1))
    universe, _ = get_universe(top, hl_set(syms), current_universe=current)

    assert coin not in universe, f"{coin} at rank {REMOVE_THRESHOLD + 1} should be removed"


def test_coin_not_removed_inside_remove_threshold():
    """A coin comfortably inside REMOVE_THRESHOLD is retained."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    coin = syms[0]

    top = make_market_cap(rerank(syms, coin, REMOVE_THRESHOLD - 5))
    universe, _ = get_universe(top, hl_set(syms), current_universe=current)

    assert coin in universe


def test_coin_not_listed_on_hl_is_removed():
    """A coin delisted from Hyperliquid must leave the universe immediately."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    delisted = syms[0]

    # delisted keeps its top rank but is absent from HL
    universe, _ = get_universe(make_market_cap(syms), hl_set(syms[1:]),
                               current_universe=current)

    assert delisted not in universe


# ── Addition logic (new coins vs ADD_THRESHOLD) ────────────────────────────────

def test_new_coin_added_at_exact_add_threshold():
    """A new coin ranked exactly ADD_THRESHOLD is added (boundary inclusive)."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    top = make_market_cap(rerank(syms + [new_coin], new_coin, ADD_THRESHOLD))
    universe, _ = get_universe(top, hl_set(syms + [new_coin]), current_universe=current)

    assert new_coin in universe, f"new coin at rank {ADD_THRESHOLD} should be added"


def test_new_coin_added_inside_add_threshold():
    """A new coin comfortably inside ADD_THRESHOLD is added."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    top = make_market_cap(rerank(syms + [new_coin], new_coin, ADD_THRESHOLD - 5))
    universe, _ = get_universe(top, hl_set(syms + [new_coin]), current_universe=current)

    assert new_coin in universe


def test_new_coin_not_added_just_past_add_threshold():
    """A new coin one rank past ADD_THRESHOLD sits in the buffer — must not be added."""
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    top = make_market_cap(rerank(syms + [new_coin], new_coin, ADD_THRESHOLD + 1))
    universe, _ = get_universe(top, hl_set(syms + [new_coin]), current_universe=current)

    assert new_coin not in universe, f"new coin at rank {ADD_THRESHOLD + 1} should not be added"


def test_new_coin_not_added_at_remove_threshold():
    """A new coin at REMOVE_THRESHOLD is inside the buffer, not the add zone — not added.

    Only an *existing* member is retained down to REMOVE_THRESHOLD; a coin that is
    not already in the universe must reach ADD_THRESHOLD to enter.
    """
    syms = coins(POOL)
    current = syms[:UNIVERSE_SIZE]
    new_coin = "NEW"

    top = make_market_cap(rerank(syms + [new_coin], new_coin, REMOVE_THRESHOLD))
    universe, _ = get_universe(top, hl_set(syms + [new_coin]), current_universe=current)

    assert new_coin not in universe, f"new coin at rank {REMOVE_THRESHOLD} should not be added"


# ── Symbol index ──────────────────────────────────────────────────────────────

def test_symbol_index_matches_universe():
    syms = coins(POOL)
    universe, symbol_index = get_universe(make_market_cap(syms), hl_set(syms),
                                          current_universe=None)

    assert set(symbol_index.keys()) == set(universe)
    for sym, idx in symbol_index.items():
        assert universe[idx] == sym
