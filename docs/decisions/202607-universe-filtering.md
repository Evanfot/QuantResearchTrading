# Universe filtering: true-market-cap ranking and non-crypto exclusions

Date: 2026-07-21
Status: Active

## Decision
The trading universe is the **top `UNIVERSE_SIZE` genuine cryptocurrencies by TRUE
market-cap rank, then intersected with what is tradable on Hyperliquid** (no
back-fill). Concretely:

1. **Rank by true market cap, not exchange-relative.** Rank the full CoinGecko
   market-cap list; do **not** filter to Hyperliquid-listed coins before ranking.
   Tradability (listed on Hyperliquid) is applied *after* ranking.
2. **Exclude tokens that are not genuine, independently-trending cryptocurrencies**
   before ranking, so a real coin's rank reflects its true standing:
   - fiat-pegged **stablecoins** (all `USD*` pegs, DAI, etc.);
   - **wrapped / liquid-staking derivatives** (WBTC, WETH, WSTETH, STETH, …) — they
     track an underlying already in the ranking;
   - **commodity trackers** (PAXG, XAUT — tokenized gold);
   - **tokenized funds / real-world-asset money-market instruments** (BUIDL, USYC,
     EUTBL, FIGR_HELOC, …).
   **Exchange/utility tokens (LEO, OKB, BGB, …) are NOT excluded** — they are genuine
   tokens; they simply drop out later if they aren't tradable on Hyperliquid.
3. **No back-fill.** The universe is never expanded past rank `UNIVERSE_SIZE` to
   compensate for untradable coins, so the tradable book may be smaller than
   `UNIVERSE_SIZE` and varies over time.
4. **Size for a ~50-coin traded book.** Because only ~50 of the top-80 genuine coins
   are HL-listed, set `UNIVERSE_SIZE = 80` (`ADD_THRESHOLD = 77`,
   `REMOVE_THRESHOLD = 83`, a ±3-rank hysteresis buffer).

Exclusion sets are hand-curated in `src/universe.py` and extended as new such tokens
climb the ranks.

## Context
The previous `main` ranked *relative to the HL-tradable set* (`_eligible_ranked`
filtered to `hl_universe` before ranking). A coin outside the true top-N could be
retained as long as it ranked top-N *among HL coins*, so the book held names ranked
~74–104 by true market cap (ATOM, APT, ALGO, ARB, INJ, ICP, …). They never fell out
of the universe, so they were never closed — surfacing as "held by live but not
dev," since `dev` had already moved to true-market-cap ranking. Separately, the
top-50 was diluted by stablecoins, wrapped tokens, gold trackers, and tokenized
funds (e.g. `FIGR_HELOC` at rank 9), leaving only ~29 genuine tradable coins.

## Alternatives considered
1. Keep HL-relative ranking (previous main behaviour) — rejected: lets lower-cap
   coins persist and never close.
2. True-rank + back-fill to always hit `UNIVERSE_SIZE` tradable — rejected: pulls in
   arbitrarily low-cap coins to fill slots left by untradable high-cap ones.
3. **True-rank, tradability applied after, no back-fill, size N for ~50 tradable
   (chosen).**
4. Also excluding exchange tokens (LEO/OKB/…) — rejected: they are genuine tokens;
   excluded only by tradability.
5. Auto-classifying exclusions via CoinGecko category data — deferred in favour of a
   simple hand-curated set (transparent; revisit if maintenance becomes a burden).

## Supporting analysis
- Implemented in `src/universe.py` (`_eligible_ranked`, `get_universe`, `STABLE` /
  `EXCLUDED` sets); PR #21 (branch `hotfix/universe-true-mcap-rank`).
- Validated against the `data/mkt_cap/20260715_2041.csv` snapshot + `get_hl_coins()`.
- Tradable-count vs `UNIVERSE_SIZE`: 50→35, 60→40, 70→45, **80→50**, 85→52.
- Matches the two-stage Eligible→Tradable design already live on `dev` (20b6cf6).

## Consequences
- Traded book becomes the top-80 genuine cryptos ∩ Hyperliquid ≈ 50 real coins.
- On first run after deploy, ~15 lower-true-cap positions (ATOM, APT, ALGO, ARB,
  INJ, …) leave the universe and are closed the next day via `run_live`'s
  `target_zeroes` path — a meaningful one-off rebalance; deploy at a deliberate time.
- Exclusion sets require occasional manual maintenance as new stablecoins/RWA tokens
  enter the top ranks.
- **Known coupling:** PEPE/SHIB/BONK/FLOKI are counted untradable because Hyperliquid
  names them `kPEPE`/`kSHIB`/`kBONK`/`kFLOKI` (1000×). A symbol map (separate change)
  would raise the tradable count, let `UNIVERSE_SIZE` come back down, and include
  those memecoins.

## Next Review Date
2027-01-21
