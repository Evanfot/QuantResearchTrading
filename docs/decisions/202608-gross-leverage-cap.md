# Hard gross-leverage cap on live sizing: 3.7x

Date: 2026-08-14
Status: Active

## Decision
`run_live`'s target-weight book is capped at **`max_gross_leverage = 3.7x`**
(`StrategyConfig.max_gross_leverage`, `src/backtester/full_backtest.py`), enforced by
`cap_gross_leverage` (`src/execution.py`): if `sum(|target_weight|)` exceeds the cap,
every weight is scaled down uniformly (relative sizing between assets preserved)
until gross leverage equals the cap exactly.

3.7x was back-solved from live margin mechanics, not chosen as a round number:
- **IM headroom.** The account's blended per-asset leverage-tier mix implies
  `initial_margin_used ≈ gross_leverage / 6.24`. To keep IM usage under ~60% of
  equity (so new/increasing orders always have room, instead of hitting
  `withdrawable = $0`): `gross_leverage ≤ ~3.7x`.
- **Liquidation buffer.** The same mix implies
  `maintenance_margin_used ≈ gross_leverage / 12.5`. 3.7x keeps the maintenance-margin
  ratio around ~28% (vs. the ~50% observed at the 6.3x incident level) — real cushion
  before liquidation risk becomes live.
- **Backtest cross-check.** The strategy's own long-run average gross leverage at the
  current `weight_multiplier=0.0225`, backtested 2025-01-01→2026-08-03 on Hyperliquid
  data, is ~2.43x. 3.7x sits meaningfully above that average (~1.5x it), so the cap
  acts as a tail-risk backstop that rarely binds in a normal vol regime, rather than a
  second vol target that fights the primary one on ordinary days.

## Context
On 2026-08-12/13 the live mainnet account hit sustained `INSUFFICIENT_MARGIN`
rejections for 24+ hours (`INJ`, `CAKE`, `CRV`, `DASH`, `LIT`, `ARB`, …) despite
two-phase reduce-then-increase submission (#24) working correctly. Diagnosis via
`exchange_state` snapshot: `totalNtlPos` $30,038 vs. `accountValue` $4,763 — **6.3x
gross leverage**, `totalMarginUsed` already exceeding equity, `withdrawable: $0`.

An initial hotfix (#25) cut `weight_multiplier` 0.025→0.0225 (~10%), on the
hypothesis that live sizing was running ~2.5x hotter than the strategy's backtested
scale. That cut alone was not the root cause fix: comparing `logs/intent_mainnet.jsonl`
history showed gross **target** leverage (the fresh number the sizing formula
produces each day, before any execution) climbing steadily all year — ~1.5–3x in
Jan–Mar 2026, ~4–5x by Apr–Jun, **5.2–6.8x consistently since mid-July**, while
account equity was flat over the same recent stretch. Cross-referencing mean
`vol_1d` across the universe over the same window showed realized vol compressing
from ~79–108% annualized (Jan–Feb) to ~54% (mid-Aug) — nearly halved.

Root cause: `compute_strategy`'s sizing (`target_pos = position_multiplier *
risk_position / expected_vo`) has `expected_vo` in the denominator with **no floor
and no gross-leverage ceiling anywhere in the risk-parity path**. As realized vol
compressed through 2026, the same signal conviction mechanically produced
ever-larger positions — a textbook vol-targeting leverage-creep failure mode. The
`weight_multiplier` cut only rescaled the same unbounded curve down by a constant
10%; it does not stop leverage from continuing to climb if vol keeps compressing (or
recur once it does again in some future low-vol stretch).

## Alternatives considered
1. **Keep hand-tuning `weight_multiplier` reactively** — rejected: it's a constant
   multiplier on an unbounded 1/vol curve, so it needs re-tuning every time the vol
   regime shifts; it does not fix the structural gap.
2. **Per-asset weight cap** (tested at 30% during the MVO comparison work) —
   rejected as the primary fix: the account's low-cap alts (CAKE, DASH, INJ, LIT,
   ARB — the coins actually starved during the incident) sit at far below 30%
   individually; the leverage bloat is diffuse across many small-to-medium
   offsetting positions, not a few oversized single-name bets. May still be worth
   layering on later if those specific coins keep getting squeezed first even under
   the gross cap.
3. **Cap inside `compute_strategy`/`run_backtest` (shared engine)** — rejected for
   this pass: `run_backtest`'s `cashposition` is a $-target scaled by
   `position_multiplier` against a fixed $1e3 baseline (and, separately, does not
   rescale to the book's own compounding equity each period, unlike
   `run_backtest_mvo` — a distinct bug, not fixed here), while live's `target_weight`
   is `weight_multiplier`-scaled against real-time equity. The two are not
   expressible under one threshold without first reconciling that scale mismatch.
   Enforcing the cap directly on `run_live`'s `target_weights` (the exact quantity
   that maps 1:1 to real notional/equity on the exchange) protects live capital
   immediately without touching the shared backtest engine other analyses depend on.
4. **Switch to MVO sizing (`feature/mvo`) instead of fixing risk-parity** —
   rejected for now: refreshed `analysis/20260814_mvo_vs_rp_hl_data/` comparison
   (Hyperliquid data, since the Binance-klines path `feature/mvo`/`dev` depends on
   is currently broken) showed MVO needs **more**, not less, gross leverage than
   risk parity for the same realized vol (~2.77–2.80x vs. risk parity's 2.43x at the
   live-equivalent scale) — an unconstrained max-Sharpe optimizer concentrates into
   correlated bets that need *more* gross notional per unit of portfolio vol, the
   same "large offsetting positions" problem relocated, not removed.  MVO's own
   `max_gross_leverage` config parameter was also left at its loose default (10.0)
   in that comparison, so it wouldn't have prevented this incident either — the gap
   is the missing cap, not the choice of sizing scheme.

## Supporting analysis
- Live gross target leverage time series and mean `vol_1d` compression, both
  computed from `logs/intent_mainnet.jsonl` (252 daily entries, Jan 2026–present).
- `exchange_state_20260813_213023.json` (`temp/`) — the incident-level margin
  snapshot the 6.24x / 12.5x ratios above are derived from.
- `analysis/20260814_mvo_vs_rp_hl_data/mvo_vs_combined_backtest_hl.png` +
  `mvo_vs_rp_stats_hl.csv` — refreshed risk-parity vs. MVO comparison on Hyperliquid
  data, used to rule out "switch to MVO" as a substitute fix.
- Implemented in `cap_gross_leverage` (`src/execution.py`), wired into `run_live`
  (`src/main.py`); regression tests in `tests/test_gross_leverage_cap.py`.
- Hotfix PR #25 (`weight_multiplier` 0.025→0.0225) — precursor fix, insufficient
  alone; superseded as the primary control by this cap.

## Consequences
- On a day the uncapped book would exceed 3.7x gross (as it has consistently since
  mid-July 2026), the whole target book is scaled down uniformly — realized vol on
  those days will run below the strategy's nominal target until the vol regime
  normalizes or `weight_multiplier` is retuned.
- `run_backtest` (and therefore any analysis built on it, including the MVO
  comparison above) does **not** enforce this cap — future backtests will keep
  overstating deployable leverage relative to what live will actually run whenever
  the uncapped book would exceed 3.7x. Worth a follow-up to bring `run_backtest`
  in line, once its separate compounding/weight-scale inconsistency vs.
  `run_backtest_mvo` is reconciled.
- The cap is portfolio-level; it does not by itself prevent specific low-leverage-tier
  coins (CAKE, DASH, INJ, LIT, ARB) from being squeezed out first if gross leverage
  is still binding-adjacent. Revisit alternative 2 (per-asset cap) if that pattern
  persists post-deploy.

## Next Review Date
2027-02-14
