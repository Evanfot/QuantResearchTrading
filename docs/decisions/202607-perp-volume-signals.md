# Volume signals require perp volume, not spot volume

Date: 2026-07-29
Status: Active

## Decision
Source the volume-based alphas (Alpha 006, Alpha 014) from **perpetual-futures** volume, not spot volume. When feeding signals from Binance, use Binance USDT-M **futures** klines rather than Binance **spot** klines. Price-based signals (EWMAC, breakout, Bollinger, Alpha 020) are unaffected — they may come from any of the feeds since prices are near-identical.

## Context
The 2026-06 decision ([Price signal data source](202606-price-signal-data.md)) moved signals from Hyperliquid to Binance for its longer history and tighter microstructure, but implicitly assumed one Binance feed. While migrating off HL data we noticed the volume alphas behaved very differently on Binance than on HL. HL is perp-only; the Binance cache we were building was **spot**. The volume alphas exploit an open↔volume relationship that is a leverage / liquidation / funding phenomenon — it shows up in perp volume but is largely absent from spot volume.

## Supporting analysis
- Script: `analysis/20260729_alpha_decomposition/three_way.py`
- Output: `analysis/20260729_alpha_decomposition/three_way_sharpe.csv`, `volume_alpha_spot_vs_futures_vs_hl.png`
- Three-way decomposition of the same universe and window (2024+), holding execution/prices constant and varying only the OHLCV feed: Binance spot vs Binance futures vs Hyperliquid.

Sharpe by feed × config, 2024+:

| Feed | Price | alpha006 | alpha014 | vol_both (a006+a014) | Combined |
|------|-------|----------|----------|----------------------|----------|
| Binance spot     | 3.68 | 0.20 | 0.15 | **0.16** | 3.78 |
| Binance futures  | 3.39 | 0.88 | 0.96 | **0.95** | 4.05 |
| Hyperliquid      | 3.69 | 1.26 | 1.81 | **1.50** | 4.44 |

Key observations:
- Price alphas are essentially identical across feeds (~3.4–3.7) — confirms prices agree and the test is clean.
- The volume sleeve is effectively dead on spot (0.16) but strongly positive on both perp feeds (futures 0.95, HL 1.50).
- Combined Sharpe rises monotonically spot → futures → HL, driven entirely by the volume sleeve.
- HL (native perp) edges out Binance futures on the volume sleeve, but Binance futures captures the bulk of the effect and carries the longer, cleaner history — so it is the right source for the Binance-fed pipeline.

## Alternatives considered
1. Binance **spot** OHLCV for all signals (previous binance-klines default) — volume alphas do not fire.
2. Binance **futures** OHLCV for all signals (chosen) — recovers the volume-alpha edge while keeping Binance's history/microstructure benefits.
3. Split feeds: price alphas from spot, volume alphas from futures — unnecessary complexity; futures serves both with no material price-signal loss.

## Consequences
- `binance-klines` must switch its collection/cache from spot to USDT-M perpetual futures klines (stream + daily download + `daily_closes.parquet` build). This is the follow-up work.
- The trader's Binance cache (`daily_closes.parquet`) will carry perp OHLCV; downstream signal code is unchanged since it only sees an OHLCV frame.
- Perp universe differs slightly from spot (some spot pairs have no perp, and vice versa); the trading universe intersection must be re-derived against the futures symbol set.
- Watch for perp-specific data quirks: funding-driven volume spikes, contract listing/delisting gaps, and any symbol remaps between spot and futures tickers.

## Next Review Date
2027-01-29
