# Price signal data source

Date: 2026-06-10
Status: Active

## Decision
Compute trading signals (EWMAC, breakout, Bollinger, Alpha 006/014/020) from Binance daily OHLCV data rather than Hyperliquid data.

## Context
The live trader executes on Hyperliquid, More volume is traded on Binance, so the theory is that signals based on Binance data would contain more information. 

## Alternatives considered
1. HL prices for both signals and execution (previous approach)
2. Binance prices for signals, HL prices for execution (chosen)

## Supporting analysis
- Backtest: analysis/202606_backtest_binance_vs_hl.py
- Both strategies trade on HL prices; only the signal source differs.
- Universe held constant (top-N by market cap, restricted to symbols present in both datasets).
- Date range: 2024-02-02 → latest (constrained by HL history).

## Consequences
- Signals benefit from Binance's longer price history and tighter microstructure.
- Requires syncing `daily_closes.parquet` from the binance-klines server before running the trader or backtests (`scp ev@192.168.86.138:~/projects/binance-klines/klines/processed/daily_closes.parquet data/cache/`).
- `build_daily_cache.build_binance()` removed; `build()` now reads directly from `$BINANCE_KLINES_DIR/klines/processed/daily_closes.parquet`.

## Next Review Date
2026-12-10
