# %%
# Backtest: Alpha014 and Alpha020 vs baseline (bollinger + ewmac + breakout)
#
# Alpha014: -rank(ts_delta(returns, 3)) * ts_corr(open, volume, 10)
#   — mean-reversion signal modulated by open/volume correlation
#
# Alpha020: -rank(open-lag(high,1)) * rank(open-lag(close,1)) * rank(open-lag(low,1))
#   — gap-open signal: how today's open sits relative to yesterday's range
#
# Backtests:
#   1. Baseline       — bollinger + ewmac + breakout
#   2. Alpha014 only
#   3. Alpha020 only
#   4. Combined       — baseline + alpha014 + alpha020

import sys
from pathlib import Path
import os

root = Path().resolve()
while not (root / "src").exists():
    root = root.parent
os.chdir(root)
sys.path.insert(0, str(root))

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from scripts.meta_data import get_hl_coins
from scripts.mkt_cap_data import get_latest_market_cap
from src.main import get_hyperliquid_trading_universe, db_path, get_ohlcv, get_final_pricing
from src.backtester.full_backtest import run_backtest, StrategyConfig
from src.signal import ewmac, breakout, alpha014, alpha020
from src.main import load_ohlcv_for_alphas

# ── Data ──────────────────────────────────────────────────────────────────────
# %%
top = get_latest_market_cap()
hl = get_hl_coins()
universe, symbol_index = get_hyperliquid_trading_universe(top, hl)

conn = duckdb.connect(db_path)
hyperliquid_prices = get_ohlcv(conn)
conn.close()

latest_view = pd.read_csv('data/snapshots/mids.csv', index_col=0)
prices, returns_adj = get_final_pricing(hyperliquid_prices, universe, latest_view)
config = StrategyConfig()

# ── Baseline signals ──────────────────────────────────────────────────────────
# %%
ewmac_f     = ewmac(returns_adj, config.ewmac_fast)
breakout_f  = breakout(prices, config.breakout_window)

ma  = prices.rolling(config.bollinger_window).mean()
std = prices.rolling(config.bollinger_window).std()
bollinger_f = ((prices - ma) / std).clip(-10, 10).values

# ── Alpha signals ─────────────────────────────────────────────────────────────
# %%
o, h, l, c, v = load_ohlcv_for_alphas(universe)
o = o.reindex(index=prices.index, columns=prices.columns)
h = h.reindex(index=prices.index, columns=prices.columns)
l = l.reindex(index=prices.index, columns=prices.columns)
c = c.reindex(index=prices.index, columns=prices.columns)
v = v.reindex(index=prices.index, columns=prices.columns)
r = np.log(c).diff()

alpha014_f = alpha014(o, v, r)
alpha020_f = alpha020(o, h, l, c)

# ── Backtest ──────────────────────────────────────────────────────────────────
# %%
def summarise(port, label):
    nav = port.nav
    ret = nav.pct_change().dropna()
    total    = nav.iloc[-1] / nav.iloc[0] - 1
    ann_ret  = (1 + total) ** (252 / len(ret)) - 1
    ann_vol  = ret.std() * np.sqrt(252)
    sharpe   = ann_ret / ann_vol
    max_dd   = ((nav / nav.cummax()) - 1).min()
    print(f'{label:45s}  ann_ret={ann_ret:.1%}  vol={ann_vol:.1%}  sharpe={sharpe:.2f}  max_dd={max_dd:.1%}')

vo  = prices.pct_change().ewm(com=config.vo_window, min_periods=20).std().values
cor = returns_adj.ewm(com=config.correlation, min_periods=config.correlation).corr()

baseline_mu  = np.mean([bollinger_f, ewmac_f, breakout_f], axis=0)
combined_mu  = np.mean([bollinger_f, ewmac_f, breakout_f, alpha014_f, alpha020_f], axis=0)

experiments = {
    'Baseline (boll + ewmac + breakout)': baseline_mu,
    'Alpha014 only':                       alpha014_f,
    'Alpha020 only':                       alpha020_f,
    'Combined (baseline + α014 + α020)':   combined_mu,
}

# %%
results = {}
for label, mu in experiments.items():
    port = run_backtest(prices, mu, vo, cor, config)
    summarise(port, label)
    results[label] = port

# ── Plots ─────────────────────────────────────────────────────────────────────
# %%
STYLES = {
    'Baseline (boll + ewmac + breakout)': ('tab:blue',   '-',  1.5),
    'Alpha014 only':                       ('tab:orange', '--', 1.0),
    'Alpha020 only':                       ('tab:green',  '--', 1.0),
    'Combined (baseline + α014 + α020)':   ('tab:red',    '-',  1.5),
}

fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True,
                         gridspec_kw={'height_ratios': [3, 1.5]})

# Equity curves
ax = axes[0]
for label, port in results.items():
    nav = port.nav / port.nav.iloc[0]
    color, ls, lw = STYLES[label]
    ax.plot(nav.index, nav.values, label=label, color=color, linestyle=ls, linewidth=lw)
ax.set_title('Alpha014 & Alpha020 — Equity Curves')
ax.set_ylabel('NAV (normalised)')
ax.legend(fontsize=9)
ax.grid(True, alpha=0.25)

# Drawdowns
ax = axes[1]
for label, port in results.items():
    nav = port.nav
    dd = (nav / nav.cummax()) - 1
    color, ls, lw = STYLES[label]
    ax.fill_between(dd.index, dd.values, 0, alpha=0.12, color=color)
    ax.plot(dd.index, dd.values, color=color, linestyle=ls, linewidth=0.8)
ax.set_title('Drawdowns')
ax.set_ylabel('Drawdown')
ax.grid(True, alpha=0.25)

for ax in axes:
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))

fig.autofmt_xdate()
plt.tight_layout()
plt.savefig('analysis/alpha_014_020_backtest.png', dpi=150)
plt.show()
