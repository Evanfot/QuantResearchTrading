# %%
"""Compare two weighting schemes on the SAME combined-alpha signal.

  * "Combined alpha" — current approach: correlation-shrunk inverse-variance
    solve + vol scaling  (src/backtester/full_backtest.run_backtest)
  * "MVO max-Sharpe"  — PyPortfolioOpt max-Sharpe efficient frontier + Kelly
    leverage             (src/backtester/full_backtest_mvo.run_backtest_mvo)

Both consume the identical period-by-period ``mu`` matrix, so the difference in
performance is attributable purely to how positions are derived from the signal.
"""
import sys
from pathlib import Path
import os

root = Path().resolve()
while not (root / "src").exists():
    root = root.parent
os.chdir(root)
sys.path.insert(0, str(root))
print("Working directory:", root)

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from scripts.meta_data import get_hl_coins
from scripts.mkt_cap_data import get_latest_market_cap
from src.backtester.full_backtest import run_backtest, StrategyConfig
from src.backtester.full_backtest_mvo import run_backtest_mvo, MVOConfig
from src.signal import ewmac, breakout, scaled_bollinger, alpha006, alpha014, alpha020
from src.data import (
    get_hyperliquid_trading_universe,
    db_path,
    get_ohlcv,
    get_final_pricing,
    load_ohlcv_for_alphas,
)

# %%
START_DATE = "2025-01-01"  # restrict backtest window (None = all data)

# ── Data ────────────────────────────────────────────────────────────────────
top = get_latest_market_cap()
hl = get_hl_coins()
universe, symbol_index = get_hyperliquid_trading_universe(top, hl)

conn = duckdb.connect(db_path)
hyperliquid_prices = get_ohlcv(conn)
conn.close()

latest_view = pd.read_csv("data/snapshots/mids.csv", index_col=0)
prices, returns_adj = get_final_pricing(hyperliquid_prices, universe, latest_view)

config = StrategyConfig()

# ── Signals (combined alpha) ────────────────────────────────────────────────
ewmac_f = ewmac(returns_adj, config.ewmac_fast)
breakout_f = breakout(prices, config.breakout_window)
bollinger_f = scaled_bollinger(prices, param=config.bollinger_window, scalar=1)

o, h, l, c_alpha, v = load_ohlcv_for_alphas(universe)
o = o.reindex(index=prices.index, columns=prices.columns)
h = h.reindex(index=prices.index, columns=prices.columns)
l = l.reindex(index=prices.index, columns=prices.columns)
v = v.reindex(index=prices.index, columns=prices.columns)
c_alpha = c_alpha.reindex(index=prices.index, columns=prices.columns)
r_alpha = np.log(c_alpha).diff()

alpha006_f = alpha006(o, v)
alpha014_f = alpha014(o, v, r_alpha)
alpha020_f = alpha020(o, h, l, c_alpha)

mu = np.mean(
    [bollinger_f, ewmac_f, breakout_f, alpha006_f, alpha014_f, alpha020_f], axis=0
)
vo = prices.pct_change().ewm(com=config.vo_window, min_periods=20).std().values
cor = returns_adj.ewm(com=config.correlation, min_periods=config.correlation).corr()

if START_DATE:
    mask = prices.index >= pd.Timestamp(START_DATE)
    prices = prices.loc[mask]
    mu = mu[mask]
    vo = vo[mask]
    cor = cor.loc[prices.index]

# ── Backtests ───────────────────────────────────────────────────────────────
# %%
COST_BPS = 4.5      # per-side transaction cost on turnover (basis points)
TARGET_VOL = None   # set after running the baseline; both books levered to it


def net_curve(port, lev=1.0, cost_bps=0.0):
    """Return a cost-adjusted, leverage-scaled daily return series + turnover.

    Leverage scales both returns and turnover linearly (positions ∝ lev); cost
    is ``cost_bps`` applied to the levered turnover fraction of NAV.
    """
    nav = port.nav
    gross_ret = nav.pct_change().fillna(0.0)
    turn_frac = port.cashposition.diff().abs().sum(axis=1).div(nav.shift(1)).fillna(0.0)
    lev_ret = lev * gross_ret
    lev_turn = lev * turn_frac
    cost = lev_turn * (cost_bps / 1e4)
    net_ret = lev_ret - cost
    return net_ret, lev_turn, cost


def summarise(port, label, lev=1.0, cost_bps=0.0):
    net_ret, lev_turn, cost = net_curve(port, lev, cost_bps)
    net_nav = (1 + net_ret).cumprod()
    ann_ret = net_nav.iloc[-1] ** (365 / len(net_ret)) - 1
    ann_vol = net_ret.std() * np.sqrt(365)
    sharpe = ann_ret / ann_vol if ann_vol else np.nan
    max_dd = ((net_nav / net_nav.cummax()) - 1).min()
    turnover = lev_turn.mean()
    cost_drag = cost.sum() / len(cost) * 365  # ~annualised cost drag
    print(
        f"{label:30s}  lev={lev:4.2f}x  ann_ret={ann_ret:8.1%}  vol={ann_vol:6.1%}  "
        f"sharpe={sharpe:6.2f}  max_dd={max_dd:7.1%}  "
        f"turnover={turnover:5.2f}  cost_drag={cost_drag:5.1%}"
    )
    return dict(net_nav=net_nav, ann_ret=ann_ret, ann_vol=ann_vol, sharpe=sharpe,
                max_dd=max_dd, turnover=turnover, lev=lev)


print(f"\nUniverse: {prices.shape[1]} assets   Window: "
      f"{prices.index[0].date()} → {prices.index[-1].date()} ({len(prices)} rows)")
print(f"Costs: {COST_BPS}bps on turnover   |   Risk: equal-vol comparison\n")

ports = {}
print("Running combined-alpha (current) backtest...")
ports["Combined alpha (current)"] = run_backtest(prices, mu, vo, cor, config)

print("Running MVO max-Sharpe + Kelly backtest...")
ports["MVO max-Sharpe (Kelly)"] = run_backtest_mvo(
    prices, mu, MVOConfig(sizing="kelly")
)

print("Running MVO max-Sharpe + vol-target backtest...")
ports["MVO max-Sharpe (vol-target)"] = run_backtest_mvo(
    prices, mu, MVOConfig(sizing="vol_target", target_vol_daily=0.022)
)

# Equal-risk: lever each book to the baseline's *gross* realised vol.
base_gross_vol = ports["Combined alpha (current)"].nav.pct_change().std() * np.sqrt(365)
TARGET_VOL = base_gross_vol
lev = {}
for label, port in ports.items():
    gross_vol = port.nav.pct_change().std() * np.sqrt(365)
    lev[label] = TARGET_VOL / gross_vol

print(f"\n── Performance (net of {COST_BPS}bps, levered to {TARGET_VOL:.1%} vol) ──")
metrics = {label: summarise(port, label, lev=lev[label], cost_bps=COST_BPS)
           for label, port in ports.items()}

# ── Plot ────────────────────────────────────────────────────────────────────
# %%
STYLES = {
    "Combined alpha (current)": ("tab:blue", "-", 1.6),
    "MVO max-Sharpe (Kelly)": ("tab:red", "--", 1.4),
    "MVO max-Sharpe (vol-target)": ("tab:green", "-", 1.6),
}

fig, axes = plt.subplots(
    2, 1, figsize=(13, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1.5]}
)

ax = axes[0]
for label, m in metrics.items():
    nav = m["net_nav"] / m["net_nav"].iloc[0]
    color, ls, lw = STYLES[label]
    ax.plot(nav.index, nav.values, label=label, color=color, linestyle=ls, linewidth=lw)
ax.set_title(
    f"Weighting scheme comparison — equal risk ({TARGET_VOL:.0%} vol), "
    f"net of {COST_BPS}bps (same combined-alpha μ)"
)
ax.set_ylabel("NAV (normalised)")
ax.legend(fontsize=10)
ax.grid(True, alpha=0.25)

ax = axes[1]
for label, m in metrics.items():
    nav = m["net_nav"]
    dd = (nav / nav.cummax()) - 1
    color, ls, lw = STYLES[label]
    ax.fill_between(dd.index, dd.values, 0, alpha=0.12, color=color)
    ax.plot(dd.index, dd.values, color=color, linestyle=ls, linewidth=0.9)
ax.set_title("Drawdowns")
ax.set_ylabel("Drawdown")
ax.grid(True, alpha=0.25)

for ax in axes:
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

fig.autofmt_xdate()
plt.tight_layout()
out_png = "analysis/mvo_vs_combined_backtest.png"
plt.savefig(out_png, dpi=150)
print(f"\nSaved plot → {out_png}")
plt.show()
# %%
