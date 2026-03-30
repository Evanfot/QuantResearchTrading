# %%
# Alpha Factor Backtester
# Implements a subset of the 101 Formulaic Alphas (Kakushadze 2015) using
# our Hyperliquid DuckDB OHLCV data and evaluates each with IC / ICIR.
#
# Reference: 03_101_formulaic_alphas.ipynb

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
from scipy.stats import spearmanr

from scripts.meta_data import get_hl_coins
from scripts.mkt_cap_data import get_latest_market_cap
from src.main import get_hyperliquid_trading_universe, db_path


# ─── Data ────────────────────────────────────────────────────────────────────

def load_ohlcv(conn, universe):
    """Load daily OHLCV from DuckDB and pivot to (date × symbol) DataFrames."""
    symbols_sql = ", ".join(f"'{s}/USDC:USDC'" for s in universe)
    df = conn.execute(f"""
        SELECT datetime AS date, symbol,
               open, high, low, close, volume
        FROM hyperliquid_1d
        WHERE symbol IN ({symbols_sql})
        ORDER BY datetime
    """).df()

    df["symbol"] = df["symbol"].str.replace("/USDC:USDC", "", regex=False)

    def pivot(col):
        return df.pivot(index="date", columns="symbol", values=col)

    o = pivot("open")
    h = pivot("high")
    l = pivot("low")
    c = pivot("close")
    v = pivot("volume")
    vwap = (o + h + l + c) / 4
    adv20 = v.rolling(20).mean()

    # Align all to symbols present across all frames
    cols = c.columns
    return o[cols], h[cols], l[cols], c[cols], v[cols], vwap[cols], adv20[cols]


# ─── Cross-section operators ──────────────────────────────────────────────────

def rank(df):
    return df.rank(axis=1, pct=True)

def scale(df):
    return df.div(df.abs().sum(axis=1), axis=0)

def sign(df):
    return np.sign(df)

def log(df):
    return np.log1p(df)

def power(df, exp):
    return df.pow(exp)


# ─── Time-series operators ────────────────────────────────────────────────────

def ts_lag(df, t=1):
    return df.shift(t)

def ts_delta(df, t=1):
    return df.diff(t)

def ts_sum(df, window=10):
    return df.rolling(window).sum()

def ts_mean(df, window=10):
    return df.rolling(window).mean()

def ts_std(df, window=10):
    return df.rolling(window).std()

def ts_min(df, window=10):
    return df.rolling(window).min()

def ts_max(df, window=10):
    return df.rolling(window).max()

def ts_rank(df, window=10):
    return df.rolling(window).apply(lambda x: x.rank().iloc[-1], raw=False)

def ts_argmin(df, window=10):
    return df.rolling(window).apply(np.argmin, raw=True).add(1)

def ts_argmax(df, window=10):
    return df.rolling(window).apply(np.argmax, raw=True).add(1)

def ts_corr(x, y, window=10):
    return x.rolling(window).corr(y)

def ts_cov(x, y, window=10):
    return x.rolling(window).cov(y)

def ts_weighted_mean(df, period=10):
    """Linearly decaying weighted moving average (no TA-Lib dependency)."""
    weights = np.arange(1, period + 1, dtype=float)
    weights /= weights.sum()
    return df.rolling(period).apply(lambda x: np.dot(x, weights), raw=True)


# ─── Alphas ───────────────────────────────────────────────────────────────────
# Skipped: alphas requiring industry/sector neutralisation (001 partial, 013,
#          017 partial) or market-cap data (100).  VWAP is approximated as
#          (O + H + L + C) / 4.

def alpha002(o, c, v):
    """-ts_corr(rank(ts_delta(log(volume), 2)), rank((close - open) / open), 6)"""
    s1 = rank(ts_delta(log(v), 2))
    s2 = rank((c / o) - 1)
    return -ts_corr(s1, s2, 6).replace([-np.inf, np.inf], np.nan)

def alpha003(o, v):
    """-ts_corr(rank(open), rank(volume), 10)"""
    return -ts_corr(rank(o), rank(v), 10).replace([-np.inf, np.inf], np.nan)

def alpha004(l):
    """-ts_rank(rank(low), 9)"""
    return -ts_rank(rank(l), 9)

def alpha006(o, v):
    """-ts_corr(open, volume, 10)"""
    return -ts_corr(o, v, 10).replace([-np.inf, np.inf], np.nan)

def alpha007(c, v, adv20):
    """(adv20 < volume) ? -ts_rank(abs(ts_delta(close,7)),60)*sign(ts_delta(close,7)) : -1"""
    delta7 = ts_delta(c, 7)
    signal = -ts_rank(delta7.abs(), 60).mul(sign(delta7))
    return signal.where(adv20 < v, -1)

def alpha008(o, r):
    """-rank(ts_sum(open,5)*ts_sum(returns,5) - ts_lag(ts_sum(open,5)*ts_sum(returns,5),10))"""
    return -rank((ts_sum(o, 5) * ts_sum(r, 5)) -
                  ts_lag(ts_sum(o, 5) * ts_sum(r, 5), 10))

def alpha009(c):
    """ts_min(ts_delta(close,1),5)>0 ? delta : (ts_max(delta,5)<0 ? delta : -delta)"""
    d = ts_delta(c, 1)
    return d.where(ts_min(d, 5) > 0,
                   d.where(ts_max(d, 5) < 0, -d))

def alpha010(c):
    """rank(ts_min(ts_delta(close,1),4)>0 ? delta : (ts_max(delta,4)<0 ? delta : -delta))"""
    d = ts_delta(c, 1)
    return rank(d.where(ts_min(d, 4) > 0,
                        d.where(ts_max(d, 4) < 0, -d)))

def alpha011(c, vwap, v):
    """(rank(ts_max(vwap-close,3)) + rank(ts_min(vwap-close,3))) * rank(ts_delta(volume,3))"""
    spread = vwap - c
    return (rank(ts_max(spread, 3)) + rank(ts_min(spread, 3))) * rank(ts_delta(v, 3))

def alpha012(v, c):
    """sign(ts_delta(volume,1)) * -ts_delta(close,1)"""
    return sign(ts_delta(v, 1)) * (-ts_delta(c, 1))

def alpha013(c, v):
    """-rank(ts_cov(rank(close), rank(volume), 5))"""
    return -rank(ts_cov(rank(c), rank(v), 5))

def alpha014(o, v, r):
    """-rank(ts_delta(returns,3)) * ts_corr(open, volume, 10)"""
    return (-rank(ts_delta(r, 3))
            .mul(ts_corr(o, v, 10).replace([-np.inf, np.inf], np.nan)))

def alpha015(h, v):
    """-ts_sum(rank(ts_corr(rank(high), rank(volume), 3)), 3)"""
    return -ts_sum(rank(ts_corr(rank(h), rank(v), 3)
                        .replace([-np.inf, np.inf], np.nan)), 3)

def alpha016(h, v):
    """-rank(ts_cov(rank(high), rank(volume), 5))"""
    return -rank(ts_cov(rank(h), rank(v), 5))

def alpha017(c, v):
    """-rank(ts_rank(close,10)) * rank(ts_delta(ts_delta(close,1),1)) * rank(ts_rank(vol/adv20,5))"""
    adv20 = ts_mean(v, 20)
    return (-rank(ts_rank(c, 10))
            .mul(rank(ts_delta(ts_delta(c, 1), 1)))
            .mul(rank(ts_rank(v.div(adv20), 5))))

def alpha018(o, c):
    """-rank(ts_std(abs(close-open),5) + (close-open) + ts_corr(close,open,10))"""
    return -rank(ts_std((c - o).abs(), 5)
                 .add(c - o)
                 .add(ts_corr(c, o, 10).replace([-np.inf, np.inf], np.nan)))

def alpha020(o, h, l, c):
    """-rank(open-ts_lag(high,1)) * rank(open-ts_lag(close,1)) * rank(open-ts_lag(low,1))"""
    return (-rank(o - ts_lag(h, 1))
            .mul(rank(o - ts_lag(c, 1)))
            .mul(rank(o - ts_lag(l, 1))))

def alpha022(h, c, v):
    """-ts_delta(ts_corr(high,volume,5),5) * rank(ts_std(close,20))"""
    return (-ts_delta(ts_corr(h, v, 5).replace([-np.inf, np.inf], np.nan), 5)
            .mul(rank(ts_std(c, 20))))

def alpha023(h):
    """ts_mean(high,20) < high ? -ts_delta(high,2) : 0"""
    return ts_delta(h, 2).mul(-1).where(ts_mean(h, 20) < h, 0)

def alpha101(o, h, l, c):
    """(close - open) / (high - low + 0.001)"""
    return (c - o) / (h - l + 0.001)


ALPHAS = {
    "alpha002": lambda o, h, l, c, v, vwap, adv20, r: alpha002(o, c, v),
    "alpha003": lambda o, h, l, c, v, vwap, adv20, r: alpha003(o, v),
    "alpha004": lambda o, h, l, c, v, vwap, adv20, r: alpha004(l),
    "alpha006": lambda o, h, l, c, v, vwap, adv20, r: alpha006(o, v),
    "alpha007": lambda o, h, l, c, v, vwap, adv20, r: alpha007(c, v, adv20),
    "alpha008": lambda o, h, l, c, v, vwap, adv20, r: alpha008(o, r),
    "alpha009": lambda o, h, l, c, v, vwap, adv20, r: alpha009(c),
    "alpha010": lambda o, h, l, c, v, vwap, adv20, r: alpha010(c),
    "alpha011": lambda o, h, l, c, v, vwap, adv20, r: alpha011(c, vwap, v),
    "alpha012": lambda o, h, l, c, v, vwap, adv20, r: alpha012(v, c),
    "alpha013": lambda o, h, l, c, v, vwap, adv20, r: alpha013(c, v),
    "alpha014": lambda o, h, l, c, v, vwap, adv20, r: alpha014(o, v, r),
    "alpha015": lambda o, h, l, c, v, vwap, adv20, r: alpha015(h, v),
    "alpha016": lambda o, h, l, c, v, vwap, adv20, r: alpha016(h, v),
    "alpha017": lambda o, h, l, c, v, vwap, adv20, r: alpha017(c, v),
    "alpha018": lambda o, h, l, c, v, vwap, adv20, r: alpha018(o, c),
    "alpha020": lambda o, h, l, c, v, vwap, adv20, r: alpha020(o, h, l, c),
    "alpha022": lambda o, h, l, c, v, vwap, adv20, r: alpha022(h, c, v),
    "alpha023": lambda o, h, l, c, v, vwap, adv20, r: alpha023(h),
    "alpha101": lambda o, h, l, c, v, vwap, adv20, r: alpha101(o, h, l, c),
}


# ─── Evaluation ──────────────────────────────────────────────────────────────

def compute_daily_ic(alpha_df, ret_fwd):
    """Spearman rank IC between alpha and next-day return, computed per day."""
    ics = []
    common_dates = alpha_df.index.intersection(ret_fwd.index)
    for date in common_dates:
        a = alpha_df.loc[date].dropna()
        r = ret_fwd.loc[date].dropna()
        common = a.index.intersection(r.index)
        if len(common) < 5:
            continue
        ic, _ = spearmanr(a[common], r[common])
        ics.append(ic)
    return pd.Series(ics)

def summarise_ic(ics, name):
    mean_ic   = ics.mean()
    std_ic    = ics.std()
    ir        = mean_ic / std_ic if std_ic > 0 else np.nan
    hit_rate  = (ics > 0).mean()
    t_stat    = mean_ic / (std_ic / np.sqrt(len(ics))) if std_ic > 0 else np.nan
    return {
        "alpha":    name,
        "mean_ic":  round(mean_ic, 4),
        "std_ic":   round(std_ic, 4),
        "ir":       round(ir, 3),
        "hit_rate": round(hit_rate, 3),
        "t_stat":   round(t_stat, 2),
        "n_days":   len(ics),
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

# %%
top = get_latest_market_cap()
hl  = get_hl_coins()
universe, _ = get_hyperliquid_trading_universe(top, hl)

conn = duckdb.connect(db_path)
o, h, l, c, v, vwap, adv20 = load_ohlcv(conn, universe)
conn.close()

r       = np.log(c).diff()                  # log returns
ret_fwd = c.shift(-1) / c - 1               # next-day return (evaluation target)

# %%
print(f"Universe: {len(universe)} coins | "
      f"Date range: {c.index[0].date()} → {c.index[-1].date()} | "
      f"{len(c)} days")
print()

results = []
for name, fn in ALPHAS.items():
    try:
        alpha_df = fn(o, h, l, c, v, vwap, adv20, r)
        ics = compute_daily_ic(alpha_df, ret_fwd)
        row = summarise_ic(ics, name)
        results.append(row)
        print(f"{name:12s}  mean_ic={row['mean_ic']:+.4f}  ir={row['ir']:+.3f}  "
              f"hit={row['hit_rate']:.1%}  t={row['t_stat']:+.2f}")
    except Exception as e:
        print(f"{name:12s}  ERROR: {e}")

# %%
print()
df = pd.DataFrame(results).sort_values("ir", ascending=False)
print("=== Ranked by Information Ratio ===")
print(df.to_string(index=False))
