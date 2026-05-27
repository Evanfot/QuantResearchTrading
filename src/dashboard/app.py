#!/usr/bin/env python3
"""
Portfolio dashboard — run with:
    python -m src.dashboard.app          (default port 8050)
    DASHBOARD_PORT=8080 python -m src.dashboard.app
"""
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
import os
os.chdir(root)

import datetime as dt
import numpy as np
import pandas as pd
from http.server import BaseHTTPRequestHandler, HTTPServer

from scripts.exchange_state import read_latest_exchange_state
from src.loggers.intent_logger import IntentLogger
from src.positions.position_rebuilder import PositionRebuilder
from src.state.strategy_state import load_state
from src.config import TRADING_ENV

intent_logger = IntentLogger(f"logs/intent_{TRADING_ENV}.jsonl")

REFRESH_SECONDS = 30

# ---------------------------------------------------------------------------
# Scheduling constants — must mirror src/main.py
# ---------------------------------------------------------------------------
DATA_HOUR_UTC               = 0
DATA_MINUTE_UTC             = 1
MKT_CAP_HOUR_UTC            = 0
MKT_CAP_MINUTE_UTC          = 5
TRADING_INTENT_HOUR_UTC     = 0
TRADING_INTENT_MINUTE_UTC   = 1
TRADING_EXEC_HOUR_UTC       = 2
TRADING_EXEC_INTERVAL_MINUTES = 30
POSITION_CHECK_INTERVAL_HOURS = 1


def _is_data_due(now, state):
    last_ms = state.get("last_data_run_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return now.date() > last.date() and now.hour >= DATA_HOUR_UTC and now.minute >= DATA_MINUTE_UTC


def _is_intent_due(now, state):
    run_id = state.get("last_trading_intent_run_id")
    if run_id is None:
        return True
    ts_str = run_id.split("_")[0]
    last = dt.datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
    return now.date() > last.date() and now.hour >= TRADING_INTENT_HOUR_UTC and now.minute >= TRADING_INTENT_MINUTE_UTC


def _is_exec_due(now, state):
    if now.hour < TRADING_EXEC_HOUR_UTC:
        return False
    last_ms = state.get("last_trading_exec_ms")
    if not last_ms:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= TRADING_EXEC_INTERVAL_MINUTES * 60


def _is_mkt_cap_due(now, state):
    last_ms = state.get("last_mkt_cap_run_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return now.date() > last.date() and now.hour >= MKT_CAP_HOUR_UTC and now.minute >= MKT_CAP_MINUTE_UTC


def _is_fills_stale(now, state):
    last_ms = state.get("fills_logged_at_ms")
    if last_ms is None:
        return True
    last = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)
    return (now - last).total_seconds() >= POSITION_CHECK_INTERVAL_HOURS * 3600


# ---------------------------------------------------------------------------
# System health helpers
# ---------------------------------------------------------------------------

def load_last_fill_ms():
    """Most recent fill_timestamp_ms from fills.jsonl, or None."""
    import json as _json
    path = root / "logs/fills.jsonl"
    if not path.exists():
        return None
    best = None
    with open(path) as fh:
        for line in fh:
            try:
                ts = _json.loads(line).get("fill_timestamp_ms")
                if ts and (best is None or ts > best):
                    best = ts
            except Exception:
                pass
    return best


def load_open_orders_count():
    """Count submitted orders from the most recent run_id that aren't filled."""
    import json as _json
    path = root / "logs/orders.jsonl"
    if not path.exists():
        return 0
    rows = []
    with open(path) as fh:
        for line in fh:
            try:
                rows.append(_json.loads(line))
            except Exception:
                pass
    if not rows:
        return 0
    latest_run = max(r["run_id"] for r in rows)
    return sum(
        1 for r in rows
        if r.get("run_id") == latest_run and r.get("exchange_status") != "filled"
    )


def load_heartbeat_ms():
    """Last loop-run timestamp written by main.py, or None."""
    path = root / "state/heartbeat.ms"
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def load_error_count_24h():
    """Count ERROR lines in logs/errors.log written in the last 24 hours."""
    path = root / "logs/errors.log"
    if not path.exists():
        return 0
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
    count = 0
    with open(path) as fh:
        for line in fh:
            try:
                ts_str = line[:23]
                ts = dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=dt.timezone.utc)
                if ts >= cutoff:
                    count += 1
            except Exception:
                pass
    return count


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_fills_split():
    """Read fills.jsonl and split at today's 00:00 UTC into (prev_fills, today_fills)."""
    import json as _json
    path = root / f"logs/fills_{TRADING_ENV}.jsonl"
    if not path.exists():
        return [], []
    records = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(_json.loads(line))
            except _json.JSONDecodeError:
                pass
    if not records:
        return [], []
    day_start_ms = int(
        dt.datetime.now(dt.timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp() * 1000
    )
    prev = [r for r in records if r.get("fill_timestamp_ms", 0) <= day_start_ms]
    today = [r for r in records if r.get("fill_timestamp_ms", 0) > day_start_ms]
    return prev, today


def load_current_mids():
    """Current mid prices from latest snapshot."""
    path = root / "data/snapshots/mids.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, index_col=0)
        return df["mid"].to_dict()
    except Exception:
        return {}


def load_day_open_mids():
    """Mid prices as of today's 00:00 UTC open snapshot."""
    path = root / "data/snapshots/day_open.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path, index_col=0)
        return df["mid"].to_dict()
    except Exception:
        return {}


def compute_portfolio_vol(exchange_state, intent):
    """Annualised portfolio volatility from current position weights and latest intent."""
    try:
        account_value = float(exchange_state["marginSummary"]["accountValue"])
        if account_value <= 0:
            return None

        vol_per_coin = {
            coin: data["model"]["vol_1d"]
            for coin, data in intent.get("assets", {}).items()
            if data.get("model", {}).get("vol_1d") is not None
        }
        corr_raw = intent.get("risk_inputs", {}).get("correlation_matrix", {})
        if not corr_raw or not vol_per_coin:
            return None

        corr_df = pd.DataFrame(corr_raw)

        # Net signed weight = signed notional / account_value
        weights = {}
        for row in exchange_state.get("assetPositions", []):
            pos = row["position"]
            qty = float(pos.get("szi", 0))
            notional = abs(float(pos.get("positionValue", 0)))
            signed_notional = np.sign(qty) * notional
            weights[pos["coin"]] = signed_notional / account_value

        common = [c for c in weights if c in vol_per_coin and c in corr_df.index]
        if len(common) < 2:
            return None

        w = np.array([weights[c] for c in common])
        vols = np.array([vol_per_coin[c] for c in common])
        corr = corr_df.loc[common, common].values
        cov = np.outer(vols, vols) * corr
        port_var = float(w @ cov @ w)
        return float(np.sqrt(max(port_var, 0)) * np.sqrt(365))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Heatmap
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_usd(v, signed=True):
    if v is None:
        return "—"
    if signed:
        prefix = "+" if v > 0 else ("-" if v < 0 else "")
    else:
        prefix = ""
    return f"{prefix}${abs(v):,.2f}"


def pnl_class(v):
    if v is None or v == 0:
        return "neutral"
    return "pos" if v > 0 else "neg"


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="{refresh}">
<title>Portfolio · {date}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d0f14;color:#e2e8f0;font-family:'SF Mono','Fira Code',monospace;font-size:13px;line-height:1.5}}
header{{padding:14px 24px;border-bottom:1px solid #1e2433;display:flex;justify-content:space-between;align-items:center}}
header h1{{font-size:13px;font-weight:600;color:#94a3b8;letter-spacing:.1em;text-transform:uppercase}}
.ts{{color:#334155;font-size:12px}}
.tabs{{display:flex;gap:0;padding:0 24px;border-bottom:1px solid #1e2433}}
.tab-btn{{background:none;border:none;border-bottom:2px solid transparent;color:#475569;font-family:inherit;font-size:12px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;padding:10px 18px;cursor:pointer;margin-bottom:-1px}}
.tab-btn.active{{color:#e2e8f0;border-bottom-color:#38bdf8}}
.tab-btn:hover:not(.active){{color:#94a3b8}}
.tab-panel{{display:none}}
.tab-panel.active{{display:block}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;padding:20px 24px}}
.card{{background:#131720;border:1px solid #1e2433;border-radius:8px;padding:16px 18px}}
.card .lbl{{color:#475569;font-size:10px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}}
.card .val{{font-size:24px;font-weight:700;letter-spacing:-.02em}}
.card .sub{{color:#475569;font-size:11px;margin-top:5px}}
.pos{{color:#22c55e}}
.neg{{color:#ef4444}}
.neutral{{color:#94a3b8}}
section{{padding:0 24px 32px}}
.sec-hdr{{font-size:10px;color:#334155;text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px}}
table{{width:100%;border-collapse:collapse}}
th{{color:#334155;font-size:10px;text-transform:uppercase;letter-spacing:.05em;padding:6px 12px;text-align:right;border-bottom:1px solid #1e2433;white-space:nowrap;cursor:pointer;user-select:none}}
th:first-child{{text-align:left}}
th.sort-asc::after{{content:' ▲'}}
th.sort-desc::after{{content:' ▼'}}
td{{padding:9px 12px;text-align:right;border-bottom:1px solid #0d0f14}}
td:first-child{{text-align:left;font-weight:600;color:#e2e8f0}}
tr:hover td{{background:#131720}}
.long{{color:#38bdf8}}
.short{{color:#f59e0b}}
footer{{text-align:center;padding:16px;color:#1e2433;font-size:11px}}
.status-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;padding:0 24px 20px}}
.scard{{background:#131720;border:1px solid #1e2433;border-radius:8px;padding:14px 16px;display:flex;align-items:center;gap:14px}}
.dot{{width:12px;height:12px;border-radius:50%;flex-shrink:0}}
.dot-green{{background:#22c55e;box-shadow:0 0 6px #22c55e88}}
.dot-red{{background:#ef4444;box-shadow:0 0 6px #ef444488}}
.scard-body{{min-width:0}}
.scard-lbl{{color:#475569;font-size:10px;text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px}}
.scard-val{{font-size:12px;color:#cbd5e1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.scard-sub{{font-size:11px;color:#334155;margin-top:2px}}
</style>
</head>
<body>
<header>
  <h1>Portfolio Overview</h1>
  <span class="ts">{now} UTC &nbsp;·&nbsp; auto-refresh {refresh}s</span>
</header>

<div class="tabs">
  <button class="tab-btn" data-tab="status">Status</button>
  <button class="tab-btn" data-tab="portfolio">Portfolio</button>
</div>

<div class="tab-panel" id="tab-status">
  <div style="padding:20px 0 0">
{status_content}
  </div>
</div>

<div class="tab-panel" id="tab-portfolio">
  <div class="cards">
    <div class="card">
      <div class="lbl">PnL Since 00:00 UTC</div>
      <div class="val {day_pnl_cls}">{day_pnl}</div>
      <div class="sub">Realised {realized_today} &nbsp;|&nbsp; MTM {mtm_today}</div>
    </div>
    <div class="card">
      <div class="lbl">Account Value</div>
      <div class="val neutral">{account_value}</div>
      <div class="sub">Withdrawable {withdrawable}</div>
    </div>
    <div class="card">
      <div class="lbl">Gross Exposure</div>
      <div class="val neutral">{gross_exposure}</div>
      <div class="sub">{gross_pct} of equity</div>
    </div>
    <div class="card">
      <div class="lbl">Net Exposure</div>
      <div class="val {net_cls}">{net_exposure}</div>
      <div class="sub">{net_pct} of equity</div>
    </div>
    <div class="card">
      <div class="lbl">Predicted Volatility</div>
      <div class="val neutral">{port_vol}</div>
      <div class="sub">Annualised · from latest intent</div>
    </div>
  </div>

  <section>
    <div class="sec-hdr">Positions ({n_rows})</div>
    <table>
      <thead>
        <tr id="sort-header-positions">
          <th data-col="0" data-type="str">Coin</th>
          <th data-col="1" data-type="str">Direction</th>
          <th data-col="2" data-type="num">Position Value</th>
          <th data-col="3" data-type="num">Entry Price</th>
          <th data-col="4" data-type="num">Mark Price</th>
          <th data-col="5" data-type="num">PnL</th>
          <th data-col="6" data-type="num">Value Traded Today</th>
          <th data-col="7" data-type="num">PnL Today</th>
        </tr>
      </thead>
      <tbody>
{positions_rows}
      </tbody>
    </table>
  </section>
</div>

<footer>exchange state &nbsp;·&nbsp; fills.jsonl &nbsp;·&nbsp; ohlcv_data.duckdb &nbsp;·&nbsp; mids.csv</footer>
<script>
(function(){{
  var TAB_KEY = 'active_tab';
  function showTab(name) {{
    document.querySelectorAll('.tab-btn').forEach(function(b){{
      b.classList.toggle('active', b.dataset.tab === name);
    }});
    document.querySelectorAll('.tab-panel').forEach(function(p){{
      p.classList.toggle('active', p.id === 'tab-' + name);
    }});
    localStorage.setItem(TAB_KEY, name);
  }}
  document.querySelectorAll('.tab-btn').forEach(function(btn){{
    btn.addEventListener('click', function(){{ showTab(btn.dataset.tab); }});
  }});
  showTab(localStorage.getItem(TAB_KEY) || 'status');

  function applySort(hdr, state) {{
    if (state.col === null) return;
    var th = hdr.querySelector('th[data-col="' + state.col + '"]');
    if (!th) return;
    var isNum = th.dataset.type === 'num';
    hdr.querySelectorAll('th').forEach(function(h){{ h.classList.remove('sort-asc','sort-desc'); }});
    th.classList.add(state.asc ? 'sort-asc' : 'sort-desc');
    var tbody = hdr.closest('table').querySelector('tbody');
    var rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort(function(a, b){{
      var av = a.cells[state.col].textContent.trim();
      var bv = b.cells[state.col].textContent.trim();
      if (isNum){{
        av = parseFloat(av.replace(/[^0-9.\-+]/g,'')) || 0;
        bv = parseFloat(bv.replace(/[^0-9.\-+]/g,'')) || 0;
        return state.asc ? av - bv : bv - av;
      }}
      return state.asc ? av.localeCompare(bv) : bv.localeCompare(av);
    }});
    rows.forEach(function(r){{ tbody.appendChild(r); }});
  }}
  function initSortTable(hdrId) {{
    var key = 'sort_' + hdrId;
    var saved = JSON.parse(localStorage.getItem(key) || 'null');
    var state = saved || {{col: null, asc: true}};
    var hdr = document.getElementById(hdrId);
    if (!hdr) return;
    applySort(hdr, state);
    hdr.querySelectorAll('th').forEach(function(th){{
      th.addEventListener('click', function(){{
        var col = +th.dataset.col;
        if (state.col === col) {{ state.asc = !state.asc; }}
        else {{ state.col = col; state.asc = (col === 0 || col === 1); }}
        applySort(hdr, state);
        localStorage.setItem(key, JSON.stringify(state));
      }});
    }});
  }}
  initSortTable('sort-header-positions');
}})();
</script>
</body>
</html>
"""


def _fmt_ms(ms):
    """Format a millisecond timestamp as a short UTC string, or '—'."""
    if not ms:
        return "—"
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _positions_match_str(state):
    v = state.get("positions_match_exchange")
    if v is None:
        return "Unknown"
    return "Match" if v else "Mismatch"


def _positions_match_red(state):
    v = state.get("positions_match_exchange")
    return v is not True  # red if mismatch or never checked


def _scard(lbl, val, sub, is_red):
    dot_cls = "dot-red" if is_red else "dot-green"
    return (
        f'<div class="scard">'
        f'<div class="dot {dot_cls}"></div>'
        f'<div class="scard-body">'
        f'<div class="scard-lbl">{lbl}</div>'
        f'<div class="scard-val">{val}</div>'
        f'<div class="scard-sub">{sub}</div>'
        f'</div></div>'
    )


def build_status_tab(now, state):
    data_stale    = _is_data_due(now, state)
    intent_stale  = _is_intent_due(now, state)
    exec_stale    = _is_exec_due(now, state)
    fills_stale   = _is_fills_stale(now, state)
    mkt_cap_stale = _is_mkt_cap_due(now, state)

    run_id = state.get("last_trading_intent_run_id") or "—"
    run_id_display = run_id.split("_")[0] if "_" in run_id else run_id

    open_orders  = load_open_orders_count()
    last_fill_ms = load_last_fill_ms()
    heartbeat_ms = load_heartbeat_ms()
    error_count  = load_error_count_24h()

    heartbeat_stale = heartbeat_ms is None or (now.timestamp() * 1000 - heartbeat_ms) > 5 * 60 * 1000
    heartbeat_val = _fmt_ms(heartbeat_ms) if heartbeat_ms else "—"

    # --- top health row ---
    health_items = [
        ("System Heartbeat", heartbeat_val,          "Loop silent > 5 min = red",                                               heartbeat_stale),
        ("Positions Match",  _positions_match_str(state), f"Checked every {POSITION_CHECK_INTERVAL_HOURS}h · last {_fmt_ms(state.get('last_position_check_ms'))}", _positions_match_red(state)),
        ("Errors (24h)",     str(error_count),        "ERROR-level log entries",                                                 error_count > 0),
    ]

    # --- trading section ---
    trading_items = [
        ("Trading Intent",  run_id_display,                             f"Due after {TRADING_INTENT_HOUR_UTC:02d}:{TRADING_INTENT_MINUTE_UTC:02d} UTC daily", intent_stale),
        ("Last Execution",  _fmt_ms(state.get("last_trading_exec_ms")), f"Every {TRADING_EXEC_INTERVAL_MINUTES}min after {TRADING_EXEC_HOUR_UTC:02d}:00 UTC", exec_stale),
        ("Last Fill",       _fmt_ms(last_fill_ms),                      "Most recent order filled",                                                last_fill_ms is None),
        ("Open Orders",     str(open_orders),                           "From latest rebalance run",                                               open_orders > 10),
        ("Fills Logged At", _fmt_ms(state.get("fills_logged_at_ms")),   f"Every {POSITION_CHECK_INTERVAL_HOURS}h",                                   fills_stale),
    ]

    # --- data section ---
    data_items = [
        ("OHLCV Updated", _fmt_ms(state.get("last_data_run_ms")),     f"Due after {DATA_HOUR_UTC:02d}:{DATA_MINUTE_UTC:02d} UTC daily",       data_stale),
        ("Market Cap",    _fmt_ms(state.get("last_mkt_cap_run_ms")),  f"Due after {MKT_CAP_HOUR_UTC:02d}:{MKT_CAP_MINUTE_UTC:02d} UTC daily", mkt_cap_stale),
    ]

    def grid(items):
        cards = "\n".join(_scard(*i) for i in items)
        return f'<div class="status-grid">\n{cards}\n</div>'

    return (
        grid(health_items)
        + '\n<div class="sec-hdr" style="padding:12px 24px 8px">Trading</div>\n'
        + grid(trading_items)
        + '\n<div class="sec-hdr" style="padding:12px 24px 8px">Data</div>\n'
        + grid(data_items)
    )


def build_position_row(coin, pos, mids, today_value=None, today_pnl_val=None):
    qty = pos["qty"]
    mid = mids.get(coin)

    if qty == 0:
        direction, dir_cls = "FLAT", "neutral"
        pos_value = None
        entry = "—"
        pnl = None
    else:
        direction = "LONG" if qty > 0 else "SHORT"
        dir_cls = "long" if qty > 0 else "short"
        pos_value = qty * mid if mid else None
        entry = f"${pos['avg_price']:,.4g}" if pos["avg_price"] else "—"
        pnl = qty * (mid - pos["avg_price"]) if mid else None

    mark = f"${mid:,.4g}" if mid else "—"

    def cell(v):
        return f'<span class="{pnl_class(v)}">{fmt_usd(v)}</span>'

    today_value_str = cell(today_value) if today_value is not None else "—"
    today_pnl_str = cell(today_pnl_val) if today_pnl_val is not None else "—"

    return (
        f"      <tr>"
        f"<td>{coin}</td>"
        f'<td class="{dir_cls}">{direction}</td>'
        f"<td>{cell(pos_value) if pos_value is not None else '—'}</td>"
        f"<td>{entry}</td>"
        f"<td>{mark}</td>"
        f"<td>{cell(pnl)}</td>"
        f"<td>{today_value_str}</td>"
        f"<td>{today_pnl_str}</td>"
        f"</tr>"
    )


def build_page():
    now = dt.datetime.now(dt.timezone.utc)

    exchange_state = read_latest_exchange_state()
    intent = intent_logger.read_latest() or {}

    from src.config import WALLET_ADDRESS as wallet, TRADING_ENV
    state_path = root / f"state/hyperliquid_{TRADING_ENV}_{wallet}_state.json"
    try:
        strategy_state = load_state(state_path)
    except Exception:
        strategy_state = {}
    mids = load_current_mids()
    day_open_mids = load_day_open_mids()
    prev_fills, today_fills = load_fills_split()

    margin = exchange_state.get("marginSummary", {})
    account_value = float(margin.get("accountValue", 0))
    withdrawable = float(exchange_state.get("withdrawable", 0))
    positions_raw = exchange_state.get("assetPositions", [])

    gross_exposure = sum(abs(float(p["position"].get("positionValue", 0))) for p in positions_raw)
    net_exposure = sum(
        np.sign(float(p["position"].get("szi", 0))) * abs(float(p["position"].get("positionValue", 0)))
        for p in positions_raw
    )

    port_vol = compute_portfolio_vol(exchange_state, intent)
    port_vol_str = f"{port_vol:.1%}" if port_vol is not None else "—"

    prev_positions = PositionRebuilder().rebuild_from_fills(prev_fills)
    day_positions  = PositionRebuilder().rebuild_from_fills(today_fills)
    all_positions  = PositionRebuilder().rebuild_from_fills(prev_fills + today_fills)

    today_value_per_coin: dict[str, float] = {}
    for fill in today_fills:
        coin = fill["coin"]
        signed_notional = np.sign(float(fill["qty"])) * abs(float(fill.get("notional", 0)))
        today_value_per_coin[coin] = today_value_per_coin.get(coin, 0.0) + signed_notional

    today_pnl_per_coin: dict[str, float] = {}
    total_realized_today = 0.0
    total_mtm_today = 0.0

    all_coins_today = set(prev_positions) | set(day_positions)
    if day_open_mids:
        for coin in all_coins_today:
            mid = mids.get(coin)
            if not mid:
                continue
            open_px  = day_open_mids.get(coin, mid)
            prev_qty = prev_positions[coin]["qty"]       if coin in prev_positions else 0.0
            day_qty  = day_positions[coin]["qty"]        if coin in day_positions  else 0.0
            day_avg  = day_positions[coin]["avg_price"]  if coin in day_positions  else 0.0

            if prev_qty != 0 and day_qty != 0 and prev_qty * day_qty < 0:
                # Opposite signs: day trades partially or fully close the prev position.
                # Closed portion priced at day_avg (realized); remainder marks to mid.
                offset_qty = min(abs(prev_qty), abs(day_qty))
                prev_sign  = 1 if prev_qty > 0 else -1
                day_sign   = 1 if day_qty  > 0 else -1
                realized   = prev_sign * offset_qty * (day_avg - open_px)
                rem_prev   = prev_qty + day_sign  * offset_qty
                rem_day    = day_qty  + prev_sign * offset_qty
                unrealized = rem_prev * (mid - open_px) + rem_day * (mid - day_avg)
                today_pnl_per_coin[coin] = realized + unrealized
                total_realized_today += realized
                total_mtm_today      += unrealized
            else:
                # No offset: prev marks to mid from day_open, day trades mark to mid from day_avg.
                pnl = prev_qty * (mid - open_px) + day_qty * (mid - day_avg)
                today_pnl_per_coin[coin] = pnl
                total_mtm_today += pnl
    else:
        # No day_open snapshot: use exchange closed_pnl for realized,
        # unrealized on day positions only (prev baseline unknown).
        for fill in today_fills:
            coin = fill["coin"]
            today_pnl_per_coin[coin] = today_pnl_per_coin.get(coin, 0.0) + float(fill.get("closed_pnl", 0))
        total_realized_today = sum(today_pnl_per_coin.values())
        for coin, pos in day_positions.items():
            mid = mids.get(coin)
            if mid:
                mtm = pos["qty"] * (mid - pos["avg_price"])
                today_pnl_per_coin[coin] = today_pnl_per_coin.get(coin, 0.0) + mtm
                total_mtm_today += mtm

    total_day_pnl = sum(today_pnl_per_coin.values())

    display_coins = sorted(
        {c for c, p in all_positions.items() if abs(p["qty"]) > 0} | set(today_value_per_coin),
        key=lambda c: abs(all_positions.get(c, {"qty": 0, "avg_price": 0})["qty"]) * (mids.get(c) or 0),
        reverse=True,
    )

    _EMPTY_8 = '      <tr><td colspan="8" style="text-align:center;color:#334155;padding:20px">—</td></tr>'

    _empty_pos = {"qty": 0, "avg_price": 0.0, "realized_pnl": 0.0}
    positions_rows = (
        "\n".join(
            build_position_row(
                coin,
                all_positions.get(coin, _empty_pos),
                mids,
                today_value=today_value_per_coin.get(coin),
                today_pnl_val=today_pnl_per_coin.get(coin),
            )
            for coin in display_coins
        ) or _EMPTY_8
    )

    status_content = build_status_tab(now, strategy_state)

    return PAGE.format(
        refresh=REFRESH_SECONDS,
        status_content=status_content,
        date=now.strftime("%Y-%m-%d"),
        now=now.strftime("%H:%M:%S"),
        day_pnl=fmt_usd(total_day_pnl),
        day_pnl_cls=pnl_class(total_day_pnl),
        realized_today=fmt_usd(total_realized_today),
        mtm_today=fmt_usd(total_mtm_today),
        account_value=fmt_usd(account_value, signed=False),
        withdrawable=fmt_usd(withdrawable, signed=False),
        gross_exposure=fmt_usd(gross_exposure, signed=False),
        gross_pct=f"{gross_exposure / account_value:.0%}" if account_value else "—",
        net_exposure=fmt_usd(net_exposure),
        net_cls=pnl_class(net_exposure),
        net_pct=f"{net_exposure / account_value:+.0%}" if account_value else "—",
        port_vol=port_vol_str,
        n_rows=len(display_coins),
        positions_rows=positions_rows,
    )


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/index.html"):
            self.send_response(404)
            self.end_headers()
            return
        try:
            body = build_page().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"<pre>Error: {e}</pre>".encode())

    def log_message(self, fmt, *args):
        pass  # suppress per-request console noise


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", 8050))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Dashboard running at http://localhost:{port}")
    server.serve_forever()
