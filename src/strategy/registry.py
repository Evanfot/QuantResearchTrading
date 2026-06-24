"""Strategy registry + run provenance.

Reads ``config/strategy_registry.toml`` (stdlib ``tomllib``, read-only) and
assembles the provenance block stamped into ``intent.meta`` for reproducibility.

A run is fully reproducible from the triple:
  * ``strategy``     — semantic id resolving to component models (this registry)
  * ``git_commit``   — the exact code
  * ``config_hash``  — the resolved sizing params actually used (catches drift)
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config" / "strategy_registry.toml"
_REPO_ROOT = _REGISTRY_PATH.parents[1]

# StrategyConfig.sizing_model -> registry strategy id
_SIZING_TO_STRATEGY = {
    "risk_parity": "trend_rp_v1",
    "mvo": "trend_mvo_v1",
}

# StrategyConfig fields that define each allocator's behaviour (for config_hash).
_CONFIG_FIELDS = {
    "risk_parity": (
        "shrinkage_value", "position_multiplier", "weight_multiplier",
        "ewmac_fast", "breakout_window", "bollinger_window", "vo_window", "correlation",
    ),
    "mvo": (
        "mvo_target_vol_daily", "mvo_trading_days", "mvo_max_position_weight",
        "mvo_gamma", "mvo_rf", "mvo_kelly_fraction", "mvo_lookback", "mvo_min_periods",
    ),
}


@lru_cache(maxsize=1)
def _load() -> Dict[str, Any]:
    with _REGISTRY_PATH.open("rb") as f:
        return tomllib.load(f)


def resolve(strategy_id: str) -> Dict[str, Any]:
    """Return the registry manifest for ``strategy_id`` or raise ``KeyError``."""
    strategies = _load().get("strategies", {})
    if strategy_id not in strategies:
        raise KeyError(f"unknown strategy id {strategy_id!r} (not in {_REGISTRY_PATH})")
    return strategies[strategy_id]


def strategy_id_for(config) -> str:
    """Map a StrategyConfig's ``sizing_model`` to its registry strategy id."""
    sm = getattr(config, "sizing_model", None)
    if sm not in _SIZING_TO_STRATEGY:
        raise KeyError(f"no registry mapping for sizing_model={sm!r}")
    return _SIZING_TO_STRATEGY[sm]


def git_provenance() -> Tuple[Optional[str], Optional[bool]]:
    """Return ``(short_commit, dirty)``; resilient to missing git (e.g. Docker).

    Falls back to the ``GIT_COMMIT`` env var, then to reading ``.git/HEAD``.
    ``dirty`` is ``None`` when it cannot be determined.
    """
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=_REPO_ROOT,
            stderr=subprocess.DEVNULL).decode().strip()
        dirty = bool(subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=_REPO_ROOT,
            stderr=subprocess.DEVNULL).decode().strip())
        return commit, dirty
    except Exception:
        pass

    env = os.environ.get("GIT_COMMIT")
    if env:
        return env[:12], None

    try:
        head = (_REPO_ROOT / ".git" / "HEAD").read_text().strip()
        if head.startswith("ref:"):
            ref = head.split(" ", 1)[1]
            commit = (_REPO_ROOT / ".git" / ref).read_text().strip()
            return commit[:12], None
        return head[:12], None
    except Exception:
        return None, None


def config_hash(config) -> str:
    """Stable short hash of the resolved sizing params actually used."""
    sm = getattr(config, "sizing_model", None)
    fields = _CONFIG_FIELDS.get(sm, ())
    payload = "|".join(f"{f}={getattr(config, f, None)!r}" for f in fields)
    return hashlib.sha1(payload.encode()).hexdigest()[:12]


def provenance(config) -> Dict[str, Any]:
    """Assemble the ``intent.meta`` provenance block for the active config."""
    strategy_id = strategy_id_for(config)
    manifest = resolve(strategy_id)
    commit, dirty = git_provenance()
    return {
        "strategy": strategy_id,
        "strategy_components": {
            "alpha_model": manifest.get("alpha_model"),
            "risk_model": manifest.get("risk_model"),
            "allocator": manifest.get("allocator"),
        },
        "git_commit": commit,
        "git_dirty": dirty,
        "config_hash": config_hash(config),
    }
