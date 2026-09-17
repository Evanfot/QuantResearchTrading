"""Tests for the strategy registry + run provenance."""
import pytest

from src.backtester.full_backtest import StrategyConfig
from src.strategy import registry


def test_every_sizing_model_resolves():
    """Each sizing_model maps to a strategy id that exists in the registry."""
    for sizing_model in ("mvo", "risk_parity"):
        config = StrategyConfig()
        config.sizing_model = sizing_model
        sid = registry.strategy_id_for(config)
        manifest = registry.resolve(sid)  # raises if missing
        assert manifest["alpha_model"]
        assert manifest["risk_model"]
        assert manifest["allocator"]


def test_retired_legacy_entry_present():
    """Historical intent logs stamped 'trend_v1.1' must still resolve."""
    manifest = registry.resolve("trend_v1.1")
    assert manifest["status"] == "retired"
    assert manifest["allocator"] == "risk_parity_v1"


def test_unknown_strategy_raises():
    with pytest.raises(KeyError):
        registry.resolve("does_not_exist")


def test_unknown_sizing_model_raises():
    config = StrategyConfig()
    config.sizing_model = "bogus"
    with pytest.raises(KeyError):
        registry.strategy_id_for(config)


def test_provenance_block_shape():
    config = StrategyConfig()
    config.sizing_model = "mvo"
    prov = registry.provenance(config)
    assert prov["strategy"] == "trend_mvo_v1"
    assert prov["strategy_components"]["allocator"] == "mvo_maxsharpe_voltarget_v1"
    assert prov["strategy_components"]["risk_model"] == "ledoit_wolf_v1"
    assert set(prov) == {"strategy", "strategy_components", "git_commit", "git_dirty", "config_hash"}
    # git_commit resolves inside this repo
    assert prov["git_commit"]


def test_config_hash_is_deterministic_and_sensitive():
    a = StrategyConfig(); a.sizing_model = "mvo"
    b = StrategyConfig(); b.sizing_model = "mvo"
    assert registry.config_hash(a) == registry.config_hash(b)
    b.mvo_target_vol_daily = a.mvo_target_vol_daily * 2
    assert registry.config_hash(a) != registry.config_hash(b)


def test_config_hash_differs_by_allocator():
    rp = StrategyConfig(); rp.sizing_model = "risk_parity"
    mvo = StrategyConfig(); mvo.sizing_model = "mvo"
    # different field sets -> different hashes
    assert registry.config_hash(rp) != registry.config_hash(mvo)
