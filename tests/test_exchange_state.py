import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts import exchange_state as es


def _write_snapshot(dir_path, stamp: datetime, usdc=None):
    """Write a fake exchange_state snapshot file named with the given timestamp."""
    fname = f"exchange_state_{stamp.strftime('%Y%m%d_%H%M%S')}.json"
    data = {"marginSummary": {"accountValue": "1.0"}}
    if usdc is not None:
        data["spotState"] = {"balances": [{"coin": "USDC", "total": usdc}]}
    (dir_path / fname).write_text(json.dumps(data))


@pytest.fixture
def isolated_snapshot_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(es, "exchange_state_DIR", tmp_path)
    return tmp_path


def test_get_account_equity_reads_spot_usdc_directly(isolated_snapshot_dir):
    exchange_state = {"spotState": {"balances": [{"coin": "USDC", "total": 5402.73}]},
                       "marginSummary": {"accountValue": "3204.65"}}
    assert es.get_account_equity(exchange_state) == 5402.73


def test_missing_spot_state_never_uses_margin_summary_accountValue(isolated_snapshot_dir):
    """The historical bug: marginSummary.accountValue measures a structurally
    different, much smaller quantity on a unified-margin account and must never
    be silently substituted for real equity."""
    now = datetime.now(timezone.utc)
    _write_snapshot(isolated_snapshot_dir, now - timedelta(minutes=10), usdc=5442.62)

    exchange_state = {"marginSummary": {"accountValue": "3141.26"}}  # spotState missing
    result = es.get_account_equity(exchange_state)

    assert result == 5442.62
    assert result != 3141.26


def test_falls_back_to_last_known_good_when_spot_state_missing(isolated_snapshot_dir):
    now = datetime.now(timezone.utc)
    _write_snapshot(isolated_snapshot_dir, now - timedelta(hours=3), usdc=5000.0)  # too old, superseded below
    _write_snapshot(isolated_snapshot_dir, now - timedelta(minutes=5), usdc=5361.20)  # most recent good one

    exchange_state = {"marginSummary": {"accountValue": "1687.14"}}
    assert es.get_account_equity(exchange_state) == 5361.20


def test_logs_error_when_fallback_reading_is_stale(isolated_snapshot_dir, caplog):
    now = datetime.now(timezone.utc)
    _write_snapshot(isolated_snapshot_dir, now - timedelta(hours=2), usdc=5000.0)

    exchange_state = {"marginSummary": {"accountValue": "1.0"}}
    with caplog.at_level("ERROR"):
        es.get_account_equity(exchange_state)
    assert any("STALE" in rec.message for rec in caplog.records)


def test_no_error_logged_when_fallback_reading_is_fresh(isolated_snapshot_dir, caplog):
    now = datetime.now(timezone.utc)
    _write_snapshot(isolated_snapshot_dir, now - timedelta(minutes=2), usdc=5000.0)

    exchange_state = {"marginSummary": {"accountValue": "1.0"}}
    with caplog.at_level("ERROR"):
        es.get_account_equity(exchange_state)
    assert not any("STALE" in rec.message for rec in caplog.records)


def test_raises_when_no_good_snapshot_exists_anywhere(isolated_snapshot_dir):
    exchange_state = {"marginSummary": {"accountValue": "1.0"}}
    with pytest.raises(RuntimeError):
        es.get_account_equity(exchange_state)
