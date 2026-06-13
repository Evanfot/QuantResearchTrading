import json

from src.loggers.order_logger import OrderLogger


def _read(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def _resp(status):
    return {"response": {"data": {"statuses": [status]}}}


def test_resting_submission_is_accepted_with_context(tmp_path):
    log = tmp_path / "orders.jsonl"
    OrderLogger(str(log)).log_order_submission(
        run_id="r1", exchange="hyperliquid", account="0xabc",
        symbol="BTC", side="buy", order_type="LIMIT", price=60000.0, qty=0.001,
        response=_resp({"resting": {"oid": 123}}),
        target_qty=0.001, current_qty=0.0, delta=0.001,
    )
    rec = _read(log)[0]
    assert rec["event_type"] == "order_submitted"
    assert rec["accepted"] is True
    assert rec["order_id"] == 123
    assert rec["error"] is None
    assert (rec["target_qty"], rec["current_qty"], rec["delta"]) == (0.001, 0.0, 0.001)


def test_error_submission_captures_message_and_is_not_accepted(tmp_path):
    log = tmp_path / "orders.jsonl"
    OrderLogger(str(log)).log_order_submission(
        run_id="r1", exchange="hyperliquid", account="0xabc",
        symbol="ZEC", side="sell", order_type="LIMIT", price=300.0, qty=0.02,
        response=_resp({"error": "Order must have minimum value of $10. asset=214"}),
        target_qty=0.0, current_qty=-0.02, delta=0.02,
    )
    rec = _read(log)[0]
    assert rec["accepted"] is False
    assert rec["order_id"] is None
    assert "minimum value" in rec["error"]


def test_filled_submission_captures_fill_detail(tmp_path):
    log = tmp_path / "orders.jsonl"
    OrderLogger(str(log)).log_order_submission(
        run_id="r1", exchange="hyperliquid", account="0xabc",
        symbol="ETH", side="buy", order_type="LIMIT", price=2500.0, qty=0.1,
        response=_resp({"filled": {"oid": 9, "totalSz": "0.1", "avgPx": "2499.5"}}),
    )
    rec = _read(log)[0]
    assert rec["accepted"] is True
    assert rec["order_id"] == 9
    assert rec["filled_sz"] == 0.1
    assert rec["avg_fill_px"] == 2499.5


