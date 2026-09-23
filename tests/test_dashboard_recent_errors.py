import datetime as dt

from src.dashboard import app as dashboard_app


def _write_errors_log(tmp_path, lines):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "errors.log").write_text("\n".join(lines) + "\n")
    return tmp_path


def test_load_recent_errors_parses_header_lines_only(tmp_path, monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    recent_ts = (now - dt.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    old_ts = (now - dt.timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    lines = [
        f"{recent_ts} [ERROR] [intent] something broke",
        '  File "main.py", line 1, in <module>',
        "ValueError: boom",
        f"{old_ts} [ERROR] this one is too old to show",
    ]
    monkeypatch.setattr(dashboard_app, "root", _write_errors_log(tmp_path, lines))

    entries = dashboard_app.load_recent_errors(hours=24)

    assert len(entries) == 1
    ts, msg = entries[0]
    assert msg == "[intent] something broke"


def test_load_recent_errors_most_recent_first(tmp_path, monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    ts1 = (now - dt.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    ts2 = (now - dt.timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    lines = [
        f"{ts1} [ERROR] first error",
        f"{ts2} [ERROR] second error",
    ]
    monkeypatch.setattr(dashboard_app, "root", _write_errors_log(tmp_path, lines))

    entries = dashboard_app.load_recent_errors(hours=24)

    assert [msg for _, msg in entries] == ["second error", "first error"]


def test_load_recent_errors_empty_when_no_file(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard_app, "root", tmp_path)
    assert dashboard_app.load_recent_errors(hours=24) == []


def test_load_recent_errors_respects_limit(tmp_path, monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    lines = [
        f"{(now - dt.timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} [ERROR] error {i}"
        for i in range(5)
    ]
    monkeypatch.setattr(dashboard_app, "root", _write_errors_log(tmp_path, lines))

    entries = dashboard_app.load_recent_errors(hours=24, limit=2)

    assert len(entries) == 2
