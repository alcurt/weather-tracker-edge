from datetime import datetime

import main
from config import TZ_UTC8


def test_dynamic_fps_mapping():
    assert main.get_dynamic_fps(1) == 4.0
    assert main.get_dynamic_fps(3) == 6.0
    assert main.get_dynamic_fps(6) == 8.0
    assert main.get_dynamic_fps(12) == 9.6
    assert main.get_dynamic_fps(24) == 14.4
    assert main.get_dynamic_fps(2) == 10.0


def test_parse_start_time_uses_beijing_timezone():
    parsed = main.parse_start_time("20260101_2330")
    assert parsed == datetime(2026, 1, 1, 23, 30, tzinfo=TZ_UTC8)


def test_cron_target_waits_until_quarter_past_midnight():
    now = datetime(2026, 1, 2, 0, 10, tzinfo=TZ_UTC8)
    assert main.cron_target(now, "20260101") is None


def test_cron_target_skips_when_yesterday_already_done():
    now = datetime(2026, 1, 2, 0, 20, tzinfo=TZ_UTC8)
    assert main.cron_target(now, "20260101") is None


def test_cron_target_catches_up_missed_day():
    now = datetime(2026, 1, 2, 0, 20, tzinfo=TZ_UTC8)
    assert main.cron_target(now, "20251231") == "20260101"
    assert main.cron_target(now, "") == "20260101"


def test_cron_target_ignores_future_state():
    now = datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    assert main.cron_target(now, "20260105") is None


def test_cron_state_round_trip(tmp_path, monkeypatch):
    state_file = tmp_path / "cron.json"
    monkeypatch.setattr(main, "CRON_STATE_FILE", state_file)
    assert main._load_cron_state() == {}

    main._save_cron_state("20260101")
    assert main._load_cron_state()["last_done_date"] == "20260101"


def test_collect_stats_ignores_part_files(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "all_target_keys", ["TempC"])
    monkeypatch.setattr(main, "flat_display_map", {"TempC": "气温填色图"})

    date_dir = tmp_path / "TempC" / "20260101"
    date_dir.mkdir(parents=True)
    (date_dir / "20260101_120000_TempC.png").write_bytes(b"x")
    (date_dir / "20260101_121000_TempC.png.part").write_bytes(b"partial")

    records = main.collect_stats(
        datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8), {"TempC": 0}, {"TempC": ""}, {}
    )
    assert records[0]["avail_24h"] == 1
    assert records[0]["latest_time"] == "12:00"


def test_collect_stats_recomputes_hourly_as_time_moves(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    monkeypatch.setattr(main, "all_target_keys", ["TempC"])
    monkeypatch.setattr(main, "flat_display_map", {"TempC": "气温填色图"})

    frame = tmp_path / "TempC" / "20260101" / "20260101_120000_TempC.png"
    frame.parent.mkdir(parents=True)
    frame.write_bytes(b"x")

    session_counts = {"TempC": 0}
    last_cursors = {"TempC": ""}
    cache = {}

    first = main.collect_stats(datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, cache)
    assert first[0]["avail_24h"] == 1
    assert first[0]["hourly"][0] == 1
    assert first[0]["latest_time"] == "12:00"

    # 目录不再写入、mtime 不变，但时间推进后聚合结果必须随 now 重算
    second = main.collect_stats(datetime(2026, 1, 1, 13, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, cache)
    assert second[0]["avail_24h"] == 1
    assert second[0]["hourly"][0] == 0
    assert second[0]["hourly"][1] == 1

    # 再推进 24 小时，帧滑出窗口，计数应归零
    third = main.collect_stats(datetime(2026, 1, 2, 13, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, cache)
    assert third[0]["avail_24h"] == 0
    assert third[0]["hourly"] == [0] * 24
