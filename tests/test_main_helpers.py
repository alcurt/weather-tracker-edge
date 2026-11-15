from datetime import date, datetime

import web
from config import TZ_UTC8
from frames import FrameCatalog


def test_dynamic_fps_mapping():
    assert web.get_dynamic_fps(1) == 4.0
    assert web.get_dynamic_fps(3) == 6.0
    assert web.get_dynamic_fps(6) == 8.0
    assert web.get_dynamic_fps(12) == 9.6
    assert web.get_dynamic_fps(24) == 14.4
    assert web.get_dynamic_fps(2) == 10.0


def test_parse_start_time_uses_beijing_timezone():
    parsed = web.parse_start_time("20260101_2330")
    assert parsed == datetime(2026, 1, 1, 23, 30, tzinfo=TZ_UTC8)


def test_cron_due_requires_process_running_before_boundary():
    started = datetime(2026, 1, 2, 0, 10, tzinfo=TZ_UTC8)
    # 00:15 之前不触发
    assert web.cron_due(datetime(2026, 1, 2, 0, 14, tzinfo=TZ_UTC8), started, None) is False
    # 跨过 00:15 触发一次
    assert web.cron_due(datetime(2026, 1, 2, 0, 15, tzinfo=TZ_UTC8), started, None) is True
    assert web.cron_due(datetime(2026, 1, 2, 0, 15, tzinfo=TZ_UTC8), started, date(2026, 1, 2)) is False
    # 当天晚些时候仍只触发一次
    assert web.cron_due(datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8), started, date(2026, 1, 2)) is False


def test_cron_due_skips_when_started_after_boundary():
    # 00:16 才启动，当天不补跑
    started = datetime(2026, 1, 2, 0, 16, tzinfo=TZ_UTC8)
    assert web.cron_due(datetime(2026, 1, 2, 0, 17, tzinfo=TZ_UTC8), started, None) is False
    assert web.cron_due(datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8), started, None) is False
    # 次日 00:15 可以触发
    assert web.cron_due(datetime(2026, 1, 3, 0, 15, tzinfo=TZ_UTC8), started, None) is True


def test_collect_stats_ignores_part_files(tmp_path, monkeypatch):
    monkeypatch.setattr(web, "DATA_DIR", tmp_path)
    monkeypatch.setattr(web, "all_target_keys", ["TempC"])
    monkeypatch.setattr(web, "flat_display_map", {"TempC": "气温填色图"})

    date_dir = tmp_path / "TempC" / "20260101"
    date_dir.mkdir(parents=True)
    (date_dir / "20260101_120000_TempC.png").write_bytes(b"x")
    (date_dir / "20260101_121000_TempC.png.part").write_bytes(b"partial")

    records = web.collect_stats(
        datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8),
        {"TempC": 0},
        {"TempC": ""},
        FrameCatalog(tmp_path),
        tmp_path,
    )
    assert records[0]["avail_24h"] == 1
    assert records[0]["latest_time"] == "12:00"


def test_collect_stats_recomputes_hourly_as_time_moves(tmp_path, monkeypatch):
    monkeypatch.setattr(web, "DATA_DIR", tmp_path)
    monkeypatch.setattr(web, "all_target_keys", ["TempC"])
    monkeypatch.setattr(web, "flat_display_map", {"TempC": "气温填色图"})

    frame = tmp_path / "TempC" / "20260101" / "20260101_120000_TempC.png"
    frame.parent.mkdir(parents=True)
    frame.write_bytes(b"x")

    session_counts = {"TempC": 0}
    last_cursors = {"TempC": ""}
    catalog = FrameCatalog(tmp_path)

    first = web.collect_stats(datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, catalog, tmp_path)
    assert first[0]["avail_24h"] == 1
    assert first[0]["hourly"][0] == 1
    assert first[0]["latest_time"] == "12:00"

    # 目录不再写入、mtime 不变，但时间推进后聚合结果必须随 now 重算
    second = web.collect_stats(datetime(2026, 1, 1, 13, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, catalog, tmp_path)
    assert second[0]["avail_24h"] == 1
    assert second[0]["hourly"][0] == 0
    assert second[0]["hourly"][1] == 1

    # 再推进 24 小时，帧滑出窗口，计数应归零
    third = web.collect_stats(datetime(2026, 1, 2, 13, 30, tzinfo=TZ_UTC8), session_counts, last_cursors, catalog, tmp_path)
    assert third[0]["avail_24h"] == 0
    assert third[0]["hourly"] == [0] * 24
