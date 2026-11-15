from datetime import datetime, timedelta

from analytics import compute_available_ranges, scan_videos, summarize_24h
from config import TZ_UTC8
from frames import parse_frame_timestamp


def _touch(path, ts: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    import os

    stat = ts.timestamp()
    os.utime(path, (stat, stat))


def test_parse_frame_timestamp_valid():
    ts = parse_frame_timestamp("20261024_155640_TempC.png")
    assert ts == datetime(2026, 10, 24, 15, 56, 40, tzinfo=TZ_UTC8)


def test_parse_frame_timestamp_rejects_bad_names():
    assert parse_frame_timestamp("not-a-frame.png") is None
    assert parse_frame_timestamp("20261024_1556") is None
    assert parse_frame_timestamp("20261024_999999_TempC.png") is None


def test_summarize_24h_buckets_and_window():
    now = datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    timestamps = [
        now - timedelta(minutes=30),   # 最近 1 小时 -> index 0
        now - timedelta(minutes=90),   # 1-2 小时前 -> index 1
        now - timedelta(hours=23, minutes=30),  # index 23
        now - timedelta(hours=25),     # 窗口外
        now + timedelta(minutes=10),   # 未来
    ]
    summary = summarize_24h(timestamps, now)
    assert summary["count"] == 3
    assert summary["hourly"][0] == 1
    assert summary["hourly"][1] == 1
    assert summary["hourly"][23] == 1
    assert summary["earliest"] == now - timedelta(hours=23, minutes=30)


def test_summarize_24h_excludes_exact_boundary_frame():
    now = datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    summary = summarize_24h([now - timedelta(hours=24)], now)
    assert summary["count"] == 0
    assert sum(summary["hourly"]) == 0


def test_ranges_cross_day(tmp_path):
    type_dir = tmp_path / "RadarGZ"
    _touch(type_dir / "20260101" / "20260101_233000_RadarGZ.png", datetime(2026, 1, 1, 23, 30, tzinfo=TZ_UTC8))
    _touch(type_dir / "20260102" / "20260102_000000_RadarGZ.png", datetime(2026, 1, 2, 0, 0, tzinfo=TZ_UTC8))

    result = compute_available_ranges(
        "RadarGZ", 1, type_dir, now=datetime(2026, 1, 2, 1, 0, tzinfo=TZ_UTC8)
    )
    dates = {d["date_str"]: d for d in result["dates"]}
    assert "20260101" in dates
    start = next(item for item in dates["20260101"]["available_starts"] if item["index"] == 47)
    assert start["cross_day"] is True
    assert start["end"] == "2026-01-02 00:30"


def test_ranges_requires_full_coverage_for_short_span(tmp_path):
    type_dir = tmp_path / "TempC"
    # 只有 10:00 有帧；10:30 缺失，1 小时跨度（2 个桶）不允许缺帧
    _touch(type_dir / "20260101" / "20260101_100000_TempC.png", datetime(2026, 1, 1, 10, 0, tzinfo=TZ_UTC8))

    result = compute_available_ranges(
        "TempC", 1, type_dir, now=datetime(2026, 1, 1, 11, 0, tzinfo=TZ_UTC8)
    )
    assert result["dates"] == []


def test_ranges_24h_tolerates_four_missing_buckets(tmp_path):
    day = "20260101"
    date_dir = tmp_path / "FY4C" / day
    missing = {0, 10, 20, 30}
    for idx in range(48):
        if idx in missing:
            continue
        minute = idx * 30
        ts = datetime(2026, 1, 1, minute // 60, minute % 60, tzinfo=TZ_UTC8)
        _touch(date_dir / f"{day}_{ts:%H%M%S}_FY4C.png", ts)

    result = compute_available_ranges(
        "FY4C", 24, tmp_path / "FY4C", now=datetime(2026, 1, 2, 1, 0, tzinfo=TZ_UTC8)
    )
    starts = {item["index"] for item in result["dates"][0]["available_starts"]}
    assert 0 in starts

    # 再删一个桶，缺口达到 5 个，超出容忍上限
    (date_dir / "20260101_040000_FY4C.png").unlink()
    result = compute_available_ranges(
        "FY4C", 24, tmp_path / "FY4C", now=datetime(2026, 1, 2, 1, 0, tzinfo=TZ_UTC8)
    )
    assert result["dates"] == []


def test_scan_videos_groups_by_end_date(tmp_path):
    output = tmp_path / "output"
    _touch(output / "TempC_20260101_1200_to_1300.mp4", datetime(2026, 1, 1, 14, 0, tzinfo=TZ_UTC8))
    _touch(
        output / "RadarGZ_20260101_2300_to_20260102_0100.mp4",
        datetime(2026, 1, 2, 2, 0, tzinfo=TZ_UTC8),
    )
    _touch(
        output / "cron" / "20260101" / "FY4C_20260101_0000_to_2359.mp4",
        datetime(2026, 1, 2, 0, 20, tzinfo=TZ_UTC8),
    )

    groups = scan_videos(output)
    by_date = {group["date"]: group for group in groups}
    assert set(by_date) == {"20260101", "20260102"}
    same_day_names = {v["name"] for v in by_date["20260101"]["videos"]}
    assert "TempC_20260101_1200_to_1300.mp4" in same_day_names
    cross_day_names = [v["name"] for v in by_date["20260102"]["videos"]]
    assert "RadarGZ_20260101_2300_to_20260102_0100.mp4" in cross_day_names
    cron_videos = [v for group in groups for v in group["videos"] if v["cron"]]
    assert cron_videos and cron_videos[0]["name"].startswith("FY4C")


def test_scan_videos_empty_dir(tmp_path):
    assert scan_videos(tmp_path / "missing") == []
