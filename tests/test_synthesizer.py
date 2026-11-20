from datetime import datetime

from config import TZ_UTC8
from synthesizer import Synthesizer, build_output_path


def test_same_day_output_name_and_location(tmp_path):
    start = datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8)
    end = datetime(2026, 1, 1, 13, 0, tzinfo=TZ_UTC8)
    path = build_output_path(tmp_path, "TempC", start, end, is_cron=False)
    assert path.parent == tmp_path
    assert path.name == "TempC_20260101_1200_to_1300.mp4"


def test_cross_day_output_includes_end_date(tmp_path):
    start = datetime(2026, 1, 1, 23, 0, tzinfo=TZ_UTC8)
    end = datetime(2026, 1, 2, 1, 0, tzinfo=TZ_UTC8)
    path = build_output_path(tmp_path, "RadarGZ", start, end, is_cron=False)
    assert path.name == "RadarGZ_20260101_2300_to_20260102_0100.mp4"


def test_cron_output_goes_into_dated_subdirectory(tmp_path):
    start = datetime(2026, 1, 1, 0, 0, tzinfo=TZ_UTC8)
    end = datetime(2026, 1, 1, 23, 59, tzinfo=TZ_UTC8)
    path = build_output_path(tmp_path, "FY4C", start, end, is_cron=True)
    assert path.parent == tmp_path / "cron" / "20260101"


def test_collect_frames_filters_window_and_sorts(tmp_path):
    type_dir = tmp_path / "TempC"
    frames = {
        "20260101_110000_TempC.png": datetime(2026, 1, 1, 11, 0, tzinfo=TZ_UTC8),
        "20260101_120000_TempC.png": datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8),
        "20260101_130000_TempC.png": datetime(2026, 1, 1, 13, 0, tzinfo=TZ_UTC8),
        "20260101_121500_HKORadar.jpg": datetime(2026, 1, 1, 12, 15, tzinfo=TZ_UTC8),
        "notes.txt": datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8),
    }
    for name, ts in frames.items():
        path = type_dir / "20260101" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        import os

        os.utime(path, (ts.timestamp(), ts.timestamp()))

    synth = Synthesizer()
    start = datetime(2026, 1, 1, 11, 30, tzinfo=TZ_UTC8)
    end = datetime(2026, 1, 1, 12, 30, tzinfo=TZ_UTC8)
    collected = synth._collect_frames(type_dir, start, end)
    assert [ts for _, ts in collected] == [
        datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8),
        datetime(2026, 1, 1, 12, 15, tzinfo=TZ_UTC8),
    ]
