import asyncio
import json
from datetime import datetime, timedelta

from config import TZ_UTC8
from download_stats import DownloadStats


def test_record_ok_and_fail(tmp_path):
    stats = DownloadStats(tmp_path / "stats.json")
    stats.record("ok")
    stats.record("ok")
    stats.record("fail")
    snap = stats.snapshot(24)
    assert snap[-1]["ok"] == 2
    assert snap[-1]["fail"] == 1
    assert sum(item["ok"] for item in snap) == 2


def test_record_missing_deduplicates_per_source_and_slot(tmp_path):
    stats = DownloadStats(tmp_path / "stats.json")
    stats.record_missing("RadarGZ", "20260101_120000")
    stats.record_missing("RadarGZ", "20260101_120000")
    stats.record_missing("RadarGZ", "20260101_120600")
    stats.record_missing("RadarGD", "20260101_120000")
    assert stats.snapshot(24)[-1]["missing"] == 3


def test_flush_and_reload(tmp_path):
    path = tmp_path / "stats.json"
    stats = DownloadStats(path)
    stats.record("ok")
    stats.record_missing("FY4C", "20260101_120000")
    asyncio.run(stats.flush())

    reloaded = DownloadStats(path)
    snap = reloaded.snapshot(24)
    assert snap[-1]["ok"] == 1
    assert snap[-1]["missing"] == 1
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["hours"]


def test_prune_drops_old_hours(tmp_path):
    stats = DownloadStats(tmp_path / "stats.json")
    old_key = (datetime.now(TZ_UTC8) - timedelta(hours=100)).strftime("%Y%m%d%H")
    stats._hours[old_key] = {"ok": 1, "fail": 0, "missing": 0, "missing_seen": []}
    stats._prune()
    assert old_key not in stats._hours


def test_snapshot_is_ordered_old_to_new(tmp_path):
    stats = DownloadStats(tmp_path / "stats.json")
    snap = stats.snapshot(24)
    assert len(snap) == 24
    assert snap[0]["key"] <= snap[-1]["key"]
