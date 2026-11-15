import asyncio
from datetime import datetime, timedelta

import poller as poller_module
from config import TZ_UTC8
from fetch import SlotProbeCache


class RecordingStats:
    def __init__(self):
        self.ok = 0
        self.fail = 0
        self.missing = []

    def record(self, category):
        if category == "ok":
            self.ok += 1
        else:
            self.fail += 1

    def record_missing(self, source, key):
        self.missing.append((source, key))


def test_pending_404_not_counted_before_grace(monkeypatch):
    stats = RecordingStats()
    monkeypatch.setattr(poller_module, "download_stats", stats)
    poller = object.__new__(poller_module.AsyncPoller)
    now = datetime.now(TZ_UTC8)
    fresh = (now - timedelta(minutes=5)).strftime("%Y%m%d_%H%M%S")
    old = (now - timedelta(minutes=25)).strftime("%Y%m%d_%H%M%S")

    poller._record_download_results(
        "RadarGZ", ["404", "404"],
        item_keys=[fresh, old],
        pending_grace_minutes=20,
        now=now,
    )
    assert stats.missing == [("RadarGZ", old)]


def test_static_404_still_counted_as_missing(monkeypatch):
    stats = RecordingStats()
    monkeypatch.setattr(poller_module, "download_stats", stats)
    poller = object.__new__(poller_module.AsyncPoller)

    poller._record_download_results(
        "TempC", ["404"],
        item_keys=None,
        pending_grace_minutes=20,
        now=datetime.now(TZ_UTC8),
    )
    assert stats.missing == [("TempC", "static")]


def test_has_recent_frames_detects_local_data(tmp_path, monkeypatch):
    monkeypatch.setattr(poller_module, "DATA_DIR", tmp_path)
    now = datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    recent = tmp_path / "FY4C" / "20260102" / "20260102_113000_FY4C.png"
    recent.parent.mkdir(parents=True)
    recent.write_bytes(b"x")

    poller = object.__new__(poller_module.AsyncPoller)
    assert poller._has_recent_frames("FY4C", now, 1) is True
    assert poller._has_recent_frames("FY4C", now, 0) is False
    assert poller._has_recent_frames("RadarGZ", now, 1) is False


def test_radar_backfill_only_probes_mature_slots():
    poller = object.__new__(poller_module.AsyncPoller)
    poller.probes = SlotProbeCache()
    fetched = []

    async def fake_fetch(radar_type, target_time, max_retries=None):
        fetched.append(target_time)
        return "EXIST"

    poller._fetch_single_radar = fake_fetch
    poller._record_download_results = lambda *args, **kwargs: None

    now = datetime(2026, 1, 2, 12, 3, tzinfo=TZ_UTC8)
    asyncio.run(poller._backfill_radar_source("RadarGZ", now))

    assert fetched
    assert max(fetched) <= now - timedelta(minutes=poller_module.RADAR_MIN_AGE_MINUTES)
    # 6 小时 / 6 分钟 = 60 个成熟时次（当前未成熟时次被排除）
    assert len(fetched) == 60
