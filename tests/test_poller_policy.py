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


def test_grid_slots_are_newest_first():
    now = datetime(2026, 1, 2, 12, 3, tzinfo=TZ_UTC8)
    slots = poller_module.AsyncPoller._grid_slots(now, 10, 0, 60)
    assert slots[0] == datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)
    assert slots[-1] == datetime(2026, 1, 2, 11, 0, tzinfo=TZ_UTC8)
    assert len(slots) == 7

    inner = poller_module.AsyncPoller._grid_slots(now, 10, 20, 60)
    assert inner[0] == datetime(2026, 1, 2, 11, 40, tzinfo=TZ_UTC8)
    assert inner[-1] == datetime(2026, 1, 2, 11, 0, tzinfo=TZ_UTC8)
    assert len(inner) == 5


def test_available_nmc_minutes_binary_search():
    async def fake_probe(img_type, now, minutes):
        return minutes <= 1500

    poller = object.__new__(poller_module.AsyncPoller)
    poller._probe_nmc_depth = fake_probe
    now = datetime(2026, 1, 2, 12, 0, tzinfo=TZ_UTC8)

    available = asyncio.run(poller._available_nmc_minutes("FY4C", now, 48 * 60))
    assert available == 1500


def test_backfill_nmc_source_runs_two_phases(monkeypatch):
    async def fake_available(img_type, now, target_minutes):
        return 1500  # 上游实际只有 25 小时

    calls = []

    async def fake_fetch(img_type, slots, label):
        calls.append((label, len(slots)))
        return {}

    poller = object.__new__(poller_module.AsyncPoller)
    poller._available_nmc_minutes = fake_available
    poller._fetch_nmc_slots = fake_fetch
    monkeypatch.setattr(poller_module, "NMC_BACKFILL_PHASE_GAP_SECONDS", 0)

    asyncio.run(poller._backfill_nmc_source("FY4C"))
    assert [label for label, _ in calls] == ["0-24.0h", "24.0-25.0h"]
    assert calls[0][1] == 144  # 0-24h 中排除未成熟的当前时次
    assert calls[1][1] == 6    # 24-25h 的 6 个时次


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
    # 20 小时 / 6 分钟 = 200 个成熟时次（当前未成熟时次被排除）
    assert len(fetched) == 200


def test_radar_loop_tick_runs_without_network(monkeypatch):
    fetched = []

    async def noop(*args, **kwargs):
        return None

    async def fake_fetch(radar_type, target_time, max_retries=None):
        fetched.append(target_time)
        return "404"

    async def scenario():
        poller = object.__new__(poller_module.AsyncPoller)
        poller.probes = SlotProbeCache()
        poller.metrics = {}
        poller._last_backfill = {}
        poller._ensure_client = noop
        poller._backfill_radar_source = noop
        poller._fetch_single_radar = fake_fetch
        poller._record_download_results = lambda *args, **kwargs: None

        task = asyncio.create_task(poller._loop_radar_source("RadarGZ"))
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return poller.metrics

    metrics = asyncio.run(scenario())
    assert fetched
    assert metrics["RadarGZ"]["status"] == "304"
