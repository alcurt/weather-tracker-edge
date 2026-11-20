import asyncio

from fastapi.testclient import TestClient

import web


def test_lifespan_cancels_daemons_and_shuts_poller_down(monkeypatch):
    flags = {"shutdown": False, "created": False}

    class FakePoller:
        def __init__(self):
            flags["created"] = True

        async def run_forever(self):
            await asyncio.sleep(3600)

        async def shutdown(self):
            flags["shutdown"] = True

    async def fake_loop(state):
        await asyncio.sleep(3600)

    monkeypatch.setattr(web, "AsyncPoller", FakePoller)
    monkeypatch.setattr(web, "stats_loop", fake_loop)
    monkeypatch.setattr(web, "gc_loop", fake_loop)
    monkeypatch.setattr(web, "cron_loop", fake_loop)
    monkeypatch.setattr(web, "synthesis_worker", fake_loop)

    state = web.AppState()
    app = web.build_app(state)
    with TestClient(app) as client:
        options = client.get("/api/options").json()
        assert options["max_hours"]["香港天文台雷达"] == 3
        assert client.get("/api/options").status_code == 200
        status = client.get("/api/status").json()
        assert "jobs" in status and "queue_size" in status

        rejected = client.post(
            "/api/synthesize",
            json={"display_name": "香港天文台雷达", "hours": 6},
        ).json()
        assert rejected["code"] == 400
        assert "最高支持 3 小时" in rejected["message"]

        first = client.post(
            "/api/synthesize",
            json={"display_name": "气温填色图", "hours": 12},
        ).json()
        assert first["code"] == 200
        assert first["job_id"]

        duplicate = client.post(
            "/api/synthesize",
            json={"display_name": "气温填色图", "hours": 12},
        ).json()
        assert duplicate["code"] == 409

        jobs = client.get("/api/jobs").json()
        assert jobs["queue_size"] == 1
        assert jobs["jobs"][0]["state"] == "queued"

    assert flags["created"] is True
    assert flags["shutdown"] is True


def test_serve_video_rejects_path_traversal(tmp_path):
    (tmp_path / "output").mkdir()
    (tmp_path / "secret.mp4").write_bytes(b"not-a-video")

    status, target = web.resolve_video_path("../secret.mp4", tmp_path)
    assert status == 403
    assert target is None


def test_serve_video_serves_existing_file(tmp_path):
    video = tmp_path / "output" / "cron" / "20260101" / "TempC_20260101_0000_to_2359.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"fake-mp4")

    status, target = web.resolve_video_path("cron/20260101/TempC_20260101_0000_to_2359.mp4", tmp_path)
    assert status == 200
    assert target == video


def test_serve_video_missing_file(tmp_path):
    (tmp_path / "output").mkdir()
    status, target = web.resolve_video_path("missing.mp4", tmp_path)
    assert status == 404
    assert target is None


def test_video_label_and_time_range_are_split():
    same_day = "TempC_20261023_0000_to_2359.mp4"
    assert web.video_label(same_day) == "气温填色图"
    assert web.video_time_range(same_day) == "2026-10-23 00:00  →  2026-10-23 23:59"

    cross_day = "RadarGZ_20261023_2300_to_20261024_0100.mp4"
    assert web.video_label(cross_day) == "广州组合反射率"
    assert web.video_time_range(cross_day) == "2026-10-23 23:00  →  2026-10-24 01:00"

    assert web.video_label("HKORadar_20261120_2112_to_2212.mp4") == "香港天文台雷达"
    assert web.video_time_range("HKORadar_20261120_2112_to_2212.mp4") == "2026-11-20 21:12  →  2026-11-20 22:12"

    assert web.video_label("weird.mp4") == "weird.mp4"
    assert web.video_time_range("weird.mp4") == ""


def test_hko_is_excluded_from_daily_cron():
    assert "HKORadar" in web.CRON_EXCLUDED_TYPES
    assert "HKORadar" in web.all_target_keys
