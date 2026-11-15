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
        assert client.get("/api/options").status_code == 200
        status = client.get("/api/status").json()
        assert "jobs" in status and "queue_size" in status

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
