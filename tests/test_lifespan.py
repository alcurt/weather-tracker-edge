import asyncio

import main


def test_lifespan_cancels_daemons_and_shuts_poller_down(monkeypatch):
    state = {"shutdown": False}

    class FakePoller:
        def __init__(self):
            state["created"] = True

        async def run_forever(self):
            await asyncio.sleep(3600)

        async def shutdown(self):
            state["shutdown"] = True

    async def fake_loop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(main, "AsyncPoller", FakePoller)
    monkeypatch.setattr(main, "stats_loop", fake_loop)
    monkeypatch.setattr(main, "gc_loop", fake_loop)
    monkeypatch.setattr(main, "cron_loop", fake_loop)
    monkeypatch.setattr(main, "synthesis_worker", fake_loop)

    async def scenario():
        async with main.lifespan(main.app):
            await asyncio.sleep(0.05)

    asyncio.run(scenario())
    assert state["created"] is True
    assert state["shutdown"] is True


def test_serve_video_rejects_path_traversal(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    (tmp_path / "output").mkdir()
    (tmp_path / "secret.mp4").write_bytes(b"not-a-video")

    response = asyncio.run(main.serve_video("../secret.mp4"))
    assert response.status_code == 403


def test_serve_video_serves_existing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DATA_DIR", tmp_path)
    video = tmp_path / "output" / "cron" / "20260101" / "TempC_20260101_0000_to_2359.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"fake-mp4")

    response = asyncio.run(main.serve_video("cron/20260101/TempC_20260101_0000_to_2359.mp4"))
    assert response.status_code == 200
