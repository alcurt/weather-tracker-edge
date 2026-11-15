import asyncio

import web


def test_synthesis_worker_updates_job_state(tmp_path, monkeypatch):
    output = tmp_path / "output" / "TempC_test.mp4"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"fake")

    class FakeSynthesizer:
        def generate_video(self, img_type, hours, is_cron, fps, start_time, progress_cb):
            progress_cb(0, 2)
            progress_cb(1, 2)
            progress_cb(2, 2)
            return output

    monkeypatch.setattr(web, "Synthesizer", FakeSynthesizer)
    state = web.AppState(data_dir=tmp_path)
    job = state.jobs.submit(img_type="TempC", display_name="气温填色图", hours=12)
    assert job is not None

    async def scenario():
        task = asyncio.create_task(web.synthesis_worker(state))
        await state.jobs.queue.join()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert job.state == "done"
    assert job.output == "TempC_test.mp4"
    assert job.to_dict()["url"] == "/videos/TempC_test.mp4"


def test_synthesis_worker_marks_failure(tmp_path, monkeypatch):
    class FakeSynthesizer:
        def generate_video(self, *args, **kwargs):
            return None

    monkeypatch.setattr(web, "Synthesizer", FakeSynthesizer)
    state = web.AppState(data_dir=tmp_path)
    job = state.jobs.submit(img_type="TempC", display_name="气温填色图", hours=12)

    async def scenario():
        task = asyncio.create_task(web.synthesis_worker(state))
        await state.jobs.queue.join()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert job.state == "failed"
    assert "数据不完整" in job.error
