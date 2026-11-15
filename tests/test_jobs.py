from datetime import datetime

from config import TZ_UTC8
from jobs import JobManager


def test_submit_deduplicates_active_jobs():
    manager = JobManager()
    start = datetime(2026, 1, 1, 12, 0, tzinfo=TZ_UTC8)
    job = manager.submit(img_type="TempC", display_name="气温填色图", hours=12, start_time=start)
    assert job is not None

    duplicate = manager.submit(img_type="TempC", display_name="气温填色图", hours=12, start_time=start)
    assert duplicate is None
    assert manager.queue_size() == 1

    manager.mark_running(job)
    assert manager.active()["id"] == job.id
    manager.mark_done(job, "TempC_20260101_1200_to_2359.mp4")
    assert job.state == "done"
    assert job.to_dict()["url"] == "/videos/TempC_20260101_1200_to_2359.mp4"

    # 完成后允许再次提交同一参数
    assert manager.submit(img_type="TempC", display_name="气温填色图", hours=12, start_time=start) is not None


def test_progress_and_snapshot_order():
    manager = JobManager()
    first = manager.submit(img_type="TempC", display_name="气温填色图", hours=1)
    second = manager.submit(img_type="RadarGZ", display_name="广州组合反射率", hours=3)
    assert first is not None and second is not None

    first.set_progress(5, 10)
    assert first.progress == 50
    assert [item["id"] for item in manager.snapshot(2)] == [second.id, first.id]

    manager.mark_failed(second, "boom")
    data = second.to_dict()
    assert data["state"] == "failed"
    assert data["error"] == "boom"
    assert data["progress"] == 0
