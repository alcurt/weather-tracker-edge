"""合成任务模型与队列管理。

任务状态是显式的（queued/running/done/failed），进度由 Synthesizer 在
写入管道时回调；WebUI 通过 /api/jobs 或 /api/status 读取，不再只依赖
一句全局 SYNTH_STATUS 字符串。
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from config import TZ_UTC8

JobState = Literal["queued", "running", "done", "failed"]


def _now() -> datetime:
    return datetime.now(TZ_UTC8)


@dataclass
class SynthesisJob:
    id: str
    img_type: str
    display_name: str
    hours: int
    start_time: datetime | None
    is_cron: bool
    state: JobState = "queued"
    created_at: datetime = field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    frames_done: int = 0
    frames_total: int = 0
    output: str | None = None
    error: str | None = None

    def dedupe_key(self) -> tuple:
        return (
            self.img_type,
            self.hours,
            self.start_time.isoformat() if self.start_time else None,
            self.is_cron,
        )

    def set_progress(self, done: int, total: int) -> None:
        self.frames_done = max(0, done)
        self.frames_total = max(0, total)

    @property
    def progress(self) -> int:
        if self.frames_total <= 0:
            return 0
        return min(100, round(self.frames_done / self.frames_total * 100))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "img_type": self.img_type,
            "display_name": self.display_name,
            "hours": self.hours,
            "is_cron": self.is_cron,
            "state": self.state,
            "progress": self.progress,
            "frames_done": self.frames_done,
            "frames_total": self.frames_total,
            "created_at": self.created_at.strftime("%H:%M:%S"),
            "started_at": self.started_at.strftime("%H:%M:%S") if self.started_at else None,
            "finished_at": self.finished_at.strftime("%H:%M:%S") if self.finished_at else None,
            "output": self.output,
            "url": f"/videos/{self.output}" if self.output else None,
            "error": self.error,
        }


class JobManager:
    def __init__(self, max_recent: int = 50):
        self.queue: asyncio.Queue[SynthesisJob] = asyncio.Queue()
        self.jobs: dict[str, SynthesisJob] = {}
        self.order: list[str] = []
        self.max_recent = max_recent

    def submit(
        self,
        *,
        img_type: str,
        display_name: str,
        hours: int,
        start_time: datetime | None = None,
        is_cron: bool = False,
    ) -> SynthesisJob | None:
        key = (img_type, hours, start_time.isoformat() if start_time else None, is_cron)
        for job in self.jobs.values():
            if job.state in ("queued", "running") and job.dedupe_key() == key:
                return None

        job = SynthesisJob(
            id=uuid.uuid4().hex[:12],
            img_type=img_type,
            display_name=display_name,
            hours=hours,
            start_time=start_time,
            is_cron=is_cron,
        )
        self.jobs[job.id] = job
        self.order.append(job.id)
        self.queue.put_nowait(job)
        return job

    def mark_running(self, job: SynthesisJob) -> None:
        job.state = "running"
        job.started_at = _now()
        job.error = None

    def mark_done(self, job: SynthesisJob, output: str | None) -> None:
        job.state = "done"
        job.output = output
        job.finished_at = _now()
        job.set_progress(job.frames_total, job.frames_total)
        self._prune()

    def mark_failed(self, job: SynthesisJob, error: str) -> None:
        job.state = "failed"
        job.error = error
        job.finished_at = _now()
        self._prune()

    def snapshot(self, limit: int = 20) -> list[dict]:
        ordered = [self.jobs[job_id] for job_id in reversed(self.order) if job_id in self.jobs]
        return [job.to_dict() for job in ordered[:limit]]

    def active(self) -> dict | None:
        for job_id in reversed(self.order):
            job = self.jobs.get(job_id)
            if job is not None and job.state == "running":
                return job.to_dict()
        return None

    def queue_size(self) -> int:
        return self.queue.qsize()

    def _prune(self) -> None:
        finished = [job for job in self.jobs.values() if job.state in ("done", "failed")]
        if len(finished) <= self.max_recent:
            return
        finished.sort(key=lambda job: job.finished_at or job.created_at)
        for job in finished[: len(finished) - self.max_recent]:
            self.jobs.pop(job.id, None)
            if job.id in self.order:
                self.order.remove(job.id)
