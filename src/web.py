"""Web 应用装配：HTTP 路由、后台协程与可注入的运行状态。

``main.py`` 只保留入口；应用状态放进 ``AppState``，路由与 lifespan 都从
app.state.runtime 读取，避免再依赖模块级可变单例。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from analytics import compute_available_ranges, scan_videos, summarize_24h
from cleaner import StorageCleaner
from config import DATA_DIR, TZ_UTC8
from download_stats import download_stats
from frames import FrameCatalog
from jobs import JobManager
from logbus import LOG_BUFFER
from poller import AsyncPoller
from synthesizer import Synthesizer

logger = logging.getLogger(__name__)

display_groups = {
    "广州观测实况": {
        "TempB": "气温分布图",
        "TempC": "气温填色图",
        "RainB": "本小时雨量分布图",
        "RainC": "本小时雨量填色图",
        "Rain24hB": "24小时雨量分布图",
        "Rain24hC": "24小时雨量填色图",
        "WindB": "风速分布图",
        "WindC": "风速填色图",
    },
    "天气雷达回波": {
        "RadarGZ": "广州组合反射率",
        "RadarGD": "广东雷达拼图",
    },
    "风云4C卫星云图": {
        "FY4C": "风云4C云图",
    },
}
flat_display_map = {k: v for group in display_groups.values() for k, v in group.items()}
reverse_map = {v: k for k, v in flat_display_map.items()}
all_target_keys = list(flat_display_map.keys())

# 每日 00:15 昨日总结排除的信源（四个 B 类分布图取消；FY4C 纳入定时任务）
CRON_EXCLUDED_TYPES = {"TempB", "RainB", "Rain24hB", "WindB"}
SUPPORTED_HOURS = (1, 3, 6, 12, 24)
FPS_BY_HOURS = {1: 4.0, 3: 6.0, 6: 8.0, 12: 9.6, 24: 14.4}

RANGE_GRANULARITY_MINUTES = 30
RANGES_CACHE_TTL = 60

# 健康检查：启动后给采集器一个宽限期；之后任一信源超过 10 分钟没有完成 tick 即降级
HEALTH_STARTUP_GRACE_SECONDS = 180
HEALTH_MAX_SILENCE_SECONDS = 600

HTML_TEMPLATE = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


def get_dynamic_fps(hours: int) -> float:
    return FPS_BY_HOURS.get(hours, 10.0)


def parse_start_time(value: str) -> datetime:
    """解析前端传入的显式合成起点，格式 YYYYMMDD_HHMM（北京时间）。"""
    return datetime.strptime(value, "%Y%m%d_%H%M").replace(tzinfo=TZ_UTC8)


def cron_due(now: datetime, started_at: datetime, last_triggered_date) -> bool:
    """该分钟是否应触发昨日总结。

    只有进程在当日 00:15 之前已经启动、且跨过 00:15 时才触发；
    启动时已过 00:15 则当天不补跑，符合"启动不补历史定时任务"的约定。
    """
    boundary = now.replace(hour=0, minute=15, second=0, microsecond=0)
    if boundary <= started_at or now < boundary:
        return False
    return last_triggered_date != now.date()


@dataclass
class AppState:
    data_dir: Path = DATA_DIR
    start_time: float = field(default_factory=time.time)
    jobs: JobManager = field(default_factory=JobManager)
    synth_status: dict = field(
        default_factory=lambda: {
            "is_running": False,
            "message": "后台抓取守护中 (待命)",
            "type": "success",
        }
    )
    global_stats: dict = field(default_factory=lambda: {"records": [], "last_updated": "--:--:--"})
    ranges_cache: dict = field(default_factory=dict)
    poller: AsyncPoller | None = None


def collect_stats(
    now: datetime,
    session_counts: dict,
    last_cursors: dict,
    catalog: FrameCatalog,
    data_dir: Path = DATA_DIR,
) -> list:
    """采集一次 UI 聚合数据（纯同步，便于单测）。"""
    records = []
    for key in all_target_keys:
        cursor_file = data_dir / key / ".latest_cursor.json"
        if cursor_file.exists():
            try:
                with open(cursor_file, encoding="utf-8") as f:
                    current_path = json.load(f).get("file_path", "")
                    if current_path and last_cursors[key] and current_path != last_cursors[key]:
                        session_counts[key] += 1
                    last_cursors[key] = current_path
            except Exception:
                pass

        timestamps = []
        for days_back in (0, 1):
            date_str = (now - timedelta(days=days_back)).strftime("%Y%m%d")
            timestamps.extend(catalog.timestamps(key, date_str))

        summary = summarize_24h(timestamps, now)
        latest_ts = max(timestamps) if timestamps else None
        records.append({
            "key": key,
            "display_name": flat_display_map[key],
            "avail_24h": summary["count"],
            "session_dl": session_counts[key],
            "hourly": summary["hourly"],
            "latest_time": latest_ts.strftime("%H:%M") if latest_ts else "--:--",
            "earliest_time": summary["earliest"].strftime("%H:%M") if summary["earliest"] else "--:--",
        })
    return records


async def stats_loop(state: AppState):
    session_counts = {k: 0 for k in all_target_keys}
    last_cursors = {k: "" for k in all_target_keys}
    catalog = FrameCatalog(state.data_dir)

    for key in all_target_keys:
        cursor_file = state.data_dir / key / ".latest_cursor.json"
        if cursor_file.exists():
            try:
                with open(cursor_file, encoding="utf-8") as f:
                    last_cursors[key] = json.load(f).get("file_path", "")
            except Exception:
                pass

    while True:
        try:
            now = datetime.now(TZ_UTC8)
            records = await asyncio.to_thread(
                collect_stats, now, session_counts, last_cursors, catalog, state.data_dir
            )
            state.global_stats["records"] = records
            state.global_stats["last_updated"] = now.strftime("%H:%M:%S")
        except Exception as e:
            logger.error(f"UI数据观测者发生异常: {e}")

        await asyncio.sleep(5)


async def gc_loop(state: AppState | None = None):
    cleaner = StorageCleaner()
    await asyncio.to_thread(cleaner.run_gc)
    while True:
        try:
            now = datetime.now(TZ_UTC8)
            target_time = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
            await asyncio.sleep((target_time - now).total_seconds())
            await asyncio.to_thread(cleaner.run_gc)
        except Exception as e:
            logger.error(f"垃圾回收器发生异常: {e}")
            await asyncio.sleep(300)


async def cron_loop(state: AppState):
    """每日 00:15 且进程已在线跨过该时刻时触发昨日总结；启动时不补跑历史任务。"""
    started_at = datetime.now(TZ_UTC8)
    last_triggered_date = None
    while True:
        try:
            now = datetime.now(TZ_UTC8)
            if cron_due(now, started_at, last_triggered_date):
                last_triggered_date = now.date()
                tasks = [
                    (k, v) for k, v in flat_display_map.items()
                    if k not in CRON_EXCLUDED_TYPES
                ]
                logger.info(f"触发昨日总结，准备向队列压入 {len(tasks)} 个后台合成任务。")
                submitted = 0
                for key, name in tasks:
                    job = state.jobs.submit(
                        img_type=key,
                        display_name=f"{name} (昨日总结)",
                        hours=24,
                        is_cron=True,
                    )
                    if job is not None:
                        submitted += 1
                logger.info(f"昨日总结已入队 {submitted} 个任务。")
                await state.jobs.queue.join()
                logger.info("昨日总结已全部完成。")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"每日总结定时器异常: {e}")
        await asyncio.sleep(30)


async def synthesis_worker(state: AppState):
    while True:
        job = await state.jobs.queue.get()
        try:
            state.jobs.mark_running(job)
            display_name = job.display_name
            state.synth_status = {
                "is_running": True,
                "message": f"正在后台压制 {display_name} (队列计算中...)",
                "type": "warning",
            }
            loop = asyncio.get_running_loop()

            def on_progress(
                done: int, total: int, target_job=job, event_loop=loop
            ) -> None:
                event_loop.call_soon_threadsafe(target_job.set_progress, done, total)

            synth = Synthesizer()
            try:
                output = await asyncio.to_thread(
                    synth.generate_video,
                    job.img_type,
                    job.hours,
                    job.is_cron,
                    get_dynamic_fps(job.hours),
                    job.start_time,
                    on_progress,
                )
                if output is not None:
                    try:
                        relative = output.relative_to(state.data_dir / "output").as_posix()
                    except ValueError:
                        relative = output.name
                    state.jobs.mark_done(job, relative)
                    state.synth_status = {
                        "is_running": False,
                        "message": f"[{display_name}] 合成完毕，已落盘。",
                        "type": "success",
                    }
                else:
                    state.jobs.mark_failed(job, "数据不完整或 FFmpeg 压制失败")
                    state.synth_status = {
                        "is_running": False,
                        "message": f"[{display_name}] 合成失败，请检查数据完整性。",
                        "type": "error",
                    }
            except Exception as e:
                state.jobs.mark_failed(job, str(e))
                state.synth_status = {
                    "is_running": False,
                    "message": f"[{display_name}] 管线崩溃: {str(e)}",
                    "type": "error",
                }

            if job.is_cron:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"合成工作机致命异常: {e}")
            await asyncio.sleep(1)
        finally:
            state.jobs.queue.task_done()


def resolve_video_path(path: str, data_dir: Path) -> tuple[int, Path | None]:
    """返回 (HTTP 状态码, 目标路径)：403 越界、404 不存在、200 可服务。"""
    output_dir = data_dir / "output"
    base = str(output_dir.resolve())
    target = str((output_dir / path).resolve())
    try:
        inside = os.path.commonpath([base, target]) == base
    except ValueError:
        inside = False
    if not inside:
        return 403, None
    if not os.path.isfile(target):
        return 404, None
    return 200, Path(target)


def health_snapshot(state: AppState) -> tuple[int, dict]:
    """返回 (HTTP 状态码, 健康快照)。

    启动 180 秒内允许 INIT；之后任一信源 10 分钟没有完成 tick 即视为 degraded。
    """
    now = time.time()
    uptime = int(now - state.start_time)
    metrics = getattr(state.poller, "metrics", {}) if state.poller is not None else {}
    sources = []
    stale = []
    all_started = True
    for key in all_target_keys:
        metric = metrics.get(key, {})
        last_updated = float(metric.get("updated_at") or 0.0)
        age = int(now - last_updated) if last_updated else None
        if last_updated:
            healthy = age <= HEALTH_MAX_SILENCE_SECONDS
        else:
            # 尚未完成首轮（例如 FY4C 回补中）：最长给 10 分钟启动时间
            healthy = uptime < HEALTH_MAX_SILENCE_SECONDS
            all_started = False
        if not healthy:
            stale.append(key)
        sources.append({
            "key": key,
            "display_name": flat_display_map[key],
            "status": metric.get("status", "INIT"),
            "last_tick_age_seconds": age,
            "healthy": healthy,
        })

    if uptime < HEALTH_STARTUP_GRACE_SECONDS or (not all_started and uptime < HEALTH_MAX_SILENCE_SECONDS):
        overall = "starting"
    elif stale:
        overall = "degraded"
    else:
        overall = "ok"

    payload = {
        "status": overall,
        "uptime": uptime,
        "max_silence_seconds": HEALTH_MAX_SILENCE_SECONDS,
        "sources": sources,
        "jobs": {
            "queue_size": state.jobs.queue_size(),
            "active": state.jobs.active(),
        },
    }
    return (200 if overall in ("ok", "starting") else 503), payload


def status_payload(state: AppState) -> dict:
    status_copy = dict(state.synth_status)
    qsize = state.jobs.queue_size()
    if status_copy["is_running"] and qsize > 0:
        status_copy["message"] += f" (排队等待中: {qsize} 项)"

    stats_out = dict(state.global_stats)
    stats_out["uptime"] = int(time.time() - state.start_time)
    return {
        "stats": stats_out,
        "synth": status_copy,
        "jobs": state.jobs.snapshot(10),
        "active_job": state.jobs.active(),
        "queue_size": qsize,
        "downloads": {
            "hours": download_stats.snapshot(24),
            "totals": download_stats.totals(24),
        },
    }


class SynthRequest(BaseModel):
    display_name: str
    hours: int
    start_time: str | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    state: AppState = app.state.runtime
    poller = AsyncPoller()
    state.poller = poller
    tasks = [
        asyncio.create_task(poller.run_forever()),
        asyncio.create_task(stats_loop(state)),
        asyncio.create_task(gc_loop(state)),
        asyncio.create_task(cron_loop(state)),
        asyncio.create_task(synthesis_worker(state)),
    ]
    logger.info("系统总线接入完毕，所有异步守护任务已激活并由护城河保护。")
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"守护任务退出时抛出异常: {result}")
        await poller.shutdown()
        await download_stats.flush()
        logger.info("系统已停止，采集任务与连接池已释放。")


def build_app(state: AppState) -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.state.runtime = state

    @app.get("/api/status")
    async def get_status():
        return status_payload(state)

    @app.get("/api/health")
    async def get_health():
        code, payload = health_snapshot(state)
        return JSONResponse(payload, status_code=code)

    @app.get("/api/events")
    async def events(request: Request):
        """状态 SSE：仅在快照变化时推送，浏览器断线可回退到 /api/status 轮询。"""

        async def stream():
            last_key = None
            while True:
                if await request.is_disconnected():
                    break
                payload = status_payload(state)
                key = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
                if key != last_key:
                    last_key = key
                    yield f"data: {key}\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/jobs")
    async def get_jobs():
        return {"code": 200, "jobs": state.jobs.snapshot(30), "queue_size": state.jobs.queue_size()}

    @app.get("/api/options")
    async def get_options():
        return {"groups": {group: list(types.values()) for group, types in display_groups.items()}}

    @app.get("/api/ranges")
    async def get_ranges(display_name: str, hours: int):
        img_type = reverse_map.get(display_name)
        if not img_type:
            return {
                "code": 400,
                "granularity_minutes": RANGE_GRANULARITY_MINUTES,
                "dates": [],
                "message": "无效的气象图类型",
            }
        if hours not in SUPPORTED_HOURS:
            return {
                "code": 400,
                "granularity_minutes": RANGE_GRANULARITY_MINUTES,
                "dates": [],
                "message": "不支持的合成跨度",
            }
        now_ts = time.time()
        cache_key = (img_type, hours)
        cached = state.ranges_cache.get(cache_key)
        if cached and cached[0] > now_ts:
            return {
                "code": 200,
                "granularity_minutes": cached[1]["granularity_minutes"],
                "dates": cached[1]["dates"],
            }
        data = await asyncio.to_thread(
            compute_available_ranges,
            img_type,
            hours,
            state.data_dir / img_type,
            datetime.now(TZ_UTC8),
        )
        state.ranges_cache[cache_key] = (now_ts + RANGES_CACHE_TTL, data)
        return {"code": 200, "granularity_minutes": data["granularity_minutes"], "dates": data["dates"]}

    @app.get("/api/logs")
    async def get_logs():
        return {"logs": list(LOG_BUFFER)}

    @app.get("/api/videos")
    async def get_videos():
        groups = await asyncio.to_thread(scan_videos, state.data_dir / "output")
        return {"code": 200, "groups": groups}

    @app.get("/videos/{path:path}")
    async def serve_video(path: str):
        status, target = resolve_video_path(path, state.data_dir)
        if status == 403:
            return HTMLResponse("Forbidden", status_code=403)
        if status == 404:
            return HTMLResponse("Not Found", status_code=404)
        return FileResponse(target, media_type="video/mp4")

    @app.post("/api/synthesize")
    async def trigger_synthesize(req: SynthRequest):
        img_type = reverse_map.get(req.display_name)
        if not img_type:
            return {"code": 400, "message": "无效的气象图类型"}
        if req.hours not in SUPPORTED_HOURS:
            return {"code": 400, "message": "不支持的合成跨度，仅支持 1/3/6/12/24 小时"}
        start_time = None
        if req.start_time:
            try:
                start_time = parse_start_time(req.start_time)
            except ValueError:
                return {"code": 400, "message": "start_time 格式应为 YYYYMMDD_HHMM"}
            if start_time > datetime.now(TZ_UTC8):
                return {"code": 400, "message": "起始时刻不能晚于当前时间"}
        job = state.jobs.submit(
            img_type=img_type,
            display_name=req.display_name,
            hours=req.hours,
            start_time=start_time,
            is_cron=False,
        )
        if job is None:
            return {"code": 409, "message": "相同任务已在队列或执行中"}
        return {"code": 200, "message": "任务已加入队列", "job_id": job.id}

    @app.get("/", response_class=HTMLResponse)
    async def serve_dashboard():
        return HTML_TEMPLATE

    return app
