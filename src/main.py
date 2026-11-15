import asyncio
import collections
import json
import logging
import os
import socket
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

from analytics import (
    compute_available_ranges,
    parse_frame_timestamp,
    scan_videos,
    summarize_24h,
)
from cleaner import StorageCleaner
from config import DATA_DIR, TZ_UTC8
from download_stats import download_stats
from poller import AsyncPoller
from synthesizer import Synthesizer

# ==========================================
# 记录服务器物理启动时间的绝对锚点
# ==========================================
SERVER_START_TIME = time.time()

# ==========================================
# 动态获取局域网 IP 与 日志系统配置
# ==========================================
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

ROOT_DIR = DATA_DIR.parent
LOG_DIR = ROOT_DIR / "logs"

LOG_BUFFER = collections.deque(maxlen=200)

class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))

memory_handler = MemoryLogHandler()
memory_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_logging_configured = False


def configure_logging() -> None:
    """配置控制台 + 轮转文件 + 内存终端三路日志。

    只在入口调用，避免 import main 时产生文件写入等副作用（便于测试）。
    """
    global _logging_configured
    if _logging_configured:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            RotatingFileHandler(
                LOG_DIR / "tracker.log",
                maxBytes=10 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            ),
            memory_handler,
        ],
        force=True,
    )
    # 上游请求明细由采集器自己记录，保留 httpx 的 INFO 会淹没业务日志
    for noisy in ("httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _logging_configured = True


logger = logging.getLogger(__name__)

# ==========================================
# 状态与队列系统
# ==========================================
GLOBAL_STATS = {"records": [], "last_updated": "--:--:--"}
SYNTH_STATUS = {"is_running": False, "message": "后台抓取守护中 (待命)", "type": "success"}
SYNTH_QUEUE = asyncio.Queue()

display_groups = {
    "广州观测实况": {
        "TempB": "气温分布图",
        "TempC": "气温填色图",
        "RainB": "本小时雨量分布图",
        "RainC": "本小时雨量填色图",
        "Rain24hB": "24小时雨量分布图",
        "Rain24hC": "24小时雨量填色图",
        "WindB": "风速分布图",
        "WindC": "风速填色图"
    },
    "天气雷达回波": {
        "RadarGZ": "广州组合反射率",
        "RadarGD": "广东雷达拼图"
    },
    "风云4C卫星云图": {
        "FY4C": "风云4C云图"
    }
}
flat_display_map = {k: v for group in display_groups.values() for k, v in group.items()}
reverse_map = {v: k for k, v in flat_display_map.items()}
all_target_keys = list(flat_display_map.keys())

# 每日 00:15 昨日总结排除的信源（四个 B 类分布图取消；FY4C 纳入定时任务）
CRON_EXCLUDED_TYPES = {"TempB", "RainB", "Rain24hB", "WindB"}

SUPPORTED_HOURS = (1, 3, 6, 12, 24)

FPS_BY_HOURS = {1: 4.0, 3: 6.0, 6: 8.0, 12: 9.6, 24: 14.4}

def get_dynamic_fps(hours: int) -> float:
    return FPS_BY_HOURS.get(hours, 10.0)


def cron_due(now: datetime, started_at: datetime, last_triggered_date) -> bool:
    """该分钟是否应触发昨日总结。

    只有进程在当日 00:15 之前已经启动、且跨过 00:15 时才触发；
    启动时已过 00:15 则当天不补跑，符合"启动不补历史定时任务"的约定。
    """
    boundary = now.replace(hour=0, minute=15, second=0, microsecond=0)
    if boundary <= started_at or now < boundary:
        return False
    return last_triggered_date != now.date()

# ==========================================
# 后台异步守护协程 (元数据门铃监听)
# ==========================================
def collect_stats(now: datetime, session_counts: dict, last_cursors: dict, dir_cache: dict) -> list:
    """采集一次 UI 聚合数据（纯同步，便于单测）。

    dir_cache 只缓存"目录 mtime -> 原始时间戳列表"，聚合值每次按 now 现算，
    因此时间推移不会让覆盖带/24h 帧数停留在旧位置。
    """
    records = []
    active_dates = {
        (k, (now - timedelta(days=days_back)).strftime("%Y%m%d"))
        for k in all_target_keys
        for days_back in (0, 1)
    }
    for stale_key in [key for key in dir_cache if key not in active_dates]:
        dir_cache.pop(stale_key, None)

    for k in all_target_keys:
        # 1. 游标对比计算本次会话新增（近似值：每 5 秒采样一次）
        cursor_file = DATA_DIR / k / ".latest_cursor.json"
        if cursor_file.exists():
            try:
                with open(cursor_file, encoding="utf-8") as f:
                    current_path = json.load(f).get("file_path", "")
                    if current_path and last_cursors[k] and current_path != last_cursors[k]:
                        session_counts[k] += 1
                    last_cursors[k] = current_path
            except Exception:
                pass

        # 2. 元数据门铃：只读取今天和昨天两个目录，mtime 不变则不重扫
        timestamps = []
        type_dir = DATA_DIR / k
        if type_dir.exists():
            for days_back in (0, 1):
                date_str = (now - timedelta(days=days_back)).strftime("%Y%m%d")
                date_dir = type_dir / date_str
                if not date_dir.is_dir():
                    continue
                try:
                    current_mtime = date_dir.stat().st_mtime
                except OSError:
                    continue
                cache_key = (k, date_str)
                cached = dir_cache.get(cache_key)
                if cached is None or cached["mtime"] != current_mtime:
                    day_timestamps = []
                    try:
                        with os.scandir(date_dir) as entries:
                            for entry in entries:
                                if not entry.name.endswith(".png"):
                                    continue
                                ts = parse_frame_timestamp(entry.name)
                                if ts is not None:
                                    day_timestamps.append(ts)
                    except OSError:
                        continue
                    cached = {"mtime": current_mtime, "timestamps": day_timestamps}
                    dir_cache[cache_key] = cached
                timestamps.extend(cached["timestamps"])

        summary = summarize_24h(timestamps, now)
        latest_ts = max(timestamps) if timestamps else None

        records.append({
            "key": k,
            "display_name": flat_display_map[k],
            "avail_24h": summary["count"],
            "session_dl": session_counts[k],
            "hourly": summary["hourly"],
            "latest_time": latest_ts.strftime("%H:%M") if latest_ts else "--:--",
            "earliest_time": summary["earliest"].strftime("%H:%M") if summary["earliest"] else "--:--",
        })
    return records


async def stats_loop():
    session_counts = {k: 0 for k in all_target_keys}
    last_cursors = {k: "" for k in all_target_keys}

    # 内存缓存：{(img_type, YYYYMMDD): {"mtime": float, "timestamps": [datetime]}}
    dir_cache = {}

    for k in all_target_keys:
        cursor_file = DATA_DIR / k / ".latest_cursor.json"
        if cursor_file.exists():
            try:
                with open(cursor_file, encoding="utf-8") as f:
                    last_cursors[k] = json.load(f).get("file_path", "")
            except Exception:
                pass

    while True:
        try:
            now = datetime.now(TZ_UTC8)
            records = await asyncio.to_thread(collect_stats, now, session_counts, last_cursors, dir_cache)
            GLOBAL_STATS["records"] = records
            GLOBAL_STATS["last_updated"] = now.strftime("%H:%M:%S")
        except Exception as e:
            logger.error(f"UI数据观测者发生异常: {e}")

        await asyncio.sleep(5)

async def gc_loop():
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

async def cron_loop():
    """每日 00:15 且进程已在线跨过该时刻时触发昨日总结；启动时不补跑历史任务。"""
    started_at = datetime.now(TZ_UTC8)
    last_triggered_date = None
    while True:
        try:
            now = datetime.now(TZ_UTC8)
            if cron_due(now, started_at, last_triggered_date):
                # 先占位：即使后续队列处理失败，也不会在同一天重复触发
                last_triggered_date = now.date()
                tasks = [
                    (k, v) for k, v in flat_display_map.items()
                    if k not in CRON_EXCLUDED_TYPES
                ]
                logger.info(f"触发昨日总结，准备向队列压入 {len(tasks)} 个后台合成任务。")
                for k, v in tasks:
                    await SYNTH_QUEUE.put({
                        "img_type": k,
                        "display_name": f"{v} (昨日总结)",
                        "hours": 24,
                        "is_cron": True
                    })
                await SYNTH_QUEUE.join()
                logger.info("昨日总结已全部完成。")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"每日总结定时器异常: {e}")
        await asyncio.sleep(30)

async def synthesis_worker():
    global SYNTH_STATUS
    while True:
        task = await SYNTH_QUEUE.get()
        try:
            img_type = task["img_type"]
            display_name = task["display_name"]
            fps = get_dynamic_fps(task["hours"])
            is_cron = task.get("is_cron", False)

            SYNTH_STATUS = {
                "is_running": True,
                "message": f"正在后台压制 {display_name} (队列计算中...)",
                "type": "warning"
            }

            synth = Synthesizer()
            try:
                success = await asyncio.to_thread(
                    synth.generate_video, img_type, task["hours"], is_cron, fps, task.get("start_time")
                )
                if success:
                    SYNTH_STATUS = {"is_running": False, "message": f"[{display_name}] 合成完毕，已落盘。", "type": "success"}
                else:
                    SYNTH_STATUS = {"is_running": False, "message": f"[{display_name}] 合成失败，请检查数据完整性。", "type": "error"}
            except Exception as e:
                SYNTH_STATUS = {"is_running": False, "message": f"[{display_name}] 管线崩溃: {str(e)}", "type": "error"}

            if is_cron:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"合成工作机致命异常: {e}")
            await asyncio.sleep(1)
        finally:
            SYNTH_QUEUE.task_done()

# ==========================================
# FastAPI 引擎
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    poller = AsyncPoller()
    tasks = [
        asyncio.create_task(poller.run_forever()),
        asyncio.create_task(stats_loop()),
        asyncio.create_task(gc_loop()),
        asyncio.create_task(cron_loop()),
        asyncio.create_task(synthesis_worker())
    ]
    logger.info("系统总线接入完毕，所有异步守护任务已激活并由护城河保护。")
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"守护任务退出时抛出异常: {result}")
        # 采集子任务与连接池由 poller 统一收尾
        await poller.shutdown()
        await download_stats.flush()
        logger.info("系统已停止，采集任务与连接池已释放。")

app = FastAPI(lifespan=lifespan)

class SynthRequest(BaseModel):
    display_name: str
    hours: int
    start_time: str | None = None


def parse_start_time(value: str) -> datetime:
    """解析前端传入的显式合成起点，格式 YYYYMMDD_HHMM（北京时间）"""
    return datetime.strptime(value, "%Y%m%d_%H%M").replace(tzinfo=TZ_UTC8)


RANGE_GRANULARITY_MINUTES = 30

# 时间轴结果缓存：数据文件每 2 分钟才新增，短 TTL 足够，
# 且避免浏览器每次刷新/切换跨度都触发全量扫盘卡死事件循环
RANGES_CACHE_TTL = 60
_ranges_cache = {}  # (img_type, hours) -> (expire_ts, data)


@app.get("/api/status")
async def get_status():
    status_copy = dict(SYNTH_STATUS)
    qsize = SYNTH_QUEUE.qsize()
    if status_copy["is_running"] and qsize > 0:
        status_copy["message"] += f" (排队等待中: {qsize} 项)"

    stats_out = dict(GLOBAL_STATS)
    # 向前端吐出后端进程绝对的运行秒数
    stats_out["uptime"] = int(time.time() - SERVER_START_TIME)

    return {
        "stats": stats_out,
        "synth": status_copy,
        "downloads": {
            "hours": download_stats.snapshot(24),
            "totals": download_stats.totals(24),
        },
    }

@app.get("/api/options")
async def get_options():
    return {"groups": {group: list(types.values()) for group, types in display_groups.items()}}


@app.get("/api/ranges")
async def get_ranges(display_name: str, hours: int):
    img_type = reverse_map.get(display_name)
    if not img_type:
        return {"code": 400, "granularity_minutes": RANGE_GRANULARITY_MINUTES, "dates": [], "message": "无效的气象图类型"}
    if hours not in SUPPORTED_HOURS:
        return {"code": 400, "granularity_minutes": RANGE_GRANULARITY_MINUTES, "dates": [], "message": "不支持的合成跨度"}
    now_ts = time.time()
    cache_key = (img_type, hours)
    cached = _ranges_cache.get(cache_key)
    if cached and cached[0] > now_ts:
        return {"code": 200, "granularity_minutes": cached[1]["granularity_minutes"], "dates": cached[1]["dates"]}
    data = await asyncio.to_thread(
        compute_available_ranges,
        img_type,
        hours,
        DATA_DIR / img_type,
        datetime.now(TZ_UTC8),
    )
    _ranges_cache[cache_key] = (now_ts + RANGES_CACHE_TTL, data)
    return {"code": 200, "granularity_minutes": data["granularity_minutes"], "dates": data["dates"]}

@app.get("/api/logs")
async def get_logs():
    return {"logs": list(LOG_BUFFER)}


@app.get("/api/videos")
async def get_videos():
    """视频产物列表：文件数量少，扫描开销低，不做缓存以保证合成后刷新即最新"""
    groups = await asyncio.to_thread(scan_videos, DATA_DIR / "output")
    return {"code": 200, "groups": groups}


@app.get("/videos/{path:path}")
async def serve_video(path: str):
    """视频文件路由（支持 Range，供播放器拖动进度）"""
    output_dir = DATA_DIR / "output"
    base = str(output_dir.resolve())
    target = str((output_dir / path).resolve())
    try:
        inside = os.path.commonpath([base, target]) == base
    except ValueError:
        inside = False
    if not inside:
        return HTMLResponse("Forbidden", status_code=403)
    if not os.path.isfile(target):
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
    await SYNTH_QUEUE.put({
        "img_type": img_type,
        "display_name": req.display_name,
        "hours": req.hours,
        "is_cron": False,
        "start_time": start_time
    })
    return {"code": 200, "message": "任务已加入队列"}

# ==========================================
# Web 前端 (100% 内嵌原生 CSS，绝对零公网依赖)
# ==========================================
HTML_TEMPLATE = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return HTML_TEMPLATE

if __name__ == "__main__":
    configure_logging()
    local_ip = get_local_ip()
    print("\n" + "="*65)
    print("天气追踪边缘节点已启动！")
    print(f"控制面板 (局域网访问): http://{local_ip}:11451")
    print("控制面板 (本机访问):   http://127.0.0.1:11451")
    print("="*65 + "\n")

    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    uvicorn.run(app, host="0.0.0.0", port=11451, access_log=False, log_config=None)
