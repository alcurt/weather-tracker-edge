import asyncio
import collections
import json
import logging
import os
import sys
import time
import socket
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

from config import TARGET_URLS, RADAR_TYPES, TZ_UTC8, DATA_DIR
from poller import AsyncPoller
from synthesizer import Synthesizer
from cleaner import StorageCleaner
from download_stats import download_stats

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
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_BUFFER = collections.deque(maxlen=200)

class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))

memory_handler = MemoryLogHandler()
memory_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "tracker.log", encoding="utf-8"),
        memory_handler
    ]
)
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

def get_dynamic_fps(hours: int) -> float:
    if hours == 1: return 4.0
    elif hours == 3: return 6.0
    elif hours == 6: return 8.0
    elif hours == 12: return 9.6
    elif hours == 24: return 14.4
    else: return 10.0

# ==========================================
# 后台异步守护协程 (元数据门铃监听)
# ==========================================
async def stats_loop():
    """采用文件系统元数据门铃 (st_mtime)：完美兼容 U 盘热插拔且在无变动时降为绝对零 I/O"""
    session_counts = {k: 0 for k in all_target_keys}
    last_cursors = {k: "" for k in all_target_keys}
    
    # 内存缓存结构：{(img_type, YYYYMMDD): {"mtime": float, "count": int}}
    dir_mtime_cache = {}

    for k in all_target_keys:
        cursor_file = DATA_DIR / k / ".latest_cursor.json"
        if cursor_file.exists():
            try:
                with open(cursor_file, 'r', encoding='utf-8') as f:
                    last_cursors[k] = json.load(f).get("file_path", "")
            except Exception:
                pass

    while True:
        try:
            records = []
            now = datetime.now(TZ_UTC8)
            cutoff_24h_str = (now - timedelta(hours=24)).strftime("%Y%m%d_%H%M%S")
            
            for k in all_target_keys:
                # 1. O(1) 游标对比计算本次会话新增
                cursor_file = DATA_DIR / k / ".latest_cursor.json"
                if cursor_file.exists():
                    try:
                        with open(cursor_file, 'r', encoding='utf-8') as f:
                            current_path = json.load(f).get("file_path", "")
                            if current_path and last_cursors[k] and current_path != last_cursors[k]:
                                session_counts[k] += 1
                            last_cursors[k] = current_path
                    except Exception:
                        pass

                # 2. 元数据门铃：只通过 os.scandir 检查今天和昨天的目录 mtime
                count_24h = 0
                hourly = [0] * 24
                earliest_ts = None
                type_dir = DATA_DIR / k
                if type_dir.exists():
                    for days_back in [0, 1]:
                        date_str = (now - timedelta(days=days_back)).strftime("%Y%m%d")
                        date_dir = type_dir / date_str
                        if not date_dir.exists() or not date_dir.is_dir():
                            continue
                        
                        try:
                            current_mtime = date_dir.stat().st_mtime
                        except Exception:
                            continue
                            
                        cache_key = f"{k}_{date_str}"
                        # 如果文件夹元数据没有变动（无网络写入也无手动复制），直接秒弹缓存！
                        if cache_key in dir_mtime_cache and dir_mtime_cache[cache_key]["mtime"] == current_mtime:
                            count_24h += dir_mtime_cache[cache_key]["count"]
                            cached_hourly = dir_mtime_cache[cache_key]["hourly"]
                            for i in range(24):
                                hourly[i] += cached_hourly[i]
                            cached_earliest = dir_mtime_cache[cache_key].get("earliest")
                            if cached_earliest and (earliest_ts is None or cached_earliest < earliest_ts):
                                earliest_ts = cached_earliest
                        else:
                            # 门铃被敲响！使用轻量级的 C-API 迭代器 scandir，不解析字符串，极速计件
                            valid_count = 0
                            dir_hourly = [0] * 24
                            dir_earliest = None
                            with os.scandir(date_dir) as entries:
                                for entry in entries:
                                    if entry.name.endswith(".png"):
                                        # 截取文件名头部快速对比时间，彻底告别 strptime
                                        if entry.name[:15] >= cutoff_24h_str[:15]:
                                            valid_count += 1
                                            try:
                                                dt = datetime.strptime(entry.name[:14], "%Y%m%d_%H%M%S").replace(tzinfo=TZ_UTC8)
                                                idx = int((now - dt).total_seconds() // 3600)
                                                if 0 <= idx < 24:
                                                    dir_hourly[idx] += 1
                                                if dir_earliest is None or dt < dir_earliest:
                                                    dir_earliest = dt
                                            except ValueError:
                                                pass
                            dir_mtime_cache[cache_key] = {
                                "mtime": current_mtime, "count": valid_count, "hourly": dir_hourly, "earliest": dir_earliest
                            }
                            count_24h += valid_count
                            for i in range(24):
                                hourly[i] += dir_hourly[i]
                            if dir_earliest and (earliest_ts is None or dir_earliest < earliest_ts):
                                earliest_ts = dir_earliest
                
                latest_time = "--:--"
                if last_cursors[k]:
                    try:
                        latest_time = datetime.strptime(
                            os.path.basename(last_cursors[k])[:14], "%Y%m%d_%H%M%S"
                        ).strftime("%H:%M")
                    except Exception:
                        pass

                records.append({
                    "key": k,
                    "display_name": flat_display_map[k],
                    "avail_24h": count_24h,
                    "session_dl": session_counts[k],
                    "hourly": hourly,
                    "latest_time": latest_time,
                    "earliest_time": earliest_ts.strftime("%H:%M") if earliest_ts else "--:--",
                })
                
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
    last_triggered_date = None
    while True:
        try:
            now = datetime.now(TZ_UTC8)
            if now.hour == 0 and now.minute == 15 and last_triggered_date != now.date():
                cron_count = len(flat_display_map) - len(CRON_EXCLUDED_TYPES)
                logger.info(f"触发 00:15 每日总结，准备向队列压入 {cron_count} 个后台合成任务。")
                last_triggered_date = now.date()
                for k, v in flat_display_map.items():
                    if k in CRON_EXCLUDED_TYPES:
                        continue
                    await SYNTH_QUEUE.put({
                        "img_type": k,
                        "display_name": f"{v} (昨日总结)",
                        "hours": 24,
                        "is_cron": True
                    })
        except Exception as e:
            logger.error(f"每日总结定时器异常: {e}")
        await asyncio.sleep(10)

async def synthesis_worker():
    global SYNTH_STATUS
    while True:
        try:
            task = await SYNTH_QUEUE.get()
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
            SYNTH_QUEUE.task_done()
        except Exception as e:
            logger.error(f"合成工作机致命异常: {e}")
            await asyncio.sleep(1)

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
    yield
    for t in tasks: t.cancel()

app = FastAPI(lifespan=lifespan)

class SynthRequest(BaseModel):
    display_name: str
    hours: int
    start_time: str | None = None


def parse_start_time(value: str) -> datetime:
    """解析前端传入的显式合成起点，格式 YYYYMMDD_HHMM（北京时间）"""
    return datetime.strptime(value, "%Y%m%d_%H%M").replace(tzinfo=TZ_UTC8)


RANGE_GRANULARITY_MINUTES = 30
SLOTS_PER_DAY = 24 * 60 // RANGE_GRANULARITY_MINUTES  # 48

# 时间轴结果缓存：数据文件每 2 分钟才新增，短 TTL 足够，
# 且避免浏览器每次刷新/切换跨度都触发全量扫盘卡死事件循环
RANGES_CACHE_TTL = 60
_ranges_cache = {}  # (img_type, hours) -> (expire_ts, data)

VIDEOS_CACHE_TTL = 30
_videos_cache = {"expire": 0.0, "groups": None}


def compute_available_ranges(img_type: str, hours: int) -> dict:
    """扫描本地帧，返回 30 分钟粒度时间轴数据。

    每个日期给出 48 个 30 分钟桶的帧存在性（buckets），并列出可用起点
    （跨度覆盖的每个 30 分钟桶至少 1 帧，支持跨日）。
    """
    result = {"granularity_minutes": RANGE_GRANULARITY_MINUTES, "dates": []}
    type_dir = DATA_DIR / img_type
    if not type_dir.exists() or hours <= 0:
        return result

    now = datetime.now(TZ_UTC8)
    bucket_sets = {}  # date_str -> set of bucket index (0..47)
    for days_back in range(7):
        date_str = (now - timedelta(days=days_back)).strftime("%Y%m%d")
        date_dir = type_dir / date_str
        if not date_dir.is_dir():
            continue
        indices = set()
        try:
            with os.scandir(date_dir) as entries:
                for entry in entries:
                    name = entry.name
                    # 文件名形如 YYYYMMDD_HHMMSS_type.png：直接切片取小时分钟，跳过 strptime
                    if name.endswith(".png") and len(name) >= 14 and name[8] == "_":
                        try:
                            hh = int(name[9:11])
                            mm = int(name[11:13])
                        except ValueError:
                            continue
                        indices.add((hh * 60 + mm) // RANGE_GRANULARITY_MINUTES)
        except OSError:
            continue
        if indices:
            bucket_sets[date_str] = indices

    if not bucket_sets:
        return result

    # 日期序数 -> 日期字符串 映射：跨日判定退化为整数 + 字典查找，避免逐槽 timedelta/strftime
    ord_map = {}
    for ds in bucket_sets:
        try:
            ord_map[datetime.strptime(ds, "%Y%m%d").toordinal()] = ds
        except ValueError:
            continue

    covered_slots = hours * 60 // RANGE_GRANULARITY_MINUTES
    max_missing_slots = 4 if covered_slots >= 48 else 0
    minutes_per_day = 24 * 60

    for date_str, indices in bucket_sets.items():
        buckets = [1 if i in indices else 0 for i in range(SLOTS_PER_DAY)]
        try:
            date_base = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=TZ_UTC8)
        except ValueError:
            continue
        date_ord = date_base.toordinal()
        available = []
        for start_idx in range(SLOTS_PER_DAY):
            missing_slots = 0
            start_min = start_idx * RANGE_GRANULARITY_MINUTES
            for k in range(covered_slots):
                total_min = start_min + k * RANGE_GRANULARITY_MINUTES
                day_offset = total_min // minutes_per_day
                slot_idx = (total_min % minutes_per_day) // RANGE_GRANULARITY_MINUTES
                target_date = ord_map.get(date_ord + day_offset)
                if target_date is None or slot_idx not in bucket_sets[target_date]:
                    missing_slots += 1
                    if missing_slots > max_missing_slots:
                        break
            if missing_slots > max_missing_slots:
                continue
            end_dt = date_base + timedelta(minutes=(start_idx + covered_slots) * RANGE_GRANULARITY_MINUTES)
            cross_day = end_dt.date() != date_base.date()
            available.append({
                "index": start_idx,
                "start": f"{start_idx * RANGE_GRANULARITY_MINUTES // 60:02d}:{start_idx * RANGE_GRANULARITY_MINUTES % 60:02d}",
                "end": end_dt.strftime("%Y-%m-%d %H:%M") if cross_day else end_dt.strftime("%H:%M"),
                "cross_day": cross_day,
            })
        if available:
            result["dates"].append({
                "date": date_base.strftime("%Y-%m-%d"),
                "date_str": date_str,
                "buckets": buckets,
                "available_starts": available,
            })

    result["dates"].sort(key=lambda d: d["date_str"], reverse=True)
    return result

@app.get("/api/status")
async def get_status():
    status_copy = dict(SYNTH_STATUS)
    qsize = SYNTH_QUEUE.qsize()
    if status_copy["is_running"] and qsize > 0:
        status_copy["message"] += f" (排队等待中: {qsize} 项)"
        
    stats_out = dict(GLOBAL_STATS)
    # 核心新增：向前端吐出后端进程绝对的运行秒数
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
    if hours not in (1, 3, 6, 12, 24):
        return {"code": 400, "granularity_minutes": RANGE_GRANULARITY_MINUTES, "dates": [], "message": "不支持的合成跨度"}
    now_ts = time.time()
    cache_key = (img_type, hours)
    cached = _ranges_cache.get(cache_key)
    if cached and cached[0] > now_ts:
        return {"code": 200, "granularity_minutes": cached[1]["granularity_minutes"], "dates": cached[1]["dates"]}
    data = await asyncio.to_thread(compute_available_ranges, img_type, hours)
    _ranges_cache[cache_key] = (now_ts + RANGES_CACHE_TTL, data)
    return {"code": 200, "granularity_minutes": data["granularity_minutes"], "dates": data["dates"]}

@app.get("/api/logs")
async def get_logs():
    return {"logs": list(LOG_BUFFER)}


def _scan_videos() -> list:
    """扫描 data/output（含 cron 子目录）的 MP4，按文件名起始日期分组（同步函数，供 to_thread）"""
    output_dir = DATA_DIR / "output"
    items = []
    if output_dir.exists():
        for mp4 in output_dir.rglob("*.mp4"):
            try:
                st = mp4.stat()
            except Exception:
                continue
            rel = mp4.relative_to(output_dir).as_posix()
            parts = mp4.name.split("_")
            date_str = parts[1] if len(parts) >= 2 and len(parts[1]) == 8 and parts[1].isdigit() else None
            group_date = date_str or datetime.fromtimestamp(st.st_mtime, TZ_UTC8).strftime("%Y%m%d")
            items.append({
                "name": mp4.name,
                "url": f"/videos/{rel}",
                "size": st.st_size,
                "mtime": datetime.fromtimestamp(st.st_mtime, TZ_UTC8).strftime("%Y-%m-%d %H:%M"),
                "cron": rel.startswith("cron/"),
                "date": group_date,
            })
    items.sort(key=lambda x: (x["date"], x["mtime"]), reverse=True)
    groups = []
    for item in items:
        if not groups or groups[-1]["date"] != item["date"]:
            groups.append({"date": item["date"], "videos": []})
        groups[-1]["videos"].append(item)
    return groups


@app.get("/api/videos")
async def get_videos():
    now_ts = time.time()
    if _videos_cache["groups"] is not None and _videos_cache["expire"] > now_ts:
        return {"code": 200, "groups": _videos_cache["groups"]}
    groups = await asyncio.to_thread(_scan_videos)
    _videos_cache["expire"] = now_ts + VIDEOS_CACHE_TTL
    _videos_cache["groups"] = groups
    return {"code": 200, "groups": groups}


@app.get("/videos/{path:path}")
async def serve_video(path: str):
    """视频文件路由（支持 Range，供播放器拖动进度）"""
    output_dir = DATA_DIR / "output"
    base = str(output_dir.resolve())
    target = str((output_dir / path).resolve())
    if os.path.commonpath([base, target]) != base:
        return HTMLResponse("Forbidden", status_code=403)
    if not os.path.isfile(target):
        return HTMLResponse("Not Found", status_code=404)
    return FileResponse(target, media_type="video/mp4")

@app.post("/api/synthesize")
async def trigger_synthesize(req: SynthRequest):
    img_type = reverse_map.get(req.display_name)
    if not img_type:
        return {"code": 400, "message": "无效的气象图类型"}
    start_time = None
    if req.start_time:
        try:
            start_time = parse_start_time(req.start_time)
        except ValueError:
            return {"code": 400, "message": "start_time 格式应为 YYYYMMDD_HHMM"}
        if req.hours <= 0:
            return {"code": 400, "message": "指定起点时必须同时给出正数小时跨度"}
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
    local_ip = get_local_ip()
    print("\n" + "="*65)
    print("天气追踪边缘节点已启动！")
    print(f"控制面板 (局域网访问): http://{local_ip}:11451")
    print(f"控制面板 (本机访问):   http://127.0.0.1:11451")
    print("="*65 + "\n")
    
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    uvicorn.run(app, host="0.0.0.0", port=11451, access_log=False, log_config=None)
