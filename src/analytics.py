"""与 IO 解耦的纯分析逻辑：文件名时间戳、24 小时聚合、30 分钟时间轴、视频分组。

这些函数不依赖 FastAPI / 全局状态，只接收路径与当前时间，因此可以直接单测。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path

from config import TZ_UTC8
from frames import FrameCatalog

RANGE_GRANULARITY_MINUTES = 30
SLOTS_PER_DAY = 24 * 60 // RANGE_GRANULARITY_MINUTES  # 48

# 24 小时跨度允许的缺帧桶数，兼容上游零散缺帧
MAX_MISSING_SLOTS_24H = 4


def summarize_24h(timestamps: Iterable[datetime], now: datetime, hours: int = 24) -> dict:
    """按"距今小时"现算 24 小时窗口内的帧数、逐小时分布与最早时刻。

    入参是真实时间戳，聚合结果不缓存，因此不会随时间推移而错位。
    """
    cutoff = now - timedelta(hours=hours)
    hourly = [0] * hours
    earliest = None
    count = 0
    for ts in timestamps:
        if ts < cutoff or ts > now:
            continue
        idx = int((now - ts).total_seconds() // 3600)
        if idx < 0 or idx >= hours:
            continue
        count += 1
        hourly[idx] += 1
        if earliest is None or ts < earliest:
            earliest = ts
    return {"count": count, "hourly": hourly, "earliest": earliest}


def compute_available_ranges(
    img_type: str,
    hours: int,
    type_dir: Path,
    now: datetime | None = None,
    lookback_days: int = 7,
    catalog: FrameCatalog | None = None,
) -> dict:
    """扫描本地帧，返回 30 分钟粒度时间轴数据。

    每个日期给出 48 个 30 分钟桶的帧存在性（buckets），并列出可用起点
    （跨度覆盖的每个 30 分钟桶至少 1 帧，支持跨日）。
    """
    result = {"granularity_minutes": RANGE_GRANULARITY_MINUTES, "dates": []}
    if hours <= 0 or not type_dir.exists():
        return result

    current = now or datetime.now(TZ_UTC8)
    catalog = catalog or FrameCatalog(type_dir.parent)
    bucket_sets: dict[str, set[int]] = {}
    for days_back in range(lookback_days):
        date_str = (current - timedelta(days=days_back)).strftime("%Y%m%d")
        indices = {
            (ts.hour * 60 + ts.minute) // RANGE_GRANULARITY_MINUTES
            for ts in catalog.timestamps(img_type, date_str)
        }
        if indices:
            bucket_sets[date_str] = indices

    if not bucket_sets:
        return result

    # 日期序数 -> 日期字符串 映射：跨日判定退化为整数 + 字典查找
    ord_map: dict[int, str] = {}
    for ds in bucket_sets:
        try:
            ord_map[datetime.strptime(ds, "%Y%m%d").toordinal()] = ds
        except ValueError:
            continue

    covered_slots = hours * 60 // RANGE_GRANULARITY_MINUTES
    max_missing_slots = MAX_MISSING_SLOTS_24H if covered_slots >= SLOTS_PER_DAY else 0
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
            available.append(
                {
                    "index": start_idx,
                    "start": f"{start_idx * RANGE_GRANULARITY_MINUTES // 60:02d}:{start_idx * RANGE_GRANULARITY_MINUTES % 60:02d}",
                    "end": end_dt.strftime("%Y-%m-%d %H:%M") if cross_day else end_dt.strftime("%H:%M"),
                    "cross_day": cross_day,
                }
            )
        if available:
            result["dates"].append(
                {
                    "date": date_base.strftime("%Y-%m-%d"),
                    "date_str": date_str,
                    "buckets": buckets,
                    "available_starts": available,
                }
            )

    result["dates"].sort(key=lambda d: d["date_str"], reverse=True)
    return result


def scan_videos(output_dir: Path) -> list:
    """扫描输出目录（含 cron 子目录）的 MP4，按结束日期分组返回。

    跨日产物（类型_起始日期_起始时间_to_结束日期_结束时间.mp4）归入结束当天，
    同日或旧格式回退起始日期，最后兜底文件修改时间。
    """
    items = []
    if not output_dir.exists():
        return []
    for mp4 in output_dir.rglob("*.mp4"):
        if mp4.name.endswith(".part"):
            continue
        try:
            st = mp4.stat()
        except OSError:
            continue
        rel = mp4.relative_to(output_dir).as_posix()
        parts = mp4.name.split("_")
        end_date = parts[4] if len(parts) >= 5 and len(parts[4]) == 8 and parts[4].isdigit() else None
        start_date = parts[1] if len(parts) >= 2 and len(parts[1]) == 8 and parts[1].isdigit() else None
        group_date = end_date or start_date or datetime.fromtimestamp(st.st_mtime, TZ_UTC8).strftime("%Y%m%d")
        items.append(
            {
                "name": mp4.name,
                "url": f"/videos/{rel}",
                "size": st.st_size,
                "mtime": datetime.fromtimestamp(st.st_mtime, TZ_UTC8).strftime("%Y-%m-%d %H:%M"),
                "cron": rel.startswith("cron/"),
                "date": group_date,
            }
        )
    items.sort(key=lambda x: (x["date"], x["mtime"]), reverse=True)
    groups = []
    for item in items:
        if not groups or groups[-1]["date"] != item["date"]:
            groups.append({"date": item["date"], "videos": []})
        groups[-1]["videos"].append(item)
    return groups
