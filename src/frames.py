"""帧文件名编解码与按日时间戳目录缓存。

把"文件名 -> 时间戳"和"目录扫描"收敛到一个地方，避免统计、时间轴、合成
各自实现一套切片解析。缓存以目录 mtime 为准，短 TTL 只用于兜底极端的
原地覆盖写入；正常的新帧落盘都会改变目录 mtime。
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from config import TZ_UTC8

FRAME_TS_FORMAT = "%Y%m%d_%H%M%S"


def parse_frame_timestamp(name: str, tz=TZ_UTC8) -> datetime | None:
    """从 ``YYYYMMDD_HHMMSS_<source>.png`` 解析时间戳；格式不符返回 None。"""
    if len(name) < 15 or name[8] != "_":
        return None
    try:
        return datetime.strptime(name[:15], FRAME_TS_FORMAT).replace(tzinfo=tz)
    except ValueError:
        return None


@dataclass(frozen=True)
class _CacheEntry:
    mtime: float
    built_at: float
    timestamps: tuple[datetime, ...]


class FrameCatalog:
    """按 (信源, 日期) 缓存目录内的帧时间戳。"""

    def __init__(self, data_dir: Path, ttl_seconds: float = 60.0):
        self.data_dir = Path(data_dir)
        self.ttl_seconds = ttl_seconds
        self._cache: dict[tuple[str, str], _CacheEntry] = {}
        self._lock = threading.Lock()

    def timestamps(self, source: str, date_str: str) -> tuple[datetime, ...]:
        date_dir = self.data_dir / source / date_str
        try:
            mtime = date_dir.stat().st_mtime
        except OSError:
            return ()

        key = (source, date_str)
        now = time.monotonic()
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None and entry.mtime == mtime and now - entry.built_at < self.ttl_seconds:
                return entry.timestamps

        values = []
        try:
            with os.scandir(date_dir) as entries:
                for item in entries:
                    if not item.name.endswith(".png"):
                        continue
                    ts = parse_frame_timestamp(item.name)
                    if ts is not None:
                        values.append(ts)
        except OSError:
            values = []

        result = tuple(values)
        with self._lock:
            self._cache[key] = _CacheEntry(mtime=mtime, built_at=now, timestamps=result)
        return result

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
