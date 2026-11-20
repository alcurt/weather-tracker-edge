"""时间网格信源的通用落盘与抓取逻辑（雷达 / FY4C 共用）。

网格信源都满足同一套约束：
- 文件名 = ``{观测时刻}_{信源}.png`` 或 ``.jpg``；
- 时次在发布前必然 404，因此有成熟期与负缓存；
- 内容需通过 Pillow 完整性校验，并以原子写落盘。

这里只保留"时次 -> 路径 / 时次规划 / 单时次抓取"，URL 构造与调度策略
仍留在 ``poller.py``，避免把产品差异塞进通用层。
"""

from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from PIL import Image

from config import MAX_RETRIES
from fetch import is_retryable_status, retry_delay
from storage import atomic_write_bytes_async

logger = logging.getLogger(__name__)

MIN_HEALTHY_BYTES = 100


@dataclass(frozen=True, slots=True)
class GridSlot:
    source: str
    ts: datetime
    file_ext: str = ".png"

    @property
    def key(self) -> str:
        return grid_slot_key(self.source, self.ts)

    @property
    def file_name(self) -> str:
        return f"{self.ts.strftime('%Y%m%d_%H%M%S')}_{self.source}{self.file_ext}"

    def path(self, data_dir: Path) -> Path:
        return data_dir / self.source / self.ts.strftime("%Y%m%d") / self.file_name


def grid_slot_key(source: str, target_time: datetime) -> str:
    return f"{source}:{target_time.strftime('%Y%m%d_%H%M%S')}"


def grid_slots(now: datetime, step_minutes: int, newest_minutes: int, oldest_minutes: int) -> list[datetime]:
    """生成 [newest, oldest] 区间内的理论时次，按从新到旧排序。"""
    base_time = now.replace(minute=(now.minute // step_minutes) * step_minutes, second=0, microsecond=0)
    newest = base_time - timedelta(minutes=newest_minutes)
    oldest = base_time - timedelta(minutes=oldest_minutes)
    slots = []
    current = newest
    while current >= oldest:
        slots.append(current)
        current -= timedelta(minutes=step_minutes)
    return slots


def slot_is_mature(now: datetime, target_time: datetime, min_age_minutes: int) -> bool:
    """时次是否已过最小成熟期；未成熟时上游通常尚未发布，探测必然是 404。"""
    return now - target_time >= timedelta(minutes=min_age_minutes)


async def fetch_grid_slot(
    client: httpx.AsyncClient,
    *,
    source: str,
    target_time: datetime,
    url: str,
    data_dir: Path,
    cursor: Any = None,
    max_retries: int = MAX_RETRIES,
    file_ext: str = ".png",
) -> str:
    """抓取单个网格时次：本地去重 + 404 容忍 + 坏图重抓 + 原子落盘。

    返回与旧实现一致的字符串结果码：200 / EXIST / 404 / ERROR / FETCH_ERR / CRASH / TIMEOUT。
    ``file_ext`` 决定落盘扩展名；HKO 使用 ``.jpg``，其余默认 ``.png``。
    """
    slot = GridSlot(source, target_time, file_ext)
    file_path = slot.path(data_dir)

    if file_path.exists():
        try:
            if file_path.stat().st_size < MIN_HEALTHY_BYTES:
                logger.warning(f"[{source}] 发现本地坏图或零字节残片 {slot.file_name}，正在物理移除并触发重抓。")
                file_path.unlink()
            else:
                return "EXIST"
        except OSError:
            return "ERROR"

    attempt = 0
    while attempt <= max_retries:
        try:
            async with client.stream("GET", url) as response:
                if response.status_code == 404:
                    return "404"
                if is_retryable_status(response.status_code):
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}", request=response.request, response=response
                    )
                response.raise_for_status()
                content = await response.aread()

                try:
                    img = Image.open(io.BytesIO(content))
                    img.verify()
                except Exception as e:
                    logger.warning(f"[{source}] 图像尾块损坏，重试: {e}")
                    raise ValueError("grid image verification failed") from e

                await atomic_write_bytes_async(file_path, content)
                if cursor is not None:
                    await cursor.update("", "", "", str(file_path))
                logger.info(f"[{source}] 新气象图成功落盘: {slot.file_name}")
                return "200"
        except httpx.HTTPStatusError as e:
            if not is_retryable_status(e.response.status_code):
                logger.error(f"[{source}] 上游返回不可重试状态 {e.response.status_code}，放弃时次")
                return "ERROR"
            attempt += 1
            if attempt > max_retries:
                logger.error(f"[{source}] 达到最大重试次数，放弃时次 {slot.key}")
                return "FETCH_ERR"
            await asyncio.sleep(retry_delay(attempt, e.response.headers.get("Retry-After")))
        except (httpx.RequestError, ValueError):
            attempt += 1
            if attempt > max_retries:
                logger.error(f"[{source}] 达到最大重试次数，放弃时次 {slot.key}")
                return "FETCH_ERR"
            await asyncio.sleep(retry_delay(attempt))
        except Exception as e:
            logger.exception(f"[{source}] 发生未预期的致命异常: {e}")
            return "CRASH"
    return "TIMEOUT"
