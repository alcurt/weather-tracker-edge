"""HTTP 结果分类与退避策略：可重试的状态码、Retry-After 解析、指数退避。

httpx 里 ``HTTPStatusError`` 与 ``RequestError`` 是兄弟类型，不是父子关系，
因此调用方必须单独捕获并判断状态码，不能只依赖 ``except httpx.RequestError``。
"""

from __future__ import annotations

import email.utils
import time
from datetime import datetime, timezone
from typing import Callable

# 明确可重试的状态码；5xx 由 is_retryable_status 统一覆盖
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

MAX_RETRY_DELAY = 60.0
MISSING_BACKOFF_START = 60.0
MISSING_BACKOFF_MAX = 300.0
TRANSIENT_BACKOFF = 30.0
PERMANENT_BACKOFF = 3600.0


def is_retryable_status(code: int) -> bool:
    return code in RETRYABLE_STATUS or 500 <= code <= 599


def parse_retry_after(value: str | None) -> float | None:
    """解析 Retry-After（秒数或 HTTP-date），限制在 1-60 秒。"""
    if not value:
        return None
    text = value.strip()
    if text.isdigit():
        return float(min(MAX_RETRY_DELAY, max(1, int(text))))
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta = (when - datetime.now(timezone.utc)).total_seconds()
    return float(min(MAX_RETRY_DELAY, max(1.0, delta)))


def retry_delay(attempt: int, retry_after: str | None = None) -> float:
    """第 attempt 次失败后的等待秒数（attempt 从 1 开始）：5, 10, 20, 40, 60。"""
    hinted = parse_retry_after(retry_after)
    if hinted is not None:
        return hinted
    return min(MAX_RETRY_DELAY, 5.0 * (2 ** max(0, attempt - 1)))


class SlotProbeCache:
    """按 (信源, 时次) 记录下次允许探测的时间。

    时间网格类信源（雷达 / FY4C）的时次在发布前必然 404。若每 2 分钟都重探，
    单个时次会被无意义地请求 20-30 次。这里对 404 做指数退避：
    60s、120s、240s，之后固定 300s；成功或本地已存在则清除状态。
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._next_at: dict[str, float] = {}
        self._misses: dict[str, int] = {}

    def should_probe(self, key: str) -> bool:
        return self._next_at.get(key, 0.0) <= self._clock()

    def mark_ok(self, key: str) -> None:
        self._next_at.pop(key, None)
        self._misses.pop(key, None)

    def mark_missing(self, key: str) -> None:
        misses = self._misses.get(key, 0) + 1
        self._misses[key] = misses
        delay = min(MISSING_BACKOFF_MAX, MISSING_BACKOFF_START * (2 ** (misses - 1)))
        self._next_at[key] = self._clock() + delay

    def mark_transient(self, key: str) -> None:
        self._next_at[key] = self._clock() + TRANSIENT_BACKOFF

    def mark_permanent(self, key: str) -> None:
        self._next_at[key] = self._clock() + PERMANENT_BACKOFF

    def prune(self, active_keys: set[str]) -> None:
        for key in [k for k in self._next_at if k not in active_keys]:
            self._next_at.pop(key, None)
        for key in [k for k in self._misses if k not in active_keys]:
            self._misses.pop(key, None)
