"""HTTP 结果分类与退避策略：可重试的状态码、Retry-After 解析、指数退避。

httpx 里 ``HTTPStatusError`` 与 ``RequestError`` 是兄弟类型，不是父子关系，
因此调用方必须单独捕获并判断状态码，不能只依赖 ``except httpx.RequestError``。
"""

from __future__ import annotations

import email.utils
from datetime import datetime, timezone

# 明确可重试的状态码；5xx 由 is_retryable_status 统一覆盖
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

MAX_RETRY_DELAY = 60.0


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
