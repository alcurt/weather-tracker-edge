"""日志总线：控制台 + 轮转文件 + WebUI 内存终端。"""

from __future__ import annotations

import collections
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import LOG_DIR

LOG_BUFFER: collections.deque = collections.deque(maxlen=200)


class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        LOG_BUFFER.append(self.format(record))


memory_handler = MemoryLogHandler()
memory_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_configured = False


def configure_logging(log_dir: Path | None = None) -> None:
    """配置三路日志；只在入口调用，避免 import 时产生文件写入副作用。"""
    global _configured
    if _configured:
        return
    target = Path(log_dir) if log_dir is not None else LOG_DIR
    target.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            RotatingFileHandler(
                target / "tracker.log",
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
    _configured = True
