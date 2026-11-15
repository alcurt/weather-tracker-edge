"""磁盘一致性工具：原子写、JSON 落盘与崩溃残片清理。

所有图片/游标/视频都遵循"先写同目录临时文件，再 os.replace 到目标名"，
确保进程被强杀或断电时不会在正式文件名下留下半成品。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

PART_SUFFIX = ".part"


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """同步原子写：临时文件写完后原子替换目标。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + PART_SUFFIX)
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


async def atomic_write_bytes_async(path: Path, data: bytes) -> None:
    """异步原子写：磁盘 I/O 抛到线程池，避免阻塞事件循环。"""
    await asyncio.to_thread(atomic_write_bytes, path, data)


def atomic_write_json(path: Path, payload: dict) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
    atomic_write_bytes(path, data)


def cleanup_part_files(root: Path, older_than_seconds: float = 600.0, now: float | None = None) -> int:
    """清理崩溃遗留的 *.part 残片，跳过仍在写入中的新文件，返回删除数量。"""
    current = time.time() if now is None else now
    removed = 0
    if not root.exists():
        return removed
    for part in root.rglob("*" + PART_SUFFIX):
        try:
            if current - part.stat().st_mtime < older_than_seconds:
                continue
            part.unlink()
            removed += 1
        except OSError:
            continue
    return removed
