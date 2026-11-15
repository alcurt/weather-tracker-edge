import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from config import DATA_DIR, TZ_UTC8

logger = logging.getLogger(__name__)

STATS_DIR = DATA_DIR / "stats"
STATS_FILE = STATS_DIR / "download_hourly.json"
RETAIN_HOURS = 72  # 持久化保留窗口，柱状图只取最近 24h


class DownloadStats:
    """下载结果计数：按北京时间小时键累计 ok / fail / missing，持久化 JSON"""

    def __init__(self, path: Path = STATS_FILE):
        self.path = path
        self._hours = {}  # "YYYYMMDDHH" -> {"ok": int, "fail": int, "missing": int}
        self._dirty = False
        self._load()

    def _load(self):
        try:
            if self.path.exists():
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._hours = {
                    k: {
                        "ok": int(v.get("ok", 0)),
                        "fail": int(v.get("fail", 0)),
                        "missing": int(v.get("missing", 0)),
                        "missing_seen": list(v.get("missing_seen", [])),
                    }
                    for k, v in data.get("hours", {}).items()
                }
                self._prune()
        except Exception as e:
            logger.warning(f"下载统计加载失败，将从空计数开始: {e}")
            self._hours = {}

    def _prune(self):
        cutoff_key = (datetime.now(TZ_UTC8) - timedelta(hours=RETAIN_HOURS)).strftime("%Y%m%d%H")
        self._hours = {k: v for k, v in self._hours.items() if k >= cutoff_key}

    def record(self, category: str):
        if category not in ("ok", "fail"):
            return
        key = datetime.now(TZ_UTC8).strftime("%Y%m%d%H")
        entry = self._hours.setdefault(key, {"ok": 0, "fail": 0, "missing": 0, "missing_seen": []})
        entry[category] += 1
        self._dirty = True

    def record_missing(self, source: str, item_key: str):
        """404 去重：同一小时同一 (信源, 时次) 只计一次，集合随 JSON 持久化"""
        key = datetime.now(TZ_UTC8).strftime("%Y%m%d%H")
        entry = self._hours.setdefault(key, {"ok": 0, "fail": 0, "missing": 0, "missing_seen": []})
        seen = entry.setdefault("missing_seen", [])
        dedupe_key = f"{source}:{item_key}"
        if dedupe_key in seen:
            return
        seen.append(dedupe_key)
        entry["missing"] += 1
        self._dirty = True

    def snapshot(self, hours: int = 24) -> list:
        """最近 N 小时按时间升序返回（旧 -> 新）"""
        now = datetime.now(TZ_UTC8)
        result = []
        for offset in range(hours - 1, -1, -1):
            dt = now - timedelta(hours=offset)
            key = dt.strftime("%Y%m%d%H")
            entry = self._hours.get(key, {"ok": 0, "fail": 0, "missing": 0, "missing_seen": []})
            result.append({
                "key": key,
                "label": dt.strftime("%H:00"),
                "ok": entry["ok"],
                "fail": entry["fail"],
                "missing": entry["missing"],
            })
        return result

    def totals(self, hours: int = 24) -> dict:
        snap = self.snapshot(hours)
        return {
            "ok": sum(x["ok"] for x in snap),
            "fail": sum(x["fail"] for x in snap),
            "missing": sum(x["missing"] for x in snap),
        }

    async def flush(self):
        if not self._dirty:
            return
        self._prune()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            payload = {
                "updated_at": datetime.now(TZ_UTC8).isoformat(),
                "hours": self._hours,
            }
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
            self._dirty = False
        except Exception as e:
            logger.warning(f"下载统计落盘失败: {e}")


download_stats = DownloadStats()
