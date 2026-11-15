import asyncio
import email.utils
import hashlib
import io
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import aiofiles
import httpx
from PIL import Image

from analytics import parse_frame_timestamp
from config import (
    BASE_TIMEOUT,
    CONNECTION_RECYCLE_SECONDS,
    DATA_DIR,
    HTTP_USER_AGENT,
    MAX_RETRIES,
    NMC_BACKFILL_BATCH_SIZE,
    NMC_BACKFILL_CONCURRENCY,
    NMC_BACKFILL_DAYS,
    NMC_BACKFILL_RECENT_HOURS,
    NMC_BATCH_PAUSE_SECONDS,
    NMC_MIN_AGE_MINUTES,
    NMC_PENDING_GRACE_MINUTES,
    NMC_POLL_WINDOW_HOURS,
    NMC_SATELLITE_TYPES,
    POLL_INTERVAL,
    RADAR_BACKFILL_HOURS,
    RADAR_BACKFILL_INTERVAL_MINUTES,
    RADAR_FETCH_CONCURRENCY,
    RADAR_INTERVAL_MINUTES,
    RADAR_MIN_AGE_MINUTES,
    RADAR_PENDING_GRACE_MINUTES,
    RADAR_POLL_WINDOW_HOURS,
    RADAR_TYPES,
    RADAR_URL_TEMPLATE,
    TARGET_URLS,
    TZ_UTC8,
)
from download_stats import download_stats
from fetch import SlotProbeCache, is_retryable_status, retry_delay
from storage import atomic_write_bytes_async

logger = logging.getLogger(__name__)


def build_radar_url(radar_type: str, target_time: datetime) -> str:
    """雷达图 URL：路径日期与文件名均使用北京时间的观测时次。"""
    return RADAR_URL_TEMPLATE.format(
        date_str=target_time.strftime("%Y%m%d"),
        upstream_time_str=target_time.strftime("%Y%m%d%H%M00"),
        suffix=RADAR_TYPES[radar_type],
    )


def slot_is_mature(now: datetime, target_time: datetime, min_age_minutes: int) -> bool:
    """时次是否已过最小成熟期；未成熟时上游通常尚未发布，探测必然是 404。"""
    return now - target_time >= timedelta(minutes=min_age_minutes)


def build_nmc_url(img_type: str, target_time: datetime) -> str:
    """由北京时观测时刻构造中央台卫星云图 URL。

    中央台文件名时间戳与 URL 日期路径均为 UTC（页面展示的北京时间 = UTC + 8h），
    因此必须先把北京时时刻转换为 UTC 再填入模板。
    """
    cfg = NMC_SATELLITE_TYPES[img_type]
    utc_time = target_time.astimezone(timezone.utc)
    return cfg["url_template"].format(
        date_path=utc_time.strftime("%Y/%m/%d"),
        timestamp=utc_time.strftime("%Y%m%d%H%M%S"),
    )

class CursorManager:
    """管理本地状态游标，结合异步锁确立文件系统为单一数据源"""
    
    def __init__(self, img_type: str):
        self.img_type = img_type
        self.type_dir = DATA_DIR / img_type
        self.type_dir.mkdir(parents=True, exist_ok=True)
        self.cursor_file = self.type_dir / ".latest_cursor.json"
        self._lock = asyncio.Lock() # 引入异步锁，杜绝高并发回溯时的竞态覆写
        
    def read(self) -> dict:
        if not self.cursor_file.exists():
            return {"last_modified": None, "content_length": None, "hash": None}
        try:
            with open(self.cursor_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"[{self.img_type}] 游标读取失败: {e}，将触发全量探测。")
            return {"last_modified": None, "content_length": None, "hash": None}

    async def update(self, last_modified: str, content_length: str, file_hash: str, file_path: str):
        """利用原子重命名与互斥锁覆写状态游标"""
        async with self._lock:
            tmp_file = self.cursor_file.with_suffix(".tmp")
            data = {
                "last_modified": last_modified,
                "content_length": content_length,
                "hash": file_hash,
                "file_path": str(file_path),
                "updated_at": datetime.now(TZ_UTC8).isoformat()
            }
            async with aiofiles.open(tmp_file, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2))
            
            os.replace(tmp_file, self.cursor_file)

class AsyncPoller:
    """完全解耦的异步多路守护探测器 (集成全局脉搏监测)"""
    
    def __init__(self):
        self.all_types = list(TARGET_URLS.keys()) + list(RADAR_TYPES.keys()) + list(NMC_SATELLITE_TYPES.keys())
        self.cursors = {img_type: CursorManager(img_type) for img_type in self.all_types}
        self._client_created_at = time.time()
        self.client = self._new_client()
        self._tasks: list[asyncio.Task] = []
        self._retire_tasks: set[asyncio.Task] = set()
        self._stop_event = asyncio.Event()
        self.probes = SlotProbeCache()

        # 核心新增：轻量化共享指标矩阵，为无头节点维护观测链路
        self.metrics = {
            img_type: {"elapsed": 0.0, "status": "INIT", "updated_at": 0.0}
            for img_type in self.all_types
        }

        for radar_type in RADAR_TYPES:
            (DATA_DIR / radar_type).mkdir(parents=True, exist_ok=True)

    def _new_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            http2=True,
            timeout=BASE_TIMEOUT,
            headers={"User-Agent": HTTP_USER_AGENT},
            # 守护进程不应继承桌面环境的代理设置（含格式异常时 httpx 会直接构造失败）
            trust_env=False,
            limits=httpx.Limits(
                max_connections=64,
                max_keepalive_connections=16,
                keepalive_expiry=30.0,
            ),
        )

    async def _ensure_client(self):
        """定期重建连接池：规避长驻连接被防火墙/网关静默掐断后卡死的风险。

        旧连接池延迟 60 秒关闭，避免关闭瞬间掐断其它信源的在途请求。
        """
        if time.time() - self._client_created_at < CONNECTION_RECYCLE_SECONDS:
            return
        old_client = self.client
        self.client = self._new_client()
        self._client_created_at = time.time()
        self._retire_client(old_client)

    def _retire_client(self, old_client: httpx.AsyncClient) -> None:
        async def closer() -> None:
            try:
                await asyncio.sleep(60)
            finally:
                try:
                    await old_client.aclose()
                except Exception:
                    logger.warning("旧连接池关闭时发生异常，已忽略。")

        task = asyncio.create_task(closer())
        self._retire_tasks.add(task)
        task.add_done_callback(self._retire_tasks.discard)

    def _on_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"信源后台任务异常退出: {exc!r}")

    def _record_download_results(
        self,
        img_type: str,
        results: list,
        item_keys: list = None,
        count_missing: bool = True,
        count_fail: bool = True,
        pending_grace_minutes: int | None = None,
        now: datetime | None = None,
    ):
        """把一轮采集的结果码归类写入下载统计（304/EXIST 不计数）。

        404 按 (信源, 时次) 去重计入 missing；回补等历史扫描可关闭
        count_missing/count_fail 只计成功落盘，避免重启导致计数虚高。

        时间网格信源在发布前必然 404，因此给出 pending_grace_minutes：
        时次年龄小于该宽限期时不记 missing，避免把"尚未发布"永久计成"上游缺失"。
        """
        current = now or datetime.now(TZ_UTC8)
        for i, r in enumerate(results):
            if r == "200":
                download_stats.record("ok")
            elif r == "404":
                if not count_missing:
                    continue
                key = item_keys[i] if item_keys and i < len(item_keys) and item_keys[i] else "static"
                if pending_grace_minutes and key != "static":
                    slot_ts = parse_frame_timestamp(key)
                    if slot_ts is not None and current - slot_ts < timedelta(minutes=pending_grace_minutes):
                        continue
                download_stats.record_missing(img_type, key)
            elif r in ("FETCH_ERR", "CRASH", "ERROR", "TIMEOUT"):
                if count_fail:
                    download_stats.record("fail")

    @staticmethod
    def _probe_key(source: str, target_time: datetime) -> str:
        return f"{source}:{target_time.strftime('%Y%m%d_%H%M%S')}"

    def _mark_probe_results(self, pairs: list, results: list) -> None:
        for (_, key), result in zip(pairs, results, strict=False):
            if result in ("200", "EXIST"):
                self.probes.mark_ok(key)
            elif result == "404":
                self.probes.mark_missing(key)
            elif result == "ERROR":
                self.probes.mark_permanent(key)
            else:
                self.probes.mark_transient(key)

    async def _fetch_single_radar(self, radar_type: str, target_time: datetime) -> str:
        """抓取单个时刻雷达图：内置坏图免疫与断点续传校验"""
        date_str = target_time.strftime("%Y%m%d")
        upstream_time_str = target_time.strftime("%Y%m%d%H%M00")
        internal_time_str = target_time.strftime("%Y%m%d_%H%M%S")
        file_name = f"{internal_time_str}_{radar_type}.png"
        
        save_dir = DATA_DIR / radar_type / date_str
        file_path = save_dir / file_name
        
        # 坚守文件系统绝对真相，同时建立物理残片剥离防线，防止迁移污染
        if file_path.exists():
            try:
                if file_path.stat().st_size < 100:
                    logger.warning(f"[{radar_type}] 发现本地坏图或零字节残片 {file_name}，正在物理移除并触发重抓。")
                    file_path.unlink()
                else:
                    return "EXIST" # 本地缓存健康，跳过
            except Exception:
                return "ERROR"
            
        url = build_radar_url(radar_type, target_time)

        attempt = 0
        while attempt <= MAX_RETRIES:
            try:
                async with self.client.stream("GET", url) as response:
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
                        logger.warning(f"[{radar_type}] 图像尾块损坏，重试: {e}")
                        raise ValueError("Radar verification failed") from e

                    await atomic_write_bytes_async(file_path, content)

                    await self.cursors[radar_type].update("", "", "", str(file_path))
                    logger.info(f"[{radar_type}] 新雷达图成功落盘: {file_name}")
                    return "200"
            except httpx.HTTPStatusError as e:
                if not is_retryable_status(e.response.status_code):
                    logger.error(f"[{radar_type}] 上游返回不可重试状态 {e.response.status_code}，放弃时次 {upstream_time_str}")
                    return "ERROR"
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.error(f"[{radar_type}] 达到最大重试次数，放弃时次 {upstream_time_str}")
                    return "FETCH_ERR"
                await asyncio.sleep(retry_delay(attempt, e.response.headers.get("Retry-After")))
            except (httpx.RequestError, ValueError):
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.error(f"[{radar_type}] 达到最大重试次数，放弃时次 {upstream_time_str}")
                    return "FETCH_ERR"
                await asyncio.sleep(retry_delay(attempt))
            except Exception as e:
                logger.exception(f"[{radar_type}] 发生未预期的致命异常: {e}")
                return "CRASH"
        return "TIMEOUT"

    async def _backfill_radar_source(self, radar_type: str, now: datetime) -> None:
        """扫描最近数小时的雷达缺口，自愈超过常规窗口的断网。"""
        slot_count = int(RADAR_BACKFILL_HOURS * 60 / RADAR_INTERVAL_MINUTES) + 1
        base_time = now.replace(
            minute=(now.minute // RADAR_INTERVAL_MINUTES) * RADAR_INTERVAL_MINUTES,
            second=0,
            microsecond=0,
        )
        pairs = [
            (target_time, self._probe_key(radar_type, target_time))
            for target_time in (
                base_time - timedelta(minutes=RADAR_INTERVAL_MINUTES * i)
                for i in range(slot_count)
            )
            if slot_is_mature(now, target_time, RADAR_MIN_AGE_MINUTES)
        ]
        semaphore = asyncio.Semaphore(RADAR_FETCH_CONCURRENCY)

        async def fetch_one(target_time: datetime) -> str:
            async with semaphore:
                return await self._fetch_single_radar(radar_type, target_time)

        results = await asyncio.gather(*[fetch_one(t) for t, _ in pairs])
        self._record_download_results(
            radar_type, results,
            item_keys=[key for _, key in pairs],
            count_missing=False, count_fail=False,
        )
        self._mark_probe_results(pairs, results)

        counts = {"200": 0, "EXIST": 0, "404": 0, "FAIL": 0}
        for result in results:
            counts[result if result in ("200", "EXIST", "404") else "FAIL"] += 1
        if counts["200"] or counts["FAIL"]:
            logger.info(
                f"[{radar_type}] 缺口回补完成：新增 {counts['200']} 张，"
                f"已存在 {counts['EXIST']} 张，上游缺失 {counts['404']} 个时次，失败 {counts['FAIL']} 个。"
            )

    async def _loop_radar_source(self, radar_type: str):
        """雷达图专属独立协程：滑动窗口 + 周期缺口回补"""
        logger.info(f"[{radar_type}] 独立滑动窗口监听线程启动...")
        last_backfill = 0.0
        while True:
            start_time = time.time()
            status_summary = "304"  # 默认若无实际落盘则定义为无变更状态

            try:
                await self._ensure_client()
                now = datetime.now(TZ_UTC8)

                # 启动即回补最近数小时缺口，之后每 30 分钟补漏一次
                if time.time() - last_backfill >= RADAR_BACKFILL_INTERVAL_MINUTES * 60:
                    await self._backfill_radar_source(radar_type, now)
                    last_backfill = time.time()

                base_time = now.replace(
                    minute=(now.minute // RADAR_INTERVAL_MINUTES) * RADAR_INTERVAL_MINUTES,
                    second=0,
                    microsecond=0,
                )
                slot_count = int(RADAR_POLL_WINDOW_HOURS * 60 / RADAR_INTERVAL_MINUTES) + 1
                pairs = [
                    (target_time, self._probe_key(radar_type, target_time))
                    for target_time in (
                        base_time - timedelta(minutes=RADAR_INTERVAL_MINUTES * i)
                        for i in range(slot_count)
                    )
                ]
                self.probes.prune({key for _, key in pairs})
                pending = [
                    (t, key) for t, key in pairs
                    if slot_is_mature(now, t, RADAR_MIN_AGE_MINUTES) and self.probes.should_probe(key)
                ]

                results = []
                if pending:
                    semaphore = asyncio.Semaphore(RADAR_FETCH_CONCURRENCY)

                    async def fetch_one(
                        target_time: datetime, sem: asyncio.Semaphore = semaphore
                    ) -> str:
                        async with sem:
                            return await self._fetch_single_radar(radar_type, target_time)

                    results = await asyncio.gather(*[fetch_one(t) for t, _ in pending])
                    self._record_download_results(
                        radar_type, results,
                        item_keys=[key for _, key in pending],
                        pending_grace_minutes=RADAR_PENDING_GRACE_MINUTES,
                    )
                    self._mark_probe_results(pending, results)

                if "200" in results:
                    status_summary = "200"
                elif any(r in ("FETCH_ERR", "CRASH", "ERROR") for r in results):
                    status_summary = "ERROR"
            except Exception as e:
                logger.error(f"[{radar_type}] 轮询异常 (隔离不扩散): {e}")
                status_summary = "CRASH"

            elapsed = time.time() - start_time
            # 增量推送当前信源指标
            self.metrics[radar_type] = {
                "elapsed": elapsed,
                "status": status_summary,
                "updated_at": time.time()
            }
            await download_stats.flush()
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _fetch_single_nmc(self, img_type: str, target_time: datetime) -> str:
        """抓取单个时次中央台卫星云图：本地去重 + 404 容忍 + 坏图重抓"""
        file_name = f"{target_time.strftime('%Y%m%d_%H%M%S')}_{img_type}.png"
        save_dir = DATA_DIR / img_type / target_time.strftime("%Y%m%d")
        file_path = save_dir / file_name

        # 本地缓存健康则零请求跳过；坏图/零字节残片物理移除后重抓
        if file_path.exists():
            try:
                if file_path.stat().st_size < 100:
                    logger.warning(f"[{img_type}] 发现本地坏图或零字节残片 {file_name}，正在物理移除并触发重抓。")
                    file_path.unlink()
                else:
                    return "EXIST"
            except Exception:
                return "ERROR"

        url = build_nmc_url(img_type, target_time)
        attempt = 0
        while attempt <= MAX_RETRIES:
            try:
                async with self.client.stream("GET", url) as response:
                    if response.status_code == 404:
                        # 上游该时次不存在（缺帧/未来时次），静默跳过避免日志刷屏
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
                        logger.warning(f"[{img_type}] 图像尾块损坏，重试: {e}")
                        raise ValueError("NMC satellite image verification failed") from e

                    await atomic_write_bytes_async(file_path, content)

                    await self.cursors[img_type].update("", "", "", str(file_path))
                    logger.info(f"[{img_type}] 新卫星云图成功落盘: {file_name}")
                    return "200"
            except httpx.HTTPStatusError as e:
                if not is_retryable_status(e.response.status_code):
                    logger.error(f"[{img_type}] 上游返回不可重试状态 {e.response.status_code}，放弃时次")
                    return "ERROR"
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.error(f"[{img_type}] 达到最大重试次数，放弃时次 {target_time.strftime('%Y%m%d%H%M%S')}")
                    return "FETCH_ERR"
                await asyncio.sleep(retry_delay(attempt, e.response.headers.get("Retry-After")))
            except (httpx.RequestError, ValueError):
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.error(f"[{img_type}] 达到最大重试次数，放弃时次 {target_time.strftime('%Y%m%d%H%M%S')}")
                    return "FETCH_ERR"
                await asyncio.sleep(retry_delay(attempt))
            except Exception as e:
                logger.exception(f"[{img_type}] 发生未预期的致命异常: {e}")
                return "CRASH"
        return "TIMEOUT"

    def _has_recent_frames(self, img_type: str, now: datetime, hours: int) -> bool:
        """判断本地是否已有最近 N 小时的帧，用于跳过启动时的全量回补。"""
        cutoff = now - timedelta(hours=hours)
        type_dir = DATA_DIR / img_type
        for days_back in (0, 1):
            date_dir = type_dir / (now - timedelta(days=days_back)).strftime("%Y%m%d")
            if not date_dir.is_dir():
                continue
            try:
                with os.scandir(date_dir) as entries:
                    for entry in entries:
                        if not entry.name.endswith(".png"):
                            continue
                        ts = parse_frame_timestamp(entry.name)
                        if ts is not None and ts >= cutoff:
                            return True
            except OSError:
                continue
        return False

    async def _backfill_nmc_source(self, img_type: str):
        """首次接入回补扫描：按时间网格枚举最近 N 天理论时次，分批并发抓取"""
        cfg = NMC_SATELLITE_TYPES[img_type]
        step_minutes = cfg["interval_minutes"]
        step = timedelta(minutes=step_minutes)
        now = datetime.now(TZ_UTC8)

        # 常规重启时最近窗口已有数据，无需每次重扫 3 天（约 430 个时次）
        if self._has_recent_frames(img_type, now, NMC_BACKFILL_RECENT_HOURS):
            logger.info(
                f"[{img_type}] 最近 {NMC_BACKFILL_RECENT_HOURS} 小时已有本地帧，跳过启动全量回补。"
            )
            return

        base_time = now.replace(minute=(now.minute // step_minutes) * step_minutes, second=0, microsecond=0)
        end_time = base_time - timedelta(days=NMC_BACKFILL_DAYS)

        slots = []
        t = base_time
        while t >= end_time:
            slots.append(t)
            t -= step
        slots = [t for t in slots if slot_is_mature(now, t, NMC_MIN_AGE_MINUTES)]

        logger.info(
            f"[{img_type}] 开始首次回补扫描：{len(slots)} 个理论时次"
            f"（最近 {NMC_BACKFILL_DAYS} 天，{step_minutes} 分钟步进）..."
        )
        semaphore = asyncio.Semaphore(NMC_BACKFILL_CONCURRENCY)

        async def fetch_one(target_time: datetime) -> str:
            async with semaphore:
                return await self._fetch_single_nmc(img_type, target_time)

        counts = {"200": 0, "EXIST": 0, "404": 0, "FAIL": 0}
        for i in range(0, len(slots), NMC_BACKFILL_BATCH_SIZE):
            await self._ensure_client()
            batch = slots[i:i + NMC_BACKFILL_BATCH_SIZE]
            pairs = [(t, self._probe_key(img_type, t)) for t in batch]
            results = await asyncio.gather(*[fetch_one(t) for t in batch])
            self._record_download_results(
                img_type, results,
                item_keys=[key for _, key in pairs],
                count_missing=False, count_fail=False,
            )
            self._mark_probe_results(pairs, results)
            for r in results:
                counts[r if r in ("200", "EXIST", "404") else "FAIL"] += 1
            if i + NMC_BACKFILL_BATCH_SIZE < len(slots):
                await asyncio.sleep(NMC_BATCH_PAUSE_SECONDS)
        await download_stats.flush()

        logger.info(
            f"[{img_type}] 回补扫描完成：新增 {counts['200']} 张，"
            f"已存在 {counts['EXIST']} 张，上游缺失 {counts['404']} 个时次，失败 {counts['FAIL']} 个。"
        )

    async def _loop_nmc_source(self, img_type: str):
        """中央台卫星源专属协程：回补后进入 4 小时滑动窗口增量探测"""
        logger.info(f"[{img_type}] 独立中央台卫星源监听线程启动...")
        await self._backfill_nmc_source(img_type)

        while True:
            start_time = time.time()
            status_summary = "304"  # 默认无新增

            try:
                await self._ensure_client()
                cfg = NMC_SATELLITE_TYPES[img_type]
                step_minutes = cfg["interval_minutes"]
                now = datetime.now(TZ_UTC8)
                base_time = now.replace(minute=(now.minute // step_minutes) * step_minutes, second=0, microsecond=0)

                slot_count = int(NMC_POLL_WINDOW_HOURS * 60 / step_minutes)
                pairs = [
                    (target_time, self._probe_key(img_type, target_time))
                    for target_time in (
                        base_time - timedelta(minutes=step_minutes * i)
                        for i in range(slot_count)
                    )
                ]
                self.probes.prune({key for _, key in pairs})
                pending = [
                    (t, key) for t, key in pairs
                    if slot_is_mature(now, t, NMC_MIN_AGE_MINUTES) and self.probes.should_probe(key)
                ]

                results = []
                if pending:
                    semaphore = asyncio.Semaphore(NMC_BACKFILL_CONCURRENCY)

                    async def fetch_one(
                        target_time: datetime, sem: asyncio.Semaphore = semaphore
                    ) -> str:
                        async with sem:
                            return await self._fetch_single_nmc(img_type, target_time)

                    results = await asyncio.gather(*[fetch_one(t) for t, _ in pending])
                    self._record_download_results(
                        img_type, results,
                        item_keys=[key for _, key in pending],
                        pending_grace_minutes=NMC_PENDING_GRACE_MINUTES,
                    )
                    self._mark_probe_results(pending, results)

                if "200" in results:
                    status_summary = "200"
                elif any(r in ("FETCH_ERR", "CRASH", "ERROR") for r in results):
                    status_summary = "ERROR"
            except Exception as e:
                logger.error(f"[{img_type}] 轮询异常 (隔离不扩散): {e}")
                status_summary = "CRASH"

            elapsed = time.time() - start_time
            self.metrics[img_type] = {
                "elapsed": elapsed,
                "status": status_summary,
                "updated_at": time.time()
            }
            await download_stats.flush()
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _loop_single_source(self, img_type: str, url: str):
        """静态实况图专属独立协程：网络故障退避在单线内收容"""
        logger.info(f"[{img_type}] 独立流式截断监听线程启动...")
        while True:
            start_time = time.time()
            status_code = "INIT"
            cursor_mgr = self.cursors[img_type]
            
            try:
                await self._ensure_client()
                cursor_data = cursor_mgr.read()
                headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
                if cursor_data["last_modified"]:
                    headers["If-Modified-Since"] = cursor_data["last_modified"]

                ts = str(time.time())
                target_url = f"{url}?_ts={ts}"

                attempt = 0
                while attempt <= MAX_RETRIES:
                    try:
                        async with self.client.stream("GET", target_url, headers=headers) as response:
                            status_code = str(response.status_code)
                            if response.status_code == 304:
                                break
                            if is_retryable_status(response.status_code):
                                raise httpx.HTTPStatusError(
                                    f"HTTP {response.status_code}", request=response.request, response=response
                                )

                            response.raise_for_status()
                            server_last_modified = response.headers.get("Last-Modified")
                            server_content_length = response.headers.get("Content-Length")

                            if (server_last_modified == cursor_data["last_modified"] and
                                server_content_length == cursor_data["content_length"]):
                                status_code = "304"
                                break

                            logger.info(f"[{img_type}] 探测到新响应头，开始下载实体流...")
                            content = await response.aread()

                            try:
                                img = Image.open(io.BytesIO(content))
                                img.verify()
                            except Exception as e:
                                logger.warning(f"[{img_type}] 图像尾块损坏(上游竞态写入): {e}")
                                raise ValueError("Image IEND validation failed.") from e

                            file_hash = hashlib.sha256(content).hexdigest()
                            if file_hash == cursor_data.get("hash"):
                                await cursor_mgr.update(server_last_modified, server_content_length, file_hash, cursor_data.get("file_path", ""))
                                status_code = "304"
                                break

                            try:
                                if server_last_modified:
                                    dt_gmt = email.utils.parsedate_to_datetime(server_last_modified)
                                    true_time = dt_gmt.astimezone(TZ_UTC8)
                                else:
                                    true_time = datetime.now(TZ_UTC8)
                            except Exception:
                                true_time = datetime.now(TZ_UTC8)

                            date_str = true_time.strftime("%Y%m%d")
                            time_str = true_time.strftime("%Y%m%d_%H%M%S")

                            file_name = f"{time_str}_{img_type}.png"
                            file_path = cursor_mgr.type_dir / date_str / file_name

                            await atomic_write_bytes_async(file_path, content)

                            logger.info(f"[{img_type}] 新气象图成功落盘: {file_name}")
                            await cursor_mgr.update(server_last_modified, server_content_length, file_hash, str(file_path))
                            status_code = "200"
                            break
                    except httpx.HTTPStatusError as e:
                        if not is_retryable_status(e.response.status_code):
                            logger.error(f"[{img_type}] 上游返回不可重试状态 {e.response.status_code}")
                            status_code = "ERROR"
                            break
                        attempt += 1
                        status_code = "ERROR"
                        if attempt > MAX_RETRIES:
                            logger.error(f"[{img_type}] 达到最大重试次数，放弃当前轮次。")
                            break
                        await asyncio.sleep(retry_delay(attempt, e.response.headers.get("Retry-After")))
                    except (httpx.RequestError, ValueError):
                        attempt += 1
                        status_code = "ERROR"
                        if attempt > MAX_RETRIES:
                            logger.error(f"[{img_type}] 达到最大重试次数，放弃当前轮次。")
                            break
                        await asyncio.sleep(retry_delay(attempt))
            except Exception as e:
                logger.error(f"[{img_type}] 致命异常: {e}")
                status_code = "CRASH"
                
            elapsed = time.time() - start_time
            # 写入全局总线字典
            self.metrics[img_type] = {
                "elapsed": elapsed,
                "status": status_code,
                "updated_at": time.time()
            }
            self._record_download_results(img_type, [status_code])
            await download_stats.flush()
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _monitor_loop(self):
        """专职全局观测协程：输出全节点脉搏报告，完美兼容飞行中状态"""
        await asyncio.sleep(5)
        while True:
            try:
                snapshot = list(self.metrics.values())
                
                max_elapsed = max(m["elapsed"] for m in snapshot)
                
                count_200 = sum(1 for m in snapshot if m["status"] == "200")
                count_304 = sum(1 for m in snapshot if m["status"] == "304")
                count_err = sum(1 for m in snapshot if m["status"] in ["ERROR", "CRASH", "FETCH_ERR"])
                # 增量拦截：捕捉那些由于网络太慢还在飞的请求
                count_fly = sum(1 for m in snapshot if m["status"] == "INIT")
                
                fly_report = f", 正在探测:{count_fly}路" if count_fly > 0 else ""
                
                logger.info(
                    f"==== [全局脉搏] {len(self.all_types)}路并行探测状态审计。最大完工耗时: {max_elapsed:.2f}秒|"
                    f"链路状态: [200落盘:{count_200}路, 304死水:{count_304}路, 异常:{count_err}路{fly_report}] =="
                )
            except Exception as e:
                logger.error(f"[Monitor] 性能度量报表生成失败: {e}")
                
            await asyncio.sleep(POLL_INTERVAL)

    async def run_forever(self):
        """派生全部信源的并发永动机链路与 1 条独立性能监控哨兵"""
        logger.info(f"系统解耦并发总线正在为 {len(self.all_types)} 个独立信源分配永久物理线程...")

        for img_type, url in TARGET_URLS.items():
            self._add_task(self._loop_single_source(img_type, url))
        for r_type in RADAR_TYPES:
            self._add_task(self._loop_radar_source(r_type))
        for img_type in NMC_SATELLITE_TYPES:
            self._add_task(self._loop_nmc_source(img_type))

        # 激活全局脉搏监测哨兵
        self._add_task(self._monitor_loop())

        await self._stop_event.wait()

    def _add_task(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        task.add_done_callback(self._on_task_done)
        self._tasks.append(task)
        return task

    async def shutdown(self):
        """停止全部信源子任务、回收连接池并落盘统计。"""
        self._stop_event.set()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        retirees = list(self._retire_tasks)
        for task in retirees:
            task.cancel()
        if retirees:
            await asyncio.gather(*retirees, return_exceptions=True)

        try:
            await self.client.aclose()
        except Exception:
            logger.warning("关闭当前连接池时发生异常，已忽略。")
        await download_stats.flush()
