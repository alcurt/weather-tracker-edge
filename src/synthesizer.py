import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from analytics import parse_frame_timestamp
from config import DATA_DIR, ROOT_DIR, TZ_UTC8

logger = logging.getLogger(__name__)


def build_output_path(output_dir: Path, img_type: str, start: datetime, end: datetime, is_cron: bool) -> Path:
    """产物路径：跨日产物带上结束日期；定时任务按起始日期归档到 cron 子目录。"""
    if end.date() != start.date():
        # 跨日产物补上结束日期，避免仅凭起始日期误判时间归属
        output_filename = (
            f"{img_type}_{start.strftime('%Y%m%d_%H%M')}"
            f"_to_{end.strftime('%Y%m%d_%H%M')}.mp4"
        )
    else:
        output_filename = f"{img_type}_{start.strftime('%Y%m%d_%H%M')}_to_{end.strftime('%H%M')}.mp4"
    target_out_dir = output_dir / "cron" / start.strftime("%Y%m%d") if is_cron else output_dir
    return target_out_dir / output_filename


class Synthesizer:
    """跨日寻址与 FFmpeg 管道推流合成器 (基于定向物理扫描)"""

    def __init__(self):
        self.ffmpeg_path = ROOT_DIR / "ffmpeg.exe"
        if sys.platform != "win32":
            self.ffmpeg_path = ROOT_DIR / "ffmpeg"

        self.output_dir = DATA_DIR / "output"
        self.output_dir.mkdir(exist_ok=True)

    def _collect_frames(self, type_dir: Path, start_time: datetime, end_time: datetime) -> list[tuple[str, datetime]]:
        """定向扫描跨度为 1~2 天的日期目录，返回时间窗内的 (路径, 时间戳)。"""
        valid_images = []
        current_date = start_time.date()
        while current_date <= end_time.date():
            date_dir = type_dir / current_date.strftime("%Y%m%d")
            if date_dir.exists() and date_dir.is_dir():
                with os.scandir(date_dir) as entries:
                    for entry in entries:
                        if not entry.name.endswith(".png"):
                            continue
                        ts = parse_frame_timestamp(entry.name)
                        if ts is not None and start_time <= ts <= end_time:
                            valid_images.append((entry.path, ts))
            current_date += timedelta(days=1)
        valid_images.sort(key=lambda x: x[1])
        return valid_images

    def generate_video(
        self,
        img_type: str,
        hours: int,
        is_cron: bool = False,
        fps: float = 10.0,
        start_time: datetime | None = None,
    ) -> bool:
        if not self.ffmpeg_path.exists():
            logger.error(f"未找到 FFmpeg 二进制文件: {self.ffmpeg_path}。请手动下载并放入项目根目录。")
            return False

        type_dir = DATA_DIR / img_type
        if not type_dir.exists():
            logger.warning(f"[{img_type}] 暂无该类型的数据目录。")
            return False

        now = datetime.now(TZ_UTC8)

        # 1. 反向推演：以物理上已存在的最晚一帧为绝对终点，避免上游延迟发布导致尾部缺帧
        end_time = None
        if is_cron:
            start_time = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time.replace(hour=23, minute=59, second=59)
        elif start_time is not None:
            start_time = start_time.astimezone(TZ_UTC8)
            end_time = start_time + timedelta(hours=hours)
        else:
            for days_back in (0, 1):
                target_date_dir = type_dir / (now - timedelta(days=days_back)).strftime("%Y%m%d")
                if target_date_dir.exists() and target_date_dir.is_dir():
                    latest = None
                    with os.scandir(target_date_dir) as entries:
                        for entry in entries:
                            if not entry.name.endswith(".png"):
                                continue
                            ts = parse_frame_timestamp(entry.name)
                            if ts is not None and (latest is None or ts > latest):
                                latest = ts
                    if latest is not None:
                        end_time = latest
                        break

        if not end_time:
            logger.warning(f"[{img_type}] 近期目录为空，没有任何可供推演的物理锚点。")
            return False

        if not is_cron and start_time is None:
            if hours == 0:
                start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                start_time = end_time - timedelta(hours=hours)

        valid_images = self._collect_frames(type_dir, start_time, end_time)
        if not valid_images:
            logger.warning(f"[{img_type}] 在截取的时间窗推演内没有任何数据。")
            return False
        sorted_paths = [item[0] for item in valid_images]

        output_path = build_output_path(self.output_dir, img_type, start_time, end_time, is_cron)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # 先写 .part 再原子替换：中断/失败不会留下可被产物面板列出的坏 mp4
        part_path = output_path.with_name(output_path.name + ".part")

        cmd = [
            str(self.ffmpeg_path),
            "-y",
            "-f", "image2pipe",
            "-framerate", str(fps),
            "-i", "-",
            "-threads", "2",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-loglevel", "error",
            "-f", "mp4",
            str(part_path),
        ]

        creationflags = 0
        if sys.platform == "win32":
            creationflags = 0x00004000

        logger.info(
            f"[{img_type}] 真实时间跨度 {start_time.strftime('%H:%M')} -> "
            f"{end_time.strftime('%H:%M')}，共 {len(sorted_paths)} 帧..."
        )

        process = None
        try:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                creationflags=creationflags
            )

            try:
                for img_path in sorted_paths:
                    with open(img_path, "rb") as f:
                        process.stdin.write(f.read())

                # 末尾定格 2 秒：ffmpeg 对无音轨流有丢最后一帧的倾向
                hold_frames = int(fps * 2)
                if hold_frames > 0:
                    with open(sorted_paths[-1], "rb") as f:
                        last_frame_data = f.read()
                        for _ in range(hold_frames):
                            process.stdin.write(last_frame_data)

                process.stdin.close()
                _, stderr_data = process.communicate(timeout=300)

            except Exception as inner_e:
                logger.error(f"[{img_type}] 数据写入管道期间崩溃，正在强制收割 FFmpeg 进程...")
                if process:
                    process.kill()
                    process.communicate()
                raise inner_e

            if process.returncode != 0:
                logger.error(f"[{img_type}] FFmpeg 压制失败，Exit Code: {process.returncode}")
                logger.error(f"FFmpeg 报错详情: {stderr_data.decode('utf-8', errors='ignore')}")
                part_path.unlink(missing_ok=True)
                return False

            try:
                os.replace(part_path, output_path)
            except OSError as e:
                # 同区间重合成时文件可能正被客户端播放占用：保留完整产物并换名落盘
                fallback = output_path.with_name(f"{output_path.stem}_{int(time.time())}.mp4")
                logger.warning(f"[{img_type}] 原子替换失败({e})，改存为 {fallback.name}")
                os.replace(part_path, fallback)
                output_path = fallback

            logger.info(f"[{img_type}] 视频合成成功！输出路径: {output_path}")
            return True

        except Exception as e:
            logger.exception(f"[{img_type}] 合成管道发生严重崩溃: {e}")
            if process and process.poll() is None:
                process.kill()
                process.communicate()
            part_path.unlink(missing_ok=True)
            return False
