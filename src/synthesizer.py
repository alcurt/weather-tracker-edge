import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from config import DATA_DIR, ROOT_DIR, TZ_UTC8

logger = logging.getLogger(__name__)

class Synthesizer:
    """跨日寻址与 FFmpeg 管道推流合成器 (基于定向物理扫描)"""
    
    def __init__(self):
        self.ffmpeg_path = ROOT_DIR / "ffmpeg.exe"
        if sys.platform != "win32":
            self.ffmpeg_path = ROOT_DIR / "ffmpeg"
            
        self.output_dir = DATA_DIR / "output"
        self.output_dir.mkdir(exist_ok=True)

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
        
        # 1. 核心修正：绝对禁止扫描过去7天所有目录！利用 os.scandir 毫秒级锁定有效时间终点
        end_time = None
        if is_cron:
            start_time = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time.replace(hour=23, minute=59, second=59)
        elif start_time is not None:
            # 显式起点：end = start + hours，跨天由下方定向扫描自然处理
            start_time = start_time.astimezone(TZ_UTC8)
            end_time = start_time + timedelta(hours=hours)
        else:
            for days_back in [0, 1]:
                target_date_dir = type_dir / (now - timedelta(days=days_back)).strftime("%Y%m%d")
                if target_date_dir.exists() and target_date_dir.is_dir():
                    files = [f.name for f in os.scandir(target_date_dir) if f.name.endswith(".png")]
                    if files:
                        files.sort()
                        parts = files[-1].split('_')
                        if len(parts) >= 2:
                            try:
                                end_time = datetime.strptime(f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S").replace(tzinfo=TZ_UTC8)
                                break
                            except ValueError:
                                pass
                                
        if not end_time:
            logger.warning(f"[{img_type}] 近期目录为空，没有任何可供推演的物理锚点。")
            return False
            
        if not is_cron and start_time is None:
            if hours == 0:
                start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                start_time = end_time - timedelta(hours=hours)

        # 2. 定向扫描：精确推算 start_time 到 end_time 跨越的那 1~2 个绝对日期目录
        valid_images = []
        current_date = start_time.date()
        while current_date <= end_time.date():
            date_dir = type_dir / current_date.strftime("%Y%m%d")
            if date_dir.exists() and date_dir.is_dir():
                with os.scandir(date_dir) as entries:
                    for entry in entries:
                        if entry.name.endswith(".png"):
                            parts = entry.name.split('_')
                            if len(parts) >= 2:
                                try:
                                    t = datetime.strptime(f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S").replace(tzinfo=TZ_UTC8)
                                    if start_time <= t <= end_time:
                                        valid_images.append((entry.path, t))
                                except ValueError:
                                    pass
            current_date += timedelta(days=1)
            
        if not valid_images:
            logger.warning(f"[{img_type}] 在截取的时间窗推演内没有任何数据。")
            return False
            
        valid_images.sort(key=lambda x: x[1])
        sorted_paths = [item[0] for item in valid_images]
        
        if end_time.date() != start_time.date():
            # 跨日产物补上结束日期，避免仅凭起始日期误判时间归属
            output_filename = (
                f"{img_type}_{start_time.strftime('%Y%m%d_%H%M')}"
                f"_to_{end_time.strftime('%Y%m%d_%H%M')}.mp4"
            )
        else:
            output_filename = f"{img_type}_{start_time.strftime('%Y%m%d_%H%M')}_to_{end_time.strftime('%H%M')}.mp4"
        if is_cron:
            target_out_dir = self.output_dir / "cron" / start_time.strftime('%Y%m%d')
        else:
            target_out_dir = self.output_dir
            
        target_out_dir.mkdir(parents=True, exist_ok=True)
        output_path = target_out_dir / output_filename
        
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
            str(output_path)
        ]
        
        creationflags = 0
        if sys.platform == "win32":
            creationflags = 0x00004000

        logger.info(f"[{img_type}] 真实时间跨度 {start_time.strftime('%H:%M')} -> {end_time.strftime('%H:%M')}，共 {len(sorted_paths)} 帧...")
        
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
                return False
                
            logger.info(f"[{img_type}] 视频合成成功！输出路径: {output_path}")
            return True
            
        except Exception as e:
            logger.exception(f"[{img_type}] 合成管道发生严重崩溃: {e}")
            if process and process.poll() is None:
                process.kill()
                process.communicate()
            return False
