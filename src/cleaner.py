import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from config import DATA_DIR, TZ_UTC8

logger = logging.getLogger(__name__)

class StorageCleaner:
    """管理本地数据的生命周期 (GC)"""
    
    def __init__(self):
        self.output_dir = DATA_DIR / "output"
        self.output_dir.mkdir(exist_ok=True)

    def run_gc(self):
        """执行垃圾回收，清理过期数据"""
        now = datetime.now(TZ_UTC8)
        png_cutoff = now - timedelta(days=7)
        mp4_cutoff = now - timedelta(days=30)
        
        logger.info("开始执行本地存储 GC 清理任务...")
        
        # 1. 清理超过 7 天的原始 PNG 日期目录
        for img_type_dir in DATA_DIR.iterdir():
            if not img_type_dir.is_dir() or img_type_dir.name == "output":
                continue
                
            for date_dir in img_type_dir.iterdir():
                if not date_dir.is_dir() or date_dir.name.startswith("."):
                    continue
                    
                try:
                    # 目录名格式为 YYYYMMDD
                    dir_date = datetime.strptime(date_dir.name, "%Y%m%d").replace(tzinfo=TZ_UTC8)
                    if dir_date < png_cutoff:
                        shutil.rmtree(date_dir)
                        logger.info(f"[GC] 已删除过期图片目录: {date_dir}")
                except ValueError:
                    continue
                    
        # 2. 清理超过 30 天的 MP4 视频 (改为递归扫描 rglob，覆盖 cron 文件夹)
        for mp4_file in self.output_dir.rglob("*.mp4"):
            try:
                # 获取文件的物理修改时间
                mtime = datetime.fromtimestamp(mp4_file.stat().st_mtime, TZ_UTC8)
                if mtime < mp4_cutoff:
                    mp4_file.unlink()
                    logger.info(f"[GC] 已删除过期视频文件: {mp4_file}")
            except Exception as e:
                logger.warning(f"[GC] 检查视频文件 {mp4_file} 失败: {e}")
                
        # 3. 顺手清理已空的 cron 历史日期文件夹
        cron_base_dir = self.output_dir / "cron"
        if cron_base_dir.exists():
            for cron_date_dir in cron_base_dir.iterdir():
                if cron_date_dir.is_dir() and not any(cron_date_dir.iterdir()):
                    try:
                        cron_date_dir.rmdir()
                        logger.info(f"[GC] 已清理空的历史任务文件夹: {cron_date_dir.name}")
                    except Exception:
                        pass
                        
        logger.info("GC 清理任务执行完毕。")