import os
from datetime import timedelta, timezone
from pathlib import Path

# 核心常量
POLL_INTERVAL = 120  # 轮询间隔(秒)，两分钟探测一次
MAX_RETRIES = 5      # 网络异常最大重试次数
BASE_TIMEOUT = 15.0  # HTTP 请求基础超时时间

# 连接池生命周期：长驻连接可能被防火墙/网关静默掐断，定期重建连接池规避
CONNECTION_RECYCLE_SECONDS = 1800

# 统一请求 UA，与实体机 Edge 浏览器一致，降低被上游识别为爬虫的概率
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0"
)

# 时区设定 (UTC+8)
TZ_UTC8 = timezone(timedelta(hours=8))

# 存储路径设定 (指向上级目录下的 data 和 logs)
ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"

# 确保基础目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 目标 URL 映射字典 (8种类型)
TARGET_URLS = {
    "TempB": "http://www.tqyb.com.cn/data/obtMap/BCGZ_TempB.png",
    "TempC": "http://www.tqyb.com.cn/data/obtMap/BCGZ_TempC.png",
    "RainB": "http://www.tqyb.com.cn/data/obtMap/BCGZ_RainB.png",
    "RainC": "http://www.tqyb.com.cn/data/obtMap/BCGZ_RainC.png",
    "Rain24hB": "http://www.tqyb.com.cn/data/obtMap/BCGZ_Rain24hB.png",
    "Rain24hC": "http://www.tqyb.com.cn/data/obtMap/BCGZ_Rain24hC.png",
    "WindB": "http://www.tqyb.com.cn/data/obtMap/BCGZ_WindB.png",
    "WindC": "http://www.tqyb.com.cn/data/obtMap/BCGZ_WindC.png",
}

# 雷达图配置
RADAR_TYPES = {
    "RadarGZ": "gz", # 广州组合反射率
    "RadarGD": "gd", # 广东雷达拼图
}

# 雷达图动态解析 URL 模板
RADAR_URL_TEMPLATE = "http://www.tqyb.com.cn/data/swan/mcr/{date_str}/{upstream_time_str}_{suffix}.png"

RADAR_INTERVAL_MINUTES = 6        # 雷达图固定刷新间隔
RADAR_POLL_WINDOW_HOURS = 1       # 常规轮询回看窗口（抓取新鲜度）
RADAR_BACKFILL_HOURS = 6          # 启动/周期缺口回补窗口（上游保留 >=24h）
RADAR_BACKFILL_INTERVAL_MINUTES = 30  # 缺口回补周期
RADAR_FETCH_CONCURRENCY = 8       # 单轮雷达抓取并发上限
RADAR_PENDING_GRACE_MINUTES = 20  # 超过该年龄仍 404 才计入"上游缺失"
RADAR_MIN_AGE_MINUTES = 6         # 时次至少成熟 6 分钟才探测，避免发布前必然 404

# ==========================================
# 中央气象台卫星云图 (FY4C) 配置
# ==========================================
# 中央台图片 URL 可直接由观测时间戳构造（?v= 缓存击穿参数可省略），
# 采用与雷达一致的"时间网格构造 + 404 容忍 + 本地去重"策略。
# 注意：文件名时间戳与 URL 中的日期路径均为 UTC，北京时间 = UTC + 8h。
NMC_SATELLITE_TYPES = {
    "FY4C": {
        "display_name": "风云4C云图",
        "product_code": "WXBL",
        "interval_minutes": 10,
        "url_template": "https://image.nmc.cn/product/{date_path}/WXBL/medium/SEVP_NSMC_WXBL_FY4C_ETCC_ACHN_LNO_PY_{timestamp}000.JPG",
    }
}

NMC_BACKFILL_START_HOURS = 24   # 启动先补最近 24 小时
NMC_BACKFILL_TARGET_HOURS = 48  # 自适应目标：上游保留更久则继续补到 48 小时
NMC_BACKFILL_PHASE_GAP_SECONDS = 60  # 两阶段之间稍作停顿，避免连续压 CDN
NMC_GAP_SWEEP_INTERVAL_MINUTES = 360  # 运行期每 6 小时扫一次历史缺口
NMC_POLL_WINDOW_HOURS = 4      # 常规轮询滑动窗口（小时），只负责新鲜帧
NMC_BACKFILL_BATCH_SIZE = 18   # 回补每批并发时次数
NMC_BACKFILL_CONCURRENCY = 8   # 回补全局并发上限
NMC_BATCH_PAUSE_SECONDS = 1.0  # 批间停顿，避免压垮上游 CDN
NMC_PENDING_GRACE_MINUTES = 40  # 超过该年龄仍 404 才计入"上游缺失"
NMC_MIN_AGE_MINUTES = 10        # 时次至少成熟 10 分钟才探测，避免发布前必然 404
