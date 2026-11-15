
---

# 天气追踪边缘节点 (Weather Tracker Edge Node)

## 产品需求文档 (PRD) 与技术架构白皮书

### 1. 项目背景与系统定位

本系统是一个专为低功耗、老旧 x86 硬件（目标宿主：Intel Core i5-4200U, 16GB RAM, SSD, Windows 10 IoT Enterprise LTSC）量身设计的 7x24 小时自动化气象数据采集与计算边缘节点。
系统负责高频监听并抓取广州气象台发布的实况气象图、雷达回波图，以及中央气象台的风云 4C 卫星云图，将离散的图像时间序列按需合成连贯的 MP4 视频流。为最大限度压榨底层硬件算力并降低系统开销，项目彻底废弃了阻塞式的桌面 GUI 框架，采用基于 ASGI 的无头（Headless）运行策略，依托局域网 Web 接口提供全方位的控制与深度可观测性。

---

### 2. 核心功能需求

#### 2.1 混合状态机协议探测 (Hybrid Protocol Polling Engine)

系统支持对 11 个独立气象图像信源进行并发非阻塞探测，针对数据源的不同更新发布特性，底层划分为三轨探测引擎：

* **静态实况图流式截断探测（8个信源）**：
* 包含：气温（分布/填色）、本小时降水（分布/填色）、24小时降水（分布/填色）、风速（分布/填色）。
* **流式截断（Stream Truncation）**：为绕过 WAF 拦截并防御服务端忽略 `If-Modified-Since` 协议头，系统对带时间戳查询参数的 URL 发起 HTTP/2 流式 GET 请求（`stream=True`）。若读取 HTTP Header 发现 `Last-Modified` 与 `Content-Length` 与本地游标一致，立即掐断 TCP 连接，拒收 Body 载荷，极大降低边缘节点网络吞吐开销。
* **时空锚点还原**：实况图命名彻底摒弃本地系统时间，通过解析 HTTP 响应头中的 `Last-Modified` GMT 时间并转化为 UTC+8 北京时间作为物理文件名，从根本上解决因本地网络延迟或挂机休眠引起的时空错位。


* **动态雷达图滑动窗口回溯探测（2个信源）**：
* 包含：广州组合反射率（`gz` 后缀）、广东雷达拼图（`gd` 后缀）。
* **时间轴推演**：雷达图 URL 路径内嵌精确到分钟的绝对时间戳（例如 `.../20260716/20260716014200_gz.png`），且固定每 6 分钟更新一帧。每轮轮询以当前时间向下取整至 6 分钟为基准，回看最近 1 小时的 11 个理论时次并发探测；时次至少成熟 6 分钟才发包（发布前探测必然是 404）。启动及之后每 30 分钟额外扫描最近 6 小时缺口，用于自愈超过 1 小时的断网（上游实测保留 24 小时以上）。
* **断点自愈与发布延迟**：本地已有帧零请求跳过，只对缺失时次发包；上游尚未发布的时次会先返回 404，系统对该时次做指数退避（60s → 120s → 240s → 300s），避免发布前每 2 分钟重复请求，同时不延迟本地已有数据的读取。

* **中央台卫星云图时间网格探测（1个信源）**：
* 包含：风云 4C 云图（FY4C，产品代码 WXBL，10 分钟一帧）。
* **UTC 时间锚点**：中央台图片 URL 的文件名时间戳与路径日期均为 UTC（北京时间 = UTC + 8h），系统以北京时轮询网格为基准，构造 URL 前先转换为 UTC，落盘仍按北京时间归档。
* **启动两阶段自适应回补**：启动后立即补最近 24 小时（Phase A）；随后用二分探测上游实际保留深度（当前实测约 24 小时，目标 48 小时），能到 48 小时则继续补 24-48 小时（Phase B），探测不到则跳过并记录实际深度。每个时次只尝试一次，失败留给下一轮，总耗时远低于 30 分钟。
* **滑动窗口增量与缺口扫描**：常规每 2 分钟扫描最近 4 小时窗口（24 个理论时次），本地已有帧零请求跳过，只对新增与漏抓时次发包；时次至少成熟 10 分钟才探测，404 时次进入指数退避。运行期每 6 小时复用两阶段回补扫描一次历史缺口，用于自愈长时间在线但断网的场景。

#### 2.1.4 连接生命周期管理 (Connection Lifecycle)

* 长驻 HTTP/2 连接可能被防火墙或中间网关静默掐断，系统每 1800 秒整体关闭并重建 httpx 连接池，规避连接假死导致的请求超时。旧连接池采用延迟退役：新池就绪后保留 60 秒再关闭，避免关闭瞬间掐断其它信源仍在途的请求。
* httpx 以 `trust_env=False` 构造，守护进程不继承桌面环境的代理设置。桌面代理配置损坏时（例如 `NO_PROXY` 含非法片段），继承环境会让 httpx 在构造阶段直接抛异常，导致整个节点无法启动。
* 所有请求统一携带浏览器 UA，降低被上游识别为爬虫而限流的概率。



#### 2.2 数据校验与防竞争防线 (Data Integrity & Deduplication)

* **内存摘要防重**：下载完整的图片二进制实体后，在内存中计算 `SHA-256` 哈希摘要，与 `.latest_cursor.json` 中的历史哈希对比。即使服务端修改了时间戳，只要图像内容同质，直接丢弃避免重复落盘。
* **IEND 块校验**：调用 Pillow 对下载的载荷进行 `img.verify()` 校验。能够精准拦截上游服务端因竞态写入（File Contention）导致的截断帧或坏图，校验失败则抛出异常进入指数退避（Exponential Backoff）重试，绝不让损坏的数据污染本地序列。
* **原子落盘**：图片先写同目录 `*.part` 临时文件，再通过 `os.replace` 原子替换为正式文件名；游标与定时任务状态同样采用原子写。进程被强杀或断电时，正式文件名下不会出现半张图/半个 MP4，残留的 `.part` 由 GC 定时清理。
* **状态码分类重试**：`404` 视为上游缺帧静默跳过；`429/5xx/网络异常/坏图` 进入指数退避重试（尊重 `Retry-After`）；其余 `4xx` 视为永久失败，不做无意义重试。

#### 2.3 动态压制与反向推演算法 (Synthesis & Reverse Deduction Pipeline)

* **反向时间轴推演**：合成视频时，系统禁止依赖请求下发时间的瞬时截断，避免因气象台延迟发布导致最后几帧缺失。算法强制先对物理文件夹进行无锁扫描，**提取当前已存在的最晚一帧作为绝对时间终点**，减去用户请求的时间跨度（1h, 3h, 6h 等）得出起始点，对时空连续体进行严谨过滤。
* **自适应降帧（Dynamic FPS）**：根据合成请求的时间窗跨度自动匹配视觉最优帧率：
* 1 小时（约 10-12 帧）-> `4.0 FPS`
* 3 小时（约 30-36 帧）-> `6.0 FPS`
* 6 小时（约 60-72 帧）-> `8.0 FPS`
* 12 小时（约 120-144 帧）-> `9.6 FPS`
* 24 小时（约 240-288 帧）-> `14.4 FPS`


* **显式任务模型**：合成请求进入 `JobManager`，状态为 `queued / running / done / failed`，记录帧进度、输出相对路径与错误信息；相同参数的任务在排队或执行中自动去重，WebUI 可查看队列、进度并直接下载产物。
* **防吞帧定格补偿（Frame Padding）**：鉴于 FFmpeg 处理无音频轨流媒体时的最后一帧丢弃通病，系统在管道写入闭合前，通过内存缓冲区将合法序列的最后一帧二进制流重复向 stdin 管道灌入 `fps * 2` 次，实现视频末尾画面的绝对 2 秒定格，以零磁盘 I/O 开销大幅提升交互观感。
* **僵尸进程收割（Process Reaper）**：FFmpeg 通过管道 `stdin` 接收数据期间，若 Python IO 线程遭遇系统锁或底层错误异常，系统会捕获异常并立即触发 `process.kill()` 强行猎杀子进程；同时对 `process.communicate()` 设定 300 秒硬性超时，彻底消除僵尸进程导致的内存泄漏危险。

#### 2.4 自动化作业与生命周期治理 (Cron & Garbage Collection)

* **每日总结任务（Cron Loop）**：每日 `00:15` 且进程在线时触发定时器，自动构建昨日全天（`00:00:00` 至 `23:59:59`）的 7 个信源（4 路广州实况填色图 + 2 路雷达 + 中央台 FY4C 云图）24 小时跨度任务，投递至合成队列。对于定时任务，文件会路由落盘至专属的按日归档目录。**启动时不补跑历史定时任务**：若 `00:15` 时进程未运行，该日不再自动补合成，需要时通过 WebUI 手动选择时间轴生成。
* **存储自清理（GC Loop）**：独立守护协程通过计算当前至次日 `00:01` 的绝对时间差进入零 CPU 占用长眠。唤醒后递归扫描磁盘：
* **原始图 GC**：精确斩除距今超过 7 天的 `YYYYMMDD` 原始图片物理文件夹。
* **视频 GC**：使用 `rglob` 递归扫描所有 MP4 产物，精准清除最后修改时间（`mtime`）超过 30 天的视频，顺手收割空的子文件夹，确保硬盘用量达到恒定平衡点。



#### 2.5 无头控制面板与虚拟控制台 (WebUI & Virtual Terminal)

* **局域网管理接口**：默认监听于 `http://0.0.0.0:11451`，剥离全部对公网 CDN 的硬依赖，HTML/JS/CSS 为单文件原生实现（零外部字体与脚本）。
* **Light 主题仪表盘**：白底浅色紧凑排版，整体按 120% 缩放显示；顶部标题栏与运行时钟，合成任务面板横贯顶部，右栏下载统计，中部落盘统计卡片与信源状态表，最底部为文字滚动日志终端。
* **下载统计可视化**：采集器将每轮结果归类为成功 / 传输失败 / 上游缺失三类，按小时键持久化到 `data/stats/download_hourly.json`（保留 72 小时），前端以近 24 小时堆叠柱状图呈现，悬停柱体显示该小时三类明细。
* **落盘统计卡片**：每个信源一张紧凑卡片，顶部按信源分配 accent 色条，展示 24h 落盘帧数、最新/最早帧时刻、小时覆盖率和等宽 key 徽章。
* **元数据门铃数据总览**：页面按“广州观测实况”、“天气雷达回波”、“风云4C卫星云图”三组下拉（`<optgroup>`），信源状态表提供 24h 覆盖带（逐小时帧分布）、帧数、本次新增与最早/最新时刻。目录 `mtime` 未变化时不重扫，只缓存各日期的原始时间戳；24h 聚合结果在每次刷新时按当前时刻现算，时间推移不会让覆盖带停留在旧位置。
* **合成时间自由选择**：除“最近 N 小时”外，WebUI 提供 30 分钟粒度的 48 格时间轴可视化（00:00–23:30），点击起点自动高亮整个跨度并显示终点，支持跨日起点（如 23:30 → 次日 02:30）；可用性判定为跨度内每个 30 分钟桶至少 1 帧（24 小时跨度允许最多 4 个缺帧桶，以兼容上游零散缺帧）；帧率仍由跨度小时数固定映射。
* **跨日产物命名与分组**：凡结束时刻日期与起始日期不同的合成产物，文件名携带结束日期（`{类型}_{起始日期}_{起始时间}_to_{结束日期}_{结束时间}.mp4`），避免仅凭起始日期误判时间归属；分组优先按结束日期（end_date），跨日产物归入结束当天，同日产物回退起始日期，最后兜底文件修改时间。
* **视频产物面板**：WebUI 按上述日期分组规则列出最近 30 天合成的 MP4（含大小、生成时间与定时标记），支持在线播放与下载；每次请求实时扫描磁盘、不做缓存，保证合成任务完成后刷新即最新。同局域网设备可直接通过管理端口浏览，替代手动 `python -m http.server` 分享流程。
* **任务队列面板**：合成面板下方展示最近 10 个任务的状态、帧进度、完成时间与产物下载链接，队列中/执行中的同参数任务由后端去重。
* **SSE 状态推送**：`/api/events` 以 Server-Sent Events 在状态快照变化时推送，前端优先使用 SSE；连接失败时自动回退到 2 秒 `/api/status` 轮询。日志终端仍按 2 秒拉取。
* **健康检查**：`/api/health` 返回整体状态与每个信源最近一次 tick 的年龄。启动 180 秒内、或仍有信源未完成首轮采集（最长 600 秒）时返回 `starting`（200）；之后任一信源超过 10 分钟没有完成 tick 返回 `degraded`（503），供计划任务看门狗判断“进程活着但采集卡死”。
* **深浅色主题**：右上角提供主题切换按钮，首选项写入 `localStorage`；也支持 `?theme=dark` / `?theme=light` 强制预览。深色模式通过 CSS 变量覆盖面板、卡片、表格、日志与视频区域。
* **终端映射（Virtual Terminal）**：后端配置内存环形队列（`collections.deque(maxlen=200)`）拦截全局日志，前端每 2 秒异步拉取并呈现于页面最底部日志终端中，具备基于滚动条容差判定的智能贴底自动滚动功能。

#### 2.6 下载结果统计与持久化 (Download Stats)

* 独立模块 `download_stats.py` 以模块级单例维护按小时键（`YYYYMMDDHH`，北京时间）累计的 `ok / fail / missing` 三类计数：200 落盘记 ok，网络错误/坏图/5xx 记 fail，404 按“信源+时次”去重记 missing（同小时只计一次，去重集合随 JSON 持久化）。时间网格信源设置发布宽限期（雷达 20 分钟、FY4C 40 分钟）：时次年龄小于宽限期内的 404 视为“尚未发布”，不记 missing；超过宽限期仍缺失才计数，避免把发布延迟永久统计成上游缺帧。启动回补属历史扫描，仅计成功落盘，不计 missing/fail；304 与本地已存在不计数。
* 每轮采集结束原子落盘（临时文件 + `os.replace`），写入时清理 72 小时前的旧键；服务重启后自动从 JSON 恢复并继续累计，API 输出最近 24 小时用于柱状图。

---

### 3. 系统技术架构与并发模型

#### 3.1 运行环境与核心栈

* **开发语言**：Python 3.12+ (采取 `src-layout` 工程标准，由 `uv` 进行全量依赖管理)
* **网络与基础通信**：`FastAPI` + `Uvicorn` + `httpx` (支持 HTTP/2 Multiplexing)
* **并发模型**：`asyncio` 异步事件循环 + 单线程先进先出队列
* **底层编码驱动**：静态链接版 `ffmpeg.exe`（直接调用二进制指令进行 `image2pipe` YUV420P 转换）

#### 3.2 异步并发模型与防卡顿解耦

在 `web.py` 的 ASGI 生命周期管理中，通过 `asyncio.create_task` 派生五个互相隔离、又通过总线通讯的底层守护实体：

```
[ HTTP/2 Remote Servers ]
       ▲        ▲
       │(Fetch) │(Sync Radar)
┌──────┴────────┴──────────────────────────────────────────────────────┐
│  AsyncPoller (网络抓取与状态机)                                       │
└──────┬───────────────────────────────────────────────────────────────┘
       │ (Atomic Rename write to disk)
       ▼
[ Local Disk: /data/{img_type}/YYYYMMDD/ ] ──(read pointer)──► [ Stats Observer ] ──► (WebUI API)
       ▲                                                                 
       │ (rglob scan)                                                    
┌──────┴────────────────────────────┐    ┌─────────────────────────────┐
│  GC Loop (30天/7天垃圾回收协程)    │    │  Cron Loop (00:15定时下发)   │
└───────────────────────────────────┘    └──────────────┬──────────────┘
                                                        │ (Put Task)
[ WebUI User Request ] ──(POST /api/synthesize)─────────┴───┐
                                                            ▼
                                                [ SYNTH_QUEUE (FIFO) ]
                                                            │ (Get Task)
                                                            ▼
                                              [ Synthesis Worker ] ──(subprocess)──► [ ffmpeg.exe ]

```

1. **`AsyncPoller.run_forever()`**：全异步抓取协程。内部以 `asyncio.gather` 并发向 11 个信源发包，由全局 `try...except` 护城河环绕，绝对防止单点网络解析崩溃击穿循环。
2. **`stats_loop()`**：UI 观察者协程。为了不占用 I/O 导致老硬盘卡顿，它**从不全量扫盘**。计算新增数时只对比各分类下的 `.latest_cursor.json` 的字符串哈希；计算 24 小时可用帧时，严格限缩在 `now` 与 `now - 1 day` 两个物理日期文件夹内。
3. **`gc_loop()`**：非阻塞定时清理协程。
4. **`cron_loop()`**：每日 00:15 准点定时任务调度协程。
5. **`synthesis_worker()`**：队列消费协程。无论前端下发多少个视频合成命令，队列消费严格串行化。通过 `asyncio.to_thread` 将阻塞的 FFmpeg 调用抛入底层线程池执行。任务间隔强制安插 5 秒冷却期（Cooldown），保障低负载平台 CPU 散温不降频。

---

### 4. 数据结构与物理部署规范

#### 4.1 磁盘存储拓扑

项目建立在明确的物理层级至上，跨平台寻址统一通过 Python `pathlib.Path` 解析：

```text
GuangzhouWeatherTracker/
├── .venv/                         # 宿主机解释器虚拟环境（深度绑定绝对路径，禁跨机复制）
├── pyproject.toml                 # 项目元数据与依赖声明
├── uv.lock                        # 跨平台依赖哈希锁定树
├── ffmpeg.exe                     # 视频压制二进制核心
├── logs/
│   └── tracker.log                # 轮转与持久化双写系统日志
├── deploy/
│   ├── configure_startup.ps1      # 启动方式选择（默认手动，可选开机自启）
│   ├── install_task.ps1           # 注册开机自启计划任务（SYSTEM）
│   ├── uninstall_task.ps1         # 卸载计划任务
│   └── watchdog.ps1               # 健康检查失败时重启计划任务
├── src/
│   ├── main.py                    # 进程入口：装配应用并启动 Uvicorn
│   ├── web.py                     # FastAPI app factory、路由、AppState 与后台协程
│   ├── logbus.py                  # 控制台/轮转文件/WebUI 内存日志三路总线
│   ├── poller.py                  # 三轨探测器、调度与状态码/宽限期策略
│   ├── synthesizer.py             # 反向时间轴推演、动态 FPS、防泄漏管道推流
│   ├── jobs.py                    # 合成任务模型、去重、进度与最近任务快照
│   ├── analytics.py               # 24h 聚合、30 分钟时间轴与视频分组（纯逻辑）
│   ├── frames.py                  # 文件名时间戳编解码与按日目录时间戳缓存
│   ├── grid.py                    # 雷达/FY4C 通用网格时次、单时次抓取与原子落盘
│   ├── fetch.py                   # 状态码分类、Retry-After 与指数退避策略
│   ├── storage.py                 # 原子写（图片/JSON）与 .part 残片清理
│   ├── download_stats.py          # 下载结果统计（ok/fail/missing）与 JSON 持久化
│   ├── cleaner.py                 # GC 垃圾清理与过期文件剔除
│   ├── config.py                  # 信源常量、URL 模板与时区定义
│   └── dashboard.html             # Light 主题 WebUI 单文件前端
└── data/
    ├── TempB/                     # 静态实况图存储树
    │   ├── .latest_cursor.json    # 原子更新的数据真理游标 (包含哈希与文件路径)
    │   └── 20260716/
    │        └── 20260716_023910_TempB.png
    ├── RadarGZ/                   # 雷达图存储树
    │   ├── .latest_cursor.json
    │   └── 20260716/
    │        └── 20260716_013600_RadarGZ.png
    ├── FY4C/                      # 中央台风云4C云图存储树
    │   ├── .latest_cursor.json
    │   └── 20260812/
    │        └── 20260812_160000_FY4C.png
    ├── stats/
    │   └── download_hourly.json   # 下载统计持久化（保留 72 小时）
    └── output/                    # 视频压制产物池
        ├── TempC_20260716_0149_to_0249.mp4
        └── cron/                  # 定时任务专属隔离池
            └── 20260716/
                 └── RadarGZ_20260715_0000_to_2359.mp4

```

#### 4.2 核心参数声明

* **局域网通信端口**：`11451`（默认绑定 `0.0.0.0` 监听所有网卡）
* **抓取轮询间隔**：`POLL_INTERVAL = 120`（秒）
* **连接池重建周期**：`CONNECTION_RECYCLE_SECONDS = 1800`（秒）
* **FY4C 回补窗口**：启动 Phase A 补最近 24 小时，Phase B 自适应延伸到目标 48 小时（实测上游当前约保留 24 小时）；常规轮询窗口 4 小时，运行期每 6 小时扫一次历史缺口，10 分钟时间网格
* **雷达回补窗口**：常规轮询 1 小时，启动及每 30 分钟扫描最近 6 小时缺口，6 分钟网格，抓取并发上限 8；时次成熟 6 分钟后才探测
* **发布宽限期**：雷达 20 分钟、FY4C 40 分钟，宽限期内 404 不计入"上游缺失"；雷达时次成熟 6 分钟、FY4C 成熟 10 分钟后才发首个请求
* **健康检查**：启动宽限 180 秒；首轮/信源 tick 静默阈值 600 秒
* **WebUI 字体**：`Google Sans Flex` 优先，中文回退 `Noto Sans SC`，再回退系统字体；不引入公网字体 CDN
* **数据时间切片格式**：`%Y%m%d_%H%M%S`
* **日志轮转**：单文件 10 MB，保留 3 个备份；httpx/httpcore 请求明细降为 WARNING，避免淹没业务日志
* **视频编码规约**：H.264 / yuv420p / crf 23 / preset fast / `+faststart` / 宏块像素奇偶数向下截断自适应对齐（`-vf scale=trunc(iw/2)*2:trunc(ih/2)*2`）；输出先写 `*.part.mp4`，成功后原子替换，失败自动清理

---

### 5. 在实际环境中如Windows 10 IoT LTSC 21H2 (i5-4200U) 生产部署操作标准

在边缘节点主机（目标路径如：`C:\PythonProjects\GuangzhouWeatherTracker`）执行实际投产时，必须严格遵守以下执行步骤：

#### 5.1 环境重建与依赖同步

由于 `.venv` 内硬链接绑定了原开发机的绝对路径，物理复制时**切勿拷贝 `.venv` 文件夹**。在目标机命令行执行：

```powershell
# 使用 uv 工具基于 pyproject.toml 与 uv.lock 原生重构环境
uv sync

```

#### 5.2 操作系统入站防火墙解封

目标主机默认拦截非公认端口的局域网入站请求。必须以管理员权限打开 PowerShell 执行规则放行：

```powershell
New-NetFirewallRule -DisplayName "Weather Tracker Edge Node" -Direction Inbound -Protocol TCP -LocalPort 11451 -Action Allow

```

#### 5.3 无头进程启动（默认手动 / 可选开机自启）

生产节点不应使用 VS Code 或 CMD 窗口挂留进程。**默认方式为手动启动，不注册任何开机任务**：双击 `start_node.vbs`，由 `pythonw.exe` 无窗口拉起。

需要开机自启时再显式选择；不带参数运行会进入交互选择，直接回车即手动（默认）：

```powershell
.\deploy\configure_startup.ps1                 # 交互选择，默认手动
.\deploy\configure_startup.ps1 -Mode manual    # 移除计划任务，回到手动（需管理员）
.\deploy\configure_startup.ps1 -Mode auto      # 注册 SYSTEM 计划任务（需管理员）
.\deploy\configure_startup.ps1 -Mode status    # 查看当前方式
```

计划任务名为 `WeatherTrackerEdgeNode`，以 SYSTEM 身份运行，`pythonw.exe` 启动，`RestartCount=3`、间隔 1 分钟。`deploy\install_task.ps1` / `uninstall_task.ps1` 是底层等价脚本；`deploy\watchdog.ps1` 会探测 `/api/health`，失败时停止并重新启动该任务，适合再注册一个每 5 分钟运行一次的计划任务。

手动启动脚本：

```vbscript
Set WshShell = CreateObject("WScript.Shell")
strScriptPath = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = strScriptPath
WshShell.Run "cmd /c .venv\Scripts\pythonw.exe src\main.py", 0, False

```

日常运维及监控：

* **启动（默认）**：双击 `start_node.vbs`。
* **启动（已开启自启）**：`Start-ScheduledTask -TaskName WeatherTrackerEdgeNode`。
* **控制**：在同局域网的任意终端浏览器访问 `http://<i5-4200U局域网IP>:11451`。
* **健康检查**：`http://<节点IP>:11451/api/health`，200 为 `ok/starting`，503 为 `degraded`。
* **终止**：`stop_node.bat` / `taskkill /f /im pythonw.exe`；若启用了计划任务，另执行 `Stop-ScheduledTask -TaskName WeatherTrackerEdgeNode`。

### 5.4 台式机开发 → 笔记本同步清单

开发机（台式机）修改完成后，仅需向笔记本复制以下代码层与文档文件（覆盖同名文件，不触碰 data/ 与 logs/）：

* `src/config.py`、`src/poller.py`、`src/synthesizer.py`、`src/download_stats.py`、`src/cleaner.py`、`src/main.py`、`src/web.py`、`src/logbus.py`、`src/dashboard.html`
* `src/analytics.py`、`src/frames.py`、`src/grid.py`、`src/jobs.py`、`src/fetch.py`、`src/storage.py`
* `README.md`、`PRD.md`
* `deploy\configure_startup.ps1`、`deploy\install_task.ps1`、`deploy\uninstall_task.ps1`、`deploy\watchdog.ps1`
* `pyproject.toml`、`uv.lock`（若依赖声明有变动）

笔记本重启服务后自动生效：启动时 FY4C 先回补最近 24 小时，再按上游实际保留深度自适应延伸到目标 48 小时（当前实测约 24 小时），下载统计从空计数开始累计。每日总结只在 `00:15` 进程在线时触发，启动时不会补跑历史任务。
