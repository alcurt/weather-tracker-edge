"""进程入口：装配 Web 应用并以无头方式启动 Uvicorn。"""

import logging
import socket

import uvicorn

from logbus import configure_logging
from web import AppState, build_app

app = build_app(AppState())


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


if __name__ == "__main__":
    configure_logging()
    local_ip = get_local_ip()
    print("\n" + "=" * 65)
    print("天气追踪边缘节点已启动！")
    print(f"控制面板 (局域网访问): http://{local_ip}:11451")
    print("控制面板 (本机访问):   http://127.0.0.1:11451")
    print("=" * 65 + "\n")

    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    uvicorn.run(app, host="0.0.0.0", port=11451, access_log=False, log_config=None)
