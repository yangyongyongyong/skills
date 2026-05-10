"""Chrome CDP Daemon 客户端 SDK。

其他 skill 使用示例：

    # 方式1：获取指定 URL 的 cookie
    from cdp_client import get_cookies
    cookies = get_cookies("https://bdp-cn.tuya-inc.com:7799")
    # cookies = {"_csrf": "xxx", "OPS_USER_TOKEN": "yyy", ...}

    # 方式2：执行任意 CDP 命令
    from cdp_client import cdp_call
    result = cdp_call("Target.getTargets")
    # result = {"targetInfos": [...]}

    # 方式3：获取全部 cookie（不过滤域名）
    from cdp_client import get_all_cookies
    all_cookies = get_all_cookies()

daemon 不在运行时会自动启动（首次弹一次授权框），后续完全静默。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# 路径（与 daemon.py 保持一致）
# ---------------------------------------------------------------------------
DAEMON_DIR = Path.home() / ".chrome-cdp-daemon"
SOCKET_PATH = str(DAEMON_DIR / "cdp.sock")
PID_FILE = str(DAEMON_DIR / "cdp.pid")
LOG_FILE = str(DAEMON_DIR / "cdp.log")

# daemon.py 的路径（同目录下）
_DAEMON_SCRIPT = str(Path(__file__).resolve().parent / "daemon.py")

# Python 解释器
_PYTHON = os.environ.get("CDP_DAEMON_PYTHON", "") or sys.executable or "python3"


# ---------------------------------------------------------------------------
# 底层通信
# ---------------------------------------------------------------------------
def _send_to_daemon(req: dict, timeout: float = 15) -> dict:
    """向 daemon 发送一个请求并返回响应。"""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(SOCKET_PATH)
    try:
        sock.sendall(json.dumps(req).encode() + b"\n")
        data = b""
        while len(data) < 1024 * 1024:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break
        return json.loads(data.split(b"\n", 1)[0].decode())
    finally:
        sock.close()


def _daemon_is_running() -> bool:
    if not Path(SOCKET_PATH).exists():
        return False
    try:
        resp = _send_to_daemon({"action": "ping"}, timeout=3)
        return resp.get("ok", False)
    except Exception:
        # socket 文件存在但连不上，清理残留
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass
        try:
            os.unlink(PID_FILE)
        except FileNotFoundError:
            pass
        return False


def _start_daemon() -> None:
    """启动 daemon 进程。"""
    if _daemon_is_running():
        return

    DAEMON_DIR.mkdir(parents=True, exist_ok=True)

    # 用 Popen 后台启动，不等待退出
    subprocess.Popen(
        [_PYTHON, _DAEMON_SCRIPT, "start"],
        stdout=open(LOG_FILE, "a"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )

    # 等待就绪
    for _ in range(30):  # 最多等 15 秒
        time.sleep(0.5)
        if _daemon_is_running():
            return
    raise RuntimeError(f"CDP daemon 启动超时，查看日志: {LOG_FILE}")


def ensure_daemon() -> None:
    """确保 daemon 在运行，不在则自动启动。"""
    if not _daemon_is_running():
        _start_daemon()


# ---------------------------------------------------------------------------
# 公开 API
# ---------------------------------------------------------------------------
def get_all_cookies() -> list[dict]:
    """获取浏览器所有 cookie（自动启动 daemon）。

    返回原始 cookie 列表，每个元素包含 name, value, domain, path 等字段。
    """
    ensure_daemon()
    resp = _send_to_daemon({"action": "get_cookies"})
    if not resp.get("ok"):
        raise RuntimeError(f"get_cookies failed: {resp.get('error', '?')}")
    return resp.get("cookies", [])


def get_cookies(url: str) -> dict[str, str]:
    """获取指定 URL 域名的 cookie（自动启动 daemon）。

    :param url: 目标 URL，如 "https://bdp-cn.tuya-inc.com:7799"
    :return: {cookie_name: cookie_value} 字典
    :raises RuntimeError: daemon 不可用或无匹配 cookie
    """
    parsed = urllib.parse.urlparse(url)
    target_host = (parsed.hostname or "").strip().lower()
    if not target_host:
        raise ValueError(f"无法解析 URL 域名: {url}")

    all_cookies = get_all_cookies()

    cookies: dict[str, str] = {}
    for item in all_cookies:
        if not isinstance(item, dict):
            continue
        domain = str(item.get("domain", "")).lstrip(".").lower()
        if not domain:
            continue
        if domain == target_host or target_host.endswith("." + domain):
            name = str(item.get("name", "")).strip()
            if name:
                cookies[name] = str(item.get("value", ""))

    if not cookies:
        raise RuntimeError(f"未读取到 {target_host} 的 cookie，请确认已在浏览器登录")
    return cookies


def cdp_call(method: str, params: dict[str, Any] | None = None) -> dict:
    """执行任意 CDP 命令（自动启动 daemon）。

    :param method: CDP 方法名，如 "Target.getTargets"
    :param params: CDP 方法参数
    :return: CDP 响应的 result 字段
    """
    ensure_daemon()
    req: dict[str, Any] = {"action": "cdp_call", "method": method}
    if params:
        req["params"] = params
    resp = _send_to_daemon(req)
    if not resp.get("ok"):
        raise RuntimeError(f"cdp_call({method}) failed: {resp.get('error', '?')}")
    return resp.get("result", {})


def daemon_status() -> dict:
    """查询 daemon 状态。"""
    if not _daemon_is_running():
        return {"running": False}
    resp = _send_to_daemon({"action": "ping"}, timeout=3)
    return {"running": True, **resp}


def stop_daemon() -> None:
    """停止 daemon。"""
    if not _daemon_is_running():
        return
    try:
        _send_to_daemon({"action": "stop"}, timeout=5)
    except Exception:
        pass
