"""Chrome CDP WebSocket 守护进程。

后台常驻，持有持久 WebSocket 连接到 Chrome 浏览器。
所有 skill 通过 Unix Socket 向本进程请求 CDP 服务（获取 cookie、执行命令等），
用户只需在首次启动时授权一次，后续完全静默。

并发安全：
- WS 连接操作加锁（threading.Lock），多个 client 同时请求不会冲突
- 每个 client 请求在独立线程中处理
- 心跳线程独立运行，不阻塞请求处理

协议：Unix Socket + 行分隔 JSON
  请求: {"action": "get_cookies"}
        {"action": "cdp_call", "method": "Target.getTargets", "params": {}}
        {"action": "ping"}
        {"action": "stop"}
  响应: {"ok": true, ...} 或 {"ok": false, "error": "..."}
"""

from __future__ import annotations

import json
import os
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import websocket

# ---------------------------------------------------------------------------
# 路径（固定位置，所有 skill 共用）
# ---------------------------------------------------------------------------
DAEMON_DIR = Path.home() / ".chrome-cdp-daemon"
SOCKET_PATH = str(DAEMON_DIR / "cdp.sock")
PID_FILE = str(DAEMON_DIR / "cdp.pid")
LOG_FILE = str(DAEMON_DIR / "cdp.log")

# ---------------------------------------------------------------------------
# CDP 连接参数
# ---------------------------------------------------------------------------
CHROME_USER_DATA_DIRS: dict[str, Path] = {
    "stable": Path.home() / "Library/Application Support/Google/Chrome",
    "beta": Path.home() / "Library/Application Support/Google/Chrome Beta",
    "dev": Path.home() / "Library/Application Support/Google/Chrome Dev",
    "canary": Path.home() / "Library/Application Support/Google/Chrome Canary",
    "chromium": Path.home() / "Library/Application Support/Chromium",
}
AUTO_HOSTS = ("127.0.0.1", "[::1]", "localhost")
HEARTBEAT_INTERVAL = 30
MAX_RECV_SIZE = 1024 * 1024  # 1MB max response


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [cdp-daemon] {msg}\n"
    try:
        DAEMON_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(line)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# CDP 发现
# ---------------------------------------------------------------------------
def _normalize_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _discover_ws_url() -> str:
    """从 DevToolsActivePort 或 /json/version 发现浏览器级 WS 地址。

    方案1: DevToolsActivePort（Chrome >= 144，勾选 Allow remote debugging）
      - 遍历所有 Chrome channel 的 user-data-dir
      - 读取端口和 browser WS 路径
      - 按 IPv4/IPv6/localhost 候选逐一检查端口可达
      - 若 browser id 过期（WS 连接时 404），从 /json/version 刷新
    方案2: --remote-debugging-port（回退）
      - 通过 /json/version 获取 WS 地址
      - 支持环境变量 CHROME_CDP_PORT 覆盖默认 9222
    """
    cdp_port = int(os.environ.get("CHROME_CDP_PORT", "9222").strip() or "9222")

    # 方案1: DevToolsActivePort
    for channel, udir in CHROME_USER_DATA_DIRS.items():
        port_file = udir / "DevToolsActivePort"
        if not port_file.exists():
            continue
        try:
            lines = [ln.strip() for ln in port_file.read_text().splitlines() if ln.strip()]
        except Exception:
            continue
        if len(lines) < 2:
            continue
        try:
            port = int(lines[0])
        except ValueError:
            continue
        ws_path = lines[1]

        for host in AUTO_HOSTS:
            h = _normalize_host(host)
            ws_url = f"ws://{h}:{port}{ws_path}"
            http_url = f"http://{h}:{port}/json/version"
            try:
                with urllib.request.urlopen(http_url, timeout=2) as resp:
                    data = json.loads(resp.read().decode())
                    # 方案2 端口的 /json/version 返回正常，优先用其中的 WS 地址
                    refreshed = data.get("webSocketDebuggerUrl")
                    if isinstance(refreshed, str) and refreshed.startswith("ws://"):
                        return refreshed
                    return ws_url
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    # 方案1 下 /json/version 404 是正常的，端口可达即可
                    return ws_url
            except Exception:
                continue

    # 方案2: --remote-debugging-port（/json/version）
    for host in AUTO_HOSTS:
        h = _normalize_host(host)
        url = f"http://{h}:{cdp_port}/json/version"
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                data = json.loads(resp.read().decode())
            ws_url = data.get("webSocketDebuggerUrl")
            if isinstance(ws_url, str) and ws_url.startswith("ws://"):
                return ws_url
        except Exception:
            continue

    raise RuntimeError(
        "CDP 双方案均失败。"
        "方案1: 确保 Chrome >= 144 且在 chrome://inspect/#remote-debugging 勾选 'Allow remote debugging'；"
        f"方案2: 确保 Chrome 以 --remote-debugging-port={cdp_port} 启动。"
    )


def _refresh_ws_from_json_version(ws_url: str) -> str | None:
    """browser ID 过期时，从同 host/port 的 /json/version 刷新 WS 地址。"""
    import urllib.parse as _up
    parsed = _up.urlparse(ws_url)
    if not parsed.hostname or parsed.port is None:
        return None
    host = _normalize_host(parsed.hostname)
    version_url = f"http://{host}:{parsed.port}/json/version"
    try:
        with urllib.request.urlopen(version_url, timeout=2) as resp:
            data = json.loads(resp.read().decode())
        refreshed = data.get("webSocketDebuggerUrl")
        if isinstance(refreshed, str) and refreshed.startswith("ws://"):
            return refreshed
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# 持久 CDP 连接（线程安全）
# ---------------------------------------------------------------------------
class CdpConnection:
    """持久 CDP WebSocket 连接，线程安全，自动重连 + 心跳。"""

    def __init__(self):
        self._ws: websocket.WebSocket | None = None
        self._ws_url: str = ""
        self._lock = threading.RLock()  # 可重入锁，防止心跳和请求死锁
        self._msg_id = 0
        self._running = True

    @property
    def ws_url(self) -> str:
        return self._ws_url

    def connect(self) -> None:
        """建立连接（会触发一次授权弹窗）。含 browser ID 过期时的自动刷新。"""
        with self._lock:
            ws_url = _discover_ws_url()
            _log(f"connecting to {ws_url}")
            try:
                ws = websocket.create_connection(ws_url, timeout=15, suppress_origin=True)
            except Exception as exc:
                # browser ID 可能过期（404），尝试从 /json/version 刷新
                if "404" in str(exc):
                    refreshed = _refresh_ws_from_json_version(ws_url)
                    if refreshed and refreshed != ws_url:
                        _log(f"browser ID expired, refreshed to {refreshed}")
                        ws = websocket.create_connection(refreshed, timeout=15, suppress_origin=True)
                        ws_url = refreshed
                    else:
                        raise
                else:
                    raise
            ws.settimeout(15)
            self._ws = ws
            self._ws_url = ws_url
            self._msg_id = 0
            result = self._call_locked("Browser.getVersion")
            _log(f"connected: {result.get('product', '?')}")

    def _call_locked(self, method: str, params: dict | None = None) -> dict:
        """在已持锁的状态下发送 CDP 命令。"""
        if not self._ws:
            raise RuntimeError("not connected")
        self._msg_id += 1
        mid = self._msg_id
        payload: dict[str, Any] = {"id": mid, "method": method}
        if params:
            payload["params"] = params
        self._ws.send(json.dumps(payload))
        while True:
            raw = self._ws.recv()
            msg = json.loads(raw if isinstance(raw, str) else raw.decode())
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"CDP {method}: {msg['error']}")
                return msg.get("result", {})

    def call(self, method: str, params: dict | None = None) -> dict:
        """线程安全的 CDP 调用。"""
        with self._lock:
            return self._call_locked(method, params)

    def get_all_cookies(self) -> list[dict]:
        """获取浏览器所有 cookie（线程安全）。"""
        with self._lock:
            resp = self._call_locked("Storage.getCookies")
            return resp.get("cookies", [])

    def ensure_connected(self) -> None:
        """确保连接可用，断线则重连。"""
        with self._lock:
            try:
                if self._ws:
                    self._call_locked("Browser.getVersion")
                    return
            except Exception:
                _log("connection lost, reconnecting...")
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                self._ws = None
        self.connect()

    def close(self) -> None:
        self._running = False
        with self._lock:
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None

    def heartbeat_loop(self) -> None:
        """后台心跳线程。"""
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break
            try:
                self.ensure_connected()
            except Exception as exc:
                _log(f"heartbeat failed: {exc}")


# ---------------------------------------------------------------------------
# Unix Socket 服务
# ---------------------------------------------------------------------------
def handle_client(conn: socket.socket, cdp: CdpConnection) -> None:
    """处理单个客户端请求（每个请求独立线程）。"""
    try:
        data = b""
        conn.settimeout(30)
        while len(data) < MAX_RECV_SIZE:
            chunk = conn.recv(8192)
            if not chunk:
                break
            data += chunk
            if b"\n" in data:
                break

        line = data.split(b"\n", 1)[0].strip()
        if not line:
            return

        req = json.loads(line.decode())
        action = req.get("action", "")

        if action == "ping":
            cdp.ensure_connected()
            resp = {"ok": True, "status": "running", "ws_url": cdp.ws_url,
                    "pid": os.getpid()}

        elif action == "get_cookies":
            cdp.ensure_connected()
            all_cookies = cdp.get_all_cookies()
            resp = {"ok": True, "cookies": all_cookies}

        elif action == "cdp_call":
            method = req.get("method", "")
            params = req.get("params")
            if not method:
                resp = {"ok": False, "error": "missing 'method'"}
            else:
                cdp.ensure_connected()
                result = cdp.call(method, params)
                resp = {"ok": True, "result": result}

        elif action == "stop":
            resp = {"ok": True, "message": "stopping"}
            conn.sendall(json.dumps(resp).encode() + b"\n")
            conn.close()
            cdp.close()
            _cleanup()
            os._exit(0)

        else:
            resp = {"ok": False, "error": f"unknown action: {action}"}

        conn.sendall(json.dumps(resp, ensure_ascii=False).encode() + b"\n")

    except Exception as exc:
        try:
            conn.sendall(json.dumps({"ok": False, "error": str(exc)}).encode() + b"\n")
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _cleanup() -> None:
    for f in (SOCKET_PATH, PID_FILE):
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass


def run_daemon() -> None:
    """守护进程主循环。"""
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup()

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    _log(f"daemon started, pid={os.getpid()}")

    cdp = CdpConnection()
    try:
        cdp.connect()
    except Exception as exc:
        _log(f"initial connect failed: {exc}")
        _cleanup()
        sys.exit(1)

    # 心跳线程
    threading.Thread(target=cdp.heartbeat_loop, daemon=True).start()

    # 信号处理
    def _shutdown(signum, frame):
        _log("received shutdown signal")
        cdp.close()
        _cleanup()
        os._exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # Unix Socket 监听
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o600)
    sock.listen(16)  # 支持多 skill 并发排队
    sock.settimeout(1.0)

    _log(f"listening on {SOCKET_PATH}")

    while True:
        try:
            conn, _ = sock.accept()
            threading.Thread(target=handle_client, args=(conn, cdp), daemon=True).start()
        except socket.timeout:
            continue
        except Exception as exc:
            _log(f"accept error: {exc}")
            time.sleep(1)


def daemonize() -> None:
    """double-fork 守护进程化。"""
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)

    pid = os.fork()
    if pid > 0:
        # 父进程等待子进程完成连接
        for _ in range(20):
            time.sleep(0.5)
            if Path(PID_FILE).exists() and Path(SOCKET_PATH).exists():
                print(f"CDP daemon started, pid={Path(PID_FILE).read_text().strip()}")
                sys.exit(0)
        print(f"CDP daemon may have failed, check: {LOG_FILE}", file=sys.stderr)
        sys.exit(1)

    os.setsid()
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    sys.stdin.close()
    sys.stdout = open(LOG_FILE, "a")
    sys.stderr = sys.stdout
    run_daemon()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} start|stop|status|restart")
        return 1

    cmd = sys.argv[1]

    if cmd == "start":
        if _daemon_is_running():
            print("CDP daemon already running")
            return 0
        daemonize()
        return 0

    elif cmd == "stop":
        if not _daemon_is_running():
            print("CDP daemon not running")
            _cleanup()
            return 0
        try:
            _send({"action": "stop"}, timeout=5)
        except Exception:
            if Path(PID_FILE).exists():
                try:
                    os.kill(int(Path(PID_FILE).read_text().strip()), signal.SIGTERM)
                except Exception:
                    pass
            _cleanup()
        print("CDP daemon stopped")
        return 0

    elif cmd == "restart":
        if _daemon_is_running():
            try:
                _send({"action": "stop"}, timeout=5)
            except Exception:
                pass
            time.sleep(1)
            _cleanup()
        daemonize()
        return 0

    elif cmd == "status":
        if _daemon_is_running():
            resp = _send({"action": "ping"}, timeout=3)
            print(f"Running: {json.dumps(resp, ensure_ascii=False)}")
        else:
            print("Not running")
        return 0

    else:
        print(f"Unknown command: {cmd}")
        return 1


# ---------------------------------------------------------------------------
# 内部通信
# ---------------------------------------------------------------------------
def _send(req: dict, timeout: float = 10) -> dict:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(SOCKET_PATH)
    try:
        sock.sendall(json.dumps(req).encode() + b"\n")
        data = b""
        while len(data) < MAX_RECV_SIZE:
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
        resp = _send({"action": "ping"}, timeout=3)
        return resp.get("ok", False)
    except Exception:
        _cleanup()
        return False


if __name__ == "__main__":
    raise SystemExit(main())
