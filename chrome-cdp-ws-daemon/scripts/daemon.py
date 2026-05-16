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
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
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
LOG_MAX_BYTES = int(os.environ.get("CDP_DAEMON_LOG_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get("CDP_DAEMON_LOG_BACKUP_COUNT", "5"))
MAX_CONSECUTIVE_CONNECT_FAILURES = int(
    os.environ.get("CDP_DAEMON_MAX_CONSECUTIVE_CONNECT_FAILURES", "3")
)
CONNECT_RETRY_BACKOFF_SECONDS = float(os.environ.get("CDP_DAEMON_CONNECT_RETRY_BACKOFF", "0.5"))
AUTH_AUTO_CLICK_ENABLED = os.environ.get("CDP_DAEMON_AUTO_CLICK_AUTH", "1").lower() not in {
    "0",
    "false",
    "no",
}
AUTH_AUTO_CLICK_TIMEOUT = float(os.environ.get("CDP_DAEMON_AUTH_CLICK_TIMEOUT", "15"))
_LOG_LOCK = threading.Lock()
_AUTH_WATCHER_LOCK = threading.Lock()
_AUTH_WATCHER_PROC: subprocess.Popen | None = None


def _rotate_logs_locked() -> None:
    try:
        if not os.path.exists(LOG_FILE):
            return
        if os.path.getsize(LOG_FILE) < LOG_MAX_BYTES:
            return
        if LOG_BACKUP_COUNT <= 0:
            os.unlink(LOG_FILE)
            return
        for idx in range(LOG_BACKUP_COUNT - 1, 0, -1):
            src = f"{LOG_FILE}.{idx}"
            dst = f"{LOG_FILE}.{idx + 1}"
            if os.path.exists(src):
                os.replace(src, dst)
        os.replace(LOG_FILE, f"{LOG_FILE}.1")
    except Exception:
        pass


def _log(msg: str, level: str = "INFO") -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] [cdp-daemon] {msg}\n"
    try:
        DAEMON_DIR.mkdir(parents=True, exist_ok=True)
        with _LOG_LOCK:
            _rotate_logs_locked()
            with open(LOG_FILE, "a") as f:
                f.write(line)
    except Exception:
        pass


def _fatal_exit(cdp: "CdpConnection", reason: str, code: int = 1) -> None:
    _log(f"fatal exit: {reason}", level="ERROR")
    try:
        cdp.close()
    except Exception:
        pass
    _cleanup()
    os._exit(code)


def _auth_prompt_applescript(timeout: float) -> str:
    """生成用于点击 Chrome 远程调试授权 sheet 的 AppleScript。"""
    return f'''
set deadlineDate to (current date) + {timeout}
set fallbackDate to (current date) + 1.5
set chromeApps to {{"Google Chrome", "Google Chrome Beta", "Google Chrome Dev", "Google Chrome Canary", "Chromium"}}

on isRemoteDebugText(sheetText)
    if sheetText contains "允许远程调试" then return true
    if sheetText contains "remote debugging" then return true
    if sheetText contains "Remote Debugging" then return true
    if sheetText contains "devtools" then return true
    if sheetText contains "DevTools" then return true
    if sheetText contains "debugging" then return true
    if sheetText contains "Debugging" then return true
    if sheetText contains "调试" then return true
    return false
end isRemoteDebugText

on remoteDebugSheetCount()
    global chromeApps
    set n to 0
    tell application "System Events"
        repeat with appName in chromeApps
            if exists process (appName as text) then
                tell process (appName as text)
                    repeat with winObj in windows
                        repeat with sheetObj in sheets of winObj
                            set sheetText to ""
                            try
                                set sheetText to sheetText & " " & (name of sheetObj as text)
                            end try
                            try
                                set allItems to entire contents of sheetObj
                                repeat with uiObj in allItems
                                    try
                                        if (role of uiObj as text) is "AXStaticText" then set sheetText to sheetText & " " & (value of uiObj as text)
                                    end try
                                end repeat
                            end try
                            if my isRemoteDebugText(sheetText) then set n to n + 1
                        end repeat
                    end repeat
                end tell
            end if
        end repeat
    end tell
    return n
end remoteDebugSheetCount

tell application "System Events"
    set sawRemoteDebugSheetWithoutButton to false
    repeat while (current date) < deadlineDate
        repeat with appName in chromeApps
            if exists process (appName as text) then
                tell process (appName as text)
                    set frontmost to true
                    repeat with winObj in windows
                        repeat with sheetObj in sheets of winObj
                            set sheetText to ""
                            set buttonList to {{}}
                            set allowButton to missing value
                            try
                                set sheetText to sheetText & " " & (name of sheetObj as text)
                            end try
                            try
                                set allItems to entire contents of sheetObj
                                repeat with uiObj in allItems
                                    try
                                        if (role of uiObj as text) is "AXStaticText" then set sheetText to sheetText & " " & (value of uiObj as text)
                                    end try
                                    try
                                        if (role of uiObj as text) is "AXButton" then
                                            set end of buttonList to uiObj
                                            try
                                                set btnDesc to description of uiObj as text
                                                if btnDesc is "允许" then set allowButton to uiObj
                                                if btnDesc is "Allow" then set allowButton to uiObj
                                                if btnDesc is "OK" then set allowButton to uiObj
                                            end try
                                        end if
                                    end try
                                end repeat
                            end try
                            if my isRemoteDebugText(sheetText) then
                                if allowButton is not missing value then
                                    perform action "AXPress" of allowButton
                                    delay 0.8
                                    set leftCount to my remoteDebugSheetCount()
                                    if leftCount is 0 then
                                        return "pressed_and_gone " & (appName as text) & " allow button"
                                    end if
                                    return "pressed_but_still_present " & (appName as text) & " remaining=" & (leftCount as text)
                                else if (count of buttonList) > 0 then
                                    try
                                        perform action "AXPress" of item (count of buttonList) of buttonList
                                    on error
                                        click item (count of buttonList) of buttonList
                                    end try
                                    delay 0.8
                                    set leftCount to my remoteDebugSheetCount()
                                    if leftCount is 0 then
                                        return "pressed_and_gone " & (appName as text) & " last AXButton count=" & ((count of buttonList) as text)
                                    end if
                                    return "pressed_but_still_present " & (appName as text) & " remaining=" & (leftCount as text)
                                else
                                    set sawRemoteDebugSheetWithoutButton to true
                                end if
                            end if
                        end repeat
                    end repeat
                end tell
            end if
        end repeat
        delay 0.2
    end repeat
end tell
if sawRemoteDebugSheetWithoutButton then return "found_remote_debug_sheet_without_buttons"
return "not_found"
'''


def _start_auth_prompt_autoclicker() -> subprocess.Popen | None:
    """启动短生命周期 AppleScript watcher，自动点击 Chrome 远程调试授权 sheet。"""
    global _AUTH_WATCHER_PROC
    if not AUTH_AUTO_CLICK_ENABLED or sys.platform != "darwin":
        return None
    with _AUTH_WATCHER_LOCK:
        if _AUTH_WATCHER_PROC and _AUTH_WATCHER_PROC.poll() is None:
            _log("Chrome remote-debugging auth auto-click watcher already running")
            return _AUTH_WATCHER_PROC
    script = _auth_prompt_applescript(AUTH_AUTO_CLICK_TIMEOUT)
    try:
        DAEMON_DIR.mkdir(parents=True, exist_ok=True)
        log = open(LOG_FILE, "a")
        proc = subprocess.Popen(
            ["osascript", "-e", script],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
        log.close()
        def _kill_later() -> None:
            global _AUTH_WATCHER_PROC
            time.sleep(AUTH_AUTO_CLICK_TIMEOUT + 3)
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
            with _AUTH_WATCHER_LOCK:
                if _AUTH_WATCHER_PROC is proc:
                    _AUTH_WATCHER_PROC = None

        with _AUTH_WATCHER_LOCK:
            _AUTH_WATCHER_PROC = proc
        threading.Thread(target=_kill_later, daemon=True).start()
        _log("started Chrome remote-debugging auth auto-click watcher")
        return proc
    except Exception as exc:
        _log(f"failed to start auth auto-click watcher: {exc}", level="WARN")
        return None


def _run_auth_prompt_autoclick_test(timeout: float | None = None) -> str:
    """同步执行一次授权 sheet 自动点击测试，供 CLI 排查使用。"""
    if sys.platform != "darwin":
        return "unsupported: only macOS supports AppleScript auto-click"
    script = _auth_prompt_applescript(timeout or AUTH_AUTO_CLICK_TIMEOUT)
    try:
        proc = subprocess.run(
            ["osascript", "-e", script],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=(timeout or AUTH_AUTO_CLICK_TIMEOUT) + 3,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "timeout: osascript did not finish, likely waiting for Accessibility permission"
    output = (proc.stdout or "").strip()
    if proc.returncode != 0:
        return f"error: {output}"
    return output or "not_found"


# ---------------------------------------------------------------------------
# CDP 发现
# ---------------------------------------------------------------------------
def _get_active_page_applescript() -> dict | None:
    """通过 Swift/CGWindowList 获取 frontmost Chrome 窗口的活动 tab title（macOS only）。
    返回 {"title": ...} 或 None（Chrome 未运行或无前台窗口）。

    不走 Apple Events（AppleScript/JXA tell application "Chrome" 在 CDP 开启时会超时），
    改用 CoreGraphics CGWindowList API 直接读窗口信息，无授权弹窗、无超时风险。
    """
    swift_code = """
import CoreGraphics
import Foundation

let windowList = CGWindowListCopyWindowInfo(
    [.optionOnScreenOnly, .excludeDesktopElements],
    kCGNullWindowID
) as! [[String: Any]]

let chromeOwners: Set<String> = [
    "Google Chrome", "Google Chrome Canary",
    "Google Chrome Beta", "Google Chrome Dev", "Chromium"
]

for w in windowList {
    guard let owner = w[kCGWindowOwnerName as String] as? String,
          chromeOwners.contains(owner),
          let layer = w[kCGWindowLayer as String] as? Int,
          layer == 0,
          let title = w[kCGWindowName as String] as? String,
          !title.isEmpty else { continue }
    print(title)
    exit(0)
}
exit(1)
"""
    try:
        r = subprocess.run(
            ["swift", "-"],
            input=swift_code,
            capture_output=True, text=True, timeout=8,
        )
        title = r.stdout.strip()
        if r.returncode != 0 or not title:
            return None
        return {"title": title}
    except Exception as exc:
        _log(f"Swift CGWindowList active tab failed: {exc}", level="WARN")
        return None


def _match_active_page(active_info: dict, pages: list[dict]) -> dict | None:
    """在 pages 缓存中匹配 CGWindowList 拿到的活动窗口 title。"""
    title = active_info.get("title", "")
    if not title:
        return None
    # 精确匹配 title
    for p in pages:
        if p.get("title") == title:
            return p
    # 前缀匹配（Chrome 有时会在 title 后追加 " - Google Chrome"）
    for p in pages:
        ptitle = p.get("title", "")
        if ptitle and (title.startswith(ptitle) or ptitle.startswith(title)):
            return p
    return None


def _normalize_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _discover_ws_url() -> tuple[str, str]:
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
                        return refreshed, "scheme2_remote_debugging_port"
                    return ws_url, "scheme1_devtools_active_port"
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    # 方案1 下 /json/version 404 是正常的，端口可达即可
                    return ws_url, "scheme1_devtools_active_port"
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
                return ws_url, "scheme2_remote_debugging_port"
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
        self._started_at = time.time()
        self._connected_at = 0.0
        self._last_ok_at = 0.0
        self._last_error = ""
        self._reconnect_count = 0
        self._connect_failures_total = 0
        self._consecutive_connect_failures = 0
        self._fatal_reason = ""
        self._inflight_requests = 0
        self._cdp_mode = "unknown"
        self._connection_session_id = ""
        # pages 缓存：由 Target 事件实时维护，避免每次 list_pages 都发 CDP 请求
        # key = targetId, value = {targetId, url, title, type}
        self._pages_cache: dict[str, dict] = {}
        self._pages_cache_lock = threading.Lock()
        # 网络事件回调：由 PageManager 注册，在 _call_locked 读 WS 时调用
        self._network_event_handler: Any = None
        # Target 事件回调：由 PageManager 注册，新 tab 创建时通知
        self._target_event_handler: Any = None

    @property
    def ws_url(self) -> str:
        return self._ws_url

    def connect(self) -> None:
        """建立连接（会触发一次授权弹窗）。含 browser ID 过期时的自动刷新。"""
        with self._lock:
            if self._fatal_reason:
                raise RuntimeError(self._fatal_reason)
            # 多个 client/心跳可能在浏览器重启后同时触发重连。
            # 第一个线程连上后，后续线程进入这里应直接复用，避免再次触发 Chrome 授权 sheet。
            if self._ws:
                try:
                    self._call_locked("Browser.getVersion")
                    return
                except Exception as exc:
                    self._last_error = str(exc)
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                    self._ws = None
            ws_url, cdp_mode = _discover_ws_url()
            _log(f"connecting to {ws_url}")
            _start_auth_prompt_autoclicker()
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
            ws.settimeout(60)
            self._ws = ws
            self._ws_url = ws_url
            self._cdp_mode = cdp_mode
            self._connection_session_id = uuid.uuid4().hex
            self._msg_id = 0
            result = self._call_locked("Browser.getVersion")
            now = time.time()
            if self._connected_at:
                self._reconnect_count += 1
            self._connected_at = now
            self._last_ok_at = now
            self._last_error = ""
            self._consecutive_connect_failures = 0
            _log(f"connected: {result.get('product', '?')}")
            # 订阅 Target 生命周期事件，用于实时维护 pages 缓存
            try:
                self._call_locked("Target.setDiscoverTargets", {"discover": True})
                self._rebuild_pages_cache_locked()
            except Exception as exc:
                _log(f"Target discovery setup failed (non-fatal): {exc}", level="WARN")

    def _rebuild_pages_cache_locked(self) -> None:
        """重建 pages 缓存（在已持 _lock 的状态下调用）。"""
        try:
            result = self._call_locked("Target.getTargets")
            infos = result.get("targetInfos", [])
            with self._pages_cache_lock:
                self._pages_cache = {
                    info["targetId"]: {
                        "targetId": info["targetId"],
                        "url": info.get("url", ""),
                        "title": info.get("title", ""),
                        "type": info.get("type", ""),
                    }
                    for info in infos
                    if isinstance(info, dict) and "targetId" in info
                }
        except Exception as exc:
            _log(f"rebuild pages cache failed: {exc}", level="WARN")

    def _handle_target_event(self, method: str, params: dict) -> None:
        """处理 Target.* 事件，实时维护 pages 缓存（不持 _lock，使用独立锁）。"""
        try:
            if method == "Target.targetCreated":
                info = params.get("targetInfo", {})
                tid = info.get("targetId")
                if tid:
                    with self._pages_cache_lock:
                        self._pages_cache[tid] = {
                            "targetId": tid,
                            "url": info.get("url", ""),
                            "title": info.get("title", ""),
                            "type": info.get("type", ""),
                        }
            elif method == "Target.targetInfoChanged":
                info = params.get("targetInfo", {})
                tid = info.get("targetId")
                if tid:
                    with self._pages_cache_lock:
                        self._pages_cache[tid] = {
                            "targetId": tid,
                            "url": info.get("url", ""),
                            "title": info.get("title", ""),
                            "type": info.get("type", ""),
                        }
            elif method == "Target.targetDestroyed":
                tid = params.get("targetId")
                if tid:
                    with self._pages_cache_lock:
                        self._pages_cache.pop(tid, None)
            # 通知 PageManager（用于 follow 模式跟踪新 tab）
            if self._target_event_handler:
                try:
                    self._target_event_handler(method, params)
                except Exception:
                    pass
        except Exception as exc:
            _log(f"handle target event error: {exc}", level="WARN")

    def get_pages(self) -> list[dict]:
        """返回当前所有 page 类型的 target（线程安全）。"""
        with self._pages_cache_lock:
            return [
                info for info in self._pages_cache.values()
                if info.get("type") == "page"
            ]

    def _call_locked(self, method: str, params: dict | None = None) -> dict:
        """在已持锁的状态下发送 CDP 命令。"""
        if not self._ws:
            raise RuntimeError("not connected")
        self._msg_id += 1
        mid = self._msg_id
        payload: dict[str, Any] = {"id": mid, "method": method}
        if params:
            payload["params"] = params
        self._inflight_requests += 1
        try:
            self._ws.send(json.dumps(payload))
            while True:
                raw = self._ws.recv()
                msg = json.loads(raw if isinstance(raw, str) else raw.decode())
                # 事件帧（无 id）：喂给缓存维护，继续等响应
                if "method" in msg and "id" not in msg:
                    evt_method = msg.get("method", "")
                    if evt_method.startswith("Target."):
                        self._handle_target_event(evt_method, msg.get("params", {}))
                    elif evt_method.startswith("Network.") and self._network_event_handler:
                        try:
                            self._network_event_handler(
                                evt_method, msg.get("params", {}),
                                msg.get("sessionId", ""))
                        except Exception:
                            pass
                    continue
                if msg.get("id") == mid:
                    if "error" in msg:
                        self._last_error = str(msg["error"])
                        raise RuntimeError(f"CDP {method}: {msg['error']}")
                    self._last_ok_at = time.time()
                    self._last_error = ""
                    return msg.get("result", {})
        finally:
            self._inflight_requests = max(0, self._inflight_requests - 1)

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
            if self._fatal_reason:
                raise RuntimeError(self._fatal_reason)
            try:
                if self._ws:
                    self._call_locked("Browser.getVersion")
                    return
            except Exception as exc:
                self._last_error = str(exc)
                _log(f"connection lost, reconnecting: {exc}", level="WARN")
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
                self._ws = None

        last_exc: Exception | None = None
        for attempt in range(1, MAX_CONSECUTIVE_CONNECT_FAILURES + 1):
            try:
                self.connect()
                return
            except Exception as exc:
                last_exc = exc
                with self._lock:
                    self._connect_failures_total += 1
                    self._consecutive_connect_failures += 1
                    self._last_error = str(exc)
                    failures = self._consecutive_connect_failures
                _log(
                    "connect attempt "
                    f"{attempt}/{MAX_CONSECUTIVE_CONNECT_FAILURES} failed: {exc}",
                    level="WARN",
                )
                if AUTH_AUTO_CLICK_ENABLED and "timed out" in str(exc).lower():
                    _log(
                        "connect timed out while auth auto-click is enabled; "
                        "stop retrying to avoid stacking Chrome auth sheets",
                        level="WARN",
                    )
                    break
                if attempt < MAX_CONSECUTIVE_CONNECT_FAILURES:
                    time.sleep(CONNECT_RETRY_BACKOFF_SECONDS * attempt)
                if failures >= MAX_CONSECUTIVE_CONNECT_FAILURES:
                    break

        with self._lock:
            self._fatal_reason = (
                "CDP reconnect failed too many times, daemon exits for self-healing"
            )
        raise RuntimeError(str(last_exc) if last_exc else self._fatal_reason)

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
        """后台心跳线程。

        心跳只检查已存在的 WS 是否仍可用；如果 Chrome 重启导致连接断开，
        只标记为 disconnected，不主动重连，避免聊天/空闲期间触发 Chrome 授权弹窗。
        重新连接只允许由显式 client 请求触发（get_cookies/cdp_call/test_connection）。
        """
        while self._running:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break
            try:
                with self._lock:
                    if not self._ws:
                        continue
                    try:
                        self._call_locked("Browser.getVersion")
                    except Exception as exc:
                        self._last_error = str(exc)
                        _log(f"heartbeat detected disconnected WS: {exc}", level="WARN")
                        try:
                            self._ws.close()
                        except Exception:
                            pass
                        self._ws = None
            except Exception as exc:
                _log(f"heartbeat failed: {exc}", level="WARN")

    def should_exit_daemon(self) -> bool:
        with self._lock:
            return bool(self._fatal_reason)

    def metrics(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            return {
                "status": "running",
                "pid": os.getpid(),
                "ws_connected": self._ws is not None,
                "ws_url": self._ws_url,
                "cdp_mode": self._cdp_mode,
                "connection_session_id": self._connection_session_id,
                "auth_autoclick_enabled": AUTH_AUTO_CLICK_ENABLED and sys.platform == "darwin",
                "uptime_sec": int(max(0, now - self._started_at)),
                "connected_since": int(self._connected_at) if self._connected_at else 0,
                "last_ok_at": int(self._last_ok_at) if self._last_ok_at else 0,
                "last_error": self._last_error,
                "reconnect_count": self._reconnect_count,
                "connect_failures_total": self._connect_failures_total,
                "consecutive_connect_failures": self._consecutive_connect_failures,
                "inflight_requests": self._inflight_requests,
                "fatal_reason": self._fatal_reason,
            }


# ---------------------------------------------------------------------------
# Unix Socket 服务
# ---------------------------------------------------------------------------
# 高级页面动作名集合（由 PageManager 处理）
_PAGE_ACTIONS = frozenset({
    "page_call", "snapshot", "click", "click_text", "find_text", "fill", "select", "check",
    "hover", "press", "scroll", "drag", "wait", "get_text", "get_url", "get_title",
    "activate", "open_tab", "close_tab",
    "group_create", "group_add", "group_remove_tab", "group_list",
    "group_close", "group_delete", "group_activate", "group_move", "group_close_tabs",
    "network_capture_start", "network_capture_stop", "network_capture_export",
    "network_fetch", "network_replay",
    "editor_get", "editor_set", "editor_type",
    "find_icon", "click_icon", "scan_tooltips",
})


def handle_client(
    conn: socket.socket, cdp: CdpConnection, page_mgr: Any = None
) -> None:
    """处理单个客户端请求（每个请求独立线程）。"""
    fatal_exit = False
    try:
        data = b""
        conn.settimeout(120)
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
            resp = {"ok": True, **cdp.metrics()}

        elif action == "status":
            resp = {"ok": True, **cdp.metrics()}

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

        elif action == "test_connection":
            cdp.ensure_connected()
            version = cdp.call("Browser.getVersion")
            targets = cdp.call("Target.getTargets")
            target_infos = targets.get("targetInfos", [])
            page_count = 0
            if isinstance(target_infos, list):
                page_count = sum(
                    1
                    for item in target_infos
                    if isinstance(item, dict) and item.get("type") == "page"
                )
            resp = {
                "ok": True,
                "connection": "ok",
                "product": version.get("product", ""),
                "protocol_version": version.get("protocolVersion", ""),
                "targets_total": len(target_infos) if isinstance(target_infos, list) else 0,
                "pages": page_count,
                "cdp_mode": cdp.metrics().get("cdp_mode", "unknown"),
                "connection_session_id": cdp.metrics().get("connection_session_id", ""),
            }

        elif action == "list_pages":
            # 直接读缓存，不发 CDP 请求；daemon 未连接时缓存为空不报错
            pages = cdp.get_pages()
            resp = {"ok": True, "pages": pages}

        elif action == "active_page":
            # macOS only：AppleScript 拿 frontmost Chrome tab → 匹配缓存
            if sys.platform != "darwin":
                resp = {"ok": False, "error": "active_page only supported on macOS"}
            else:
                try:
                    active_info = _get_active_page_applescript()
                    if active_info is None:
                        resp = {"ok": False, "error": "Chrome not running or no active tab"}
                    else:
                        # 在 pages 缓存里匹配（优先 url 精确，退回 title）
                        pages = cdp.get_pages()
                        matched = _match_active_page(active_info, pages)
                        if matched:
                            resp = {"ok": True, "page": matched}
                        else:
                            # 缓存可能还没建，触发一次 CDP 刷新
                            if cdp._ws:
                                try:
                                    with cdp._lock:
                                        cdp._rebuild_pages_cache_locked()
                                    pages = cdp.get_pages()
                                    matched = _match_active_page(active_info, pages)
                                except Exception:
                                    pass
                            if matched:
                                resp = {"ok": True, "page": matched}
                            else:
                                # fallback：返回 AppleScript 拿到的信息（无 targetId）
                                resp = {
                                    "ok": True,
                                    "page": {**active_info, "targetId": None},
                                    "warning": "targetId not found in CDP cache",
                                }
                except Exception as exc:
                    resp = {"ok": False, "error": f"active_page failed: {exc}"}

        elif action in _PAGE_ACTIONS:
            # 高级页面操作：委托给 PageManager
            if page_mgr is None:
                resp = {"ok": False, "error": "page_manager not initialized"}
            else:
                cdp.ensure_connected()
                resp = page_mgr.handle_action(action, req)

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
        if cdp.should_exit_daemon():
            fatal_exit = True
        try:
            conn.sendall(json.dumps({"ok": False, "error": str(exc)}).encode() + b"\n")
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
        if fatal_exit:
            _fatal_exit(cdp, "self-healing restart after repeated reconnect failures")


def _cleanup() -> None:
    for f in (SOCKET_PATH, PID_FILE):
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass


def _read_pid() -> int | None:
    try:
        return int(Path(PID_FILE).read_text().strip())
    except Exception:
        return None


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _force_stop(timeout: float = 8.0) -> None:
    if _daemon_is_running():
        try:
            _send({"action": "stop"}, timeout=2)
        except Exception:
            pass

    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = _read_pid()
        sock_exists = Path(SOCKET_PATH).exists()
        pid_alive = bool(pid and _is_pid_alive(pid))
        if not sock_exists and not pid_alive:
            _cleanup()
            return
        time.sleep(0.2)

    pid = _read_pid()
    if pid and _is_pid_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    time.sleep(0.3)
    _cleanup()


def run_daemon() -> None:
    """守护进程主循环。"""
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup()

    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    _log(f"daemon started, pid={os.getpid()}")

    cdp = CdpConnection()

    # 实例化页面级操作管理器
    from page_manager import PageManager
    page_mgr = PageManager(cdp)

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
            threading.Thread(
                target=handle_client, args=(conn, cdp, page_mgr), daemon=True
            ).start()
        except socket.timeout:
            continue
        except Exception as exc:
            _log(f"accept error: {exc}", level="WARN")
            time.sleep(1)


def daemonize() -> None:
    """启动后台守护进程并尽快返回。"""
    DAEMON_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_FILE, "a")
    try:
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "__run_daemon__"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        log.close()

    deadline = time.time() + 5
    while time.time() < deadline:
        if Path(PID_FILE).exists() and Path(SOCKET_PATH).exists():
            print(f"CDP daemon started, pid={Path(PID_FILE).read_text().strip()}")
            return
        time.sleep(0.1)

    print(f"CDP daemon may have failed, check: {LOG_FILE}", file=sys.stderr)
    raise SystemExit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    if len(sys.argv) < 2:
        print(
            f"Usage: {sys.argv[0]} <command>\n"
            "Commands:\n"
            "  start|stop|status|restart|test   daemon 管理\n"
            "  list-pages|active-page            页面列表\n"
            "  snapshot [-i] [-C] [-s scope]     交互元素快照\n"
            "  click <@ref|selector>             点击 [--dblclick] [--right] [--at x,y]\n"
            "  click-text \"文本\"                  按文本查找并点击 [--tag] [--nth] [--region] [--dblclick]\n"
            "  find-text \"文本\"                   按文本搜索元素 [--tag] [--region]\n"
            "  find-icon \"save\"                   按 title/aria-label/icon-class 搜索 [--region]\n"
            "  click-icon \"save\"                  按图标搜索并点击 [--region] [--nth] [--dblclick]\n"
            "  scan-tooltips                       扫描图标按钮 tooltip [--region] [--scope]\n"
            "  editor-get                          读取 Monaco/CodeMirror 编辑器内容\n"
            "  editor-set \"text\"                  整段替换编辑器内容 [--append]\n"
            "  editor-type \"text\"                 逐字符输入（触发 autocomplete）\n"
            "  activate [target]                  切换活动页面到前台\n"
            "  open <url>                         打开新标签页 [--group <name>]\n"
            "  close [target]                     关闭标签页\n"
            "  group create <name> [targets...]    创建群组 [--color]\n"
            "  group add <name> <targets...>       添加标签页到群组\n"
            "  group move <name> <targets...>      移入群组(从其它组移出)\n"
            "  group remove <name> <targets...>    从群组移出(不关闭标签页)\n"
            "  group close-tabs <name> <targets..> 关闭群组内指定标签页\n"
            "  group list [name]                   列出群组\n"
            "  group close <name>                  关闭群组(关闭所有标签页)\n"
            "  group delete <name>                 删除群组(不关闭标签页)\n"
            "  group activate <name>               切换到群组第一个标签页\n"
            "  fill <@ref|selector> <text>       填充表单 [--submit] [--no-native]\n"
            "  select <@ref|selector> <value>    下拉选择（原生+Ant/Element）\n"
            "  check <@ref|selector>             勾选 checkbox\n"
            "  press <key> [@ref]                按键（CDP 原生事件）\n"
            "  hover <@ref|--at x,y>             鼠标悬浮（触发 hover 下拉等）\n"
            "  scroll <up|down|left|right> [px]  滚动 [--at x,y] [@ref]\n"
            "  drag <startX,startY> <endX,endY>  拖拽 [--steps N] [--hold-ms N]\n"
            "  wait --selector|--text <val>      等待元素/文本\n"
            "  get-text [@ref|selector]          获取文本\n"
            "  get-url|get-title                 获取页面信息\n"
            "  network-capture start [--follow]   开始抓包（--follow 跟踪新 tab）\n"
            "  network-capture stop [--body]     停止抓包，输出请求列表\n"
            "  network-capture export [--curl]   导出为 Python/curl 代码\n"
            "  network fetch <url>                在页面上下文 fetch（带 cookie）\n"
            "  network replay [N]                 重放抓包的第 N 个请求\n"
            "  auth-click-test [timeout]         授权弹窗测试"
        )
        return 1

    cmd = sys.argv[1]

    if cmd == "__run_daemon__":
        run_daemon()
        return 0

    if cmd == "start":
        if _daemon_is_running():
            print("CDP daemon already running")
            return 0
        daemonize()
        return 0

    elif cmd == "stop":
        if not _daemon_is_running() and not Path(PID_FILE).exists() and not Path(SOCKET_PATH).exists():
            print("CDP daemon not running")
            return 0
        _force_stop(timeout=8)
        print("CDP daemon stopped")
        return 0

    elif cmd == "restart":
        _force_stop(timeout=8)
        daemonize()
        return 0

    elif cmd == "status":
        if _daemon_is_running():
            try:
                resp = _send({"action": "ping"}, timeout=5)
                print(f"Running: {json.dumps(resp, ensure_ascii=False)}")
            except TimeoutError:
                print(
                    "Running: "
                    + json.dumps(
                        {
                            "ok": True,
                            "status": "busy",
                            "pid": _read_pid(),
                            "message": "daemon is alive but ping timed out",
                        },
                        ensure_ascii=False,
                    )
                )
        else:
            print("Not running")
        return 0

    elif cmd == "test":
        if not _daemon_is_running():
            daemonize()
        fallback_to_legacy = False
        try:
            resp = _send({"action": "test_connection"}, timeout=15)
            if not resp.get("ok") and "unknown action" in str(resp.get("error", "")):
                fallback_to_legacy = True
        except TimeoutError as exc:
            print(f"Test failed: daemon test_connection timed out: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"Test failed: {exc}", file=sys.stderr)
            return 1

        if fallback_to_legacy:
            # 向后兼容旧 daemon：退化为 cdp_call 测试
            try:
                version_resp = _send(
                    {"action": "cdp_call", "method": "Browser.getVersion"}, timeout=15
                )
                target_resp = _send(
                    {"action": "cdp_call", "method": "Target.getTargets"}, timeout=15
                )
                if not version_resp.get("ok"):
                    raise RuntimeError(version_resp.get("error", "Browser.getVersion failed"))
                if not target_resp.get("ok"):
                    raise RuntimeError(target_resp.get("error", "Target.getTargets failed"))
                version = version_resp.get("result", {})
                targets = target_resp.get("result", {})
                infos = targets.get("targetInfos", [])
                pages = 0
                if isinstance(infos, list):
                    pages = sum(1 for item in infos if isinstance(item, dict) and item.get("type") == "page")
                ping = {}
                try:
                    ping = _send({"action": "ping"}, timeout=3)
                except Exception:
                    ping = {}
                resp = {
                    "ok": True,
                    "connection": "ok",
                    "product": version.get("product", ""),
                    "protocol_version": version.get("protocolVersion", ""),
                    "targets_total": len(infos) if isinstance(infos, list) else 0,
                    "pages": pages,
                    "cdp_mode": ping.get("cdp_mode", "unknown"),
                    "compat_mode": "legacy_daemon_fallback",
                }
            except Exception as exc:
                print(f"Test failed: {exc}", file=sys.stderr)
                return 1
        try:
            print(f"Test: {json.dumps(resp, ensure_ascii=False)}")
            return 0 if resp.get("ok") else 1
        except Exception as exc:
            print(f"Test failed: {exc}", file=sys.stderr)
            return 1

    elif cmd == "auth-click-test":
        timeout = AUTH_AUTO_CLICK_TIMEOUT
        if len(sys.argv) >= 3:
            try:
                timeout = float(sys.argv[2])
            except ValueError:
                print(f"Invalid timeout: {sys.argv[2]}", file=sys.stderr)
                return 1
        print(f"AuthClickTest: {_run_auth_prompt_autoclick_test(timeout)}")
        return 0

    elif cmd == "list-pages":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        resp = _send({"action": "list_pages"}, timeout=5)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        pages = resp.get("pages", [])
        if not pages:
            print("(no pages)")
            return 0
        for i, p in enumerate(pages):
            title = p.get("title", "")[:60]
            url = p.get("url", "")
            tid = p.get("targetId", "")[:8]
            print(f"[{i}] {tid}  {url}  — {title}")
        return 0

    elif cmd == "active-page":
        if sys.platform != "darwin":
            print("active-page is only supported on macOS", file=sys.stderr)
            return 1
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        resp = _send({"action": "active_page"}, timeout=8)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        page = resp.get("page", {})
        print(f"target_id: {page.get('targetId', '(unknown)')}")
        print(f"url:       {page.get('url', '')}")
        print(f"title:     {page.get('title', '')}")
        if resp.get("warning"):
            print(f"warning:   {resp['warning']}", file=sys.stderr)
        return 0

    # ------------------------------------------------------------------
    # 高级页面操作 CLI（对齐 agent-browser 风格）
    # ------------------------------------------------------------------
    elif cmd == "snapshot":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "snapshot"}
        # 解析参数: -i (interactive, 默认), -C (cursor), -s <scope>, --target <t>, --json
        i = 2
        output_json = False
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "-i":
                pass  # 默认就是 interactive
            elif arg == "-C":
                req["include_cursor"] = True
            elif arg == "-s" and i + 1 < len(sys.argv):
                i += 1
                req["scope"] = sys.argv[i]
            elif arg == "--target" and i + 1 < len(sys.argv):
                i += 1
                req["target"] = sys.argv[i]
            elif arg == "--json":
                output_json = True
            i += 1
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        elements = resp.get("elements", [])
        if output_json:
            print(json.dumps(resp, ensure_ascii=False, indent=2))
        else:
            for el in elements:
                ref = el.get("ref", "?")
                desc = el.get("desc", "")
                val = el.get("value", "")
                line = f"{ref}  {desc}"
                if val:
                    line += f"  value={val}"
                print(line)
            print(f"\n--- {len(elements)} interactive elements ---")
        return 0

    elif cmd == "click":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: click <@ref|selector> [--dblclick] [--right] [--at x,y] [--target <t>]", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "click", "ref": sys.argv[2]}
        if "--dblclick" in sys.argv:
            req["dblclick"] = True
        if "--right" in sys.argv:
            req["right"] = True
        if "--at" in sys.argv and sys.argv.index("--at") + 1 < len(sys.argv):
            xy = sys.argv[sys.argv.index("--at") + 1].split(",")
            if len(xy) == 2:
                req["at"] = [int(xy[0]), int(xy[1])]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Clicked: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "find-text":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: find-text "文本" [--tag button] [--region top-right] [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "find_text", "text": sys.argv[2]}
        if "--tag" in sys.argv and sys.argv.index("--tag") + 1 < len(sys.argv):
            req["tag"] = sys.argv[sys.argv.index("--tag") + 1]
        if "--region" in sys.argv and sys.argv.index("--region") + 1 < len(sys.argv):
            req["region"] = sys.argv[sys.argv.index("--region") + 1]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        matches = resp.get("matches", [])
        if not matches:
            region_hint = f" (region={req.get('region', '')})" if req.get("region") else ""
            print(f"未找到: \"{sys.argv[2]}\"{region_hint}")
            return 0
        for i, m in enumerate(matches, 1):
            vis = "✓" if m.get("visible") else "✗"
            print(f"[{i}] {vis} <{m['tag']}> \"{m['text']}\"  at=({m['x']},{m['y']})  {m['w']}x{m['h']}  {m.get('method','')}")
        print(f"\n--- {len(matches)} matches ---")
        return 0

    elif cmd == "click-text":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: click-text "文本" [--tag button] [--nth 1] [--region top-right] [--dblclick] [--right] [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "click_text", "text": sys.argv[2]}
        if "--tag" in sys.argv and sys.argv.index("--tag") + 1 < len(sys.argv):
            req["tag"] = sys.argv[sys.argv.index("--tag") + 1]
        if "--nth" in sys.argv and sys.argv.index("--nth") + 1 < len(sys.argv):
            req["nth"] = int(sys.argv[sys.argv.index("--nth") + 1])
        if "--region" in sys.argv and sys.argv.index("--region") + 1 < len(sys.argv):
            req["region"] = sys.argv[sys.argv.index("--region") + 1]
        if "--dblclick" in sys.argv:
            req["dblclick"] = True
        if "--right" in sys.argv:
            req["right"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Clicked: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    # ---- Monaco / CodeMirror 编辑器 ----
    elif cmd == "editor-get":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "editor_get"}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Type: {resp.get('type')}")
        print(resp.get("value", ""))
        return 0

    elif cmd == "editor-set":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: editor-set "SQL text" [--append] [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "editor_set", "text": sys.argv[2]}
        if "--append" in sys.argv:
            req["append"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        mode = "追加" if resp.get("append") else "替换"
        print(f"Editor {mode}: {resp.get('length')} 字符")
        return 0

    elif cmd == "editor-type":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: editor-type "text" [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "editor_type", "text": sys.argv[2]}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=60)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Typed: {resp.get('length')} 字符")
        return 0

    # ---- 图标搜索 / 点击 ----
    elif cmd == "find-icon":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: find-icon "save" [--region top-right] [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "find_icon", "query": sys.argv[2]}
        if "--region" in sys.argv and sys.argv.index("--region") + 1 < len(sys.argv):
            req["region"] = sys.argv[sys.argv.index("--region") + 1]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        matches = resp.get("matches", [])
        for i, m in enumerate(matches, 1):
            vis = "✓" if m.get("visible") else "✗"
            dis = " [DISABLED]" if m.get("disabled") else ""
            title = m.get("title") or m.get("ariaLabel") or m.get("cls", "")[:30]
            print(f"[{i}] {vis} <{m['tag']}> \"{title}\"  at=({m['x']},{m['y']})  {m['w']}x{m['h']}{dis}")
        print(f"\n--- {len(matches)} matches ---")
        return 0

    elif cmd == "click-icon":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: click-icon "save" [--region top-right] [--nth 1] [--dblclick] [--right] [--target <t>]', file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "click_icon", "query": sys.argv[2]}
        if "--region" in sys.argv and sys.argv.index("--region") + 1 < len(sys.argv):
            req["region"] = sys.argv[sys.argv.index("--region") + 1]
        if "--nth" in sys.argv and sys.argv.index("--nth") + 1 < len(sys.argv):
            req["nth"] = int(sys.argv[sys.argv.index("--nth") + 1])
        if "--dblclick" in sys.argv:
            req["dblclick"] = True
        if "--right" in sys.argv:
            req["right"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Clicked: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "scan-tooltips":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "scan_tooltips"}
        if "--region" in sys.argv and sys.argv.index("--region") + 1 < len(sys.argv):
            req["region"] = sys.argv[sys.argv.index("--region") + 1]
        if "--scope" in sys.argv and sys.argv.index("--scope") + 1 < len(sys.argv):
            req["scope"] = sys.argv[sys.argv.index("--scope") + 1]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        # scan-tooltips 需要逐个 hover，timeout 给大些
        resp = _send(req, timeout=120)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        buttons = resp.get("buttons", [])
        print(f"Found {len(buttons)} tooltip button(s):")
        for i, btn in enumerate(buttons, 1):
            icon_info = f" icon={btn['icon']}" if btn.get("icon") else ""
            disabled_info = " [DISABLED]" if btn.get("disabled") else ""
            source = f" ({btn.get('source', '?')})" if btn.get("source") else ""
            print(f"  {i}. \"{btn['tooltip']}\"{icon_info} at ({btn['x']},{btn['y']}){disabled_info}{source}")
        return 0

    elif cmd == "activate":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "activate"}
        if len(sys.argv) >= 3:
            req["target"] = sys.argv[2]
        elif "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Activated: {resp.get('targetId', '?')[:8]}  {resp.get('title', '')}")
        return 0

    elif cmd == "open":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: open <url> [--group <name>] [--no-activate] [--wait <ms>]", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "open_tab", "url": sys.argv[2]}
        if "--no-activate" in sys.argv:
            req["activate"] = False
        if "--wait" in sys.argv and sys.argv.index("--wait") + 1 < len(sys.argv):
            req["wait_ms"] = int(sys.argv[sys.argv.index("--wait") + 1])
        if "--group" in sys.argv and sys.argv.index("--group") + 1 < len(sys.argv):
            req["group"] = sys.argv[sys.argv.index("--group") + 1]
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        group_info = f"  → group '{resp['group']}'" if resp.get("group") else ""
        print(f"Opened: {resp.get('targetId', '?')[:8]}  {resp.get('title', '')}{group_info}")
        print(f"  URL: {resp.get('url', '')}")
        return 0

    elif cmd == "close":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "close_tab"}
        if len(sys.argv) >= 3 and not sys.argv[2].startswith("--"):
            req["target"] = sys.argv[2]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Closed: {resp.get('targetId', '?')[:8]}  {resp.get('title', '')}")
        return 0

    elif cmd == "group":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print(
                "Usage: group <subcommand>\n"
                "  create <name> [target1 target2...]  创建群组\n"
                "  add <name> <target1> [target2...]   添加标签页\n"
                "  list [name]                         列出群组\n"
                "  close <name>                        关闭群组(关闭标签页)\n"
                "  delete <name>                       删除群组(保留标签页)\n"
                "  activate <name>                     切换到群组",
                file=sys.stderr,
            )
            return 1
        sub = sys.argv[2]

        if sub == "create":
            if len(sys.argv) < 4:
                print("Usage: group create <name> [target1 target2...] [--color <c>]", file=sys.stderr)
                return 1
            name = sys.argv[3]
            # 收集 targets（排除 --color 及其值）
            targets = []
            color = ""
            i = 4
            while i < len(sys.argv):
                if sys.argv[i] == "--color" and i + 1 < len(sys.argv):
                    color = sys.argv[i + 1]
                    i += 2
                elif sys.argv[i].startswith("--"):
                    i += 1
                else:
                    targets.append(sys.argv[i])
                    i += 1
            req: dict[str, Any] = {"action": "group_create", "name": name, "color": color}
            if targets:
                req["targets"] = targets
            resp = _send(req, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Group '{name}' created ({resp.get('count', 0)} tabs)")
            for t in resp.get("tabs", []):
                print(f"  {t['targetId'][:8]}  {t.get('title', '')}  {t.get('url', '')}")
            return 0

        elif sub == "add":
            if len(sys.argv) < 5:
                print("Usage: group add <name> <target1> [target2...]", file=sys.stderr)
                return 1
            name = sys.argv[3]
            targets = [a for a in sys.argv[4:] if not a.startswith("--")]
            resp = _send({"action": "group_add", "name": name, "targets": targets}, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Added {resp.get('added', 0)} tabs to '{name}' (total: {resp.get('total', 0)})")
            for t in resp.get("tabs", []):
                print(f"  + {t['targetId'][:8]}  {t.get('title', '')}  {t.get('url', '')}")
            return 0

        elif sub == "move":
            if len(sys.argv) < 5:
                print("Usage: group move <name> <target1> [target2...]", file=sys.stderr)
                return 1
            name = sys.argv[3]
            targets = [a for a in sys.argv[4:] if not a.startswith("--")]
            resp = _send({"action": "group_move", "name": name, "targets": targets}, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Moved {resp.get('moved', 0)} tabs to '{name}' (total: {resp.get('total', 0)})")
            for t in resp.get("tabs", []):
                print(f"  → {t['targetId'][:8]}  {t.get('title', '')}  {t.get('url', '')}")
            return 0

        elif sub == "remove":
            if len(sys.argv) < 5:
                print("Usage: group remove <name> <target1> [target2...]", file=sys.stderr)
                return 1
            name = sys.argv[3]
            targets = [a for a in sys.argv[4:] if not a.startswith("--")]
            resp = _send({"action": "group_remove_tab", "name": name, "targets": targets}, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Removed {resp.get('removed', 0)} tabs from '{name}' (remaining: {resp.get('remaining', 0)})")
            return 0

        elif sub == "list":
            name = sys.argv[3] if len(sys.argv) >= 4 else ""
            resp = _send({"action": "group_list", "name": name}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            groups = resp.get("groups", [])
            if not groups:
                print("(无群组)")
                return 0
            for g in groups:
                color_tag = f" [{g['color']}]" if g.get("color") else ""
                print(f"[{g['name']}]{color_tag}  ({g['count']} tabs)")
                for t in g.get("tabs", []):
                    tid = t.get("targetId", "?")[:8]
                    title = t.get("title", "")
                    url = t.get("url", "")
                    print(f"  {tid}  {title}  {url}")
            return 0

        elif sub == "close":
            if len(sys.argv) < 4:
                print("Usage: group close <name>", file=sys.stderr)
                return 1
            name = sys.argv[3]
            resp = _send({"action": "group_close", "name": name}, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Group '{name}' closed ({resp.get('closed', 0)} tabs)")
            for t in resp.get("tabs", []):
                print(f"  ✕ {t['targetId'][:8]}  {t.get('title', '')}")
            return 0

        elif sub == "close-tabs":
            if len(sys.argv) < 5:
                print("Usage: group close-tabs <name> <target1> [target2...]", file=sys.stderr)
                return 1
            name = sys.argv[3]
            targets = [a for a in sys.argv[4:] if not a.startswith("--")]
            resp = _send({"action": "group_close_tabs", "name": name, "targets": targets}, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Closed {resp.get('closed', 0)} tabs in '{name}' (remaining: {resp.get('remaining', 0)})")
            for t in resp.get("tabs", []):
                print(f"  ✕ {t['targetId'][:8]}  {t.get('title', '')}")
            return 0

        elif sub == "delete":
            if len(sys.argv) < 4:
                print("Usage: group delete <name>", file=sys.stderr)
                return 1
            name = sys.argv[3]
            resp = _send({"action": "group_delete", "name": name}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Group '{name}' deleted (released {resp.get('released', 0)} tabs)")
            return 0

        elif sub == "activate":
            if len(sys.argv) < 4:
                print("Usage: group activate <name>", file=sys.stderr)
                return 1
            name = sys.argv[3]
            resp = _send({"action": "group_activate", "name": name}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Activated: {resp.get('targetId', '?')[:8]}  {resp.get('title', '')}")
            return 0

        else:
            print(f"Unknown group subcommand: {sub}", file=sys.stderr)
            return 1

    elif cmd == "fill":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 4:
            print('Usage: fill <@ref|selector> "text" [--no-native] [--no-clear] [--submit] [--target <t>]', file=sys.stderr)
            return 1
        req = {"action": "fill", "ref": sys.argv[2], "text": sys.argv[3]}
        # 默认 native 模式（CDP Input.insertText），--no-native 回退到 JS setter
        req["native"] = "--no-native" not in sys.argv
        if "--no-clear" in sys.argv:
            req["clear"] = False
        if "--submit" in sys.argv:
            req["submit"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Filled: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "select":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 4:
            print('Usage: select <@ref|selector> "value" [--by label] [--target <t>]', file=sys.stderr)
            return 1
        req = {"action": "select", "ref": sys.argv[2], "value": sys.argv[3]}
        if "--by" in sys.argv:
            idx = sys.argv.index("--by")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1] == "label":
                req["by_label"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Selected: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "check":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: check <@ref|selector> [--target <t>]", file=sys.stderr)
            return 1
        req = {"action": "check", "ref": sys.argv[2]}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Checked: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "press":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: press <key> [@ref] [--target <t>]", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "press", "key": sys.argv[2]}
        if len(sys.argv) > 3 and sys.argv[3].startswith("@e"):
            req["ref"] = sys.argv[3]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Pressed: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "hover":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3 and "--at" not in sys.argv:
            print("Usage: hover <@ref> | hover --at x,y [--target <t>]", file=sys.stderr)
            return 1
        req = {"action": "hover"}
        # @ref 或 --at x,y
        if len(sys.argv) > 2 and sys.argv[2].startswith("@e"):
            req["ref"] = sys.argv[2]
        if "--at" in sys.argv and sys.argv.index("--at") + 1 < len(sys.argv):
            xy = sys.argv[sys.argv.index("--at") + 1].split(",")
            if len(xy) == 2:
                req["at"] = [int(xy[0]), int(xy[1])]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Hovered: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "scroll":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        direction = sys.argv[2] if len(sys.argv) > 2 else "down"
        # amount: 第3个位置参数（跳过 --/@ 开头的选项）
        amount = 500
        if len(sys.argv) > 3 and not sys.argv[3].startswith("--") and not sys.argv[3].startswith("@"):
            amount = int(sys.argv[3])
        req: dict[str, Any] = {"action": "scroll", "direction": direction, "amount": amount}
        # @ref: 在元素位置滚动
        for a in sys.argv[3:]:
            if a.startswith("@e"):
                req["ref"] = a
                break
        # --at x,y: 在指定坐标滚动
        if "--at" in sys.argv and sys.argv.index("--at") + 1 < len(sys.argv):
            xy = sys.argv[sys.argv.index("--at") + 1].split(",")
            if len(xy) == 2:
                req["at"] = [int(xy[0]), int(xy[1])]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Scrolled: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "drag":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print('Usage: drag <startX,startY> <endX,endY> [--steps N] [--hold-ms N] [--target <t>]', file=sys.stderr)
            return 1
        # 解析起点终点
        try:
            sx, sy = map(int, sys.argv[2].split(","))
            ex, ey = map(int, sys.argv[3].split(","))
        except (ValueError, IndexError):
            print('Usage: drag <startX,startY> <endX,endY>', file=sys.stderr)
            return 1
        req: dict[str, Any] = {
            "action": "drag",
            "start_x": sx, "start_y": sy,
            "end_x": ex, "end_y": ey,
        }
        if "--steps" in sys.argv and sys.argv.index("--steps") + 1 < len(sys.argv):
            req["steps"] = int(sys.argv[sys.argv.index("--steps") + 1])
        if "--hold-ms" in sys.argv and sys.argv.index("--hold-ms") + 1 < len(sys.argv):
            req["hold_ms"] = int(sys.argv[sys.argv.index("--hold-ms") + 1])
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=30)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Dragged: ({sx},{sy}) → ({ex},{ey})  steps={resp.get('steps')}")
        return 0

    elif cmd == "wait":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req = {"action": "wait"}
        if "--selector" in sys.argv and sys.argv.index("--selector") + 1 < len(sys.argv):
            req["selector"] = sys.argv[sys.argv.index("--selector") + 1]
        if "--text" in sys.argv and sys.argv.index("--text") + 1 < len(sys.argv):
            req["text"] = sys.argv[sys.argv.index("--text") + 1]
        if "--timeout" in sys.argv and sys.argv.index("--timeout") + 1 < len(sys.argv):
            req["timeout_ms"] = int(sys.argv[sys.argv.index("--timeout") + 1])
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=30)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        if resp.get("timeout"):
            print(f"Timeout: {json.dumps(resp, ensure_ascii=False)}")
            return 1
        print(f"Found: {json.dumps(resp, ensure_ascii=False)}")
        return 0

    elif cmd == "get-text":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req = {"action": "get_text"}
        if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
            req["ref"] = sys.argv[2]
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(resp.get("text", ""))
        return 0

    elif cmd == "get-url":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req = {"action": "get_url"}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(resp.get("url", ""))
        return 0

    elif cmd == "get-title":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req = {"action": "get_title"}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(resp.get("title", ""))
        return 0

    elif cmd == "network-capture":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""

        if subcmd == "start":
            req: dict[str, Any] = {"action": "network_capture_start"}
            if "--follow" in sys.argv:
                req["follow"] = True
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            resp = _send(req, timeout=15)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            mode = " (follow mode)" if resp.get("follow") else ""
            print(f"Network capture started on {resp.get('target_id', '?')[:8]}{mode}")
            return 0

        elif subcmd == "stop":
            req = {"action": "network_capture_stop"}
            if "--body" in sys.argv:
                req["get_body"] = True
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            resp = _send(req, timeout=30)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            requests = resp.get("requests", [])
            new_tabs = resp.get("new_tabs", [])
            # 先显示新 tab 信息
            if new_tabs:
                print("=== New tabs opened ===")
                for t in new_tabs:
                    print(f"  {t.get('targetId', '?')[:8]}  {t.get('url', '')}")
                    if t.get("title"):
                        print(f"  {'':8s}  title: {t['title']}")
                print()
            if not requests:
                print("(no API requests captured)")
                return 0
            # 表格输出
            for i, r in enumerate(requests):
                method = r.get("method", "?")
                url = r.get("url", "")
                status = r.get("status", "?")
                post_data = r.get("postData")
                source = r.get("_source", "")
                body_hint = ""
                if post_data:
                    body_hint = f"  body={len(post_data)}B"
                tab_hint = ""
                if source == "new_tab":
                    tab_hint = "  [new_tab]"
                print(f"[{i+1}] {status} {method:6s} {url}{body_hint}{tab_hint}")
            print(f"\n--- {len(requests)} API requests captured ---")
            # 将请求列表写到临时文件供 export 使用
            import tempfile
            tmp = Path(tempfile.gettempdir()) / "cdp_network_capture.json"
            tmp.write_text(json.dumps(requests, ensure_ascii=False, indent=2))
            print(f"Saved to: {tmp}")
            print(f"Export:   {sys.argv[0]} network-capture export [--curl]")
            return 0

        elif subcmd == "export":
            # 从临时文件读取上次 stop 的结果
            import tempfile
            tmp = Path(tempfile.gettempdir()) / "cdp_network_capture.json"
            if not tmp.exists():
                print("No capture data. Run 'network-capture stop' first.", file=sys.stderr)
                return 1
            requests = json.loads(tmp.read_text())
            fmt = "curl" if "--curl" in sys.argv else "python"
            # 直接本地生成，不需要 daemon
            from page_manager import PageManager
            code = PageManager._export_curl(requests) if fmt == "curl" \
                else PageManager._export_python(requests)
            print(code)
            # 同时写到文件
            ext = "sh" if fmt == "curl" else "py"
            out = Path(tempfile.gettempdir()) / f"cdp_network_capture.{ext}"
            out.write_text(code)
            print(f"\n# Saved to: {out}", file=sys.stderr)
            return 0

        else:
            print(
                "Usage: network-capture <start|stop|export>\n"
                "  start [--follow] [--target <t>]  开始抓包（--follow 自动跟踪新开 tab）\n"
                "  stop  [--body] [--target <t>]    停止并输出请求列表\n"
                "  export [--curl]                  导出为 Python（默认）或 curl",
                file=sys.stderr,
            )
            return 1

    elif cmd == "network":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""

        if subcmd == "fetch":
            if len(sys.argv) < 4:
                print('Usage: network fetch <url> [--method POST] [--body \'{"k":"v"}\'] [--target <t>]', file=sys.stderr)
                return 1
            req: dict[str, Any] = {"action": "network_fetch", "url": sys.argv[3]}
            if "--method" in sys.argv and sys.argv.index("--method") + 1 < len(sys.argv):
                req["method"] = sys.argv[sys.argv.index("--method") + 1]
            if "--body" in sys.argv and sys.argv.index("--body") + 1 < len(sys.argv):
                req["body"] = sys.argv[sys.argv.index("--body") + 1]
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            resp = _send(req, timeout=30)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"HTTP {resp.get('status')} {resp.get('statusText', '')}")
            # 输出 body
            body = resp.get("body", "")
            if isinstance(body, dict):
                print(json.dumps(body, ensure_ascii=False, indent=2))
            else:
                print(body[:5000] if isinstance(body, str) else str(body)[:5000])
            return 0

        elif subcmd == "replay":
            index = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 1
            req: dict[str, Any] = {"action": "network_replay", "index": index}
            if "--url" in sys.argv and sys.argv.index("--url") + 1 < len(sys.argv):
                req["override_url"] = sys.argv[sys.argv.index("--url") + 1]
            if "--method" in sys.argv and sys.argv.index("--method") + 1 < len(sys.argv):
                req["override_method"] = sys.argv[sys.argv.index("--method") + 1]
            if "--body" in sys.argv and sys.argv.index("--body") + 1 < len(sys.argv):
                req["override_body"] = sys.argv[sys.argv.index("--body") + 1]
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            resp = _send(req, timeout=30)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            original = resp.get("original_url", "?")
            print(f"Replayed [{resp.get('replayed_index')}]: {original}")
            print(f"HTTP {resp.get('status')} {resp.get('statusText', '')}  (original: {resp.get('original_status', '?')})")
            body = resp.get("body", "")
            if isinstance(body, dict):
                print(json.dumps(body, ensure_ascii=False, indent=2))
            else:
                print(body[:5000] if isinstance(body, str) else str(body)[:5000])
            return 0

        else:
            print(
                "Usage: network <fetch|replay>\n"
                "  fetch <url> [--method POST] [--body '{...}']  在页面上下文 fetch（带 cookie）\n"
                "  replay [N] [--url ...] [--method ...] [--body ...]  重放抓包的第 N 个请求",
                file=sys.stderr,
            )
            return 1
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
    sock_exists = Path(SOCKET_PATH).exists()
    pid = _read_pid()
    pid_alive = bool(pid and _is_pid_alive(pid))

    if not sock_exists and not pid_alive:
        return False
    try:
        resp = _send({"action": "ping"}, timeout=3)
        return resp.get("ok", False)
    except TimeoutError:
        # daemon 可能正在处理长请求，ping 暂时排队；若 pid/socket 存在则视为运行中
        return sock_exists and pid_alive
    except Exception:
        if not pid_alive:
            _cleanup()
            return False
        return True


if __name__ == "__main__":
    raise SystemExit(main())
