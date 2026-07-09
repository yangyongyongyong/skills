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
import urllib.parse
import urllib.request
import uuid
import contextlib
import difflib
import fnmatch
import io
import shlex
from pathlib import Path
from typing import Any

import requests
import websocket

# ---------------------------------------------------------------------------
# 路径（按实例隔离，避免多 Chrome 实例串连）
# ---------------------------------------------------------------------------
DAEMON_BASE_DIR = Path.home() / ".chrome-cdp-daemon"
CURRENT_INSTANCE_ID = ""
DAEMON_DIR = DAEMON_BASE_DIR / "default"
SOCKET_PATH = str(DAEMON_DIR / "cdp.sock")
PID_FILE = str(DAEMON_DIR / "cdp.pid")
LOG_FILE = str(DAEMON_DIR / "cdp.log")
SNAPSHOT_BASELINE_DIR = DAEMON_DIR / "snapshot-baselines"
AUTOMATION_GROUP_NAME = "CDP自动化"

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
DAEMON_START_SOCKET_WAIT_SECONDS = float(
    os.environ.get("CDP_DAEMON_START_SOCKET_WAIT_SECONDS", "5")
)
DAEMON_START_READY_WAIT_SECONDS = float(
    os.environ.get("CDP_DAEMON_START_READY_WAIT_SECONDS", "8")
)
DAEMON_READY_POLL_INTERVAL = float(
    os.environ.get("CDP_DAEMON_READY_POLL_INTERVAL", "0.2")
)
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
_CONFIG_CACHE: dict[str, Any] | None = None
_CONFIG_PATHS: tuple[Path, ...] = ()


class CdpInstanceSelectionError(RuntimeError):
    """表示 Chrome CDP 实例选择失败。"""

    def __init__(self, message: str, candidates: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.candidates = candidates or []


def _sanitize_instance_segment(value: str) -> str:
    """把实例相关字段转成稳定的目录/实例名片段。"""
    text = "".join(
        ch.lower() if ch.isalnum() else "-"
        for ch in str(value or "").strip()
    )
    text = "-".join(part for part in text.split("-") if part)
    return text[:80] or "default"


def _default_instance_dir_name(instance_id: str) -> str:
    """返回实例目录名。"""
    return _sanitize_instance_segment(instance_id or "default")


def _compute_daemon_paths(instance_id: str) -> dict[str, Any]:
    """按实例计算 daemon 运行时文件路径。"""
    runtime_dir = DAEMON_BASE_DIR / _default_instance_dir_name(instance_id)
    return {
        "dir": runtime_dir,
        "socket": str(runtime_dir / "cdp.sock"),
        "pid": str(runtime_dir / "cdp.pid"),
        "log": str(runtime_dir / "cdp.log"),
        "snapshot_baselines": runtime_dir / "snapshot-baselines",
    }


def _configure_runtime_instance(instance_id: str) -> None:
    """切换当前进程使用的实例目录。"""
    global CURRENT_INSTANCE_ID, DAEMON_DIR, SOCKET_PATH, PID_FILE, LOG_FILE
    global SNAPSHOT_BASELINE_DIR, _CONFIG_PATHS, _CONFIG_CACHE
    normalized = str(instance_id or "").strip()
    CURRENT_INSTANCE_ID = normalized
    paths = _compute_daemon_paths(normalized)
    DAEMON_DIR = paths["dir"]
    SOCKET_PATH = paths["socket"]
    PID_FILE = paths["pid"]
    LOG_FILE = paths["log"]
    SNAPSHOT_BASELINE_DIR = paths["snapshot_baselines"]
    _CONFIG_PATHS = (
        DAEMON_BASE_DIR / "config.json",
        DAEMON_DIR / "config.json",
        Path.cwd() / "chrome-cdp-daemon.json",
        Path.cwd() / "cdp-daemon.json",
    )
    _CONFIG_CACHE = None
    if normalized:
        os.environ["CHROME_CDP_INSTANCE"] = normalized
    else:
        os.environ.pop("CHROME_CDP_INSTANCE", None)


_configure_runtime_instance(os.environ.get("CHROME_CDP_INSTANCE", ""))


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


def _cookie_domain_matches(hostname: str, cookie_domain: str) -> bool:
    """判断 cookie domain 是否匹配目标 URL host。"""
    host = str(hostname or "").strip().lower().rstrip(".")
    domain = str(cookie_domain or "").strip().lower().lstrip(".").rstrip(".")
    if not host or not domain:
        return False
    return host == domain or host.endswith("." + domain)


def _cookie_path_matches(url_path: str, cookie_path: str) -> bool:
    """按 RFC path-match 规则判断 cookie path 是否匹配目标 URL path。"""
    request_path = str(url_path or "/")
    if not request_path.startswith("/"):
        request_path = "/" + request_path
    path = str(cookie_path or "/")
    if not path.startswith("/"):
        path = "/" + path
    if request_path == path:
        return True
    if not request_path.startswith(path):
        return False
    return path.endswith("/") or request_path[len(path):].startswith("/")


def _cookie_not_expired(cookie: dict[str, Any]) -> bool:
    """判断 cookie 是否未过期；session cookie 没有正数 expires。"""
    expires = cookie.get("expires")
    try:
        expires_float = float(expires)
    except (TypeError, ValueError):
        return True
    return expires_float <= 0 or expires_float > time.time()


def _cookie_matches_url(cookie: dict[str, Any], parsed_url: urllib.parse.ParseResult) -> bool:
    """判断单个 CDP cookie 是否适用于目标 URL。"""
    hostname = parsed_url.hostname or ""
    if not _cookie_domain_matches(hostname, str(cookie.get("domain", ""))):
        return False
    if not _cookie_path_matches(parsed_url.path or "/", str(cookie.get("path", "/"))):
        return False
    if bool(cookie.get("secure")) and parsed_url.scheme != "https":
        return False
    return _cookie_not_expired(cookie)


def _filter_cookies_for_url(cookies: list[dict], url: str) -> list[dict]:
    """从浏览器全量 cookie 中筛选目标 URL 可用的 cookie。"""
    target_url = str(url or "").strip()
    parsed = urllib.parse.urlparse(target_url)
    if not parsed.scheme or not parsed.hostname:
        raise RuntimeError(f"invalid url: {url}")
    return [
        cookie for cookie in cookies
        if isinstance(cookie, dict) and _cookie_matches_url(cookie, parsed)
    ]


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


def _match_active_pages(active_info: dict, pages: list[dict]) -> list[dict]:
    """在 pages 缓存中匹配 CGWindowList 拿到的活动窗口 title。

    返回所有候选，而不是命中第一个，避免同名页面误判。
    """
    title = active_info.get("title", "")
    if not title:
        return []
    # 精确匹配 title
    exact = [p for p in pages if p.get("title") == title]
    if exact:
        return exact
    # 前缀匹配（Chrome 有时会在 title 后追加 " - Google Chrome"）
    fuzzy = []
    for p in pages:
        ptitle = p.get("title", "")
        if ptitle and (title.startswith(ptitle) or ptitle.startswith(title)):
            fuzzy.append(p)
    return fuzzy


def _normalize_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _guess_channel(command: str, user_data_dir: str = "") -> str:
    """根据命令行或 user-data-dir 粗略推断 Chrome 通道。"""
    text = f"{command} {user_data_dir}".lower()
    if "chrome beta" in text:
        return "beta"
    if "chrome dev" in text:
        return "dev"
    if "chrome canary" in text:
        return "canary"
    if "chromium" in text:
        return "chromium"
    return "stable"


def _fetch_version_payload(port: int) -> dict[str, Any]:
    """读取指定调试端口的 /json/version 信息。"""
    for host in AUTO_HOSTS:
        h = _normalize_host(host)
        url = f"http://{h}:{port}/json/version"
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                data = json.loads(resp.read().decode())
            if isinstance(data, dict):
                data["_host"] = h
                return data
        except Exception:
            continue
    return {}


def _build_instance_id(channel: str, user_data_dir: str, port: int) -> str:
    """构造稳定实例 ID，供 CLI/SDK 显式选择。"""
    base = Path(user_data_dir).name if user_data_dir else channel or "chrome"
    return f"{_sanitize_instance_segment(base)}-{int(port)}"


def _upsert_instance(
    instances: dict[str, dict[str, Any]],
    *,
    channel: str,
    source: str,
    port: int,
    user_data_dir: str = "",
    ws_url: str = "",
    ws_path: str = "",
    host: str = "",
    product: str = "",
    pid: int = 0,
    command: str = "",
) -> None:
    """向实例字典中写入或合并一条候选实例。"""
    instance_id = _build_instance_id(channel, user_data_dir, port)
    entry = instances.get(instance_id, {
        "instance_id": instance_id,
        "channel": channel,
        "source": source,
        "port": int(port),
        "user_data_dir": user_data_dir,
        "ws_url": ws_url,
        "ws_path": ws_path,
        "host": host,
        "product": product,
        "pid": pid or None,
        "command": command,
    })
    if not entry.get("channel"):
        entry["channel"] = channel
    if not entry.get("source"):
        entry["source"] = source
    if not entry.get("user_data_dir"):
        entry["user_data_dir"] = user_data_dir
    if not entry.get("ws_url"):
        entry["ws_url"] = ws_url
    if not entry.get("ws_path"):
        entry["ws_path"] = ws_path
    if not entry.get("host"):
        entry["host"] = host
    if not entry.get("product"):
        entry["product"] = product
    if not entry.get("pid") and pid:
        entry["pid"] = pid
    if not entry.get("command"):
        entry["command"] = command
    entry["display_name"] = (
        f"{entry['instance_id']} "
        f"(port={entry['port']}, source={entry['source']}, profile={entry.get('user_data_dir', '') or '-'})"
    )
    instances[instance_id] = entry


def _browser_identity_key(item: dict[str, Any]) -> tuple:
    """同一浏览器的稳定标识：优先 ws_url（含 browser GUID），否则回退 host:port。"""
    ws_url = str(item.get("ws_url") or "").strip()
    if ws_url:
        return ("ws", ws_url)
    port = int(item.get("port") or 0)
    if port:
        host = _normalize_host(str(item.get("host") or ""))
        return ("hostport", host, port)
    return ("id", str(item.get("instance_id") or ""))


def _dedupe_same_browser(discovered: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """合并指向同一浏览器的实例。

    Chrome 同一个浏览器会被两种探测方式各登记一次（DevToolsActivePort 文件 vs
    进程命令行），二者 ws_url 相同。这里按浏览器身份去重：保留信息更全（有 pid /
    source=remote_debugging_port）的一条为主，其余 instance_id 作为 alias 保留，
    使旧 id 仍可显式选择。
    """
    groups: dict[tuple, list[dict[str, Any]]] = {}
    order: list[tuple] = []
    for item in discovered:
        key = _browser_identity_key(item)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(item)

    merged: list[dict[str, Any]] = []
    for key in order:
        group = groups[key]
        if len(group) == 1:
            merged.append(group[0])
            continue
        # 选主：有 pid 优先；其次 source=remote_debugging_port（进程命令行为准）；最后按 id 稳定
        primary = sorted(
            group,
            key=lambda it: (
                1 if it.get("pid") else 0,
                1 if it.get("source") == "remote_debugging_port" else 0,
                str(it.get("instance_id") or ""),
            ),
            reverse=True,
        )[0]
        aliases: set[str] = set(primary.get("aliases") or [])
        for it in group:
            if it is primary:
                continue
            iid = str(it.get("instance_id") or "")
            if iid and iid != primary.get("instance_id"):
                aliases.add(iid)
            for fld in ("ws_url", "ws_path", "host", "product", "command", "user_data_dir"):
                if not primary.get(fld) and it.get(fld):
                    primary[fld] = it[fld]
            if not primary.get("pid") and it.get("pid"):
                primary["pid"] = it["pid"]
        primary["aliases"] = sorted(aliases)
        primary["display_name"] = (
            f"{primary['instance_id']} "
            f"(port={primary['port']}, source={primary['source']}, "
            f"profile={primary.get('user_data_dir', '') or '-'})"
        )
        merged.append(primary)
    return merged


def discover_cdp_instances() -> list[dict[str, Any]]:
    """发现当前可连接的 Chrome CDP 实例。"""
    instances: dict[str, dict[str, Any]] = {}

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
        version_data = _fetch_version_payload(port)
        ws_url = str(version_data.get("webSocketDebuggerUrl", "") or "").strip()
        host = str(version_data.get("_host", "") or "").strip()
        _upsert_instance(
            instances,
            channel=channel,
            source="devtools_active_port",
            port=port,
            user_data_dir=str(udir),
            ws_url=ws_url,
            ws_path=ws_path,
            host=host,
            product=str(version_data.get("Browser", "") or ""),
        )

    try:
        output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    except Exception:
        output = ""
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid_text, _, command = line.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if "--remote-debugging-port" not in command:
            continue
        try:
            argv = shlex.split(command)
        except Exception:
            continue
        port = 0
        user_data_dir = ""
        for index, arg in enumerate(argv):
            if arg.startswith("--remote-debugging-port="):
                try:
                    port = int(arg.split("=", 1)[1].strip())
                except ValueError:
                    port = 0
            elif arg == "--remote-debugging-port" and index + 1 < len(argv):
                try:
                    port = int(argv[index + 1].strip())
                except ValueError:
                    port = 0
            elif arg.startswith("--user-data-dir="):
                user_data_dir = arg.split("=", 1)[1].strip()
            elif arg == "--user-data-dir" and index + 1 < len(argv):
                user_data_dir = argv[index + 1].strip()
        if not port:
            continue
        version_data = _fetch_version_payload(port)
        host = str(version_data.get("_host", "") or "").strip()
        ws_url = str(version_data.get("webSocketDebuggerUrl", "") or "").strip()
        _upsert_instance(
            instances,
            channel=_guess_channel(command, user_data_dir),
            source="remote_debugging_port",
            port=port,
            user_data_dir=user_data_dir,
            ws_url=ws_url,
            host=host,
            product=str(version_data.get("Browser", "") or ""),
            pid=pid,
            command=command,
        )

    deduped = _dedupe_same_browser(list(instances.values()))
    discovered = sorted(
        deduped,
        key=lambda item: (
            int(item.get("port") or 0),
            str(item.get("instance_id") or ""),
        ),
    )
    return discovered


def _match_instance_candidates(
    selector: str,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """按实例 ID / 端口 / profile 路径匹配候选实例。"""
    value = str(selector or "").strip()
    if not value:
        return []
    direct = [item for item in candidates if item.get("instance_id") == value]
    if direct:
        return direct
    alias_hit = [item for item in candidates if value in (item.get("aliases") or [])]
    if alias_hit:
        return alias_hit
    by_profile = [item for item in candidates if item.get("user_data_dir") == value]
    if by_profile:
        return by_profile
    if value.isdigit():
        by_port = [item for item in candidates if int(item.get("port") or 0) == int(value)]
        if by_port:
            return by_port
    fuzzy = [
        item for item in candidates
        if value in str(item.get("instance_id", "")) or value in str(item.get("user_data_dir", ""))
    ]
    return fuzzy


def resolve_cdp_instance(selector: str = "") -> dict[str, Any]:
    """解析要连接的实例；多实例且未指定时直接报错。"""
    candidates = discover_cdp_instances()
    chosen = str(selector or os.environ.get("CHROME_CDP_INSTANCE", "") or "").strip()
    if not chosen:
        # 兜底：单用户机器可在 config.json 写 default_instance，避免每次撞多实例歧义
        chosen = str(_load_cli_config().get("default_instance", "") or "").strip()
    if chosen:
        matched = _match_instance_candidates(chosen, candidates)
        if len(matched) == 1:
            return matched[0]
        if not matched:
            raise CdpInstanceSelectionError(
                f"未找到匹配的 Chrome CDP 实例: {chosen}",
                candidates=candidates,
            )
        raise CdpInstanceSelectionError(
            f"实例选择不唯一，请使用更精确的 --instance: {chosen}",
            candidates=matched,
        )
    if not candidates:
        cdp_port = int(os.environ.get("CHROME_CDP_PORT", "9222").strip() or "9222")
        raise CdpInstanceSelectionError(
            "未发现可用的 Chrome CDP 实例。"
            "方案1: 确保 Chrome >= 144 且在 chrome://inspect/#remote-debugging 勾选 'Allow remote debugging'；"
            f"方案2: 确保 Chrome 以 --remote-debugging-port={cdp_port} 启动。",
            candidates=[],
        )
    if len(candidates) > 1:
        raise CdpInstanceSelectionError(
            "检测到多个 Chrome CDP 实例，请显式传入 --instance。",
            candidates=candidates,
        )
    return candidates[0]


def _discover_ws_url(instance_selector: str = "") -> tuple[str, str, dict[str, Any]]:
    """根据实例选择解析浏览器级 WS 地址。"""
    selected = resolve_cdp_instance(instance_selector)
    ws_url = str(selected.get("ws_url", "") or "").strip()
    if not ws_url:
        host = str(selected.get("host", "") or "127.0.0.1").strip()
        ws_path = str(selected.get("ws_path", "") or "").strip()
        port = int(selected.get("port") or 0)
        if ws_path and port:
            ws_url = f"ws://{_normalize_host(host)}:{port}{ws_path}"
    if not ws_url:
        raise CdpInstanceSelectionError(
            f"实例 {selected.get('instance_id', '')} 未暴露可用的 CDP WebSocket 地址",
            candidates=[selected],
        )
    return ws_url, str(selected.get("source", "") or "unknown"), selected


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

    def __init__(self, instance_selector: str = ""):
        self._ws: websocket.WebSocket | None = None
        self._ws_url: str = ""
        self._instance_selector = str(instance_selector or "").strip()
        self._selected_instance: dict[str, Any] = {}
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
        # 最后激活的 page targetId（由 Target.targetActivated 事件实时更新）
        # 用于替代 macOS CGWindowList active-page 逻辑，跨平台稳定
        self._last_activated_target: str = ""
        self._last_activated_lock = threading.Lock()
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
            ws_url, cdp_mode, selected_instance = _discover_ws_url(self._instance_selector)
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
            self._selected_instance = selected_instance
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
                # 首连后立即可用，页面缓存重建放到后台做，避免阻塞首个请求。
                threading.Thread(
                    target=self._rebuild_pages_cache_after_connect,
                    daemon=True,
                ).start()
            except Exception as exc:
                _log(f"Target discovery setup failed (non-fatal): {exc}", level="WARN")

    def _rebuild_pages_cache_after_connect(self) -> None:
        """连接建立后在后台补建页面缓存，减少首连阻塞。"""
        try:
            with self._lock:
                if not self._ws:
                    return
                self._rebuild_pages_cache_locked()
        except Exception as exc:
            _log(f"async rebuild pages cache failed: {exc}", level="WARN")

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
                    # 如果销毁的是当前 last_activated，清空
                    with self._last_activated_lock:
                        if self._last_activated_target == tid:
                            self._last_activated_target = ""
            elif method == "Target.targetActivated":
                # Chrome 切换 tab 时触发，记录用户当前聚焦的 tab
                info = params.get("targetInfo", {})
                tid = info.get("targetId", "")
                if tid and info.get("type") == "page":
                    with self._last_activated_lock:
                        self._last_activated_target = tid
                    _log(f"tab activated: {tid} url={info.get('url','')[:80]}")
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

    def get_last_activated_target(self) -> str:
        """返回用户最后切换到的 tab targetId（由 Target.targetActivated 实时更新）。
        若尚未记录（daemon 刚启动、用户未切换过 tab），返回空字符串。
        """
        with self._last_activated_lock:
            return self._last_activated_target

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

    def get_cookies_for_url(self, url: str) -> list[dict]:
        """按目标 URL 获取匹配 cookie，避免向客户端返回全域 cookie。"""
        target_url = str(url or "").strip()
        if not target_url:
            raise RuntimeError("missing url")
        with self._lock:
            resp = self._call_locked("Storage.getCookies")
            return _filter_cookies_for_url(resp.get("cookies", []), target_url)

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
                "instance_id": self._selected_instance.get("instance_id", self._instance_selector),
                "instance": self._selected_instance,
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
    "screenshot",
    "activate", "open_tab", "close_tab",
    "tab_bind", "tab_get", "tab_list", "tab_remove",
    "target_resolve",
    "group_create", "group_add", "group_remove_tab", "group_list",
    "group_close", "group_delete", "group_activate", "group_move", "group_close_tabs",
    "network_capture_start", "network_capture_stop", "network_capture_peek", "network_capture_export",
    "network_fetch", "network_replay",
    "navigate_page", "reload_page",
    "editor_get", "editor_set", "editor_type",
    "find_icon", "click_icon", "scan_tooltips",
    "diagnose_page",
    # 新增（11 个缺失点修复）
    "eval_js", "capture_headers", "scan_shortcuts",
    # 新增（本次）
    "local_storage_get", "local_storage_set", "local_storage_remove",
    # #27 图表数值提取
    "extract_metric",
})


def _handle_get_cookies_for_url_request(
    req: dict[str, Any], cdp: CdpConnection, page_mgr: Any = None
) -> dict[str, Any]:
    """处理指定 URL 的 cookie 查询，默认不绑定浏览器当前活动页面。"""
    url = str(req.get("url", "")).strip()
    if not url:
        return {"ok": False, "error": "missing 'url'"}

    cdp.ensure_connected()
    target = str(req.get("target", "") or "").strip()
    if page_mgr is not None and target:
        result = page_mgr.get_cookies_for_url(url, target=target)
        if not result.get("ok"):
            return result
        return {"ok": True, "url": url, "cookies": result.get("cookies", [])}

    # SDK/CLI 只传 url 时必须按 URL 做浏览器级 cookie 查询，避免被 active tab 污染。
    cookies = cdp.get_cookies_for_url(url)
    return {"ok": True, "url": url, "cookies": cookies}


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
        resp: dict[str, Any] = {}

        if action == "ping":
            resp = {"ok": True, **cdp.metrics()}

        elif action == "status":
            resp = {"ok": True, **cdp.metrics()}

        elif action == "get_cookies":
            cdp.ensure_connected()
            all_cookies = cdp.get_all_cookies()
            resp = {"ok": True, "cookies": all_cookies}

        elif action == "get_cookies_for_url":
            resp = _handle_get_cookies_for_url_request(req, cdp, page_mgr)

        elif action == "auth_material":
            url = str(req.get("url", "")).strip()
            if not url:
                resp = {"ok": False, "error": "missing 'url'"}
            else:
                cdp.ensure_connected()
                target = str(req.get("target", "active") or "active")
                reveal = bool(req.get("reveal", False))
                key_filter = str(req.get("key_filter", "") or "")
                cookie_error = ""
                if page_mgr is not None:
                    cookie_result = page_mgr.get_cookies_for_url(url, target=target)
                    if not cookie_result.get("ok"):
                        cookies = []
                        cookie_error = str(cookie_result.get("error", "unknown"))
                    else:
                        cookies = cookie_result.get("cookies", [])
                else:
                    cookies = cdp.get_cookies_for_url(url)

                local_items: dict[str, Any] = {}
                session_items: dict[str, Any] = {}
                storage_errors: dict[str, str] = {}
                if page_mgr is not None:
                    for storage_kind, output_name in (("local", "localStorage"), ("session", "sessionStorage")):
                        try:
                            storage_result = page_mgr.local_storage_get("", storage=storage_kind, target=target)
                            if storage_result.get("ok"):
                                if storage_kind == "local":
                                    local_items = storage_result.get("items", {}) or {}
                                else:
                                    session_items = storage_result.get("items", {}) or {}
                            else:
                                storage_errors[output_name] = str(storage_result.get("error", "unknown"))
                        except Exception as exc:
                            storage_errors[output_name] = str(exc)

                capture_requests: list[dict[str, Any]] = []
                capture_error = ""
                try:
                    capture_requests = _load_capture_requests(file_path=str(req.get("file", "") or ""))
                except Exception as exc:
                    capture_error = str(exc)
                resp = _auth_material_payload(
                    url=url,
                    cookies=[x for x in cookies if isinstance(x, dict)],
                    local_items=local_items,
                    session_items=session_items,
                    capture_requests=capture_requests,
                    reveal=reveal,
                    key_filter=key_filter,
                    target=target,
                    capture_error=capture_error,
                    storage_errors=storage_errors,
                )
                if cookie_error:
                    resp["cookie_error"] = cookie_error

        elif action == "auth_token":
            request_url = str(req.get("request_url", "")).strip()
            if not request_url:
                resp = {"ok": False, "error": "missing 'request_url'"}
            else:
                cdp.ensure_connected()
                cookie_url = str(req.get("cookie_url", "") or _origin_from_url(request_url))
                if page_mgr is not None:
                    cookie_result = page_mgr.get_cookies_for_url(cookie_url, target=req.get("target", "active"))
                    if not cookie_result.get("ok"):
                        resp = cookie_result
                    else:
                        cookies = _cookie_name_map(cookie_result.get("cookies", []))
                        if not cookies:
                            resp = {"ok": False, "error": f"未读取到 {cookie_url} 的 cookie", "cookie_url": cookie_url}
                        else:
                            resp = _request_auth_token_from_cookies(
                                request_url=request_url,
                                cookies=cookies,
                                method=str(req.get("method", "GET") or "GET"),
                                body=str(req.get("body", "") or ""),
                                headers=req.get("headers") if isinstance(req.get("headers"), dict) else {},
                                extract=str(req.get("extract", "") or ""),
                                header_templates=[
                                    str(x) for x in (req.get("header_templates") or []) if str(x).strip()
                                ],
                                timeout=int(req.get("timeout", 30) or 30),
                                verify=bool(req.get("verify", True)),
                                reveal=bool(req.get("reveal", False)),
                            )
                            resp["cookie_url"] = cookie_url
                else:
                    cookies = _cookie_name_map(cdp.get_cookies_for_url(cookie_url))
                    if not cookies:
                        resp = {"ok": False, "error": f"未读取到 {cookie_url} 的 cookie", "cookie_url": cookie_url}
                    else:
                        resp = _request_auth_token_from_cookies(
                            request_url=request_url,
                            cookies=cookies,
                            method=str(req.get("method", "GET") or "GET"),
                            body=str(req.get("body", "") or ""),
                            headers=req.get("headers") if isinstance(req.get("headers"), dict) else {},
                            extract=str(req.get("extract", "") or ""),
                            header_templates=[
                                str(x) for x in (req.get("header_templates") or []) if str(x).strip()
                            ],
                            timeout=int(req.get("timeout", 30) or 30),
                            verify=bool(req.get("verify", True)),
                            reveal=bool(req.get("reveal", False)),
                        )
                        resp["cookie_url"] = cookie_url

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
                "instance_id": cdp.metrics().get("instance_id", ""),
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
            if page_mgr is None:
                resp = {"ok": False, "error": "page_manager not initialized"}
            else:
                cdp.ensure_connected()
                resp = page_mgr.detect_active_page()

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


def _find_daemon_pids(instance_id: str = "") -> list[int]:
    """查找指定实例遗留的 daemon 进程；instance_id 为空时返回全部。"""
    try:
        out = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    except Exception:
        return []
    current = os.getpid()
    wanted = str(instance_id or "").strip()
    pids: list[int] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        pid_s, _, command = line.partition(" ")
        try:
            pid = int(pid_s)
        except ValueError:
            continue
        if pid == current:
            continue
        if "chrome-cdp-ws-daemon" in command and "daemon.py" in command and "__run_daemon__" in command:
            if wanted:
                marker = f"--instance {wanted}"
                marker_eq = f"--instance={wanted}"
                if marker not in command and marker_eq not in command:
                    continue
            pids.append(pid)
    return pids


def _force_stop(timeout: float = 8.0) -> None:
    if _daemon_is_running():
        try:
            _send({"action": "stop"}, timeout=2)
        except Exception:
            pass

    orphan_pids = _find_daemon_pids(CURRENT_INSTANCE_ID)
    for orphan_pid in orphan_pids:
        try:
            os.kill(orphan_pid, signal.SIGTERM)
        except Exception:
            pass

    deadline = time.time() + timeout
    while time.time() < deadline:
        pid = _read_pid()
        sock_exists = Path(SOCKET_PATH).exists()
        pid_alive = bool(pid and _is_pid_alive(pid)) or any(
            _is_pid_alive(orphan_pid) for orphan_pid in orphan_pids
        )
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
    for orphan_pid in orphan_pids:
        if _is_pid_alive(orphan_pid):
            try:
                os.kill(orphan_pid, signal.SIGTERM)
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

    _log(f"daemon started, pid={os.getpid()}, instance={CURRENT_INSTANCE_ID or 'default'}")

    cdp = CdpConnection(instance_selector=CURRENT_INSTANCE_ID)

    # 实例化页面级操作管理器
    from page_manager import PageManager
    page_mgr = PageManager(cdp)

    # 心跳线程
    threading.Thread(target=cdp.heartbeat_loop, daemon=True).start()
    # 启动后立即后台预热一次连接，减少首个业务请求的冷启动等待。
    threading.Thread(target=_prewarm_cdp_connection, args=(cdp,), daemon=True).start()

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
    cmd = [sys.executable, str(Path(__file__).resolve()), "__run_daemon__"]
    if CURRENT_INSTANCE_ID:
        cmd.extend(["--instance", CURRENT_INSTANCE_ID])
    try:
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        log.close()

    if not _wait_for_daemon_socket_ready(DAEMON_START_SOCKET_WAIT_SECONDS):
        print(f"CDP daemon may have failed, check: {LOG_FILE}", file=sys.stderr)
        raise SystemExit(1)

    pid_text = Path(PID_FILE).read_text().strip() if Path(PID_FILE).exists() else "?"
    ready_resp = _wait_for_daemon_ready(DAEMON_START_READY_WAIT_SECONDS)
    if ready_resp and ready_resp.get("ok"):
        print(
            f"CDP daemon started and ready, pid={pid_text}, "
            f"instance={CURRENT_INSTANCE_ID or 'default'}"
        )
        return

    print(
        f"CDP daemon started, pid={pid_text}, "
        f"instance={CURRENT_INSTANCE_ID or 'default'} (warming up)"
    )


def _prewarm_cdp_connection(cdp: "CdpConnection") -> None:
    """后台预热一次 CDP 连接，减少首个外部请求的冷启动延迟。"""
    try:
        cdp.ensure_connected()
    except Exception as exc:
        _log(f"background prewarm failed: {exc}", level="WARN")


def _wait_for_daemon_socket_ready(timeout_seconds: float) -> bool:
    """等待 daemon 的 pid/socket 就绪。"""
    deadline = time.time() + max(0.1, timeout_seconds)
    while time.time() < deadline:
        if Path(PID_FILE).exists() and Path(SOCKET_PATH).exists():
            return True
        time.sleep(0.1)
    return False


def _wait_for_daemon_ready(timeout_seconds: float) -> dict[str, Any] | None:
    """等待 daemon 完成一次真实 CDP 连接并返回可用状态。"""
    deadline = time.time() + max(0.1, timeout_seconds)
    while time.time() < deadline:
        try:
            resp = _send({"action": "test_connection"}, timeout=5)
            if resp.get("ok"):
                return resp
            if "unknown action" in str(resp.get("error", "")):
                legacy = _send({"action": "cdp_call", "method": "Browser.getVersion"}, timeout=5)
                if legacy.get("ok"):
                    return {"ok": True, "compat_mode": "legacy_ready_probe"}
        except Exception:
            pass
        time.sleep(DAEMON_READY_POLL_INTERVAL)
    return None


# ---------------------------------------------------------------------------
# CLI 增强：配置、输出护栏、批处理、安全策略
# ---------------------------------------------------------------------------
def _load_cli_config() -> dict[str, Any]:
    """加载 CLI 配置。优先级：用户配置 < 项目配置 < 环境变量/命令行。"""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE
    cfg: dict[str, Any] = {}
    for path in _CONFIG_PATHS:
        try:
            if not path.exists():
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update(data)
        except Exception as exc:
            print(f"Warning: 配置读取失败 {path}: {exc}", file=sys.stderr)
    _CONFIG_CACHE = cfg
    return cfg


def _cfg_value(key: str, env_name: str, default: Any = None) -> Any:
    env_val = os.environ.get(env_name)
    if env_val is not None:
        return env_val
    return _load_cli_config().get(key, default)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"", "0", "false", "no", "off"}


def _as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _argv_value(argv: list[str], flag: str) -> str | None:
    if flag in argv:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            return argv[idx + 1]
    return None


def _output_options(argv: list[str]) -> tuple[bool, int | None]:
    content_boundaries = _as_bool(
        _cfg_value("content_boundaries", "CDP_DAEMON_CONTENT_BOUNDARIES"),
        False,
    ) or "--content-boundaries" in argv
    max_output = _as_int(_cfg_value("max_output", "CDP_DAEMON_MAX_OUTPUT"), None)
    cli_max = _argv_value(argv, "--max-output")
    if cli_max is not None:
        max_output = _as_int(cli_max, max_output)
    return content_boundaries, max_output


def _truncate_output(text: str, max_output: int | None) -> str:
    if not max_output or max_output <= 0 or len(text) <= max_output:
        return text
    suffix = f"\n\n--- output truncated: {len(text)} chars -> {max_output} chars ---"
    keep = max(0, max_output - len(suffix))
    return text[:keep] + suffix


def _page_output(text: str, origin: str = "", argv: list[str] | None = None) -> None:
    """输出页面来源内容，支持长度限制和边界标记，降低 token 与 prompt injection 风险。"""
    argv = argv or sys.argv
    content_boundaries, max_output = _output_options(argv)
    text = _truncate_output(text, max_output)
    if content_boundaries:
        nonce = uuid.uuid4().hex[:12]
        safe_origin = origin or "unknown"
        print(f"--- CDP_DAEMON_PAGE_CONTENT nonce={nonce} origin={safe_origin} ---")
        print(text)
        print(f"--- END_CDP_DAEMON_PAGE_CONTENT nonce={nonce} ---")
    else:
        print(text)


def _json_output(obj: Any, argv: list[str] | None = None) -> None:
    argv = argv or sys.argv
    _, max_output = _output_options(argv)
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    if max_output and max_output > 0 and len(text) > max_output:
        preview_len = max(0, max_output - 220)
        truncated: dict[str, Any] = {
            "ok": obj.get("ok", True) if isinstance(obj, dict) else True,
            "truncated": True,
            "original_chars": len(text),
            "preview": text[:preview_len],
        }
        print(json.dumps(truncated, ensure_ascii=False, indent=2))
        return
    print(text)


def _format_snapshot_lines(elements: list[dict], include_urls: bool = False) -> list[str]:
    lines = []
    for el in elements:
        ref = el.get("ref", "?")
        desc = el.get("desc", "")
        val = el.get("value", "")
        line = f"{ref}  {desc}"
        if val:
            line += f"  value={val}"
        if include_urls and el.get("href"):
            line += f"  href={el['href']}"
        lines.append(line)
    lines.append(f"\n--- {len(elements)} interactive elements ---")
    return lines


def _snapshot_text(elements: list[dict], include_urls: bool = False) -> str:
    return "\n".join(_format_snapshot_lines(elements, include_urls=include_urls))


def _snapshot_baseline_path(target_id: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in (target_id or "active"))
    return SNAPSHOT_BASELINE_DIR / f"{safe}.txt"


def _save_snapshot_baseline(target_id: str, text: str) -> None:
    try:
        SNAPSHOT_BASELINE_DIR.mkdir(parents=True, exist_ok=True)
        _snapshot_baseline_path(target_id).write_text(text, encoding="utf-8")
    except Exception:
        pass


def _load_snapshot_baseline(target_id: str) -> str | None:
    path = _snapshot_baseline_path(target_id)
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


def _domain_patterns() -> list[str]:
    raw = _cfg_value("allowed_domains", "CDP_DAEMON_ALLOWED_DOMAINS", "")
    if isinstance(raw, list):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    return [x.strip().lower() for x in str(raw).split(",") if x.strip()]


def _domain_allowed(host: str, pattern: str) -> bool:
    host = host.lower().strip(".")
    pattern = pattern.lower().strip(".")
    if not pattern:
        return True
    if pattern.startswith("*."):
        bare = pattern[2:]
        return host == bare or host.endswith("." + bare)
    return fnmatch.fnmatch(host, pattern)


def _check_allowed_url(url: str) -> tuple[bool, str]:
    patterns = _domain_patterns()
    if not patterns:
        return True, ""
    parsed = urllib.parse.urlparse(url if "://" in url else "https://" + url)
    host = parsed.hostname or ""
    if not host:
        return False, f"无法解析 URL 域名: {url}"
    if any(_domain_allowed(host, pat) for pat in patterns):
        return True, ""
    return False, f"domain '{host}' is not in allowed_domains: {', '.join(patterns)}"


def _action_policy_path() -> str:
    return str(_cfg_value("action_policy", "CDP_DAEMON_ACTION_POLICY", "") or "")


def _policy_match(patterns: Any, command: str) -> bool:
    if isinstance(patterns, str):
        patterns = [patterns]
    if not isinstance(patterns, list):
        return False
    return any(fnmatch.fnmatch(command, str(p)) for p in patterns)


def _check_action_policy(command: str) -> tuple[bool, str]:
    path = _action_policy_path()
    if not path:
        return True, ""
    try:
        policy = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"action policy 读取失败: {exc}"
    default = str(policy.get("default", "allow")).lower()
    if _policy_match(policy.get("deny", []), command):
        return False, f"action '{command}' denied by policy"
    if _policy_match(policy.get("allow", []), command):
        return True, ""
    if default == "deny":
        return False, f"action '{command}' denied by policy default"
    return True, ""


def _confirm_patterns() -> list[str]:
    raw = _cfg_value("confirm_actions", "CDP_DAEMON_CONFIRM_ACTIONS", "")
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def _needs_confirmation(command: str) -> bool:
    patterns = _confirm_patterns()
    return bool(patterns and _policy_match(patterns, command))


def _confirm_action(command: str, argv: list[str]) -> tuple[bool, str]:
    """按配置确认敏感动作。非交互环境默认拒绝，避免静默误操作。"""
    if not _needs_confirmation(command):
        return True, ""
    if "--yes" in argv:
        return True, ""
    interactive = _as_bool(
        _cfg_value("confirm_interactive", "CDP_DAEMON_CONFIRM_INTERACTIVE"),
        False,
    )
    if not interactive or not sys.stdin.isatty():
        return False, (
            f"action '{command}' requires confirmation; "
            "set CDP_DAEMON_CONFIRM_INTERACTIVE=1 in a TTY or pass --yes explicitly"
        )
    prompt = f"Confirm CDP action '{command}'? Type 'yes' to continue: "
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False, f"action '{command}' confirmation aborted"
    if answer == "yes":
        return True, ""
    return False, f"action '{command}' not confirmed"


def _run_cli_argv(argv: list[str]) -> tuple[int, str, str]:
    """在当前进程执行一个 CLI 子命令，供 batch 复用。"""
    old_argv = sys.argv
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        sys.argv = [old_argv[0], *argv]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                rc = main()
            except SystemExit as exc:
                rc = int(exc.code or 0)
        return rc, stdout.getvalue(), stderr.getvalue()
    finally:
        sys.argv = old_argv


def _parse_batch_commands(args: list[str], json_mode: bool) -> list[list[str]]:
    payload_args: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in {"--bail", "--json", "--content-boundaries"}:
            continue
        if arg == "--max-output":
            skip_next = True
            continue
        payload_args.append(arg)
    if json_mode:
        raw = " ".join(payload_args) if payload_args else sys.stdin.read().strip()
        data = json.loads(raw)
        commands: list[list[str]] = []
        for item in data:
            if isinstance(item, list):
                commands.append([str(x) for x in item])
            elif isinstance(item, str):
                commands.append(shlex.split(item))
            else:
                raise ValueError(f"unsupported batch item: {item!r}")
        return commands
    return [shlex.split(item) for item in payload_args]


def _run_batch(args: list[str]) -> int:
    json_mode = "--json" in args
    bail = "--bail" in args
    try:
        commands = _parse_batch_commands(args, json_mode)
    except Exception as exc:
        print(f"Error: batch parse failed: {exc}", file=sys.stderr)
        return 1
    results = []
    final_rc = 0
    for idx, argv in enumerate(commands, 1):
        if not argv:
            continue
        if argv[0] == "batch":
            rc, out, err = 1, "", "nested batch is not supported\n"
        else:
            rc, out, err = _run_cli_argv(argv)
        results.append({"index": idx, "command": argv, "code": rc, "stdout": out, "stderr": err})
        if not json_mode:
            print("$ " + " ".join(shlex.quote(x) for x in argv))
            if out:
                print(out, end="" if out.endswith("\n") else "\n")
            if err:
                print(err, end="" if err.endswith("\n") else "\n", file=sys.stderr)
        if rc != 0:
            final_rc = rc
            if bail:
                break
    if json_mode:
        _json_output({"ok": final_rc == 0, "results": results, "code": final_rc}, sys.argv)
    return final_rc


# ---------------------------------------------------------------------------
# Network capture 高级分析 / 动作级抓包
# ---------------------------------------------------------------------------

def _capture_path(filtered: bool = False) -> Path:
    import tempfile
    name = "cdp_network_capture_filtered.json" if filtered else "cdp_network_capture.json"
    return Path(tempfile.gettempdir()) / name


def _load_capture_requests(
    prefer_filtered: bool = False,
    file_path: str = "",
) -> list[dict[str, Any]]:
    """加载抓包 JSON；默认读临时目录，也可指定 --file。"""
    if file_path:
        path = Path(file_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"抓包文件不存在: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        raise FileNotFoundError(f"抓包文件格式无效: {path}")

    candidates = [_capture_path(True), _capture_path(False)] if prefer_filtered else [_capture_path(False)]
    for path in candidates:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
    raise FileNotFoundError("无抓包数据，请先执行 network-capture stop 或 load-file")


def _save_capture_requests(requests: list[dict[str, Any]], filtered: bool = False) -> Path:
    path = _capture_path(filtered)
    path.write_text(json.dumps(requests, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _filter_requests(
    requests: list[dict[str, Any]],
    method_filter: str = "",
    url_filter: str = "",
    exclude_domain: str = "",
    status_filter: str = "",
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for req in requests:
        if method_filter and str(req.get("method", "")).upper() != method_filter.upper():
            continue
        url = str(req.get("url", ""))
        if url_filter and url_filter.lower() not in url.lower():
            continue
        if exclude_domain and exclude_domain.lower() in url.lower():
            continue
        if status_filter and str(req.get("status", "")) != str(status_filter):
            continue
        result.append(req)
    return result


def _json_maybe(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text or text[0] not in "[{":
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _request_json_body(req: dict[str, Any]) -> Any:
    return _json_maybe(req.get("postData"))


def _response_json_body(req: dict[str, Any]) -> Any:
    body = req.get("responseBody")
    if body is None and req.get("responseBodyFile"):
        try:
            body = Path(str(req["responseBodyFile"])).read_text(encoding="utf-8")
        except Exception:
            body = None
    return _json_maybe(body)


def _json_data(payload: Any) -> Any:
    if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
        return payload.get("data")
    return payload


def _url_key(url: str, include_query: bool = False) -> str:
    parsed = urllib.parse.urlparse(url)
    key = parsed.path or url
    if include_query and parsed.query:
        key += "?" + parsed.query
    return key


def _flatten_json(value: Any, prefix: str = "", limit: int = 2000) -> dict[str, Any]:
    out: dict[str, Any] = {}

    def walk(v: Any, path: str) -> None:
        if len(out) >= limit:
            return
        if isinstance(v, dict):
            if not v:
                out[path or "$"] = {}
            for k, child in v.items():
                child_path = f"{path}.{k}" if path else str(k)
                walk(child, child_path)
        elif isinstance(v, list):
            if not v:
                out[path or "$"] = []
            else:
                for idx, child in enumerate(v[:50]):
                    walk(child, f"{path}[{idx}]")
        else:
            out[path or "$"] = v

    walk(value, prefix)
    return out


def _is_volatile_capture_path(path: str) -> bool:
    """过滤服务端保存时常见自动改写字段，减少 diff 噪音。"""
    tail = path.split(".")[-1]
    if tail in {"updateTime", "createTime", "modifiedTime", "jobId"}:
        return True
    return tail.startswith("aiReview")


def _is_sensitive_header(name: str) -> bool:
    n = name.lower()
    if n in {"cookie", "authorization", "proxy-authorization"}:
        return True
    return any(part in n for part in ("token", "csrf", "xsrf", "session", "auth"))


def _is_credential_header(name: str) -> bool:
    """识别通用认证/会话/防 CSRF 类请求头，不绑定具体业务平台。"""
    n = name.lower()
    exact = {
        "cookie", "authorization", "proxy-authorization", "www-authenticate",
        "csrf-token", "x-csrf-token", "x-xsrf-token", "xsrf-token",
        "x-auth-token", "x-api-key", "api-key",
    }
    if n in exact:
        return True
    return any(part in n for part in ("token", "session", "auth", "csrf", "xsrf"))


def _cookie_name_map(cookies: list[dict[str, Any]]) -> dict[str, str]:
    """将 CDP cookie 列表压缩为 requests 可直接使用的 name -> value。"""
    result: dict[str, str] = {}
    for item in cookies:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if name:
            result[name] = str(item.get("value", ""))
    return result


def _redact_cookie(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """输出 cookie 元信息，刻意不包含 value。"""
    fields = ("name", "domain", "path", "expires", "session", "httpOnly", "secure", "sameSite")
    redacted: list[dict[str, Any]] = []
    for item in cookies:
        if not isinstance(item, dict):
            continue
        redacted.append({key: item.get(key) for key in fields if key in item})
    return redacted


def _validate_cookie_names(cookies: list[dict[str, Any]], expected_names: list[str]) -> dict[str, Any]:
    """校验目标 URL cookie 是否包含期望名称。"""
    cookie_names = {
        str(item.get("name", "")).strip()
        for item in cookies
        if isinstance(item, dict) and str(item.get("name", "")).strip()
    }
    present = [name for name in expected_names if name in cookie_names]
    missing = [name for name in expected_names if name not in cookie_names]
    return {
        "ok": not missing,
        "present": present,
        "missing": missing,
        "count": len(cookie_names),
    }


def _origin_from_url(url: str) -> str:
    """从完整 URL 提取 scheme://host[:port]，用于限定 cookie 读取域。"""
    parsed = urllib.parse.urlparse(url if "://" in url else "https://" + url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"无法解析 URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}"


def _redact_secret(value: Any, reveal: bool = False) -> str:
    """按安全默认值输出敏感字段，只有显式 reveal 时返回原文。"""
    text = "" if value is None else str(value)
    if reveal:
        return text
    if not text:
        return ""
    return "<redacted>"


def _matches_key_filter(name: str, key_filter: str = "") -> bool:
    """判断 key/name 是否命中用户传入的过滤词。"""
    if not key_filter:
        return True
    return key_filter.lower() in str(name or "").lower()


def _is_token_like_key(name: str) -> bool:
    """识别 storage/cookie/header 中常见的认证材料 key 名。"""
    n = str(name or "").lower()
    if _is_credential_header(n):
        return True
    return any(part in n for part in (
        "jwt", "bearer", "ticket", "credential", "sso", "login", "sessionid",
    ))


def _cookie_material(
    cookies: list[dict[str, Any]],
    reveal: bool = False,
    key_filter: str = "",
) -> list[dict[str, Any]]:
    """将 CDP cookie 列表转换为认证材料清单。"""
    rows: list[dict[str, Any]] = []
    for item in cookies:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name or not _matches_key_filter(name, key_filter):
            continue
        rows.append({
            "name": name,
            "domain": item.get("domain", ""),
            "path": item.get("path", ""),
            "httpOnly": bool(item.get("httpOnly", False)),
            "secure": bool(item.get("secure", False)),
            "token_like": _is_token_like_key(name),
            "value": _redact_secret(item.get("value", ""), reveal=reveal),
        })
    return rows


def _storage_material(
    items: dict[str, Any],
    storage_name: str,
    reveal: bool = False,
    key_filter: str = "",
) -> list[dict[str, Any]]:
    """筛出 localStorage/sessionStorage 中疑似认证相关的 key。"""
    rows: list[dict[str, Any]] = []
    for key, value in sorted((items or {}).items()):
        key_text = str(key)
        token_like = _is_token_like_key(key_text)
        if key_filter:
            if not _matches_key_filter(key_text, key_filter):
                continue
        elif not token_like:
            continue
        rows.append({
            "storage": storage_name,
            "key": key_text,
            "token_like": token_like,
            "value": _redact_secret(value, reveal=reveal),
        })
    return rows


def _capture_header_material(
    requests_: list[dict[str, Any]],
    url: str,
    reveal: bool = False,
    key_filter: str = "",
) -> list[dict[str, Any]]:
    """从抓包记录中提取通用认证 header 名称和值。"""
    target_host = urllib.parse.urlparse(url if "://" in url else "https://" + url).hostname or ""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for req in requests_:
        req_url = str(req.get("url", ""))
        req_host = urllib.parse.urlparse(req_url).hostname or ""
        if target_host and req_host and target_host not in req_host and req_host not in target_host:
            continue
        headers = req.get("headers") or {}
        if not isinstance(headers, dict):
            continue
        for key, value in headers.items():
            key_text = str(key)
            credential = _is_credential_header(key_text)
            if key_filter:
                if not _matches_key_filter(key_text, key_filter):
                    continue
            elif not credential:
                continue
            dedup = (req_url, key_text.lower())
            if dedup in seen:
                continue
            seen.add(dedup)
            rows.append({
                "name": key_text,
                "sample_url": req_url,
                "sample_method": req.get("method", ""),
                "credential_like": credential,
                "value": _redact_secret(value, reveal=reveal),
            })
    return rows


def _auth_material_payload(
    url: str,
    cookies: list[dict[str, Any]],
    local_items: dict[str, Any] | None = None,
    session_items: dict[str, Any] | None = None,
    capture_requests: list[dict[str, Any]] | None = None,
    reveal: bool = False,
    key_filter: str = "",
    target: str = "active",
    capture_error: str = "",
    storage_errors: dict[str, str] | None = None,
) -> dict[str, Any]:
    """汇总 cookie、storage、抓包 header 三类通用认证材料。"""
    local_rows = _storage_material(local_items or {}, "localStorage", reveal=reveal, key_filter=key_filter)
    session_rows = _storage_material(session_items or {}, "sessionStorage", reveal=reveal, key_filter=key_filter)
    result: dict[str, Any] = {
        "ok": True,
        "url": url,
        "target": target,
        "reveal": reveal,
        "key_filter": key_filter,
        "cookies": _cookie_material(cookies, reveal=reveal, key_filter=key_filter),
        "storage": {
            "localStorage": local_rows,
            "sessionStorage": session_rows,
        },
        "auth_headers": _capture_header_material(
            capture_requests or [], url, reveal=reveal, key_filter=key_filter
        ),
    }
    if capture_error:
        result["capture_error"] = capture_error
    if storage_errors:
        result["storage_errors"] = storage_errors
    return result


def _parse_json_object(text: str, option_name: str) -> dict[str, Any]:
    """解析 CLI JSON 对象参数。"""
    if not text:
        return {}
    try:
        value = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{option_name} 不是合法 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{option_name} 必须是 JSON object")
    return value


def _json_path_tokens(path: str) -> list[str]:
    """将 dotted path / $.path 转为 token 列表，支持简单数组下标。"""
    p = str(path or "").strip()
    if not p or p == "$":
        return []
    if p.startswith("$."):
        p = p[2:]
    elif p.startswith("$"):
        p = p[1:].lstrip(".")
    tokens: list[str] = []
    for part in p.split("."):
        rest = part
        while rest:
            if "[" in rest:
                before, _, after = rest.partition("[")
                if before:
                    tokens.append(before)
                idx, _, tail = after.partition("]")
                if idx:
                    tokens.append(idx)
                rest = tail
            else:
                tokens.append(rest)
                break
    return [t for t in tokens if t != ""]


def _extract_json_path(value: Any, path: str) -> Any:
    """按 dotted path / $.path 从 JSON 响应里提取字段值。"""
    current = value
    for token in _json_path_tokens(path):
        if isinstance(current, dict):
            current = current.get(token)
        elif isinstance(current, list):
            try:
                current = current[int(token)]
            except Exception:
                return None
        else:
            return None
    return current


def _render_header_templates(templates: list[str], token: str) -> dict[str, str]:
    """按 Name=Value 或 Name: Value 模板渲染认证 header。"""
    rendered: dict[str, str] = {}
    for raw in templates:
        if "=" in raw:
            name, value_tpl = raw.split("=", 1)
        elif ":" in raw:
            name, value_tpl = raw.split(":", 1)
        else:
            raise ValueError(f"header-template 格式错误: {raw!r}，应为 Name=Value")
        name = name.strip()
        if not name:
            raise ValueError(f"header-template 缺少 header 名称: {raw!r}")
        rendered[name] = value_tpl.strip().format(token=token)
    return rendered


def _request_auth_token_from_cookies(
    request_url: str,
    cookies: dict[str, str],
    method: str = "GET",
    body: str = "",
    headers: dict[str, Any] | None = None,
    extract: str = "",
    header_templates: list[str] | None = None,
    timeout: int = 30,
    verify: bool = True,
    reveal: bool = False,
) -> dict[str, Any]:
    """用浏览器 cookie 请求任意 token 接口，并按用户规则提取 token。"""
    session = requests.Session()
    session.cookies.update(cookies)
    req_headers = {str(k): str(v) for k, v in (headers or {}).items()}
    json_body: Any = None
    data_body: Any = None
    if body:
        try:
            json_body = json.loads(body)
        except Exception:
            data_body = body
    try:
        resp = session.request(
            method.upper(),
            request_url,
            headers=req_headers,
            json=json_body,
            data=data_body,
            timeout=timeout,
            verify=verify,
        )
    except Exception as exc:
        return {"ok": False, "error": f"token request failed: {exc}", "request_url": request_url}

    text = resp.text or ""
    try:
        payload: Any = resp.json()
    except Exception:
        payload = None

    if not resp.ok:
        return {
            "ok": False,
            "request_url": request_url,
            "status_code": resp.status_code,
            "error": text[:500],
        }

    token_value: Any = None
    if extract:
        token_value = _extract_json_path(payload, extract) if payload is not None else None
        if token_value in (None, ""):
            return {
                "ok": False,
                "request_url": request_url,
                "status_code": resp.status_code,
                "extract": extract,
                "error": f"未从响应中提取到 token: {extract}",
                "response_shape": _json_shape_summary(payload) if payload is not None else {"type": "text"},
            }

    token_text = "" if token_value is None else str(token_value)
    rendered_headers = _render_header_templates(header_templates or [], token_text) if token_text else {}
    return {
        "ok": True,
        "request_url": request_url,
        "status_code": resp.status_code,
        "extract": extract,
        "token": _redact_secret(token_text, reveal=reveal) if extract else "",
        "headers": {
            key: _redact_secret(value, reveal=reveal)
            for key, value in rendered_headers.items()
        },
        "response_shape": _json_shape_summary(payload) if payload is not None else {"type": "text", "bytes": len(text)},
    }


def _json_shape_summary(value: Any, max_keys: int = 30) -> dict[str, Any]:
    """生成 JSON 结构摘要，方便跨平台判断响应是否有数据。"""
    if isinstance(value, dict):
        keys = list(value.keys())[:max_keys]
        summary: dict[str, Any] = {"type": "object", "keys": keys}
        children: dict[str, Any] = {}
        for key in keys:
            child = value.get(key)
            if isinstance(child, list):
                item: dict[str, Any] = {"type": "array", "length": len(child)}
                if child:
                    first = child[0]
                    item["item_type"] = type(first).__name__
                    if isinstance(first, dict):
                        item["item_keys"] = list(first.keys())[:max_keys]
                children[str(key)] = item
            elif isinstance(child, dict):
                children[str(key)] = {"type": "object", "keys": list(child.keys())[:max_keys]}
            elif child is None:
                children[str(key)] = {"type": "null"}
            else:
                children[str(key)] = {"type": type(child).__name__}
        if children:
            summary["children"] = children
        return summary
    if isinstance(value, list):
        summary = {"type": "array", "length": len(value)}
        if value:
            first = value[0]
            summary["item_type"] = type(first).__name__
            if isinstance(first, dict):
                summary["item_keys"] = list(first.keys())[:max_keys]
            elif isinstance(first, list):
                summary["item_length"] = len(first)
        return summary
    if value is None:
        return {"type": "null"}
    return {"type": type(value).__name__}


def _summarize_requests(requests: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for idx, req in enumerate(requests, 1):
        headers_data: Any = req.get("headers")
        headers: dict[str, Any] = headers_data if isinstance(headers_data, dict) else {}
        response_headers_data: Any = req.get("responseHeaders")
        response_headers: dict[str, Any] = (
            response_headers_data if isinstance(response_headers_data, dict) else {}
        )
        url_text = str(req.get("url", ""))
        parsed_url = urllib.parse.urlparse(url_text)
        body = _request_json_body(req)
        resp = _response_json_body(req)
        response_bytes = req.get("responseBodySize")
        if response_bytes is None:
            response_bytes = len(str(req.get("responseBody") or ""))
        response_body_file = str(req.get("responseBodyFile") or "")
        content_type = (
            response_headers.get("content-type")
            or response_headers.get("Content-Type")
            or req.get("mimeType")
            or headers.get("content-type")
            or headers.get("Content-Type")
            or ""
        )
        item: dict[str, Any] = {
            "index": idx,
            "status": req.get("status", ""),
            "method": req.get("method", ""),
            "url": url_text,
            "host": parsed_url.hostname or "",
            "path": parsed_url.path or _url_key(url_text),
            "path_or_url": parsed_url.path or url_text,
            "query_keys": sorted(urllib.parse.parse_qs(parsed_url.query).keys()),
            "content_type": content_type,
            "request_bytes": len(str(req.get("postData") or "")),
            "response_bytes": response_bytes,
            "body_file": response_body_file,
            "body_size": response_bytes,
        }
        if isinstance(body, dict):
            item["request_body_keys"] = list(body.keys())[:30]
            item["request_shape"] = _json_shape_summary(body)
            if str(req.get("method", "")).upper() in {"PUT", "PATCH", "POST"} and len(body.keys()) >= 4:
                item["full_payload_write_candidate"] = True
        elif isinstance(body, list):
            item["request_shape"] = _json_shape_summary(body)
        if isinstance(resp, dict):
            item["response_keys"] = list(resp.keys())[:30]
            item["response_shape"] = _json_shape_summary(resp)
        elif isinstance(resp, list):
            item["response_shape"] = _json_shape_summary(resp)
        present_auth = {}
        for k, v in headers.items():
            if _is_credential_header(k):
                present_auth[k] = "***" if _is_sensitive_header(k) else v
        if present_auth:
            item["auth_headers"] = present_auth
            item["auth_headers_redacted"] = present_auth
        items.append(item)
    return {"ok": True, "count": len(requests), "requests": items}


def _find_related_get_before(requests: list[dict[str, Any]], idx: int, write_req: dict[str, Any]) -> dict[str, Any] | None:
    write_path = _url_key(str(write_req.get("url", "")))
    for prev in reversed(requests[:idx]):
        if str(prev.get("method", "")).upper() != "GET":
            continue
        if _url_key(str(prev.get("url", ""))) == write_path and _response_json_body(prev) is not None:
            return prev
    return None


def _find_related_get_after(requests: list[dict[str, Any]], idx: int, write_req: dict[str, Any]) -> dict[str, Any] | None:
    write_path = _url_key(str(write_req.get("url", "")))
    for nxt in requests[idx + 1:]:
        if str(nxt.get("method", "")).upper() != "GET":
            continue
        if _url_key(str(nxt.get("url", ""))) == write_path and _response_json_body(nxt) is not None:
            return nxt
    return None


def _diff_body(requests: list[dict[str, Any]]) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for idx, req in enumerate(requests):
        method = str(req.get("method", "")).upper()
        if method not in {"POST", "PUT", "PATCH"}:
            continue
        body = _request_json_body(req)
        if not isinstance(body, (dict, list)):
            continue
        entry: dict[str, Any] = {
            "index": idx + 1,
            "method": method,
            "url": req.get("url", ""),
            "path": _url_key(str(req.get("url", ""))),
        }
        before = _find_related_get_before(requests, idx, req)
        after = _find_related_get_after(requests, idx, req)
        compare_base = before or after
        compare_kind = "before_get" if before else ("after_get" if after else "")
        if compare_base:
            old_payload = _json_data(_response_json_body(compare_base))
            old_flat = _flatten_json(old_payload)
            new_flat = _flatten_json(body)
            changes = []
            for path in sorted(set(old_flat) | set(new_flat)):
                if _is_volatile_capture_path(path):
                    continue
                old = old_flat.get(path, "<missing>")
                new = new_flat.get(path, "<missing>")
                if old != new:
                    changes.append({"path": path, "old": old, "new": new})
                    if len(changes) >= 80:
                        break
            entry["compare"] = compare_kind
            entry["related_get_index"] = requests.index(compare_base) + 1
            entry["changes"] = changes
            if compare_kind == "after_get" and not changes:
                entry["note"] = "写入请求体与后续 GET data 基本一致，可作为保存回读校验。"
        else:
            flat = _flatten_json(body)
            interesting = [
                p for p in flat
                if p in {"script", "configs.varParams", "configs.otherParams"}
                or p.endswith(".script") or p.endswith(".varParams") or p.endswith(".otherParams")
            ]
            entry["changed_candidates"] = interesting[:80] or list(flat.keys())[:40]
            entry["note"] = "未捕获到同 path 的 GET 前置/后置响应，输出请求体候选字段。"
        results.append(entry)
    return {"ok": True, "writes": results, "count": len(results)}


def _infer_crud(requests: list[dict[str, Any]]) -> dict[str, Any]:
    paths: dict[str, dict[str, Any]] = {}
    for idx, req in enumerate(requests, 1):
        path = _url_key(str(req.get("url", "")))
        method = str(req.get("method", "")).upper()
        item = paths.setdefault(path, {"path": path, "methods": [], "requests": []})
        if method not in item["methods"]:
            item["methods"].append(method)
        item["requests"].append(idx)

    flows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for path, info in paths.items():
        methods = set(info["methods"])
        if "GET" in methods and methods.intersection({"PUT", "PATCH"}):
            flow = {
                "pattern": "detail-read + full-object-write",
                "path": path,
                "read": "GET",
                "write": sorted(methods.intersection({"PUT", "PATCH"})),
                "requests": info["requests"],
            }
            related_writes = [requests[i - 1] for i in info["requests"] if str(requests[i - 1].get("method", "")).upper() in {"PUT", "PATCH"}]
            for write in related_writes:
                body = _request_json_body(write)
                if isinstance(body, dict) and len(body) >= 4:
                    flow["full_payload_write_candidate"] = True
                    warnings.append(f"{path}: 检测到整包 {write.get('method')}，同资源多字段更新应串行或合并后单次 PUT。")
                    break
            flows.append(flow)
        elif methods.intersection({"POST"}) and ("status" in path.lower() or "preview" in path.lower()):
            flows.append({"pattern": "run/status/preview candidate", "path": path, "methods": info["methods"], "requests": info["requests"]})

    return {"ok": True, "flows": flows, "warnings": list(dict.fromkeys(warnings)), "count": len(flows)}


def _print_capture_table(requests: list[dict[str, Any]]) -> None:
    for idx, req in enumerate(requests, 1):
        method = req.get("method", "?")
        url = req.get("url", "")
        status = req.get("status", "?")
        post_data = req.get("postData")
        body_hint = f"  req={len(post_data)}B" if post_data else ""
        resp_body = req.get("responseBody")
        if resp_body:
            body_hint += f"  resp={len(str(resp_body))}B"
        elif req.get("responseBodyFile"):
            body_hint += f"  resp=file:{req.get('responseBodyFile')}"
        elif req.get("responseBodySkipped"):
            body_hint += f"  resp=skip:{req.get('responseBodySkipped')}"
        print(f"[{idx}] {status} {method:6s} {url}{body_hint}")


def _parse_capture_filters(
    argv: list[str],
    *,
    include_url_flag: bool = True,
) -> tuple[str, str, str, str]:
    method_filter = ""
    url_filter = ""
    exclude_domain = ""
    status_filter = ""
    if "--method" in argv and argv.index("--method") + 1 < len(argv):
        method_filter = argv[argv.index("--method") + 1].upper()
    if include_url_flag and "--url" in argv and argv.index("--url") + 1 < len(argv):
        url_filter = argv[argv.index("--url") + 1]
    if "--filter-url" in argv and argv.index("--filter-url") + 1 < len(argv):
        url_filter = argv[argv.index("--filter-url") + 1]
    if "--exclude-domain" in argv and argv.index("--exclude-domain") + 1 < len(argv):
        exclude_domain = argv[argv.index("--exclude-domain") + 1]
    if "--status" in argv and argv.index("--status") + 1 < len(argv):
        status_filter = argv[argv.index("--status") + 1]
    return method_filter, url_filter, exclude_domain, status_filter


def _has_capture_filters(
    method_filter: str = "",
    url_filter: str = "",
    exclude_domain: str = "",
    status_filter: str = "",
) -> bool:
    """判断当前是否存在抓包过滤条件。"""
    return any([method_filter, url_filter, exclude_domain, status_filter])


def _parse_capture_stop_options(
    argv: list[str],
    *,
    default_wait_ms: int = 0,
    default_body_mode: str = "none",
    include_url_flag: bool = True,
) -> dict[str, Any]:
    """统一解析 stop/discover/capture-action 的抓包控制参数。"""
    method_filter, url_filter, exclude_domain, status_filter = _parse_capture_filters(
        argv,
        include_url_flag=include_url_flag,
    )
    body_mode = _cli_option_value(argv, "--body-mode", default_body_mode).strip().lower()
    if "--no-body" in argv:
        body_mode = "none"
    if body_mode not in {"none", "filtered", "all"}:
        body_mode = default_body_mode
    wait_ms = _cli_flag_int(argv, "--wait-ms", default_wait_ms)
    idle_ms = max(0, _cli_flag_int(argv, "--idle-ms", 0))
    max_bodies = max(0, _cli_flag_int(argv, "--max-bodies", 0))
    max_body_bytes = max(0, _cli_flag_int(argv, "--max-body-bytes", 0))
    until_match = _cli_option_value(argv, "--until-match", "")
    return {
        "method_filter": method_filter,
        "url_filter": url_filter,
        "exclude_domain": exclude_domain,
        "status_filter": status_filter,
        "body_mode": body_mode,
        "get_body": body_mode != "none",
        "wait_ms": wait_ms,
        "idle_ms": idle_ms,
        "max_bodies": max_bodies,
        "max_body_bytes": max_body_bytes,
        "until_match": until_match,
    }


def _with_target(argv: list[str], target: str) -> list[str]:
    if not target or "--target" in argv:
        return argv
    targetable = {
        "snapshot", "click", "click-text", "find-text", "find-icon", "click-icon",
        "scan-tooltips", "editor-get", "editor-set", "editor-type", "fill", "select",
        "check", "press", "hover", "scroll", "drag", "wait", "get-text", "get-url",
        "get-title", "screenshot", "diagnose-page", "network", "reload", "navigate",
        "eval-js",
    }
    if argv and argv[0] in targetable:
        return [*argv, "--target", target]
    return argv


def _cli_target(argv: list[str], default: str = "active") -> str:
    """从 argv 解析 --target。"""
    if "--target" in argv and argv.index("--target") + 1 < len(argv):
        return argv[argv.index("--target") + 1]
    return default


def _cli_flag_int(argv: list[str], flag: str, default: int = 0) -> int:
    """从 argv 解析整数 flag。"""
    if flag in argv and argv.index(flag) + 1 < len(argv):
        try:
            return int(argv[argv.index(flag) + 1])
        except ValueError:
            return default
    return default


def _cli_option_value(argv: list[str], flag: str, default: str = "") -> str:
    """从 argv 解析单值 flag。"""
    if flag in argv and argv.index(flag) + 1 < len(argv):
        return argv[argv.index(flag) + 1]
    return default


def _cli_option_values(argv: list[str], flag: str) -> list[str]:
    """从 argv 解析可重复出现的 flag 值。"""
    values: list[str] = []
    idx = 0
    while idx < len(argv):
        if argv[idx] == flag and idx + 1 < len(argv):
            values.append(argv[idx + 1])
            idx += 2
            continue
        idx += 1
    return values


_SENSITIVE_HEADER_KEYS = frozenset({
    "cookie", "authorization", "x-token", "x-api-key", "ops_user_token",
})


def _redact_header_value(key: str, value: Any) -> str:
    """脱敏 header 值，仅保留结构供模板复用。"""
    k = str(key).lower()
    text = str(value or "")
    if k in _SENSITIVE_HEADER_KEYS or "token" in k or "auth" in k:
        if len(text) <= 8:
            return "<redacted>"
        return f"{text[:4]}...{text[-4:]}"
    return text


def _auth_template_from_requests(
    requests: list[dict[str, Any]],
    domain: str,
) -> dict[str, Any]:
    """从抓包记录提取认证 header 模板（值已脱敏）。"""
    domain_key = domain.strip().lower()
    picked: dict[str, Any] | None = None
    for req in requests:
        url = str(req.get("url", "")).lower()
        if domain_key and domain_key not in url:
            continue
        if not str(req.get("url", "")).startswith("http"):
            continue
        picked = req
        if "/api/" in url:
            break
    if not picked:
        return {"ok": False, "error": f"未找到域名 {domain} 的 API 请求，请先抓包"}

    headers = picked.get("headers") or {}
    template = {
        str(k): _redact_header_value(str(k), v)
        for k, v in headers.items()
        if str(k).lower() not in {"content-length", "host", "connection", "accept-encoding"}
    }
    cookie_keys = [
        part.split("=", 1)[0].strip()
        for part in str(headers.get("Cookie") or headers.get("cookie") or "").split(";")
        if "=" in part
    ]
    return {
        "ok": True,
        "domain": domain,
        "sample_url": picked.get("url"),
        "sample_method": picked.get("method"),
        "headers": template,
        "cookie_keys": cookie_keys,
        "note": "业务 CLI 请用 cdp_client.get_cookies(url)，勿复制脱敏后的 authorization",
    }


def _run_discover_api(args: list[str]) -> int:
    """SPA 接口发现：抓包 + reload/navigate + 等待 + 过滤 + 摘要。"""
    if not _daemon_is_running():
        print("CDP daemon not running", file=sys.stderr)
        return 1

    target = _cli_target(args, "active")
    page_url = _cli_option_value(args, "--url", "")
    no_nav = "--no-nav" in args
    fast = "--fast" in args
    export_client = "--export-python-client" in args
    actions = _cli_option_values(args, "--do")
    discover_opts = _parse_capture_stop_options(
        args,
        default_wait_ms=6000,
        default_body_mode="none",
        include_url_flag=False,
    )
    if fast:
        if "--wait-ms" not in args:
            discover_opts["wait_ms"] = 0
        if "--idle-ms" not in args:
            discover_opts["idle_ms"] = 800
        if "--body-mode" not in args and "--no-body" not in args:
            if discover_opts["url_filter"] or discover_opts["until_match"]:
                discover_opts["body_mode"] = "filtered"
                discover_opts["get_body"] = True
            else:
                discover_opts["body_mode"] = "none"
                discover_opts["get_body"] = False
    elif "--body-mode" not in args and "--no-body" not in args:
        if discover_opts["url_filter"] or discover_opts["until_match"]:
            discover_opts["body_mode"] = "filtered"
            discover_opts["get_body"] = True
        else:
            discover_opts["body_mode"] = "none"
            discover_opts["get_body"] = False

    start = _send({"action": "network_capture_start", "target": target}, timeout=15)
    if not start.get("ok"):
        print(f"Error: {start.get('error', '?')}", file=sys.stderr)
        return 1

    final_rc = 0
    try:
        if not no_nav:
            if page_url:
                nav = _send(
                    {"action": "navigate_page", "target": target, "url": page_url},
                    timeout=30,
                )
            else:
                nav = _send({"action": "reload_page", "target": target}, timeout=30)
            if not nav.get("ok"):
                print(f"Error: navigate failed: {nav.get('error', '?')}", file=sys.stderr)
                return 1

        for raw in actions:
            argv = _with_target(shlex.split(raw), target)
            rc, out, err = _run_cli_argv(argv)
            print("$ " + " ".join(shlex.quote(x) for x in argv))
            if out:
                print(out, end="" if out.endswith("\n") else "\n")
            if err:
                print(err, end="" if err.endswith("\n") else "\n", file=sys.stderr)
            if rc != 0 and final_rc == 0:
                final_rc = rc
    finally:
        stop = _send(
            {
                "action": "network_capture_stop",
                "target": target,
                "get_body": discover_opts["get_body"],
                "body_mode": discover_opts["body_mode"],
                "wait_ms": discover_opts["wait_ms"],
                "idle_ms": discover_opts["idle_ms"],
                "max_bodies": discover_opts["max_bodies"],
                "max_body_bytes": discover_opts["max_body_bytes"],
                "method_filter": discover_opts["method_filter"],
                "url_filter": discover_opts["url_filter"],
                "exclude_domain": discover_opts["exclude_domain"],
                "status_filter": discover_opts["status_filter"],
                "until_match": discover_opts["until_match"],
            },
            timeout=max(
                120,
                discover_opts["wait_ms"] // 1000 + discover_opts["idle_ms"] // 1000 + 60,
            ),
        )
    if not stop.get("ok"):
        print(f"Error: {stop.get('error', '?')}", file=sys.stderr)
        return 1

    requests: list[dict[str, Any]] = stop.get("requests", [])
    filtered = _filter_requests(
        requests,
        method_filter=discover_opts["method_filter"],
        url_filter=discover_opts["url_filter"],
        exclude_domain=discover_opts["exclude_domain"],
        status_filter=discover_opts["status_filter"],
    )
    _save_capture_requests(requests, filtered=False)
    save_path = _save_capture_requests(filtered, filtered=True) if filtered != requests else _capture_path(False)
    body_fetch = stop.get("body_fetch") or {}

    print(f"Captured {len(requests)} API request(s), kept {len(filtered)} after filter")
    print(f"Saved to: {save_path}")
    if body_fetch:
        print(
            "Body fetch: "
            f"mode={body_fetch.get('mode')} "
            f"selected={body_fetch.get('selected', 0)} "
            f"fetched={body_fetch.get('fetched', 0)} "
            f"skipped={body_fetch.get('skipped_unmatched', 0) + body_fetch.get('skipped_limit', 0) + body_fetch.get('skipped_too_large', 0) + body_fetch.get('skipped_error', 0)}"
        )
    summary = _summarize_requests(filtered or requests)
    if summary.get("requests"):
        print("\n--- summary (first 8) ---")
        for item in summary["requests"][:8]:
            print(
                f"  {item.get('method')} {item.get('path_or_url')} "
                f"keys={item.get('request_body_keys', [])}"
            )
    print("\nNext:")
    print(f"  {sys.argv[0]} network-capture export --python-client")
    if export_client:
        import tempfile

        from page_manager import PageManager

        code = PageManager._export_python_client(
            filtered or requests,
            daemon_script=os.path.abspath(sys.argv[0]),
        )
        out = Path(tempfile.gettempdir()) / "cdp_discover_api_client.py"
        out.write_text(code, encoding="utf-8")
        print(f"Exported client: {out}")
    return final_rc


def _run_capture_action(args: list[str]) -> int:
    if not _daemon_is_running():
        print("CDP daemon not running", file=sys.stderr)
        return 1

    target = "active"
    actions: list[str] = []
    follow = "--follow" in args
    bail = "--no-bail" not in args
    output_json = "--json" in args
    print_export = "--print-export" in args
    export_client = "--export-python-client" in args
    export_curl = "--export-curl" in args
    stop_opts = _parse_capture_stop_options(
        args,
        default_wait_ms=0,
        default_body_mode="none",
    )
    if "--body-mode" not in args and "--no-body" not in args:
        if stop_opts["url_filter"] or stop_opts["until_match"]:
            stop_opts["body_mode"] = "filtered"
            stop_opts["get_body"] = True
        else:
            stop_opts["body_mode"] = "none"
            stop_opts["get_body"] = False

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--target" and i + 1 < len(args):
            i += 1
            target = args[i]
        elif arg == "--do" and i + 1 < len(args):
            i += 1
            actions.append(args[i])
        i += 1

    if not actions:
        print('Usage: capture-action --do "editor-set aaa" --do "press Meta+S" [--target <t>]', file=sys.stderr)
        return 1

    start_req: dict[str, Any] = {"action": "network_capture_start", "target": target, "follow": follow}
    start = _send(start_req, timeout=15)
    if not start.get("ok"):
        print(f"Error: capture start failed: {start.get('error', '?')}", file=sys.stderr)
        return 1

    action_results = []
    final_rc = 0
    try:
        for raw in actions:
            argv = _with_target(shlex.split(raw), target)
            rc, out, err = _run_cli_argv(argv)
            action_results.append({"command": argv, "code": rc, "stdout": out, "stderr": err})
            if rc != 0:
                final_rc = rc
                if bail:
                    break
    finally:
        stop_req: dict[str, Any] = {
            "action": "network_capture_stop",
            "target": target,
            "get_body": stop_opts["get_body"],
            "body_mode": stop_opts["body_mode"],
            "wait_ms": stop_opts["wait_ms"],
            "idle_ms": stop_opts["idle_ms"],
            "max_bodies": stop_opts["max_bodies"],
            "max_body_bytes": stop_opts["max_body_bytes"],
            "method_filter": stop_opts["method_filter"],
            "url_filter": stop_opts["url_filter"],
            "exclude_domain": stop_opts["exclude_domain"],
            "status_filter": stop_opts["status_filter"],
            "until_match": stop_opts["until_match"],
        }
        stop = _send(
            stop_req,
            timeout=max(
                120,
                stop_opts["wait_ms"] // 1000 + stop_opts["idle_ms"] // 1000 + 90,
            ),
        )

    if not stop.get("ok"):
        print(f"Error: capture stop failed: {stop.get('error', '?')}", file=sys.stderr)
        return 1

    requests: list[dict[str, Any]] = stop.get("requests", [])
    method_filter, url_filter, exclude_domain, status_filter = (
        stop_opts["method_filter"],
        stop_opts["url_filter"],
        stop_opts["exclude_domain"],
        stop_opts["status_filter"],
    )
    filtered = _filter_requests(requests, method_filter, url_filter, exclude_domain, status_filter)
    _save_capture_requests(requests, filtered=False)
    save_path = _save_capture_requests(filtered, filtered=True) if filtered != requests else _capture_path(False)

    result = {
        "ok": final_rc == 0,
        "target": target,
        "actions": action_results,
        "captured": len(requests),
        "filtered": len(filtered) if (method_filter or url_filter or exclude_domain or status_filter) else len(requests),
        "capture_file": str(save_path),
        "summary": _summarize_requests(filtered or requests),
        "crud": _infer_crud(filtered or requests),
        "body_diff": _diff_body(filtered or requests),
        "body_fetch": stop.get("body_fetch") or {},
    }

    if export_client or export_curl:
        import tempfile
        from page_manager import PageManager
        if export_client:
            code = PageManager._export_python_client(filtered or requests, daemon_script=os.path.abspath(sys.argv[0]))
            ext = "py"
        else:
            code = PageManager._export_curl(filtered or requests)
            ext = "sh"
        out = Path(tempfile.gettempdir()) / f"cdp_capture_action.{ext}"
        out.write_text(code, encoding="utf-8")
        result["export_file"] = str(out)
        if print_export:
            result["export_code"] = code

    if output_json:
        _json_output(result, sys.argv)
    else:
        for item in action_results:
            print("$ " + " ".join(shlex.quote(x) for x in item["command"]))
            if item["stdout"]:
                print(item["stdout"], end="" if item["stdout"].endswith("\n") else "\n")
            if item["stderr"]:
                print(item["stderr"], end="" if item["stderr"].endswith("\n") else "\n", file=sys.stderr)
        print(f"\n--- capture-action: {len(requests)} captured, saved to {save_path} ---")
        if result["body_fetch"]:
            body_fetch = result["body_fetch"]
            print(
                "Body fetch: "
                f"mode={body_fetch.get('mode')} "
                f"selected={body_fetch.get('selected', 0)} "
                f"fetched={body_fetch.get('fetched', 0)}"
            )
        _print_capture_table(filtered or requests)
        crud = result["crud"]
        if crud.get("warnings"):
            print("\nWarnings:")
            for warning in crud["warnings"]:
                print(f"  - {warning}")
        if result.get("export_file"):
            print(f"\nExport saved to: {result['export_file']}")
    return final_rc


def _capture_flow_dir() -> Path:
    """返回 capture-flow 会话目录。"""
    return DAEMON_BASE_DIR / "capture-flows"


def _capture_flow_path(session_id: str) -> Path:
    """返回指定 capture-flow 会话文件路径。"""
    return _capture_flow_dir() / f"{session_id}.json"


def _load_capture_flow_session(session_id: str) -> dict[str, Any]:
    """加载 capture-flow 会话文件。"""
    path = _capture_flow_path(session_id)
    if not path.exists():
        raise FileNotFoundError(f"capture-flow 会话不存在: {session_id}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"capture-flow 会话格式无效: {path}")
    return data


def _save_capture_flow_session(session: dict[str, Any]) -> Path:
    """保存 capture-flow 会话文件。"""
    session["updated_at"] = time.time()
    path = _capture_flow_path(str(session.get("session_id", "")))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(session, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _capture_flow_request_profile(req: dict[str, Any]) -> dict[str, Any]:
    """为单条请求生成用于分组和判定的稳定画像。"""
    method = str(req.get("method", "")).upper()
    url_text = str(req.get("url", ""))
    parsed = urllib.parse.urlparse(url_text)
    path = parsed.path or _url_key(url_text)
    query_map = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    query_keys = sorted(query_map.keys())
    value_sensitive_keys = {
        "get", "metric", "metrics", "name", "type", "tab",
        "id", "jobid", "vertexid", "subtask", "operator",
    }
    query_parts: list[str] = []
    for key in query_keys:
        values = query_map.get(key) or [""]
        if key.lower() in value_sensitive_keys:
            normalized = ",".join(values[:3])
        elif len(values) == 1 and values[0].isdigit():
            normalized = "{num}"
        else:
            normalized = "{value}"
        query_parts.append(f"{key}={normalized}")
    query_signature = "&".join(query_parts)
    path_key = f"{method} {path}"
    group_key = path_key if not query_signature else f"{path_key}?{query_signature}"
    return {
        "method": method,
        "url": url_text,
        "path": path,
        "path_key": path_key,
        "query_keys": query_keys,
        "query_signature": query_signature,
        "group_key": group_key,
    }


def _capture_flow_build_baseline_summary(requests: list[dict[str, Any]]) -> dict[str, Any]:
    """构建 capture-flow 使用的 baseline 摘要。"""
    summary = _capture_guide_build_baseline_summary(requests)
    group_counts: dict[str, int] = {}
    for req in requests:
        profile = _capture_flow_request_profile(req)
        group_counts[profile["group_key"]] = group_counts.get(profile["group_key"], 0) + 1
    summary["group_keys"] = group_counts
    return summary


def _capture_flow_reason_text(reason: str, path: str) -> str:
    """把候选分组判定原因转成可读文本。"""
    if reason == "new_path":
        return f"全流程首次出现 {path}"
    if reason == "new_query_shape":
        return f"{path} 出现新的查询参数模式"
    if reason == "new_query_value":
        return f"{path} 出现新的查询参数取值"
    return f"{path} 在流程中出现新的请求模式"


def _capture_flow_candidate_groups(
    requests: list[dict[str, Any]],
    baseline_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    """基于 baseline 识别全流程中的新增候选请求分组。"""
    baseline_paths = set((baseline_summary.get("request_keys") or {}).keys())
    baseline_groups = set((baseline_summary.get("group_keys") or {}).keys())
    candidates: dict[str, dict[str, Any]] = {}
    for index, req in enumerate(requests, 1):
        profile = _capture_flow_request_profile(req)
        reason = ""
        if profile["group_key"] in baseline_groups:
            continue
        if profile["path_key"] not in baseline_paths:
            reason = "new_path"
        else:
            if profile["query_signature"]:
                reason = "new_query_shape"
            else:
                reason = "new_query_value"
        entry = candidates.setdefault(
            profile["group_key"],
            {
                "group_key": profile["group_key"],
                "method": profile["method"],
                "path": profile["path"],
                "query_keys": profile["query_keys"],
                "query_signature": profile["query_signature"],
                "sample_url": profile["url"],
                "count": 0,
                "request_indexes": [],
                "reason": reason,
                "reason_text": _capture_flow_reason_text(reason, profile["path"]),
                "is_polling_candidate": False,
            },
        )
        entry["count"] += 1
        entry["request_indexes"].append(index)

    groups = list(candidates.values())
    for entry in groups:
        entry["is_polling_candidate"] = bool(
            entry["count"] >= 3
            and entry["method"] == "GET"
            and entry["query_keys"]
        )
    reason_rank = {"new_path": 0, "new_query_shape": 1, "new_query_value": 2}
    groups.sort(
        key=lambda item: (
            reason_rank.get(str(item.get("reason", "")), 99),
            -int(item.get("count", 0)),
            str(item.get("path", "")),
            str(item.get("query_signature", "")),
        )
    )
    for group_index, entry in enumerate(groups, 1):
        entry["index"] = group_index
    return groups


def _capture_flow_make_phase_key(seed: str, fallback_index: int) -> str:
    """把阶段描述压成稳定 key。"""
    safe = "".join(
        ch.lower() if ch.isalnum() else "_"
        for ch in str(seed or "").strip()
    )
    safe = "_".join(part for part in safe.split("_") if part)
    return safe[:48] or f"phase_{fallback_index}"


def _capture_flow_recommend_phases(
    candidate_groups: list[dict[str, Any]],
    goal: str = "",
) -> list[dict[str, Any]]:
    """根据候选请求分组生成推荐阶段。"""
    phases: list[dict[str, Any]] = []
    used_groups: set[int] = set()
    by_path: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for group in candidate_groups:
        by_path.setdefault((str(group.get("method", "")), str(group.get("path", ""))), []).append(group)

    for (_, path), groups in by_path.items():
        plain_groups = [group for group in groups if not group.get("query_keys")]
        query_groups = [group for group in groups if group.get("query_keys")]
        has_get_group = any("get" in (group.get("query_keys") or []) for group in query_groups)
        if path.endswith("/metrics") and plain_groups and has_get_group:
            plain = plain_groups[0]
            phases.append({
                "key": "open_metric_panel",
                "text": "打开 metrics 面板并展开下拉框",
                "reason": plain.get("reason_text"),
                "suspected_requests": [f"{plain.get('method')} {plain.get('path')}"],
            })
            used_groups.add(int(plain.get("index", 0)))
            metric_group = next(group for group in query_groups if "get" in (group.get("query_keys") or []))
            phases.append({
                "key": "select_metric_value",
                "text": "选择具体 metric 并等待数值刷新",
                "reason": metric_group.get("reason_text"),
                "suspected_requests": [
                    f"{metric_group.get('method')} {metric_group.get('path')}"
                    + (f"?{metric_group.get('query_signature')}" if metric_group.get("query_signature") else "")
                ],
            })
            used_groups.add(int(metric_group.get("index", 0)))

    for fallback_index, group in enumerate(candidate_groups, 1):
        group_index = int(group.get("index", 0))
        if group_index in used_groups:
            continue
        text = f"执行触发 {group.get('path')} 的页面操作"
        if group.get("is_polling_candidate"):
            text = f"完成触发 {group.get('path')} 的操作并等待页面刷新"
        elif str(group.get("method", "")).upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            text = f"执行与 {group.get('path')} 对应的提交或确认操作"
        phases.append({
            "key": _capture_flow_make_phase_key(str(group.get("path", "")), fallback_index),
            "text": text,
            "reason": group.get("reason_text"),
            "suspected_requests": [
                f"{group.get('method')} {group.get('path')}"
                + (f"?{group.get('query_signature')}" if group.get("query_signature") else "")
            ],
        })
        if len(phases) >= 4:
            break

    return phases[:4]


def _capture_flow_clarity_status(
    candidate_groups: list[dict[str, Any]],
    total_requests: int,
) -> str:
    """根据候选分组数量与强度判断链路是否已足够清晰。"""
    if not candidate_groups:
        return "clear_no_network"
    strong_groups = [
        group for group in candidate_groups
        if int(group.get("count", 0)) >= 2
        or str(group.get("reason", "")) in {"new_path", "new_query_shape"}
    ]
    if 1 <= len(strong_groups) <= 3:
        return "clear"
    if len(candidate_groups) <= 3 and total_requests <= 20:
        return "clear"
    return "unclear"


def _capture_flow_build_analysis(
    requests: list[dict[str, Any]],
    baseline_summary: dict[str, Any],
    goal: str = "",
) -> dict[str, Any]:
    """构建 capture-flow 的整段抓包分析结果。"""
    candidate_groups = _capture_flow_candidate_groups(requests, baseline_summary)
    clarity_status = _capture_flow_clarity_status(candidate_groups, len(requests))
    candidate_requests = [
        {
            "index": int(group.get("index", 0)),
            "method": group.get("method"),
            "path": group.get("path"),
            "query_keys": list(group.get("query_keys") or []),
            "query_signature": group.get("query_signature", ""),
            "sample_url": group.get("sample_url", ""),
            "count": int(group.get("count", 0)),
            "reason": group.get("reason"),
            "reason_text": group.get("reason_text"),
            "is_polling_candidate": bool(group.get("is_polling_candidate")),
        }
        for group in candidate_groups[:8]
    ]
    candidate_group_payload = [
        {
            "index": int(group.get("index", 0)),
            "group_key": group.get("group_key"),
            "method": group.get("method"),
            "path": group.get("path"),
            "query_keys": list(group.get("query_keys") or []),
            "query_signature": group.get("query_signature", ""),
            "sample_url": group.get("sample_url", ""),
            "count": int(group.get("count", 0)),
            "request_indexes": list(group.get("request_indexes") or []),
            "reason": group.get("reason"),
            "reason_text": group.get("reason_text"),
            "is_polling_candidate": bool(group.get("is_polling_candidate")),
        }
        for group in candidate_groups
    ]
    recommended_phases = []
    recommended_next_action = "inspect_candidate_requests"
    if clarity_status == "unclear":
        recommended_phases = _capture_flow_recommend_phases(candidate_groups, goal=goal)
        recommended_next_action = "fallback_to_capture_guide"
    elif clarity_status == "clear_no_network":
        recommended_next_action = "likely_frontend_only"
    noise_summary = {
        "total_request_count": len(requests),
        "candidate_group_count": len(candidate_groups),
        "candidate_request_count": sum(int(group.get("count", 0)) for group in candidate_groups),
        "baseline_request_count": max(0, len(requests) - sum(int(group.get("count", 0)) for group in candidate_groups)),
    }
    return {
        "clarity_status": clarity_status,
        "candidate_requests": candidate_requests,
        "candidate_groups": candidate_group_payload,
        "noise_summary": noise_summary,
        "recommended_phases": recommended_phases,
        "recommended_next_action": recommended_next_action,
    }


def _capture_flow_result(session: dict[str, Any]) -> dict[str, Any]:
    """构建 capture-flow 对外返回结构。"""
    result = {
        "ok": True,
        "session_id": session.get("session_id"),
        "status": session.get("status"),
        "goal": session.get("goal", ""),
        "target": session.get("target", ""),
        "baseline_summary": session.get("baseline_summary") or {},
        "capture_file": session.get("capture_file", ""),
        "filtered_capture_file": session.get("filtered_capture_file", ""),
        "analysis_file": session.get("analysis_file", ""),
        "analysis_source": session.get("analysis_source", ""),
        "summary": session.get("summary"),
        "crud": session.get("crud"),
        "body_diff": session.get("body_diff"),
        "clarity_status": session.get("clarity_status", ""),
        "candidate_requests": session.get("candidate_requests") or [],
        "candidate_groups": session.get("candidate_groups") or [],
        "noise_summary": session.get("noise_summary") or {},
        "recommended_phases": session.get("recommended_phases") or [],
        "recommended_next_action": session.get("recommended_next_action", ""),
    }
    return result


def _capture_flow_finalize(session: dict[str, Any]) -> dict[str, Any]:
    """停止 capture-flow 抓包并写入最终分析结果。"""
    stop_opts = dict(session.get("stop_options") or {})
    timeout_sec = max(
        120,
        int(stop_opts.get("wait_ms") or 0) // 1000
        + int(stop_opts.get("idle_ms") or 0) // 1000
        + 90,
    )
    stop_req = {
        "action": "network_capture_stop",
        "target": session.get("target", "active"),
        "get_body": stop_opts.get("get_body", True),
        "body_mode": stop_opts.get("body_mode", "filtered"),
        "wait_ms": stop_opts.get("wait_ms", 0),
        "idle_ms": stop_opts.get("idle_ms", 0),
        "max_bodies": stop_opts.get("max_bodies", 0),
        "max_body_bytes": stop_opts.get("max_body_bytes", 0),
        "method_filter": stop_opts.get("method_filter", ""),
        "url_filter": stop_opts.get("url_filter", ""),
        "exclude_domain": stop_opts.get("exclude_domain", ""),
        "status_filter": stop_opts.get("status_filter", ""),
        "until_match": stop_opts.get("until_match", ""),
    }
    resp = _send(stop_req, timeout=timeout_sec)
    if not resp.get("ok"):
        raise RuntimeError(str(resp.get("error", "capture-flow stop failed")))

    requests: list[dict[str, Any]] = resp.get("requests", [])
    filtered = _filter_requests(
        requests,
        method_filter=str(stop_opts.get("method_filter") or ""),
        url_filter=str(stop_opts.get("url_filter") or ""),
        exclude_domain=str(stop_opts.get("exclude_domain") or ""),
        status_filter=str(stop_opts.get("status_filter") or ""),
    )
    full_path = _save_capture_requests(requests, filtered=False)
    filtered_path = _save_capture_requests(filtered, filtered=True)
    use_filtered = _has_capture_filters(
        str(stop_opts.get("method_filter") or ""),
        str(stop_opts.get("url_filter") or ""),
        str(stop_opts.get("exclude_domain") or ""),
        str(stop_opts.get("status_filter") or ""),
    ) and bool(filtered)
    analysis_requests = filtered if use_filtered else requests
    analysis_file = filtered_path if use_filtered else full_path

    session["capture_file"] = str(full_path)
    session["filtered_capture_file"] = str(filtered_path)
    session["analysis_file"] = str(analysis_file)
    session["analysis_source"] = "filtered" if use_filtered else "full"
    session["summary"] = _summarize_requests(analysis_requests)
    session["crud"] = _infer_crud(analysis_requests)
    session["body_diff"] = _diff_body(analysis_requests)
    session["body_fetch"] = resp.get("body_fetch") or {}
    analysis = _capture_flow_build_analysis(
        analysis_requests,
        baseline_summary=dict(session.get("baseline_summary") or {}),
        goal=str(session.get("goal", "")),
    )
    session.update(analysis)
    session["status"] = "completed"
    return _capture_flow_result(session)


def _run_capture_flow(args: list[str]) -> int:
    """执行 capture-flow：先整段监听，再按需推荐阶段化回退。"""
    subcmd = args[0] if args else ""
    output_json = "--json" in args
    session_id = _cli_option_value(args, "--session", "")
    known_subcmds = {"start", "stop", "status", "analyze", "export", "abort"}
    if subcmd not in known_subcmds:
        print(
            "Usage: capture-flow <start|stop|status|analyze|export|abort>\n"
            "  start  [--goal \"文本\"] [--target <t>] [--json]\n"
            "  stop   --session <id> [--json]\n"
            "  status --session <id> [--json]\n"
            "  analyze --session <id> [--json]\n"
            "  export --session <id> [--candidate-group N] [--python-client|--curl] [--json]\n"
            "  abort  --session <id> [--json]\n",
            file=sys.stderr,
        )
        return 1

    daemon_required = {"start", "stop"}
    if subcmd in daemon_required and not _daemon_is_running():
        print("CDP daemon not running", file=sys.stderr)
        return 1

    try:
        if subcmd == "start":
            goal = _cli_option_value(args, "--goal", "")
            target = _cli_option_value(args, "--target", "active") or "active"
            stop_opts = _parse_capture_stop_options(
                args,
                default_wait_ms=0,
                default_body_mode="filtered",
            )
            if "--body-mode" not in args and "--no-body" not in args:
                stop_opts["body_mode"] = "filtered"
                stop_opts["get_body"] = True
            if "--idle-ms" not in args:
                stop_opts["idle_ms"] = 800
            if "--max-bodies" not in args:
                stop_opts["max_bodies"] = 6
            baseline_ms = _cli_flag_int(args, "--baseline-ms", 1500)

            start_resp = _send({"action": "network_capture_start", "target": target}, timeout=15)
            if not start_resp.get("ok"):
                print(f"Error: {start_resp.get('error', '?')}", file=sys.stderr)
                return 1
            if baseline_ms > 0:
                time.sleep(baseline_ms / 1000.0)
            baseline_snapshot = _capture_guide_peek(target)
            baseline_requests = list(baseline_snapshot.get("requests") or [])
            baseline_summary = _capture_flow_build_baseline_summary(baseline_requests)
            session = {
                "session_id": uuid.uuid4().hex[:12],
                "status": "capturing",
                "goal": goal,
                "target": target,
                "created_at": time.time(),
                "updated_at": time.time(),
                "capture_started": True,
                "baseline_ms": baseline_ms,
                "stop_options": stop_opts,
                "baseline_summary": baseline_summary,
                "capture_file": "",
                "filtered_capture_file": "",
                "analysis_file": "",
                "analysis_source": "",
                "summary": None,
                "crud": None,
                "body_diff": None,
                "body_fetch": {},
                "clarity_status": "",
                "candidate_requests": [],
                "candidate_groups": [],
                "noise_summary": {},
                "recommended_phases": [],
                "recommended_next_action": "",
            }
            _save_capture_flow_session(session)
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "status": session.get("status"),
                "goal": session.get("goal"),
                "target": session.get("target"),
                "baseline_summary": session.get("baseline_summary"),
                "message": "请完成整段操作流，完成后调用 capture-flow stop。",
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if not session_id:
            print("Usage: capture-flow <stop|status|analyze|export|abort> --session <id>", file=sys.stderr)
            return 1

        session = _load_capture_flow_session(session_id)

        if subcmd == "stop":
            if str(session.get("status")) == "completed":
                result = _capture_flow_result(session)
            else:
                result = _capture_flow_finalize(session)
                _save_capture_flow_session(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if subcmd == "status":
            result = _capture_flow_result(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if subcmd == "analyze":
            analysis_file = str(session.get("analysis_file") or session.get("capture_file") or "")
            if not analysis_file:
                print("Error: 当前会话尚无可分析的抓包文件，请先 stop 或 abort", file=sys.stderr)
                return 1
            requests = _load_capture_requests(file_path=analysis_file)
            analysis = _capture_flow_build_analysis(
                requests,
                baseline_summary=dict(session.get("baseline_summary") or {}),
                goal=str(session.get("goal", "")),
            )
            session.update(analysis)
            if not session.get("summary"):
                session["summary"] = _summarize_requests(requests)
            if not session.get("crud"):
                session["crud"] = _infer_crud(requests)
            if not session.get("body_diff"):
                session["body_diff"] = _diff_body(requests)
            _save_capture_flow_session(session)
            result = _capture_flow_result(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if subcmd == "export":
            analysis_file = str(session.get("analysis_file") or session.get("capture_file") or "")
            if not analysis_file:
                print("Error: 当前会话尚无可导出的抓包文件，请先 stop 或 abort", file=sys.stderr)
                return 1
            requests = _load_capture_requests(file_path=analysis_file)
            export_requests = list(requests)
            group_arg = _cli_option_value(args, "--candidate-group", "")
            if group_arg:
                try:
                    group_index = int(group_arg)
                except ValueError:
                    print(f"Error: 无效 --candidate-group: {group_arg}", file=sys.stderr)
                    return 1
                matched = next(
                    (
                        group for group in (session.get("candidate_groups") or [])
                        if int(group.get("index", 0)) == group_index
                    ),
                    None,
                )
                if matched is None:
                    print(f"Error: 未找到候选分组 {group_index}", file=sys.stderr)
                    return 1
                indexes = [
                    int(index)
                    for index in (matched.get("request_indexes") or [])
                    if isinstance(index, int) or str(index).isdigit()
                ]
                export_requests = [
                    requests[index - 1]
                    for index in indexes
                    if 1 <= index <= len(requests)
                ]
            elif session.get("candidate_groups"):
                picked_indexes: list[int] = []
                for group in session.get("candidate_groups") or []:
                    for index in group.get("request_indexes") or []:
                        try:
                            number = int(index)
                        except (TypeError, ValueError):
                            continue
                        if number not in picked_indexes:
                            picked_indexes.append(number)
                export_requests = [
                    requests[index - 1]
                    for index in picked_indexes
                    if 1 <= index <= len(requests)
                ]

            from page_manager import PageManager
            if "--python-client" in args:
                code = PageManager._export_python_client(export_requests, daemon_script=os.path.abspath(sys.argv[0]))
                ext = "py"
                fmt = "python-client"
            elif "--curl" in args:
                code = PageManager._export_curl(export_requests)
                ext = "sh"
                fmt = "curl"
            else:
                code = PageManager._export_python(export_requests)
                ext = "py"
                fmt = "python"
            import tempfile
            out = Path(tempfile.gettempdir()) / f"cdp_capture_flow_export.{ext}"
            out.write_text(code, encoding="utf-8")
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "format": fmt,
                "request_count": len(export_requests),
                "export_file": str(out),
                "code": code if output_json else "",
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(code)
                print(f"\n# Saved to: {out}", file=sys.stderr)
            return 0

        if subcmd == "abort":
            if str(session.get("status")) not in {"completed", "aborted"} and session.get("capture_started"):
                if _daemon_is_running():
                    try:
                        _capture_flow_finalize(session)
                    except Exception as exc:
                        session["abort_error"] = str(exc)
                else:
                    session["abort_error"] = "daemon offline, skip stop"
            session["status"] = "aborted"
            _save_capture_flow_session(session)
            result = _capture_flow_result(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        raise RuntimeError(f"未知 capture-flow 子命令: {subcmd}")
    except Exception as exc:
        if session_id:
            try:
                failed_session = _load_capture_flow_session(session_id)
                if str(failed_session.get("status")) not in {"completed", "aborted"}:
                    failed_session["status"] = "failed"
                    failed_session["error"] = str(exc)
                    _save_capture_flow_session(failed_session)
            except Exception:
                pass
        print(f"Error: {exc}", file=sys.stderr)
        return 1


def _capture_guide_dir() -> Path:
    """返回 capture-guide 会话目录。"""
    return DAEMON_BASE_DIR / "capture-guides"


def _capture_guide_path(session_id: str) -> Path:
    """返回指定 capture-guide 会话文件路径。"""
    return _capture_guide_dir() / f"{session_id}.json"


def _load_capture_guide_session(session_id: str) -> dict[str, Any]:
    """加载 capture-guide 会话文件。"""
    path = _capture_guide_path(session_id)
    if not path.exists():
        raise FileNotFoundError(f"capture-guide 会话不存在: {session_id}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"capture-guide 会话格式无效: {path}")
    return data


def _save_capture_guide_session(session: dict[str, Any]) -> Path:
    """保存 capture-guide 会话文件。"""
    session["updated_at"] = time.time()
    path = _capture_guide_path(str(session.get("session_id", "")))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(session, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _capture_guide_compact_step(step: dict[str, Any]) -> dict[str, Any]:
    """提取对话/JSON 输出需要的步骤摘要字段。"""
    return {
        "index": int(step.get("index", 0)),
        "key": str(step.get("key", "")),
        "text": str(step.get("text", "")),
        "status": str(step.get("status", "")),
        "retry_count": int(step.get("retry_count", 0)),
        "capture_count_at_step_start": int(step.get("capture_count_at_step_start") or 0),
        "capture_count_at_ack": int(step.get("capture_count_at_ack") or 0),
        "capture_count_after_idle": int(step.get("capture_count_after_idle") or 0),
        "captured_indexes": list(step.get("captured_indexes") or []),
        "summary": step.get("summary"),
    }


def _capture_guide_active_step(session: dict[str, Any]) -> tuple[int, dict[str, Any]] | tuple[None, None]:
    """返回当前 active 步骤及其索引。"""
    steps = session.get("steps") or []
    for index, step in enumerate(steps):
        if str(step.get("status", "")) == "active":
            return index, step
    return None, None


def _capture_guide_prompt(step: dict[str, Any], total_steps: int) -> str:
    """生成下一步对话提示。"""
    index = int(step.get("index", 0))
    text = str(step.get("text", "")).strip()
    return f"请执行第 {index}/{total_steps} 步：{text}。完成后回复“已完成”，再由 agent 调用 capture-guide ack。"


def _capture_guide_step_key(req: dict[str, Any]) -> str:
    """生成用于 baseline 噪音识别的请求键。"""
    return f"{str(req.get('method', '')).upper()} {_url_key(str(req.get('url', '')))}"


def _capture_guide_build_baseline_summary(requests: list[dict[str, Any]]) -> dict[str, Any]:
    """构建 baseline 摘要，用于后续步骤降噪。"""
    counts: dict[str, int] = {}
    for req in requests:
        key = _capture_guide_step_key(req)
        counts[key] = counts.get(key, 0) + 1
    top = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    return {
        "count": len(requests),
        "request_keys": counts,
        "top_requests": [
            {"key": key, "count": count}
            for key, count in top
        ],
    }


def _capture_guide_mark_noise(
    requests: list[dict[str, Any]],
    baseline_keys: dict[str, int],
) -> list[dict[str, Any]]:
    """基于 baseline 键为请求打噪音标记。"""
    marked: list[dict[str, Any]] = []
    for req in requests:
        copied = dict(req)
        key = _capture_guide_step_key(req)
        copied["is_baseline_noise"] = key in baseline_keys
        copied["noise_reason"] = "baseline" if key in baseline_keys else ""
        marked.append(copied)
    return marked


def _capture_guide_step_summary(
    requests: list[dict[str, Any]],
    captured_indexes: list[int],
    baseline_keys: dict[str, int],
) -> dict[str, Any]:
    """构建步骤级抓包摘要。"""
    marked = _capture_guide_mark_noise(requests, baseline_keys)
    summary = _summarize_requests(marked)
    summary_items = summary.get("requests", [])
    important: list[dict[str, Any]] = []
    write_candidates: list[dict[str, Any]] = []
    for item, req in zip(summary_items, marked):
        item["is_baseline_noise"] = bool(req.get("is_baseline_noise"))
        item["noise_reason"] = str(req.get("noise_reason", ""))
        if not item["is_baseline_noise"]:
            important.append(item)
        if str(item.get("method", "")).upper() in {"POST", "PUT", "PATCH", "DELETE"}:
            write_candidates.append(item)

    notes: list[str] = []
    if not requests:
        notes.append("该步未捕获到新增 API 请求。")
    elif requests and not important:
        notes.append("该步仅捕获到 baseline 噪音请求，未发现明显新增业务请求。")
    if write_candidates:
        notes.append("该步包含写请求候选，建议优先检查 write_requests。")

    return {
        "count": len(requests),
        "request_indexes": captured_indexes,
        "requests": summary_items,
        "important_requests": important[:8],
        "write_requests": write_candidates[:8],
        "notes": notes,
    }


def _capture_guide_peek(target: str) -> dict[str, Any]:
    """读取当前抓包快照。"""
    resp = _send({"action": "network_capture_peek", "target": target}, timeout=30)
    if not resp.get("ok"):
        raise RuntimeError(str(resp.get("error", "capture peek failed")))
    return resp


def _capture_guide_wait_for_idle(target: str, idle_ms: int) -> dict[str, Any]:
    """等待网络进入空闲窗口，并返回最新抓包快照。"""
    if idle_ms <= 0:
        return _capture_guide_peek(target)

    idle_seconds = max(0.1, idle_ms / 1000.0)
    deadline = time.time() + max(3.0, idle_seconds * 8)
    latest = _capture_guide_peek(target)
    while True:
        last_event_at = float(latest.get("last_event_at") or 0.0)
        if last_event_at and time.time() - last_event_at >= idle_seconds:
            return latest
        if time.time() >= deadline:
            return latest
        time.sleep(min(0.2, idle_seconds / 2))
        latest = _capture_guide_peek(target)


def _capture_guide_parse_steps_payload(payload: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """解析 steps-file 中的步骤与默认配置。"""
    steps: list[dict[str, Any]] = []
    defaults: dict[str, Any] = {}
    if isinstance(payload, dict):
        defaults = {
            "target": payload.get("target", ""),
            "filter_url": payload.get("filter_url", ""),
            "exclude_domain": payload.get("exclude_domain", ""),
            "body_mode": payload.get("body_mode", ""),
            "idle_ms": payload.get("idle_ms"),
            "baseline_ms": payload.get("baseline_ms"),
            "max_bodies": payload.get("max_bodies"),
            "max_body_bytes": payload.get("max_body_bytes"),
        }
        raw_steps = payload.get("steps", [])
    elif isinstance(payload, list):
        raw_steps = payload
    else:
        raise RuntimeError("steps-file 仅支持对象或数组结构")

    for index, raw_step in enumerate(raw_steps, 1):
        if isinstance(raw_step, str):
            steps.append({
                "index": index,
                "key": f"step_{index}",
                "text": raw_step.strip(),
                "idle_ms": None,
                "expect": [],
            })
            continue
        if not isinstance(raw_step, dict):
            raise RuntimeError(f"steps-file 第 {index} 步格式无效")
        text = str(raw_step.get("text") or raw_step.get("prompt") or "").strip()
        if not text:
            raise RuntimeError(f"steps-file 第 {index} 步缺少 text")
        steps.append({
            "index": index,
            "key": str(raw_step.get("key") or f"step_{index}"),
            "text": text,
            "idle_ms": raw_step.get("idle_ms"),
            "expect": list(raw_step.get("expect") or []),
        })
    return steps, defaults


def _capture_guide_load_steps_file(file_path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """加载 JSON/YAML 步骤文件。"""
    path = Path(file_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"steps-file 不存在: {path}")
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix == ".json":
        payload = json.loads(text)
        return _capture_guide_parse_steps_payload(payload)
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"当前环境缺少 YAML 依赖，无法解析 {path.name}: {exc}") from exc
        payload = yaml.safe_load(text)
        return _capture_guide_parse_steps_payload(payload)
    raise RuntimeError(f"steps-file 仅支持 .json/.yaml/.yml: {path}")


def _capture_guide_effective_step_idle_ms(session: dict[str, Any], step: dict[str, Any]) -> int:
    """计算步骤实际使用的 idle_ms。"""
    step_idle = step.get("idle_ms")
    if step_idle is not None:
        try:
            return max(0, int(step_idle))
        except (TypeError, ValueError):
            return max(0, int(session.get("idle_ms") or 0))
    return max(0, int(session.get("idle_ms") or 0))


def _capture_guide_advance_to_next_step(session: dict[str, Any], start_count: int) -> dict[str, Any] | None:
    """把下一个 pending 步骤切到 active。"""
    for step in session.get("steps") or []:
        if str(step.get("status", "")) == "pending":
            step["status"] = "active"
            step["capture_count_at_step_start"] = start_count
            step["capture_count_at_ack"] = 0
            step["capture_count_after_idle"] = 0
            step["captured_indexes"] = []
            step["summary"] = None
            return step
    return None


def _capture_guide_reset_step(step: dict[str, Any], start_count: int) -> None:
    """重置步骤边界，便于 retry 后重新统计。"""
    step["status"] = "active"
    step["capture_count_at_step_start"] = start_count
    step["capture_count_at_ack"] = 0
    step["capture_count_after_idle"] = 0
    step["captured_indexes"] = []
    step["summary"] = None
    step["last_completed_at"] = 0.0


def _capture_guide_push_attempt(step: dict[str, Any]) -> None:
    """把旧的步骤结果压入 attempts，保留重试历史。"""
    attempt = {
        "status": step.get("status"),
        "capture_count_at_step_start": step.get("capture_count_at_step_start"),
        "capture_count_at_ack": step.get("capture_count_at_ack"),
        "capture_count_after_idle": step.get("capture_count_after_idle"),
        "captured_indexes": list(step.get("captured_indexes") or []),
        "summary": step.get("summary"),
        "last_completed_at": step.get("last_completed_at"),
    }
    if attempt["summary"] or attempt["captured_indexes"]:
        step.setdefault("attempts", []).append(attempt)


def _capture_guide_finalize_capture(session: dict[str, Any]) -> dict[str, Any]:
    """停止抓包并落盘最终结果。"""
    stop_opts = dict(session.get("stop_options") or {})
    timeout_sec = max(
        120,
        int(stop_opts.get("wait_ms") or 0) // 1000
        + int(stop_opts.get("idle_ms") or 0) // 1000
        + 90,
    )
    stop_req: dict[str, Any] = {
        "action": "network_capture_stop",
        "target": session.get("target", "active"),
        "get_body": stop_opts.get("get_body", True),
        "body_mode": stop_opts.get("body_mode", "filtered"),
        "wait_ms": stop_opts.get("wait_ms", 0),
        "idle_ms": stop_opts.get("idle_ms", 0),
        "max_bodies": stop_opts.get("max_bodies", 0),
        "max_body_bytes": stop_opts.get("max_body_bytes", 0),
        "method_filter": stop_opts.get("method_filter", ""),
        "url_filter": stop_opts.get("url_filter", ""),
        "exclude_domain": stop_opts.get("exclude_domain", ""),
        "status_filter": stop_opts.get("status_filter", ""),
        "until_match": stop_opts.get("until_match", ""),
    }
    resp = _send(stop_req, timeout=timeout_sec)
    if not resp.get("ok"):
        raise RuntimeError(str(resp.get("error", "capture stop failed")))

    requests: list[dict[str, Any]] = resp.get("requests", [])
    filtered = _filter_requests(
        requests,
        method_filter=str(stop_opts.get("method_filter") or ""),
        url_filter=str(stop_opts.get("url_filter") or ""),
        exclude_domain=str(stop_opts.get("exclude_domain") or ""),
        status_filter=str(stop_opts.get("status_filter") or ""),
    )
    full_path = _save_capture_requests(requests, filtered=False)
    filtered_path = _save_capture_requests(filtered, filtered=True)
    preferred = filtered or requests
    session["capture_file"] = str(full_path)
    session["filtered_capture_file"] = str(filtered_path)
    session["summary"] = _summarize_requests(preferred)
    session["crud"] = _infer_crud(preferred)
    session["body_diff"] = _diff_body(preferred)
    session["body_fetch"] = resp.get("body_fetch") or {}
    return {
        "requests": requests,
        "filtered_requests": filtered,
        "capture_file": str(full_path),
        "filtered_capture_file": str(filtered_path),
        "summary": session["summary"],
        "crud": session["crud"],
        "body_diff": session["body_diff"],
        "body_fetch": session["body_fetch"],
    }


def _capture_guide_build_start_result(session: dict[str, Any]) -> dict[str, Any]:
    """构建 start/status 共用的会话摘要。"""
    _, current_step = _capture_guide_active_step(session)
    result = {
        "ok": True,
        "session_id": session.get("session_id"),
        "status": session.get("status"),
        "current_step": _capture_guide_compact_step(current_step) if current_step else None,
        "next_prompt": _capture_guide_prompt(current_step, int(session.get("total_steps", 0))) if current_step else "",
        "total_steps": int(session.get("total_steps", 0)),
        "baseline_summary": session.get("baseline_summary") or {},
        "target": session.get("target"),
        "capture_started": bool(session.get("capture_started")),
        "completed_steps": sum(1 for step in session.get("steps") or [] if str(step.get("status")) in {"done", "skipped"}),
        "recent_step_summary": session.get("recent_step_summary"),
        "capture_file": session.get("capture_file", ""),
        "filtered_capture_file": session.get("filtered_capture_file", ""),
    }
    if str(session.get("status")) == "completed":
        result["summary"] = session.get("summary")
        result["crud"] = session.get("crud")
        result["body_diff"] = session.get("body_diff")
    return result


def _capture_guide_requests_for_step(requests: list[dict[str, Any]], step: dict[str, Any]) -> list[dict[str, Any]]:
    """按步骤记录的 captured_indexes 截取请求子集。"""
    indexes = [
        int(index)
        for index in (step.get("captured_indexes") or [])
        if isinstance(index, int) or str(index).isdigit()
    ]
    result: list[dict[str, Any]] = []
    for index in indexes:
        if 1 <= index <= len(requests):
            result.append(requests[index - 1])
    return result


def _capture_guide_print_text_result(payload: dict[str, Any]) -> None:
    """输出 capture-guide 的文本调试结果。"""
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _run_capture_guide(args: list[str]) -> int:
    """面向 skill 的多轮手动抓包编排命令。"""
    subcmd = args[0] if args else ""
    output_json = "--json" in args
    session_id = _cli_option_value(args, "--session", "")
    known_subcmds = {"start", "status", "ack", "skip", "retry", "analyze", "export", "abort"}
    if subcmd not in known_subcmds:
        print(
            "Usage: capture-guide <start|status|ack|skip|retry|analyze|export|abort>\n"
            "  start  --step \"文本\"... [--steps-file path] [--target <t>] [--json]\n"
            "  status --session <id> [--json]\n"
            "  ack    --session <id> [--json]\n"
            "  skip   --session <id> [--json]\n"
            "  retry  --session <id> [--previous] [--json]\n"
            "  analyze --session <id> [--json]\n"
            "  export --session <id> [--step N|--final-write-only] [--python-client|--curl] [--json]\n"
            "  abort  --session <id> [--json]\n",
            file=sys.stderr,
        )
        return 1

    daemon_required = {"start", "ack", "skip", "retry", "abort"}
    if subcmd in daemon_required and not _daemon_is_running():
        print("CDP daemon not running", file=sys.stderr)
        return 1

    try:
        if subcmd == "start":
            file_steps: list[dict[str, Any]] = []
            file_defaults: dict[str, Any] = {}
            steps_file = _cli_option_value(args, "--steps-file", "")
            if steps_file:
                file_steps, file_defaults = _capture_guide_load_steps_file(steps_file)

            cli_steps = [
                {
                    "index": index,
                    "key": f"step_{index}",
                    "text": text.strip(),
                    "idle_ms": None,
                    "expect": [],
                }
                for index, text in enumerate(_cli_option_values(args, "--step"), 1)
                if text.strip()
            ]
            steps = cli_steps or file_steps
            if not steps:
                print("Usage: capture-guide start --step \"步骤1\" [--step \"步骤2\"] [--json]", file=sys.stderr)
                return 1

            for index, step in enumerate(steps, 1):
                step["index"] = index
                step.setdefault("key", f"step_{index}")
                step.setdefault("text", "")
                step.setdefault("idle_ms", None)
                step.setdefault("expect", [])
                step["status"] = "pending"
                step["retry_count"] = 0
                step["capture_count_at_step_start"] = 0
                step["capture_count_at_ack"] = 0
                step["capture_count_after_idle"] = 0
                step["captured_indexes"] = []
                step["summary"] = None
                step["attempts"] = []
                step["last_completed_at"] = 0.0

            target = _cli_option_value(args, "--target", str(file_defaults.get("target") or "active")) or "active"
            stop_opts = _parse_capture_stop_options(
                args,
                default_wait_ms=0,
                default_body_mode=str(file_defaults.get("body_mode") or "filtered"),
            )
            if "--body-mode" not in args and "--no-body" not in args and not stop_opts["body_mode"]:
                stop_opts["body_mode"] = "filtered"
                stop_opts["get_body"] = True
            if "--filter-url" not in args and not stop_opts["url_filter"]:
                stop_opts["url_filter"] = str(file_defaults.get("filter_url") or "")
            if "--exclude-domain" not in args and not stop_opts["exclude_domain"]:
                stop_opts["exclude_domain"] = str(file_defaults.get("exclude_domain") or "")
            if "--max-bodies" not in args:
                try:
                    stop_opts["max_bodies"] = max(0, int(file_defaults.get("max_bodies") or 6))
                except (TypeError, ValueError):
                    stop_opts["max_bodies"] = 6
            if "--max-body-bytes" not in args and file_defaults.get("max_body_bytes") is not None:
                try:
                    stop_opts["max_body_bytes"] = max(0, int(file_defaults.get("max_body_bytes") or 0))
                except (TypeError, ValueError):
                    stop_opts["max_body_bytes"] = 0
            baseline_ms = _cli_flag_int(args, "--baseline-ms", int(file_defaults.get("baseline_ms") or 1500))
            idle_ms = _cli_flag_int(args, "--idle-ms", int(file_defaults.get("idle_ms") or 800))

            start_resp = _send({"action": "network_capture_start", "target": target}, timeout=15)
            if not start_resp.get("ok"):
                print(f"Error: {start_resp.get('error', '?')}", file=sys.stderr)
                return 1

            if baseline_ms > 0:
                time.sleep(baseline_ms / 1000.0)
            baseline_snapshot = _capture_guide_peek(target)
            baseline_requests = list(baseline_snapshot.get("requests") or [])
            baseline_summary = _capture_guide_build_baseline_summary(baseline_requests)
            steps[0]["status"] = "active"
            steps[0]["capture_count_at_step_start"] = int(baseline_snapshot.get("count") or 0)

            session = {
                "session_id": uuid.uuid4().hex[:12],
                "status": "awaiting_user",
                "target": target,
                "created_at": time.time(),
                "updated_at": time.time(),
                "capture_started": True,
                "total_steps": len(steps),
                "idle_ms": idle_ms,
                "baseline_ms": baseline_ms,
                "stop_options": stop_opts,
                "baseline_summary": baseline_summary,
                "steps": steps,
                "recent_step_summary": None,
                "capture_file": "",
                "filtered_capture_file": "",
                "summary": None,
                "crud": None,
                "body_diff": None,
                "body_fetch": {},
            }
            _save_capture_guide_session(session)
            result = _capture_guide_build_start_result(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if not session_id:
            print("Usage: capture-guide <status|ack|skip|retry|analyze|export|abort> --session <id>", file=sys.stderr)
            return 1

        session = _load_capture_guide_session(session_id)

        if subcmd == "status":
            result = _capture_guide_build_start_result(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "ack":
            active_index, active_step = _capture_guide_active_step(session)
            if active_step is None:
                print("Error: 当前会话没有 active 步骤", file=sys.stderr)
                return 1
            session["status"] = "settling"
            _save_capture_guide_session(session)

            current_snapshot = _capture_guide_peek(str(session.get("target", "active")))
            active_step["capture_count_at_ack"] = int(current_snapshot.get("count") or 0)
            step_idle_ms = _capture_guide_effective_step_idle_ms(session, active_step)
            settled_snapshot = _capture_guide_wait_for_idle(str(session.get("target", "active")), step_idle_ms)
            step_end_count = int(settled_snapshot.get("count") or 0)
            step_start_count = int(active_step.get("capture_count_at_step_start") or 0)
            step_requests = list((settled_snapshot.get("requests") or [])[step_start_count:step_end_count])
            captured_indexes = list(range(step_start_count + 1, step_end_count + 1))
            active_step["capture_count_after_idle"] = step_end_count
            active_step["captured_indexes"] = captured_indexes
            active_step["summary"] = _capture_guide_step_summary(
                step_requests,
                captured_indexes,
                dict((session.get("baseline_summary") or {}).get("request_keys") or {}),
            )
            active_step["status"] = "done"
            active_step["last_completed_at"] = time.time()
            session["recent_step_summary"] = {
                "step": _capture_guide_compact_step(active_step),
            }

            next_step = _capture_guide_advance_to_next_step(session, step_end_count)
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "completed_step": _capture_guide_compact_step(active_step),
                "step_summary": active_step.get("summary"),
                "is_finished": next_step is None,
            }
            if next_step is not None:
                session["status"] = "awaiting_user"
                result["status"] = session["status"]
                result["next_step"] = _capture_guide_compact_step(next_step)
                result["next_prompt"] = _capture_guide_prompt(next_step, int(session.get("total_steps", 0)))
            else:
                final = _capture_guide_finalize_capture(session)
                session["status"] = "completed"
                result["status"] = session["status"]
                result["next_step"] = None
                result["next_prompt"] = ""
                result["capture_file"] = final["capture_file"]
                result["filtered_capture_file"] = final["filtered_capture_file"]
                result["summary"] = final["summary"]
                result["crud"] = final["crud"]
                result["body_diff"] = final["body_diff"]
                result["body_fetch"] = final["body_fetch"]
            _save_capture_guide_session(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "skip":
            active_index, active_step = _capture_guide_active_step(session)
            if active_step is None:
                print("Error: 当前会话没有 active 步骤", file=sys.stderr)
                return 1
            current_snapshot = _capture_guide_peek(str(session.get("target", "active")))
            current_count = int(current_snapshot.get("count") or 0)
            active_step["capture_count_at_ack"] = current_count
            active_step["capture_count_after_idle"] = current_count
            active_step["captured_indexes"] = []
            active_step["summary"] = {
                "count": 0,
                "request_indexes": [],
                "requests": [],
                "important_requests": [],
                "write_requests": [],
                "notes": ["该步骤已跳过，未做抓包分析。"],
            }
            active_step["status"] = "skipped"
            active_step["last_completed_at"] = time.time()
            session["recent_step_summary"] = {
                "step": _capture_guide_compact_step(active_step),
            }
            next_step = _capture_guide_advance_to_next_step(session, current_count)
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "skipped_step": _capture_guide_compact_step(active_step),
                "is_finished": next_step is None,
            }
            if next_step is not None:
                session["status"] = "awaiting_user"
                result["status"] = session["status"]
                result["next_step"] = _capture_guide_compact_step(next_step)
                result["next_prompt"] = _capture_guide_prompt(next_step, int(session.get("total_steps", 0)))
            else:
                final = _capture_guide_finalize_capture(session)
                session["status"] = "completed"
                result["status"] = session["status"]
                result["next_step"] = None
                result["next_prompt"] = ""
                result["capture_file"] = final["capture_file"]
                result["filtered_capture_file"] = final["filtered_capture_file"]
                result["summary"] = final["summary"]
                result["crud"] = final["crud"]
                result["body_diff"] = final["body_diff"]
            _save_capture_guide_session(session)
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "retry":
            active_index, active_step = _capture_guide_active_step(session)
            retry_previous = "--previous" in args
            if retry_previous:
                if active_index is None or active_index <= 0:
                    print("Error: 当前没有可重试的上一已完成步骤", file=sys.stderr)
                    return 1
                prev_step = (session.get("steps") or [])[active_index - 1]
                if str(prev_step.get("status")) not in {"done", "skipped", "retried"}:
                    print("Error: 上一步尚未完成，不能 --previous retry", file=sys.stderr)
                    return 1
                current_snapshot = _capture_guide_peek(str(session.get("target", "active")))
                current_count = int(current_snapshot.get("count") or 0)
                _capture_guide_push_attempt(prev_step)
                prev_step["retry_count"] = int(prev_step.get("retry_count") or 0) + 1
                _capture_guide_reset_step(prev_step, current_count)
                active_step["status"] = "pending"
                session["status"] = "awaiting_user"
                session["recent_step_summary"] = {
                    "step": _capture_guide_compact_step(prev_step),
                    "note": "已切回上一已完成步骤，等待重新执行。",
                }
                _save_capture_guide_session(session)
                result = {
                    "ok": True,
                    "session_id": session.get("session_id"),
                    "status": session.get("status"),
                    "current_step": _capture_guide_compact_step(prev_step),
                    "next_prompt": _capture_guide_prompt(prev_step, int(session.get("total_steps", 0))),
                }
                if output_json:
                    _json_output(result, sys.argv)
                else:
                    _capture_guide_print_text_result(result)
                return 0

            if active_step is None:
                print("Error: 当前会话没有 active 步骤", file=sys.stderr)
                return 1
            current_snapshot = _capture_guide_peek(str(session.get("target", "active")))
            current_count = int(current_snapshot.get("count") or 0)
            _capture_guide_push_attempt(active_step)
            active_step["retry_count"] = int(active_step.get("retry_count") or 0) + 1
            _capture_guide_reset_step(active_step, current_count)
            active_step["status"] = "active"
            session["status"] = "awaiting_user"
            _save_capture_guide_session(session)
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "status": session.get("status"),
                "current_step": _capture_guide_compact_step(active_step),
                "next_prompt": _capture_guide_prompt(active_step, int(session.get("total_steps", 0))),
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "abort":
            if str(session.get("status")) not in {"completed", "aborted"} and session.get("capture_started"):
                try:
                    final = _capture_guide_finalize_capture(session)
                    session["capture_file"] = final["capture_file"]
                    session["filtered_capture_file"] = final["filtered_capture_file"]
                except Exception as exc:
                    session["abort_error"] = str(exc)
            session["status"] = "aborted"
            _save_capture_guide_session(session)
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "status": session.get("status"),
                "capture_file": session.get("capture_file", ""),
                "filtered_capture_file": session.get("filtered_capture_file", ""),
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "analyze":
            capture_file = str(session.get("capture_file") or "")
            if capture_file:
                requests = _load_capture_requests(file_path=capture_file)
            else:
                requests = []
            steps_payload = []
            for step in session.get("steps") or []:
                steps_payload.append({
                    "step": _capture_guide_compact_step(step),
                    "requests": _capture_guide_requests_for_step(requests, step),
                })
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "status": session.get("status"),
                "steps": [
                    {
                        "step": item["step"],
                        "summary": item["step"].get("summary"),
                        "request_count": len(item["requests"]),
                    }
                    for item in steps_payload
                ],
                "summary": session.get("summary"),
                "crud": session.get("crud"),
                "body_diff": session.get("body_diff"),
                "capture_file": session.get("capture_file", ""),
                "filtered_capture_file": session.get("filtered_capture_file", ""),
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                _capture_guide_print_text_result(result)
            return 0

        if subcmd == "export":
            capture_file = str(session.get("capture_file") or "")
            if not capture_file:
                print("Error: 当前会话尚未产出 capture_file，请先完成或 abort 会话", file=sys.stderr)
                return 1
            requests = _load_capture_requests(file_path=capture_file)
            export_requests = list(requests)
            step_arg = _cli_option_value(args, "--step", "")
            if step_arg:
                try:
                    step_index = int(step_arg)
                except ValueError:
                    print(f"Error: 无效 --step: {step_arg}", file=sys.stderr)
                    return 1
                matched = next(
                    (step for step in session.get("steps") or [] if int(step.get("index", 0)) == step_index),
                    None,
                )
                if matched is None:
                    print(f"Error: 未找到步骤 {step_index}", file=sys.stderr)
                    return 1
                export_requests = _capture_guide_requests_for_step(requests, matched)
            elif "--final-write-only" in args:
                final_step = None
                for step in reversed(session.get("steps") or []):
                    if str(step.get("status")) in {"done", "skipped"}:
                        final_step = step
                        break
                if final_step is not None:
                    candidates = _capture_guide_requests_for_step(requests, final_step)
                    writes = [
                        req for req in candidates
                        if str(req.get("method", "")).upper() in {"POST", "PUT", "PATCH", "DELETE"}
                    ]
                    export_requests = writes or candidates

            from page_manager import PageManager
            if "--python-client" in args:
                code = PageManager._export_python_client(export_requests, daemon_script=os.path.abspath(sys.argv[0]))
                ext = "py"
                fmt = "python-client"
            elif "--curl" in args:
                code = PageManager._export_curl(export_requests)
                ext = "sh"
                fmt = "curl"
            else:
                code = PageManager._export_python(export_requests)
                ext = "py"
                fmt = "python"
            import tempfile
            out = Path(tempfile.gettempdir()) / f"cdp_capture_guide_export.{ext}"
            out.write_text(code, encoding="utf-8")
            result = {
                "ok": True,
                "session_id": session.get("session_id"),
                "format": fmt,
                "request_count": len(export_requests),
                "export_file": str(out),
                "code": code if output_json else "",
            }
            if output_json:
                _json_output(result, sys.argv)
            else:
                print(code)
                print(f"\n# Saved to: {out}", file=sys.stderr)
            return 0

        raise RuntimeError(f"未知 capture-guide 子命令: {subcmd}")
    except Exception as exc:
        if session_id:
            try:
                failed_session = _load_capture_guide_session(session_id)
                if str(failed_session.get("status")) not in {"completed", "aborted"}:
                    failed_session["status"] = "failed"
                    failed_session["error"] = str(exc)
                    _save_capture_guide_session(failed_session)
            except Exception:
                pass
        print(f"Error: {exc}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _extract_global_instance_arg(argv: list[str]) -> tuple[str, list[str]]:
    """抽取全局 --instance 参数，避免每个子命令重复解析。"""
    cleaned: list[str] = []
    instance = ""
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--instance":
            if index + 1 >= len(argv):
                raise ValueError("--instance 缺少实例值")
            instance = argv[index + 1].strip()
            index += 2
            continue
        if arg.startswith("--instance="):
            instance = arg.split("=", 1)[1].strip()
            index += 1
            continue
        cleaned.append(arg)
        index += 1
    return instance, cleaned


def _print_instance_candidates(candidates: list[dict[str, Any]], *, stream: Any = sys.stderr) -> None:
    """把候选实例打印给用户选择。"""
    for index, item in enumerate(candidates, 1):
        print(
            f"[{index}] {item.get('instance_id', '')}  "
            f"port={item.get('port', '')}  "
            f"source={item.get('source', '')}  "
            f"user_data_dir={item.get('user_data_dir', '') or '-'}",
            file=stream,
        )


def _print_instance_error(exc: CdpInstanceSelectionError) -> None:
    """统一输出实例歧义/未命中错误。"""
    print(f"Error: {exc}", file=sys.stderr)
    if exc.candidates:
        _print_instance_candidates(exc.candidates)
    print(
        "\n示例:\n"
        f"  {sys.argv[0]} test --instance <instance-id>\n"
        f"  {sys.argv[0]} instance list --json",
        file=sys.stderr,
    )


def _resolve_runtime_instance_or_exit() -> dict[str, Any]:
    """解析并切换到当前命令要使用的实例；失败时直接退出。"""
    try:
        selected = resolve_cdp_instance(CURRENT_INSTANCE_ID)
    except CdpInstanceSelectionError as exc:
        _print_instance_error(exc)
        raise SystemExit(1) from exc
    _configure_runtime_instance(str(selected.get("instance_id", "") or ""))
    return selected


def main() -> int:
    try:
        explicit_instance, cleaned_argv = _extract_global_instance_arg(sys.argv)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if cleaned_argv != sys.argv:
        sys.argv = cleaned_argv
    if explicit_instance:
        _configure_runtime_instance(explicit_instance)

    if len(sys.argv) < 2:
        print(
            f"Usage: {sys.argv[0]} [--instance <instance-id>] <command>\n"
            "Commands:\n"
            "  start|stop|status|restart|test   daemon 管理\n"
            "  instance list                    列出可选 Chrome 实例\n"
            "  list-pages|active-page            页面列表\n"
            "  tab bind|get|list|rm              标签页 alias 绑定\n"
            "  snapshot [-i] [-C] [-s scope]     交互元素快照 [--json] [-c] [-d N] [-u]\n"
            "  diff snapshot                     对比当前快照和上次 snapshot baseline\n"
            "  screenshot [path]                 截图 [--annotate] [--target <t>]\n"
            "  diagnose-page [path]              页面渲染诊断（DOM + 图表候选 + 截图像素）\n"
            "  batch [--bail] \"cmd args\"...       批量执行多条 CLI 命令（也支持 --json stdin）\n"
            "  capture-action --do \"cmd\"...      执行动作并自动抓包/摘要/推断（支持 idle/body-mode）\n"
            "  capture-flow <subcmd>               默认入口：整段抓包后自动分析是否需要回退\n"
            "  discover-api [--url <page>]         SPA 接口发现（支持 --fast/--no-nav/--do）\n"
            "  capture-guide <subcmd>              面向 skill 的会话式手动抓包编排\n"
            "  navigate <url> | reload             页面跳转/刷新（替代 eval-js location.*）\n"
            "  auth material|token                 汇总认证材料 / 用 cookie 请求 token 接口\n"
            "  auth-template <domain>              从抓包提取认证 header 模板（脱敏）\n"
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
            "  open <url>                         打开新标签页 [--alias <name>]（强制进入固定分组 CDP自动化）\n"
            "  close [target]                     关闭标签页\n"
            "  target list|resolve                列出或解析 target 选择器\n"
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
            "  network-capture stop [--wait-ms N|--idle-ms N]  停止抓包（默认不抓 body）\n"
            "  network-capture load-file <json>  载入已有抓包到默认缓存\n"
            "  network-capture export [--curl]   导出为 Python/curl 代码\n"
            "  network-capture summary           摘要抓包请求/认证头/请求体 keys [--out path]\n"
            "  network-capture diff-body         分析写请求体和相关 GET 响应差异\n"
            "  network-capture infer-crud        推断 GET+PUT 等 CRUD 流程和风险\n"
            "  network fetch <url>                在页面上下文 fetch（带 cookie）\n"
            "  network replay [N]                 重放抓包的第 N 个请求\n"
            "  cookies get <url>                  导出浏览器登录 cookie（按域名过滤）\n"
            "                                     [--json] 默认，键值对 JSON\n"
            "                                     [--header] 输出 Cookie: a=1; b=2 格式\n"
            "                                     [--raw]    输出原始 cookie 列表\n"
            "  cookies inspect <url>              输出脱敏 cookie 元信息\n"
            "  cookies validate <url> --expect A,B 校验 cookie 名称是否存在\n"
            "  auth-click-test [timeout]         授权弹窗测试"
        )
        return 1

    cmd = sys.argv[1]
    if cmd == "eval":
        sys.argv[1] = "eval-js"
        cmd = "eval-js"

    if cmd == "__run_daemon__":
        run_daemon()
        return 0

    if cmd == "batch":
        return _run_batch(sys.argv[2:])

    allowed, reason = _check_action_policy(cmd)
    if not allowed:
        print(f"Error: {reason}", file=sys.stderr)
        return 1

    confirmed, reason = _confirm_action(cmd, sys.argv[2:])
    if not confirmed:
        print(f"Error: {reason}", file=sys.stderr)
        return 1

    if cmd == "capture-action":
        return _run_capture_action(sys.argv[2:])

    if cmd == "capture-flow":
        return _run_capture_flow(sys.argv[2:])

    if cmd == "discover-api":
        return _run_discover_api(sys.argv[2:])

    if cmd == "capture-guide":
        return _run_capture_guide(sys.argv[2:])

    if cmd == "instance":
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""
        if subcmd != "list":
            print("Usage: instance list [--json]", file=sys.stderr)
            return 1
        instances = discover_cdp_instances()
        result = {"ok": True, "count": len(instances), "instances": instances}
        if "--json" in sys.argv:
            _json_output(result, sys.argv)
            return 0
        if not instances:
            print("(no Chrome CDP instances)")
            return 0
        _print_instance_candidates(instances, stream=sys.stdout)
        return 0

    if cmd in {"stop", "stop-daemon", "status"} and not CURRENT_INSTANCE_ID:
        discovered = discover_cdp_instances()
        if len(discovered) > 1:
            _print_instance_error(
                CdpInstanceSelectionError(
                    "检测到多个 Chrome CDP 实例，请显式传入 --instance。",
                    candidates=discovered,
                )
            )
            return 1

    if cmd == "start":
        _resolve_runtime_instance_or_exit()
        if _daemon_is_running():
            print("CDP daemon already running")
            return 0
        if _find_daemon_pids(CURRENT_INSTANCE_ID):
            _force_stop(timeout=8)
        daemonize()
        return 0

    elif cmd in ("stop", "stop-daemon"):
        if (
            not _daemon_is_running()
            and not Path(PID_FILE).exists()
            and not Path(SOCKET_PATH).exists()
            and not _find_daemon_pids(CURRENT_INSTANCE_ID)
        ):
            print("CDP daemon not running")
            return 0
        _force_stop(timeout=8)
        print("CDP daemon stopped")
        return 0

    elif cmd in ("restart", "restart-daemon"):
        _resolve_runtime_instance_or_exit()
        _force_stop(timeout=8)
        daemonize()
        return 0

    elif cmd == "status":
        if _daemon_is_running():
            try:
                resp = _send_with_retry({"action": "ping"}, timeout=5, retries=2)
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
        elif _find_daemon_pids(CURRENT_INSTANCE_ID):
            print(
                "Orphaned daemon process(es) without socket/pid file: "
                + ",".join(str(pid) for pid in _find_daemon_pids(CURRENT_INSTANCE_ID))
                + f". Run: {sys.argv[0]} restart"
            )
        else:
            print("Not running")
        return 0

    elif cmd == "test":
        _resolve_runtime_instance_or_exit()
        if not _daemon_is_running():
            daemonize()
        fallback_to_legacy = False
        try:
            resp = _send_with_retry({"action": "test_connection"}, timeout=15, retries=3)
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
                version_resp = _send_with_retry(
                    {"action": "cdp_call", "method": "Browser.getVersion"}, timeout=15
                )
                target_resp = _send_with_retry(
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
                    ping = _send_with_retry({"action": "ping"}, timeout=3, retries=2)
                except Exception:
                    ping = {}
                resp = {
                    "ok": True,
                    "connection": "ok",
                    "instance_id": ping.get("instance_id", CURRENT_INSTANCE_ID),
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
        resp = _send_with_retry({"action": "list_pages"}, timeout=5, retries=2)
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
        resp = _send_with_retry({"action": "active_page"}, timeout=25, retries=2)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            for item in resp.get("candidates", []):
                print(
                    f"  candidate: {item.get('targetId', '')[:8]}  {item.get('url', '')}",
                    file=sys.stderr,
                )
            return 1
        page = resp.get("page", {})
        print(f"target_id: {page.get('targetId', '(unknown)')}")
        print(f"url:       {page.get('url', '')}")
        print(f"title:     {page.get('title', '')}")
        if resp.get("source"):
            print(f"source:    {resp.get('source')}")
        if resp.get("warning"):
            print(f"warning:   {resp['warning']}", file=sys.stderr)
        return 0

    elif cmd == "target":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""
        if subcmd == "list":
            resp = _send({"action": "list_pages"}, timeout=5)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            pages = resp.get("pages", [])
            result = {"ok": True, "count": len(pages), "targets": pages}
            if "--json" in sys.argv:
                _json_output(result, sys.argv)
            else:
                if not pages:
                    print("(no targets)")
                for page in pages:
                    print(
                        f"{page.get('targetId', '')[:8]}  "
                        f"{page.get('type', 'page'):6s}  "
                        f"{page.get('url', '')}  — {page.get('title', '')}"
                    )
            return 0
        if subcmd == "resolve":
            if len(sys.argv) < 4:
                print("Usage: target resolve <selector> [--json]", file=sys.stderr)
                return 1
            selector = sys.argv[3]
            resp = _send({"action": "target_resolve", "target": selector}, timeout=15)
            if "--json" in sys.argv:
                _json_output(resp, sys.argv)
            elif resp.get("ok"):
                print(f"target_id: {resp.get('targetId', '')}")
                print(f"url:       {resp.get('url', '')}")
                print(f"title:     {resp.get('title', '')}")
            else:
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                for item in resp.get("candidates", []):
                    print(
                        f"  candidate: {item.get('targetId', '')[:8]}  "
                        f"{item.get('url', '')}  — {item.get('title', '')}",
                        file=sys.stderr,
                    )
                if resp.get("suggestion"):
                    print(f"  suggestion: {resp['suggestion']}", file=sys.stderr)
            return 0 if resp.get("ok") else 1
        print("Usage: target <list|resolve> [--json]", file=sys.stderr)
        return 1

    elif cmd == "tab":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print(
                "Usage: tab bind <name> [--target <t>]\n"
                "       tab get <name>\n"
                "       tab list\n"
                "       tab rm <name>",
                file=sys.stderr,
            )
            return 1
        sub = sys.argv[2]
        if sub == "bind":
            if len(sys.argv) < 4:
                print("Usage: tab bind <name> [--target <t>]", file=sys.stderr)
                return 1
            req: dict[str, Any] = {"action": "tab_bind", "name": sys.argv[3]}
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            resp = _send(req, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Bound: {resp.get('alias', '')} -> {resp.get('targetId', '')[:8]}  {resp.get('url', '')}")
            return 0
        elif sub == "get":
            if len(sys.argv) < 4:
                print("Usage: tab get <name>", file=sys.stderr)
                return 1
            resp = _send({"action": "tab_get", "name": sys.argv[3]}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"alias:     {resp.get('alias', '')}")
            print(f"target_id: {resp.get('targetId', '')}")
            print(f"url:       {resp.get('url', '')}")
            print(f"title:     {resp.get('title', '')}")
            return 0
        elif sub == "list":
            resp = _send({"action": "tab_list"}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            bindings = resp.get("bindings", [])
            if not bindings:
                print("(no tab aliases)")
                return 0
            for item in bindings:
                print(f"{item.get('alias', ''):16} {item.get('targetId', '')[:8]}  {item.get('url', '')}")
            return 0
        elif sub in ("rm", "remove", "unbind"):
            if len(sys.argv) < 4:
                print("Usage: tab rm <name>", file=sys.stderr)
                return 1
            resp = _send({"action": "tab_remove", "name": sys.argv[3]}, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            state = "removed" if resp.get("removed") else "not-found"
            print(f"Tab alias {state}: {resp.get('alias', '')}")
            return 0
        else:
            print(f"Unknown tab subcommand: {sub}", file=sys.stderr)
            return 1

    # ------------------------------------------------------------------
    # 高级页面操作 CLI（对齐 agent-browser 风格）
    # ------------------------------------------------------------------
    elif cmd == "snapshot":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "snapshot"}
        # 解析参数: -i (interactive, 默认), -C (cursor), -T (with-tooltips),
        # -s <scope>, --target <t>, --json, -c/--compact, -d/--depth, -u/--urls
        i = 2
        output_json = False
        include_urls = False
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "-i":
                pass  # 默认就是 interactive
            elif arg == "-C":
                req["include_cursor"] = True
            elif arg in ("-T", "--with-tooltips"):
                req["with_tooltips"] = True
            elif arg in ("-c", "--compact"):
                req["compact"] = True
            elif arg in ("-u", "--urls"):
                req["include_urls"] = True
                include_urls = True
            elif arg in ("-d", "--depth") and i + 1 < len(sys.argv):
                i += 1
                try:
                    req["depth"] = int(sys.argv[i])
                except ValueError:
                    print(f"Error: invalid depth: {sys.argv[i]}", file=sys.stderr)
                    return 1
            elif arg == "-s" and i + 1 < len(sys.argv):
                i += 1
                req["scope"] = sys.argv[i]
            elif arg == "--target" and i + 1 < len(sys.argv):
                i += 1
                req["target"] = sys.argv[i]
            elif arg == "--json":
                output_json = True
            elif arg == "--max-output" and i + 1 < len(sys.argv):
                i += 1  # 由 _output_options 统一处理
            elif arg == "--content-boundaries":
                pass
            i += 1
        resp = _send(req, timeout=30)  # with_tooltips 需要更长时间
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        elements = resp.get("elements", [])
        snapshot_text = _snapshot_text(elements, include_urls=include_urls)
        _save_snapshot_baseline(resp.get("target_id", req.get("target", "active")), snapshot_text)
        if output_json:
            _json_output(resp, sys.argv)
        else:
            _page_output(
                snapshot_text,
                origin=resp.get("url", resp.get("target_id", "")),
                argv=sys.argv,
            )
        return 0

    elif cmd == "diff":
        if len(sys.argv) < 3 or sys.argv[2] != "snapshot":
            print("Usage: diff snapshot [--target <t>] [-u] [-s scope] [-d N] [--baseline <file>]", file=sys.stderr)
            return 1
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "snapshot"}
        include_urls = False
        baseline_file = ""
        i = 3
        while i < len(sys.argv):
            arg = sys.argv[i]
            if arg == "-C":
                req["include_cursor"] = True
            elif arg in ("-u", "--urls"):
                req["include_urls"] = True
                include_urls = True
            elif arg in ("-d", "--depth") and i + 1 < len(sys.argv):
                i += 1
                try:
                    req["depth"] = int(sys.argv[i])
                except ValueError:
                    print(f"Error: invalid depth: {sys.argv[i]}", file=sys.stderr)
                    return 1
            elif arg == "-s" and i + 1 < len(sys.argv):
                i += 1
                req["scope"] = sys.argv[i]
            elif arg == "--target" and i + 1 < len(sys.argv):
                i += 1
                req["target"] = sys.argv[i]
            elif arg == "--baseline" and i + 1 < len(sys.argv):
                i += 1
                baseline_file = sys.argv[i]
            elif arg == "--max-output" and i + 1 < len(sys.argv):
                i += 1
            elif arg == "--content-boundaries":
                pass
            i += 1
        resp = _send(req, timeout=30)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        target_key = resp.get("target_id", req.get("target", "active"))
        current = _snapshot_text(resp.get("elements", []), include_urls=include_urls)
        if baseline_file:
            try:
                baseline = Path(baseline_file).expanduser().read_text(encoding="utf-8")
            except Exception as exc:
                print(f"Error: read baseline failed: {exc}", file=sys.stderr)
                return 1
        else:
            baseline = _load_snapshot_baseline(target_key)
        if baseline is None:
            _save_snapshot_baseline(target_key, current)
            print("No previous snapshot baseline. Saved current snapshot as baseline.")
            return 0
        diff_text = "".join(difflib.unified_diff(
            baseline.splitlines(keepends=True),
            current.splitlines(keepends=True),
            fromfile="previous-snapshot",
            tofile="current-snapshot",
        ))
        _save_snapshot_baseline(target_key, current)
        if not diff_text:
            print("No snapshot changes.")
        else:
            _page_output(diff_text, origin=resp.get("url", target_key), argv=sys.argv)
        return 0

    elif cmd == "screenshot":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "screenshot"}
        if len(sys.argv) >= 3 and not sys.argv[2].startswith("--"):
            req["path"] = sys.argv[2]
        if "--annotate" in sys.argv:
            req["annotate"] = True
        if "--full" in sys.argv:
            req["full_page"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=30)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(f"Screenshot saved: {resp.get('path', '')}")
        legend = resp.get("legend", [])
        if legend:
            for item in legend:
                print(f"[{item.get('index')}] {item.get('ref')} {item.get('desc', '')}")
        return 0

    elif cmd == "diagnose-page":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "diagnose_page"}
        if len(sys.argv) >= 3 and not sys.argv[2].startswith("--"):
            req["path"] = sys.argv[2]
        if "--full" in sys.argv:
            req["full_page"] = True
        if "--wait-ms" in sys.argv and sys.argv.index("--wait-ms") + 1 < len(sys.argv):
            req["wait_ms"] = int(sys.argv[sys.argv.index("--wait-ms") + 1])
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=60)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        if "--json" in sys.argv:
            _json_output(resp, sys.argv)
        else:
            dom = resp.get("dom") or {}
            px = resp.get("pixel_stats") or {}
            print(f"URL: {dom.get('url', '')}")
            print(f"readyState: {dom.get('readyState', '')}  textLength: {dom.get('bodyTextLength', 0)}")
            print(f"chartCandidates: {len(dom.get('chartCandidates') or [])}  canvas: {dom.get('visibleCanvas', 0)}  svg: {dom.get('visibleSvg', 0)}")
            if px.get("ok"):
                print(f"pixels: non_white={px.get('non_white_ratio')} colors={px.get('distinct_color_buckets')} size={px.get('width')}x{px.get('height')}")
            print(f"screenshot: {resp.get('screenshot', '')}")
            for item in resp.get("conclusions", []):
                print(f"- {item}")
        return 0

    elif cmd == "click":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: click <@ref|selector> [--dblclick] [--right] [--at x,y] [--force-js] [--target <t>]", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "click", "ref": sys.argv[2]}
        if "--dblclick" in sys.argv:
            req["dblclick"] = True
        if "--right" in sys.argv:
            req["right"] = True
        if "--force-js" in sys.argv:
            req["force_js"] = True
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
            print("Usage: open <url> [--alias <name>] [--no-activate] [--wait <ms>] [--group <ignored>]", file=sys.stderr)
            return 1
        ok, reason = _check_allowed_url(sys.argv[2])
        if not ok:
            print(f"Error: {reason}", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "open_tab", "url": sys.argv[2], "group": AUTOMATION_GROUP_NAME}
        if "--no-activate" in sys.argv:
            req["activate"] = False
        if "--wait" in sys.argv and sys.argv.index("--wait") + 1 < len(sys.argv):
            req["wait_ms"] = int(sys.argv[sys.argv.index("--wait") + 1])
        if "--group" in sys.argv and sys.argv.index("--group") + 1 < len(sys.argv):
            requested_group = sys.argv[sys.argv.index("--group") + 1]
            if requested_group != AUTOMATION_GROUP_NAME:
                req["requested_group"] = requested_group
        if "--alias" in sys.argv and sys.argv.index("--alias") + 1 < len(sys.argv):
            req["alias"] = sys.argv[sys.argv.index("--alias") + 1]
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        group_info = f"  → group '{resp['group']}'" if resp.get("group") else ""
        alias_info = f"  [alias=tab:{resp.get('alias')}]" if resp.get("alias") else ""
        ignore_info = f"  [ignored-group={resp.get('requested_group_ignored')}]" if resp.get("requested_group_ignored") else ""
        print(f"Opened: {resp.get('targetId', '?')[:8]}  {resp.get('title', '')}{group_info}{alias_info}{ignore_info}")
        print(f"  URL: {resp.get('url', '')}")
        if resp.get("group_error"):
            print(f"  group-warning: {resp.get('group_error')}")
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
            print('Usage: fill <@ref|selector|--at x,y> "text" [--no-native] [--no-clear] [--submit] [--target <t>]', file=sys.stderr)
            return 1

        # 解析位置参数：fill <ref> <text>（text 可能在 ref 后面不同位置）
        # 规则：sys.argv[2] = ref，第一个非 -- 标志的 sys.argv[3+] = text
        ref_val = sys.argv[2]
        text_val = None
        at_val = None

        # 处理 --at 紧跟 ref 的情况：fill --at x,y "text"（ref 为 dummy）
        if ref_val == "--at" and len(sys.argv) > 3:
            at_raw = sys.argv[3].split(",")
            if len(at_raw) == 2:
                try:
                    at_val = [int(at_raw[0]), int(at_raw[1])]
                except ValueError:
                    pass
            ref_val = "__dummy__"
            # text 从 sys.argv[4] 开始找
            for i in range(4, len(sys.argv)):
                if not sys.argv[i].startswith("--"):
                    text_val = sys.argv[i]
                    break
        else:
            # 常规：fill <ref> <text>，text 在 sys.argv[3+]
            # 注意跳过 --at x,y、--target t 等标志对
            skip_next = False
            for i in range(3, len(sys.argv)):
                if skip_next:
                    skip_next = False
                    continue
                tok = sys.argv[i]
                if tok in ("--at", "--target"):
                    # --at 的值如果是坐标就顺手解析
                    if tok == "--at" and i + 1 < len(sys.argv):
                        xy = sys.argv[i + 1].split(",")
                        if len(xy) == 2:
                            try:
                                at_val = [int(xy[0]), int(xy[1])]
                            except ValueError:
                                pass
                    skip_next = True
                    continue
                if tok.startswith("--"):
                    continue
                text_val = tok
                break

        if text_val is None:
            print("Error: missing text argument", file=sys.stderr)
            return 1

        req: dict[str, Any] = {"action": "fill", "ref": ref_val, "text": text_val}
        if at_val:
            req["at"] = at_val
        # 默认 native 模式（CDP Input.insertText），--no-native 回退到 JS setter
        req["native"] = "--no-native" not in sys.argv
        if "--no-clear" in sys.argv:
            req["clear"] = False
        if "--submit" in sys.argv:
            req["submit"] = True
        if "--target" in sys.argv:
            idx = sys.argv.index("--target")
            if idx + 1 < len(sys.argv):
                req["target"] = sys.argv[idx + 1]
        # at_val 已在参数解析阶段处理，这里补充到 req
        if at_val and "at" not in req:
            req["at"] = at_val
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
            print('Usage: select <@ref|selector> "value" [--by label] [--search text] [--target <t>]', file=sys.stderr)
            return 1
        req = {"action": "select", "ref": sys.argv[2], "value": sys.argv[3]}
        if "--by" in sys.argv:
            idx = sys.argv.index("--by")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1] == "label":
                req["by_label"] = True
        if "--search" in sys.argv:
            idx = sys.argv.index("--search")
            if idx + 1 < len(sys.argv):
                req["search_text"] = sys.argv[idx + 1]
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
            print("Usage: press <key> [@ref] [--target <t>]\n"
                  "  支持组合键：press Meta+S / press Ctrl+Shift+P / press Alt+F4", file=sys.stderr)
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

    elif cmd == "extract-metric":
        # 从 SVG/canvas/DOM 图表提取数值，支持 Flink REST API 兜底
        # Usage: extract-metric [--title <name>] [--api <url>] [--target <t>]
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "extract_metric"}
        if "--title" in sys.argv:
            idx = sys.argv.index("--title")
            if idx + 1 < len(sys.argv):
                req["title"] = sys.argv[idx + 1]
        if "--api" in sys.argv:
            idx = sys.argv.index("--api")
            if idx + 1 < len(sys.argv):
                req["api_url"] = sys.argv[idx + 1]
        if "--target" in sys.argv:
            idx = sys.argv.index("--target")
            if idx + 1 < len(sys.argv):
                req["target"] = sys.argv[idx + 1]
        resp = _send(req, timeout=15)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(json.dumps(resp, ensure_ascii=False, indent=2))
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
        _page_output(resp.get("text", ""), origin=resp.get("target_id", ""), argv=sys.argv)
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
        url_str = resp.get("url", "")

        # --decode-param <name>: 从 URL query string 提取参数并 JSON-decode
        if "--decode-param" in sys.argv and sys.argv.index("--decode-param") + 1 < len(sys.argv):
            param_name = sys.argv[sys.argv.index("--decode-param") + 1]
            from urllib.parse import urlparse, parse_qs, unquote
            parsed = urlparse(url_str)
            params = parse_qs(parsed.query)
            raw_val = params.get(param_name, [None])[0]
            if raw_val is None:
                print(f"(param '{param_name}' not found in URL)", file=sys.stderr)
                print(url_str)
                return 1
            decoded = unquote(raw_val)
            # 尝试 JSON 解析
            try:
                obj = json.loads(decoded)
                print(json.dumps(obj, ensure_ascii=False, indent=2))
            except json.JSONDecodeError:
                print(decoded)
            return 0

        print(url_str)
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
            stop_opts = _parse_capture_stop_options(
                sys.argv,
                default_wait_ms=0,
                default_body_mode="none",
            )
            req = {
                "action": "network_capture_stop",
                "get_body": stop_opts["get_body"],
                "body_mode": stop_opts["body_mode"],
                "wait_ms": stop_opts["wait_ms"],
                "idle_ms": stop_opts["idle_ms"],
                "max_bodies": stop_opts["max_bodies"],
                "max_body_bytes": stop_opts["max_body_bytes"],
                "method_filter": stop_opts["method_filter"],
                "url_filter": stop_opts["url_filter"],
                "exclude_domain": stop_opts["exclude_domain"],
                "status_filter": stop_opts["status_filter"],
                "until_match": stop_opts["until_match"],
            }
            print(
                "Stopping capture... "
                f"body_mode={stop_opts['body_mode']} wait_ms={stop_opts['wait_ms']} idle_ms={stop_opts['idle_ms']}",
                file=sys.stderr,
            )
            if stop_opts["until_match"]:
                print(f"Until-match: {stop_opts['until_match']}", file=sys.stderr)
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                req["target"] = sys.argv[sys.argv.index("--target") + 1]
            timeout_sec = max(
                120,
                stop_opts["wait_ms"] // 1000 + stop_opts["idle_ms"] // 1000 + 90,
            )
            resp = _send(req, timeout=timeout_sec)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            requests: list[dict[str, Any]] = resp.get("requests", [])
            filtered = _filter_requests(
                requests,
                stop_opts["method_filter"],
                stop_opts["url_filter"],
                stop_opts["exclude_domain"],
                stop_opts["status_filter"],
            )
            display_requests = filtered if _has_capture_filters(
                stop_opts["method_filter"],
                stop_opts["url_filter"],
                stop_opts["exclude_domain"],
                stop_opts["status_filter"],
            ) else requests
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
            # 表格输出（#12：标注 bodyFile 的请求）
            total_body_files = 0
            for i, r in enumerate(display_requests):
                method = r.get("method", "?")
                url = r.get("url", "")
                status = r.get("status", "?")
                post_data = r.get("postData")
                source = r.get("_source", "")
                body_hint = ""
                if post_data:
                    body_hint = f"  req={len(post_data)}B"
                # #12: 区分 body 在内存 vs 文件
                if r.get("responseBodyFile"):
                    resp_size = r.get("responseBodySize", 0)
                    body_hint += f"  resp={resp_size//1024}KB→file"
                    total_body_files += 1
                elif r.get("responseBody"):
                    resp_size = len(str(r["responseBody"]))
                    body_hint += f"  resp={resp_size}B"
                elif r.get("responseBodySkipped"):
                    body_hint += f"  resp=skip:{r.get('responseBodySkipped')}"
                tab_hint = "  [new_tab]" if source == "new_tab" else ""
                print(f"[{i+1}] {status} {method:6s} {url}{body_hint}{tab_hint}")
            print(f"\n--- {len(requests)} API requests captured, showing {len(display_requests)} ---")
            if total_body_files:
                print(f"  ⚠ {total_body_files} large response(s) saved to /tmp/cdp_body_*.txt (>512KB)")
            body_fetch = resp.get("body_fetch") or {}
            if body_fetch:
                skipped_total = (
                    body_fetch.get("skipped_unmatched", 0)
                    + body_fetch.get("skipped_limit", 0)
                    + body_fetch.get("skipped_too_large", 0)
                    + body_fetch.get("skipped_error", 0)
                )
                print(
                    "  body-fetch: "
                    f"mode={body_fetch.get('mode')} "
                    f"selected={body_fetch.get('selected', 0)} "
                    f"fetched={body_fetch.get('fetched', 0)} "
                    f"skipped={skipped_total}"
                )
            # 将请求列表写到临时文件供 export 使用
            full_path = _save_capture_requests(requests, filtered=False)
            filtered_path = _save_capture_requests(filtered, filtered=True) if filtered != requests else None
            print(f"Saved to: {full_path}")
            if filtered_path is not None:
                print(f"Filtered: {filtered_path}")
            print(f"Export:   {sys.argv[0]} network-capture export [--curl|--python-client]")
            return 0

        elif subcmd == "load-file":
            if len(sys.argv) < 4:
                print("Usage: network-capture load-file <path.json>", file=sys.stderr)
                return 1
            try:
                requests = _load_capture_requests(file_path=sys.argv[3])
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            save_path = _save_capture_requests(requests)
            print(f"Loaded {len(requests)} request(s) from {sys.argv[3]}")
            print(f"Saved to: {save_path}")
            return 0

        elif subcmd == "export":
            # 从临时文件读取上次 stop 的结果
            try:
                file_arg = ""
                if "--file" in sys.argv and sys.argv.index("--file") + 1 < len(sys.argv):
                    file_arg = sys.argv[sys.argv.index("--file") + 1]
                requests = _load_capture_requests(
                    prefer_filtered="--filtered" in sys.argv,
                    file_path=file_arg,
                )
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            from page_manager import PageManager
            if "--python-client" in sys.argv:
                # 完整可运行的 Python 客户端（带 cookie 获取 + 认证头）
                daemon_script = os.path.abspath(sys.argv[0])
                code = PageManager._export_python_client(requests, daemon_script=daemon_script)
                ext = "py"
            elif "--curl" in sys.argv:
                code = PageManager._export_curl(requests)
                ext = "sh"
            else:
                code = PageManager._export_python(requests)
                ext = "py"
            print(code)
            # 同时写到文件
            import tempfile
            out = Path(tempfile.gettempdir()) / f"cdp_network_capture.{ext}"
            out.write_text(code)
            print(f"\n# Saved to: {out}", file=sys.stderr)
            return 0

        elif subcmd == "filter":
            # 过滤上次 stop 的抓包数据
            try:
                requests = _load_capture_requests()
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            # 解析过滤参数
            method_filter, url_filter, exclude_domain, status_filter = _parse_capture_filters(sys.argv)
            capture_filtered = _filter_requests(requests, method_filter, url_filter, exclude_domain, status_filter)
            if not capture_filtered:
                print("(no requests match filter)")
                return 0
            _print_capture_table(capture_filtered)
            print(f"\n--- {len(capture_filtered)}/{len(requests)} requests after filter ---")
            # 保存过滤结果供后续 export 使用
            filtered_file = _save_capture_requests(capture_filtered, filtered=True)
            print(f"Filtered saved to: {filtered_file}")
            print(f"Export: {sys.argv[0]} network-capture export --filtered [--curl|--python-client]")
            return 0

        elif subcmd == "summary":
            try:
                requests = _load_capture_requests(prefer_filtered="--filtered" in sys.argv)
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            summary = _summarize_requests(requests)
            if "--out" in sys.argv and sys.argv.index("--out") + 1 < len(sys.argv):
                out_path = Path(sys.argv[sys.argv.index("--out") + 1]).expanduser()
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"Summary saved to: {out_path}", file=sys.stderr)
            if "--json" in sys.argv:
                _json_output(summary, sys.argv)
            else:
                print(f"--- {summary['count']} captured API requests ---")
                for item in summary["requests"]:
                    print(f"[{item['index']}] {item.get('status')} {item.get('method'):6s} {item.get('path')}")
                    if item.get("request_body_keys"):
                        print("  request_body_keys: " + ", ".join(item["request_body_keys"]))
                    if item.get("response_keys"):
                        print("  response_keys: " + ", ".join(item["response_keys"]))
                    shape = item.get("response_shape") or {}
                    if shape:
                        if shape.get("type") == "array":
                            print(f"  response_shape: array length={shape.get('length')}")
                        elif shape.get("type") == "object":
                            array_children = [
                                f"{k}[{v.get('length')}]"
                                for k, v in (shape.get("children") or {}).items()
                                if isinstance(v, dict) and v.get("type") == "array"
                            ]
                            if array_children:
                                print("  response_arrays: " + ", ".join(array_children[:12]))
                    if item.get("auth_headers"):
                        pairs = [f"{k}={v}" for k, v in item["auth_headers"].items()]
                        print("  auth_headers: " + ", ".join(pairs))
                    if item.get("full_payload_write_candidate"):
                        print("  warning: full payload write candidate")
            return 0

        elif subcmd == "diff-body":
            try:
                requests = _load_capture_requests(prefer_filtered="--filtered" in sys.argv)
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            result = _diff_body(requests)
            if "--json" in sys.argv:
                _json_output(result, sys.argv)
            else:
                if not result["writes"]:
                    print("(no JSON write requests found)")
                    return 0
                for item in result["writes"]:
                    print(f"[{item['index']}] {item['method']} {item['path']}")
                    if item.get("compare"):
                        print(f"  compare: {item['compare']} request #{item.get('related_get_index')}")
                    if item.get("note"):
                        print(f"  note: {item['note']}")
                    for change in item.get("changes", [])[:40]:
                        old = json.dumps(change.get("old"), ensure_ascii=False)
                        new = json.dumps(change.get("new"), ensure_ascii=False)
                        print(f"  changed {change['path']}: {old} -> {new}")
                    if item.get("changed_candidates"):
                        print("  candidate_fields: " + ", ".join(item["changed_candidates"][:40]))
            return 0

        elif subcmd == "infer-crud":
            try:
                requests = _load_capture_requests(prefer_filtered="--filtered" in sys.argv)
            except Exception as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            result = _infer_crud(requests)
            if "--json" in sys.argv:
                _json_output(result, sys.argv)
            else:
                if not result["flows"]:
                    print("(no CRUD flow inferred)")
                for flow in result["flows"]:
                    print(f"pattern: {flow.get('pattern')}  path: {flow.get('path')}")
                    print(f"  requests: {flow.get('requests')}")
                    if flow.get("full_payload_write_candidate"):
                        print("  warning: full payload write candidate")
                if result.get("warnings"):
                    print("\nWarnings:")
                    for warning in result["warnings"]:
                        print(f"  - {warning}")
            return 0

        else:
            print(
                "Usage: network-capture <start|stop|filter|summary|diff-body|infer-crud|export>\n"
                "  start  [--follow] [--target <t>]          开始抓包\n"
                "  stop   [--body-mode none|filtered|all]    停止并输出请求列表（默认不抓 body）\n"
                "         [--idle-ms N] [--wait-ms N] [--max-bodies N] [--max-body-bytes N]\n"
                "  filter [--method GET] [--url <kw>]        过滤抓包结果\n"
                "         [--exclude-domain <d>] [--status 200]\n"
                "  summary [--filtered] [--json] [--out path] 摘要请求/认证头/请求体 keys\n"
                "  diff-body [--filtered] [--json]            分析写请求体与相关 GET 差异\n"
                "  infer-crud [--filtered] [--json]           推断 CRUD 流程和整包写风险\n"
                "  export [--filtered] [--curl|--python-client] 导出代码（默认 Python）\n"
                "         --python-client  生成完整可运行客户端（含 cookie 获取 + 认证头）",
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
                print('Usage: network fetch <url> [--method POST] [--body \'{"k":"v"}\'] [--headers \'{"k":"v"}\'] [--target <t>]', file=sys.stderr)
                return 1
            ok, reason = _check_allowed_url(sys.argv[3])
            if not ok:
                print(f"Error: {reason}", file=sys.stderr)
                return 1
            req: dict[str, Any] = {"action": "network_fetch", "url": sys.argv[3]}
            if "--method" in sys.argv and sys.argv.index("--method") + 1 < len(sys.argv):
                req["method"] = sys.argv[sys.argv.index("--method") + 1]
            if "--body" in sys.argv and sys.argv.index("--body") + 1 < len(sys.argv):
                req["body"] = sys.argv[sys.argv.index("--body") + 1]
            if "--headers" in sys.argv and sys.argv.index("--headers") + 1 < len(sys.argv):
                try:
                    req["headers"] = json.loads(sys.argv[sys.argv.index("--headers") + 1])
                except json.JSONDecodeError as e:
                    print(f"Error: --headers must be valid JSON: {e}", file=sys.stderr)
                    return 1
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
                _json_output(body, sys.argv)
            else:
                _page_output(body if isinstance(body, str) else str(body), origin=sys.argv[3], argv=sys.argv)
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
                "  fetch <url> [--method POST] [--body '{...}'] [--headers '{...}']  在页面上下文 fetch（带 cookie）\n"
                "  replay [N] [--url ...] [--method ...] [--body ...]  重放抓包的第 N 个请求",
                file=sys.stderr,
            )
            return 1

    elif cmd == "navigate":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: navigate <url> [--target <t>]", file=sys.stderr)
            return 1
        req = {"action": "navigate_page", "url": sys.argv[2]}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=45)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(json.dumps(resp, ensure_ascii=False, indent=2))
        return 0

    elif cmd == "reload":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "reload_page"}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=45)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        print(json.dumps(resp, ensure_ascii=False, indent=2))
        return 0

    elif cmd == "auth":
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""
        if subcmd not in {"material", "token"}:
            print(
                "Usage: auth <material|token>\n"
                "  auth material <url> [--target <t>] [--key <kw>] [--file capture.json] [--reveal]\n"
                "  auth token <request-url> [--method GET|POST] [--body JSON] [--headers JSON]\n"
                "             [--cookie-url <url>] [--extract data.token]\n"
                "             [--header-template 'Authorization=Bearer {token}'] [--reveal] [--insecure]",
                file=sys.stderr,
            )
            return 1
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 4:
            print(f"Usage: auth {subcmd} <url> [options]", file=sys.stderr)
            return 1

        if subcmd == "material":
            req: dict[str, Any] = {
                "action": "auth_material",
                "url": sys.argv[3],
                "target": _cli_option_value(sys.argv, "--target", "active"),
                "key_filter": _cli_option_value(sys.argv, "--key", ""),
                "file": _cli_option_value(sys.argv, "--file", ""),
                "reveal": "--reveal" in sys.argv,
            }
            resp = _send(req, timeout=20)
            _json_output(resp, sys.argv)
            return 0 if resp.get("ok") else 1

        try:
            headers = _parse_json_object(_cli_option_value(sys.argv, "--headers", ""), "--headers")
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        timeout = _cli_flag_int(sys.argv, "--timeout", 30)
        timeout = timeout if timeout > 0 else 30
        req = {
            "action": "auth_token",
            "request_url": sys.argv[3],
            "cookie_url": _cli_option_value(sys.argv, "--cookie-url", ""),
            "method": _cli_option_value(sys.argv, "--method", "GET"),
            "body": _cli_option_value(sys.argv, "--body", ""),
            "headers": headers,
            "extract": _cli_option_value(sys.argv, "--extract", ""),
            "header_templates": _cli_option_values(sys.argv, "--header-template"),
            "timeout": timeout,
            "verify": "--insecure" not in sys.argv,
            "reveal": "--reveal" in sys.argv,
            "target": _cli_option_value(sys.argv, "--target", "active"),
        }
        resp = _send(req, timeout=max(30, timeout + 15))
        _json_output(resp, sys.argv)
        return 0 if resp.get("ok") else 1

    elif cmd == "auth-template":
        if len(sys.argv) < 3:
            print("Usage: auth-template <domain> [--file capture.json]", file=sys.stderr)
            return 1
        domain = sys.argv[2]
        file_arg = ""
        if "--file" in sys.argv and sys.argv.index("--file") + 1 < len(sys.argv):
            file_arg = sys.argv[sys.argv.index("--file") + 1]
        try:
            requests = _load_capture_requests(file_path=file_arg)
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        result = _auth_template_from_requests(requests, domain)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("ok") else 1

    elif cmd == "eval-js":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        if len(sys.argv) < 3:
            print("Usage: eval-js <expression> [--await] [--target <t>]  (alias: eval)", file=sys.stderr)
            return 1
        req: dict[str, Any] = {
            "action": "eval_js",
            "expression": sys.argv[2],
        }
        if "--await" in sys.argv:
            req["await_promise"] = True
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=30)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        result = resp.get("result")
        if isinstance(result, (dict, list)):
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif result is None:
            print("null")
        else:
            print(result)
        return 0

    elif cmd == "capture-headers":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "capture_headers"}
        if "--url-filter" in sys.argv and sys.argv.index("--url-filter") + 1 < len(sys.argv):
            req["url_filter"] = sys.argv[sys.argv.index("--url-filter") + 1]
        if "--wait" in sys.argv and sys.argv.index("--wait") + 1 < len(sys.argv):
            req["wait_sec"] = float(sys.argv[sys.argv.index("--wait") + 1])
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        wait = req.get("wait_sec", 10)
        print(f"Capturing headers for {wait}s... (interact with the page now)", file=sys.stderr)
        resp = _send(req, timeout=float(wait) + 20)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        captured = resp.get("requests", [])
        if not captured:
            print("(no matching requests captured)")
            return 0
        for i, r in enumerate(captured):
            print(f"[{i+1}] {r.get('status','?')} {r.get('method','?'):6s} {r.get('url','')}")
            for k, v in r.get("headers", {}).items():
                print(f"       {k}: {v}")
            print()
        print(f"--- {len(captured)} request(s) ---")
        return 0

    elif cmd == "scan-shortcuts":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        req: dict[str, Any] = {"action": "scan_shortcuts"}
        if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
            req["target"] = sys.argv[sys.argv.index("--target") + 1]
        resp = _send(req, timeout=20)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1
        shortcuts = resp.get("shortcuts", [])
        if not shortcuts:
            print("(no keyboard shortcuts found on page)")
            return 0
        for s in shortcuts:
            print(f"  {s.get('key_hint'):20s}  {s.get('text','')[:60]}  [{s.get('tag','')}@{s.get('attr','')}]")
        print(f"\n--- {len(shortcuts)} shortcut(s) found ---")
        return 0

    elif cmd == "local-storage":
        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""

        def _ls_target() -> str | None:
            if "--target" in sys.argv and sys.argv.index("--target") + 1 < len(sys.argv):
                return sys.argv[sys.argv.index("--target") + 1]
            return None

        def _ls_storage() -> str:
            return "session" if "--session" in sys.argv else "local"

        if subcmd == "get":
            key = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith("--") else ""
            req: dict[str, Any] = {
                "action": "local_storage_get",
                "key": key,
                "storage": _ls_storage(),
            }
            t = _ls_target()
            if t:
                req["target"] = t
            resp = _send(req, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            if key:
                # 单 key 模式
                if not resp.get("found"):
                    print(f"(key '{key}' not found)", file=sys.stderr)
                    return 1
                val = resp.get("value", "")
                parsed = resp.get("parsed")
                if "--json" in sys.argv and parsed is not None:
                    print(json.dumps(parsed, ensure_ascii=False, indent=2))
                else:
                    print(val)
            else:
                # 列出所有
                items = resp.get("items", {})
                if not items:
                    print("(empty)")
                    return 0
                if "--json" in sys.argv:
                    print(json.dumps(items, ensure_ascii=False, indent=2))
                else:
                    for k, v in sorted(items.items()):
                        preview = (v or "")[:80].replace("\n", "\\n")
                        print(f"  {k}  =  {preview}")
                    print(f"\n--- {len(items)} item(s) in {_ls_storage()}Storage ---")
            return 0

        elif subcmd == "set":
            if len(sys.argv) < 5:
                print("Usage: local-storage set <key> <value> [--session] [--target <t>]", file=sys.stderr)
                return 1
            key = sys.argv[3]
            value = sys.argv[4]
            req = {"action": "local_storage_set", "key": key, "value": value, "storage": _ls_storage()}
            t = _ls_target()
            if t:
                req["target"] = t
            resp = _send(req, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Set {_ls_storage()}Storage[{key!r}] = {value[:60]!r}")
            return 0

        elif subcmd == "remove":
            if len(sys.argv) < 4:
                print("Usage: local-storage remove <key> [--session] [--target <t>]", file=sys.stderr)
                return 1
            key = sys.argv[3]
            req = {"action": "local_storage_remove", "key": key, "storage": _ls_storage()}
            t = _ls_target()
            if t:
                req["target"] = t
            resp = _send(req, timeout=10)
            if not resp.get("ok"):
                print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
                return 1
            print(f"Removed {_ls_storage()}Storage[{key!r}]")
            return 0

        else:
            print(
                "Usage: local-storage <get|set|remove>\n"
                "  get [<key>] [--session] [--json] [--target <t>]  读取单个 key（或列出全部）\n"
                "  set <key> <value> [--session] [--target <t>]     写入\n"
                "  remove <key> [--session] [--target <t>]          删除\n"
                "  --session  使用 sessionStorage（默认 localStorage）\n"
                "  --json     输出 JSON 格式（get 时如果 value 本身是 JSON 则解析后格式化输出）",
                file=sys.stderr,
            )
            return 1

    elif cmd == "cookies":
        subcmd = sys.argv[2] if len(sys.argv) > 2 else ""
        if subcmd not in {"get", "inspect", "validate"}:
            print(
                "Usage: cookies <get|inspect|validate> <url> [options]\n"
                "  get <url> [--json|--header|--raw]\n"
                "  --json    输出 {name: value} JSON（默认）\n"
                "  --header  输出 Cookie: a=1; b=2 格式（可直接用于 curl -H）\n"
                "  --raw     输出原始 cookie 列表（含 domain/path/httpOnly 等字段）\n"
                "  inspect <url> [--json]                    输出脱敏 cookie 元信息\n"
                "  validate <url> --expect A,B [--json]      校验 cookie 名称是否存在",
                file=sys.stderr,
            )
            return 1
        if len(sys.argv) < 4:
            print("Usage: cookies <get|inspect|validate> <url> [options]", file=sys.stderr)
            return 1
        url_arg = sys.argv[3]

        if not _daemon_is_running():
            print("CDP daemon not running", file=sys.stderr)
            return 1

        # 按 URL 向 daemon 请求 cookie，避免向客户端返回全域 cookie。
        resp = _send({"action": "get_cookies_for_url", "url": url_arg}, timeout=10)
        if not resp.get("ok"):
            print(f"Error: {resp.get('error', '?')}", file=sys.stderr)
            return 1

        matched_raw: list[dict] = [x for x in resp.get("cookies", []) if isinstance(x, dict)]
        filtered = _cookie_name_map(matched_raw)

        if subcmd == "inspect":
            result = {"ok": True, "url": url_arg, "count": len(matched_raw), "cookies": _redact_cookie(matched_raw)}
            if "--json" in sys.argv:
                _json_output(result, sys.argv)
            else:
                if not matched_raw:
                    print("(no cookies)")
                for item in result["cookies"]:
                    print(
                        f"{item.get('name', '')}\t{item.get('domain', '')}\t"
                        f"{item.get('path', '')}\texpires={item.get('expires', '')}\t"
                        f"httpOnly={item.get('httpOnly', False)} secure={item.get('secure', False)}"
                    )
            return 0

        if subcmd == "validate":
            if "--expect" not in sys.argv or sys.argv.index("--expect") + 1 >= len(sys.argv):
                print("Usage: cookies validate <url> --expect A,B [--json]", file=sys.stderr)
                return 1
            expected = [
                name.strip()
                for name in sys.argv[sys.argv.index("--expect") + 1].split(",")
                if name.strip()
            ]
            if not expected:
                print("--expect 不能为空", file=sys.stderr)
                return 1
            result = {"url": url_arg, **_validate_cookie_names(matched_raw, expected)}
            if "--json" in sys.argv:
                _json_output(result, sys.argv)
            else:
                print(f"present: {', '.join(result['present']) or '(none)'}")
                print(f"missing: {', '.join(result['missing']) or '(none)'}")
                print(f"count: {result['count']}")
            return 0 if result["ok"] else 1

        if not filtered:
            print(f"未读取到 {url_arg} 的 cookie，请确认已在浏览器登录该网站", file=sys.stderr)
            return 1

        # 输出格式
        fmt = "json"
        if "--header" in sys.argv:
            fmt = "header"
        elif "--raw" in sys.argv:
            fmt = "raw"

        if fmt == "header":
            parts = "; ".join(f"{k}={v}" for k, v in filtered.items())
            print(f"Cookie: {parts}")
        elif fmt == "raw":
            print(json.dumps(matched_raw, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(filtered, ensure_ascii=False, indent=2))

        return 0

    aliases = {
        "eval": "eval-js",
    }
    if cmd in aliases:
        print(f"Unknown command: {cmd}. Did you mean '{aliases[cmd]}'?", file=sys.stderr)
        return 1
    close = difflib.get_close_matches(cmd, [
        "snapshot", "reload", "navigate", "discover-api", "capture-action", "capture-flow", "capture-guide",
        "network-capture", "eval-js", "cookies", "target", "auth", "auth-template",
    ], n=3, cutoff=0.6)
    if close:
        print(f"Unknown command: {cmd}. Did you mean: {', '.join(close)}?", file=sys.stderr)
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# 内部通信
# ---------------------------------------------------------------------------
def _send(req: dict, timeout: float = 10) -> dict:
    """通过 Unix Socket 发送单次请求并返回 JSON 响应。"""
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
        line = data.split(b"\n", 1)[0].decode().strip()
        if not line:
            raise RuntimeError("daemon returned empty response")
        return json.loads(line)
    finally:
        sock.close()


def _send_with_retry(req: dict, timeout: float = 10, retries: int = 3, retry_delay: float = 0.2) -> dict:
    """对短暂空响应/启动竞态做有限重试，提升首发命令稳定性。"""
    last_exc: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            return _send(req, timeout=timeout)
        except Exception as exc:
            last_exc = exc
            text = str(exc).lower()
            if attempt >= max(1, retries) - 1:
                break
            if "empty response" not in text and "expecting value" not in text:
                raise
            time.sleep(retry_delay * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError("unknown send retry failure")


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
