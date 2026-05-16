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

导入本 SDK、查询状态不会启动 daemon，也不会连接 Chrome。
默认情况下，get_cookies/cdp_call 仅连接已运行的 daemon；如需自动启动，显式传 auto_start=True。
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


def ensure_daemon(auto_start: bool = False) -> None:
    """确保 daemon 在运行。

    默认不自动启动后台 daemon，避免仅加载 skill/导入 SDK 就触发 Chrome 授权弹窗。
    只有调用方明确传入 auto_start=True 时，才会拉起 daemon。
    """
    if not _daemon_is_running():
        if not auto_start:
            raise RuntimeError(
                "CDP daemon 未运行。为避免自动触发 Chrome 授权弹窗，"
                "SDK 默认不启动后台 daemon；请先手动运行 daemon.py start/test，"
                "或在本次显式 CDP 操作中传 auto_start=True。"
            )
        _start_daemon()


# ---------------------------------------------------------------------------
# 公开 API
# ---------------------------------------------------------------------------
def get_all_cookies(auto_start: bool = False) -> list[dict]:
    """获取浏览器所有 cookie。

    返回原始 cookie 列表，每个元素包含 name, value, domain, path 等字段。
    """
    ensure_daemon(auto_start=auto_start)
    resp = _send_to_daemon({"action": "get_cookies"})
    if not resp.get("ok"):
        raise RuntimeError(f"get_cookies failed: {resp.get('error', '?')}")
    return resp.get("cookies", [])


def get_cookies(url: str, auto_start: bool = False) -> dict[str, str]:
    """获取指定 URL 域名的 cookie。

    :param url: 目标 URL，如 "https://bdp-cn.tuya-inc.com:7799"
    :param auto_start: daemon 未运行时是否显式自动启动
    :return: {cookie_name: cookie_value} 字典
    :raises RuntimeError: daemon 不可用或无匹配 cookie
    """
    parsed = urllib.parse.urlparse(url)
    target_host = (parsed.hostname or "").strip().lower()
    if not target_host:
        raise ValueError(f"无法解析 URL 域名: {url}")

    all_cookies = get_all_cookies(auto_start=auto_start)

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


def cdp_call(
    method: str,
    params: dict[str, Any] | None = None,
    auto_start: bool = False,
) -> dict:
    """执行任意 CDP 命令。

    :param method: CDP 方法名，如 "Target.getTargets"
    :param params: CDP 方法参数
    :param auto_start: daemon 未运行时是否显式自动启动
    :return: CDP 响应的 result 字段
    """
    ensure_daemon(auto_start=auto_start)
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


# ---------------------------------------------------------------------------
# 页面级 API（高级操作）
# ---------------------------------------------------------------------------
def page_call(
    target: str,
    method: str,
    params: dict[str, Any] | None = None,
    auto_start: bool = False,
) -> dict:
    """在指定页面上执行 CDP 命令。

    :param target: 目标页面标识（"active" / targetId / "url:keyword"）
    :param method: CDP 方法名
    :param params: CDP 参数
    """
    ensure_daemon(auto_start=auto_start)
    req: dict[str, Any] = {
        "action": "page_call",
        "target": target,
        "method": method,
    }
    if params:
        req["params"] = params
    resp = _send_to_daemon(req)
    if not resp.get("ok"):
        raise RuntimeError(f"page_call({method}) failed: {resp.get('error', '?')}")
    return resp.get("result", {})


def _page_action(action: str, auto_start: bool = False, **kwargs) -> dict:
    """高级页面动作的统一请求封装。"""
    ensure_daemon(auto_start=auto_start)
    req: dict[str, Any] = {"action": action, **kwargs}
    resp = _send_to_daemon(req, timeout=30)
    if not resp.get("ok"):
        raise RuntimeError(f"{action} failed: {resp.get('error', '?')}")
    return resp


def snapshot(
    target: str = "active",
    scope: str | None = None,
    include_cursor: bool = False,
    auto_start: bool = False,
) -> dict:
    """获取页面可交互元素快照（带 @eN 引用）。"""
    kw: dict[str, Any] = {"target": target}
    if scope:
        kw["scope"] = scope
    if include_cursor:
        kw["include_cursor"] = True
    return _page_action("snapshot", auto_start=auto_start, **kw)


def click(
    ref: str, target: str = "active", auto_start: bool = False,
    dblclick: bool = False, right: bool = False,
    at: tuple[int, int] | None = None,
) -> dict:
    """点击元素（CDP 原生鼠标事件）。"""
    kw: dict = {"ref": ref, "target": target}
    if dblclick:
        kw["dblclick"] = True
    if right:
        kw["right"] = True
    if at:
        kw["at"] = list(at)
    return _page_action("click", auto_start=auto_start, **kw)


def click_text(
    text: str, target: str = "active", tag: str = "",
    nth: int = 1, dblclick: bool = False, right: bool = False,
    auto_start: bool = False,
) -> dict:
    """通过文本内容查找并点击元素（无需先 snapshot）。"""
    kw: dict = {"text": text, "target": target}
    if tag:
        kw["tag"] = tag
    if nth != 1:
        kw["nth"] = nth
    if dblclick:
        kw["dblclick"] = True
    if right:
        kw["right"] = True
    return _page_action("click_text", auto_start=auto_start, **kw)


def activate(target: str = "active", auto_start: bool = False) -> dict:
    """将指定 tab 切换到前台。"""
    return _page_action("activate", auto_start=auto_start, target=target)


def open_tab(url: str, wait_ms: int = 3000, activate: bool = True, auto_start: bool = False) -> dict:
    """打开新标签页。"""
    return _page_action("open_tab", auto_start=auto_start, url=url, wait_ms=wait_ms, activate=activate)


def close_tab(target: str = "active", auto_start: bool = False) -> dict:
    """关闭标签页。"""
    return _page_action("close_tab", auto_start=auto_start, target=target)


def group_create(name: str, targets: list[str] | None = None, color: str = "", auto_start: bool = False) -> dict:
    """创建标签页群组。"""
    kw: dict = {"name": name, "color": color}
    if targets:
        kw["targets"] = targets
    return _page_action("group_create", auto_start=auto_start, **kw)


def group_add(name: str, targets: list[str], auto_start: bool = False) -> dict:
    """向群组添加标签页。"""
    return _page_action("group_add", auto_start=auto_start, name=name, targets=targets)


def group_list(name: str = "", auto_start: bool = False) -> dict:
    """列出群组。"""
    return _page_action("group_list", auto_start=auto_start, name=name)


def group_close(name: str, auto_start: bool = False) -> dict:
    """关闭群组（关闭所有标签页）。"""
    return _page_action("group_close", auto_start=auto_start, name=name)


def group_delete(name: str, auto_start: bool = False) -> dict:
    """删除群组定义（不关闭标签页）。"""
    return _page_action("group_delete", auto_start=auto_start, name=name)


def group_activate(name: str, auto_start: bool = False) -> dict:
    """切换到群组第一个标签页。"""
    return _page_action("group_activate", auto_start=auto_start, name=name)


def group_move(name: str, targets: list[str], auto_start: bool = False) -> dict:
    """移入群组（从其它组移出）。"""
    return _page_action("group_move", auto_start=auto_start, name=name, targets=targets)


def group_close_tabs(name: str, targets: list[str], auto_start: bool = False) -> dict:
    """关闭群组内指定标签页。"""
    return _page_action("group_close_tabs", auto_start=auto_start, name=name, targets=targets)


def fill(
    ref: str,
    text: str,
    clear: bool = True,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """填充 input/textarea。"""
    return _page_action(
        "fill", auto_start=auto_start,
        ref=ref, text=text, clear=clear, target=target,
    )


def select(
    ref: str,
    value: str,
    by_label: bool = False,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """选择下拉框选项。"""
    return _page_action(
        "select", auto_start=auto_start,
        ref=ref, value=value, by_label=by_label, target=target,
    )


def check(
    ref: str, target: str = "active", auto_start: bool = False
) -> dict:
    """勾选 checkbox。"""
    return _page_action("check", auto_start=auto_start, ref=ref, target=target)


def press(
    key: str,
    ref: str | None = None,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """发送按键事件。"""
    kw: dict[str, Any] = {"key": key, "target": target}
    if ref:
        kw["ref"] = ref
    return _page_action("press", auto_start=auto_start, **kw)


def scroll(
    direction: str = "down",
    amount: int = 500,
    selector: str | None = None,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """滚动页面。"""
    kw: dict[str, Any] = {"direction": direction, "amount": amount, "target": target}
    if selector:
        kw["selector"] = selector
    return _page_action("scroll", auto_start=auto_start, **kw)


def wait_for(
    selector: str | None = None,
    text: str | None = None,
    timeout_ms: int = 10000,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """等待元素或文本出现。"""
    kw: dict[str, Any] = {"timeout_ms": timeout_ms, "target": target}
    if selector:
        kw["selector"] = selector
    if text:
        kw["text"] = text
    return _page_action("wait", auto_start=auto_start, **kw)


def get_text(
    ref: str | None = None,
    target: str = "active",
    auto_start: bool = False,
) -> str:
    """获取元素或页面文本。"""
    kw: dict[str, Any] = {"target": target}
    if ref:
        kw["ref"] = ref
    resp = _page_action("get_text", auto_start=auto_start, **kw)
    return resp.get("text", "")


def get_url(target: str = "active", auto_start: bool = False) -> str:
    """获取页面 URL。"""
    resp = _page_action("get_url", auto_start=auto_start, target=target)
    return resp.get("url", "")


def get_title(target: str = "active", auto_start: bool = False) -> str:
    """获取页面标题。"""
    resp = _page_action("get_title", auto_start=auto_start, target=target)
    return resp.get("title", "")


# ---------------------------------------------------------------------------
# 网络抓包 API
# ---------------------------------------------------------------------------
def network_capture_start(
    target: str = "active", auto_start: bool = False
) -> dict:
    """开始网络抓包，记录页面发出的 API 请求。"""
    return _page_action("network_capture_start", auto_start=auto_start, target=target)


def network_capture_stop(
    target: str = "active",
    get_body: bool = False,
    auto_start: bool = False,
) -> dict:
    """停止网络抓包，返回捕获的 API 请求列表。

    :param get_body: 是否同时获取响应 body
    :return: {"ok": True, "count": N, "requests": [...]}
    """
    return _page_action(
        "network_capture_stop", auto_start=auto_start,
        target=target, get_body=get_body,
    )


def network_fetch(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    body: str = "",
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """在页面上下文中执行 fetch（自动带 cookie）。

    :return: {"ok": True, "status": 200, "statusText": "OK", "headers": {...}, "body": ...}
    """
    req: dict = {"url": url, "method": method}
    if headers:
        req["headers"] = headers
    if body:
        req["body"] = body
    return _page_action(
        "network_fetch", auto_start=auto_start,
        target=target, **req,
    )


def network_replay(
    index: int = 1,
    target: str = "active",
    override_url: str = "",
    override_method: str = "",
    override_body: str = "",
    auto_start: bool = False,
) -> dict:
    """重放抓包文件中的第 N 个请求。

    :param index: 请求序号（从 1 开始）
    :return: {"ok": True, "replayed_index": N, "status": 200, "body": ...}
    """
    return _page_action(
        "network_replay", auto_start=auto_start,
        target=target, index=index,
        override_url=override_url,
        override_method=override_method,
        override_body=override_body,
    )


# ---------------------------------------------------------------------------
# Monaco / CodeMirror 编辑器操作
# ---------------------------------------------------------------------------

def editor_get(target: str = "active", auto_start: bool = False) -> dict:
    """读取 Monaco/CodeMirror 编辑器当前内容。

    :return: {"ok": True, "type": "monaco"|"codemirror5"|"codemirror6"|"textarea", "value": "..."}
    """
    return _page_action("editor_get", auto_start=auto_start, target=target)


def editor_set(
    text: str,
    target: str = "active",
    append: bool = False,
    auto_start: bool = False,
) -> dict:
    """设置编辑器内容（整段写入）。Ctrl+A 全选后 Input.insertText 替换。

    :param append: True 时追加到末尾，False 时全选后替换
    :return: {"ok": True, "length": N, "append": bool}
    """
    return _page_action(
        "editor_set", auto_start=auto_start,
        target=target, text=text, append=append,
    )


def editor_type(
    text: str,
    target: str = "active",
    auto_start: bool = False,
) -> dict:
    """在编辑器中逐字符输入（模拟真实打字，触发 autocomplete）。

    :return: {"ok": True, "length": N}
    """
    return _page_action(
        "editor_type", auto_start=auto_start,
        target=target, text=text,
    )


# ---------------------------------------------------------------------------
# 图标搜索 / 点击
# ---------------------------------------------------------------------------

def find_icon(
    query: str,
    target: str = "active",
    region: str = "",
    auto_start: bool = False,
) -> dict:
    """通过 title / aria-label / anticon class 搜索图标按钮。

    :return: {"ok": True, "matches": [...], "count": N}
    """
    return _page_action(
        "find_icon", auto_start=auto_start,
        target=target, query=query, region=region,
    )


def click_icon(
    query: str,
    target: str = "active",
    region: str = "",
    nth: int = 1,
    dblclick: bool = False,
    right: bool = False,
    auto_start: bool = False,
) -> dict:
    """通过图标属性搜索并点击按钮。

    :return: {"ok": True, "tag": "...", "title": "...", "at": [x, y]}
    """
    return _page_action(
        "click_icon", auto_start=auto_start,
        target=target, query=query, region=region,
        nth=nth, dblclick=dblclick, right=right,
    )


def scan_tooltips(
    target: str = "active",
    region: str = "",
    scope: str = "",
    auto_start: bool = False,
) -> dict:
    """扫描区域内图标按钮，逐个 hover 收集 tooltip 文字。

    :param region: 九宫格区域限定（top-left / top / top-right / ...）
    :param scope: CSS 选择器限定扫描范围
    :return: {"ok": True, "buttons": [{"tooltip", "icon", "tag", "x", "y", ...}], "count": N}
    """
    return _page_action(
        "scan_tooltips", auto_start=auto_start,
        target=target, region=region, scope=scope,
    )


# ---------------------------------------------------------------------------
# 拖拽
# ---------------------------------------------------------------------------

def drag(
    start_x: int,
    start_y: int,
    end_x: int,
    end_y: int,
    target: str = "active",
    steps: int = 10,
    hold_ms: int = 100,
    auto_start: bool = False,
) -> dict:
    """鼠标拖拽操作。

    :param start_x, start_y: 起点坐标
    :param end_x, end_y: 终点坐标
    :param steps: 移动分几步（越多越平滑）
    :param hold_ms: 按下后等待毫秒数（长按识别）
    :return: {"ok": True, "from": [sx, sy], "to": [ex, ey], "steps": N}
    """
    return _page_action(
        "drag", auto_start=auto_start,
        target=target,
        start_x=start_x, start_y=start_y,
        end_x=end_x, end_y=end_y,
        steps=steps, hold_ms=hold_ms,
    )
