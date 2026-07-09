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
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# 路径（与 daemon.py 保持一致）
# ---------------------------------------------------------------------------
DAEMON_BASE_DIR = Path.home() / ".chrome-cdp-daemon"

# daemon.py 的路径（同目录下）
_DAEMON_SCRIPT = str(Path(__file__).resolve().parent / "daemon.py")

# Python 解释器
_PYTHON = os.environ.get("CDP_DAEMON_PYTHON", "") or sys.executable or "python3"


def _sanitize_instance_segment(value: str) -> str:
    """把实例字段转换成稳定路径片段。"""
    text = "".join(
        ch.lower() if ch.isalnum() else "-"
        for ch in str(value or "").strip()
    )
    text = "-".join(part for part in text.split("-") if part)
    return text[:80] or "default"


def _instance_paths(instance: str = "") -> dict[str, str]:
    """按实例返回 daemon 运行时文件路径。"""
    runtime_dir = DAEMON_BASE_DIR / _sanitize_instance_segment(instance or "default")
    return {
        "dir": str(runtime_dir),
        "socket": str(runtime_dir / "cdp.sock"),
        "pid": str(runtime_dir / "cdp.pid"),
        "log": str(runtime_dir / "cdp.log"),
    }


def _discover_instances() -> list[dict[str, Any]]:
    """复用 daemon.py 的实例发现逻辑，避免 SDK 自己猜实例。"""
    try:
        proc = subprocess.run(
            [_PYTHON, _DAEMON_SCRIPT, "instance", "list", "--json"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"调用 daemon.py instance list 失败: {exc}") from exc
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(detail or "实例发现失败")
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception as exc:
        raise RuntimeError(f"实例发现输出不是合法 JSON: {exc}") from exc
    instances = payload.get("instances", [])
    return [item for item in instances if isinstance(item, dict)]


def _resolve_instance(instance: str = "") -> str:
    """解析 SDK 本次操作应使用的实例 ID。"""
    chosen = str(instance or os.environ.get("CHROME_CDP_INSTANCE", "") or "").strip()
    instances = _discover_instances()
    if chosen:
        direct = [item for item in instances if item.get("instance_id") == chosen]
        if direct:
            return str(direct[0]["instance_id"])
        alias_hit = [item for item in instances if chosen in (item.get("aliases") or [])]
        if alias_hit:
            return str(alias_hit[0]["instance_id"])
        if chosen.isdigit():
            by_port = [
                item for item in instances
                if int(item.get("port") or 0) == int(chosen)
            ]
            if len(by_port) == 1:
                return str(by_port[0]["instance_id"])
        paths = _instance_paths(chosen)
        if Path(paths["socket"]).exists() or Path(paths["pid"]).exists():
            return chosen
        raise RuntimeError(f"未找到匹配的 Chrome CDP 实例: {chosen}")
    if not instances:
        raise RuntimeError("未发现可用的 Chrome CDP 实例，请先打开带 CDP 的 Chrome 实例")
    if len(instances) > 1:
        lines = [
            "检测到多个 Chrome CDP 实例，请显式传入 instance 或设置 CHROME_CDP_INSTANCE：",
        ]
        for index, item in enumerate(instances, 1):
            lines.append(
                f"[{index}] {item.get('instance_id', '')} "
                f"port={item.get('port', '')} "
                f"user_data_dir={item.get('user_data_dir', '') or '-'}"
            )
        raise RuntimeError("\n".join(lines))
    return str(instances[0]["instance_id"])


# ---------------------------------------------------------------------------
# 底层通信
# ---------------------------------------------------------------------------
def _send_to_daemon(req: dict, timeout: float = 15, instance: str = "") -> dict:
    """向 daemon 发送一个请求并返回响应。"""
    resolved_instance = _resolve_instance(instance)
    paths = _instance_paths(resolved_instance)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(paths["socket"])
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
    return _daemon_is_running_for_instance("")


def _daemon_is_running_for_instance(instance: str = "") -> bool:
    """判断指定实例的 daemon 是否正在运行。"""
    resolved_instance = _resolve_instance(instance)
    paths = _instance_paths(resolved_instance)
    if not Path(paths["socket"]).exists():
        return False
    try:
        resp = _send_to_daemon({"action": "ping"}, timeout=3, instance=resolved_instance)
        return resp.get("ok", False)
    except Exception:
        # socket 文件存在但连不上，清理残留
        try:
            os.unlink(paths["socket"])
        except FileNotFoundError:
            pass
        try:
            os.unlink(paths["pid"])
        except FileNotFoundError:
            pass
        return False


def _start_daemon(instance: str = "") -> None:
    """启动 daemon 进程。"""
    resolved_instance = _resolve_instance(instance)
    if _daemon_is_running_for_instance(resolved_instance):
        return

    paths = _instance_paths(resolved_instance)
    Path(paths["dir"]).mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["CHROME_CDP_INSTANCE"] = resolved_instance

    # 用 Popen 后台启动，不等待退出
    subprocess.Popen(
        [_PYTHON, _DAEMON_SCRIPT, "start", "--instance", resolved_instance],
        stdout=open(paths["log"], "a"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )

    # 等待就绪
    for _ in range(30):  # 最多等 15 秒
        time.sleep(0.5)
        if _daemon_is_running_for_instance(resolved_instance):
            return
    raise RuntimeError(f"CDP daemon 启动超时，查看日志: {paths['log']}")


def ensure_daemon(auto_start: bool = False, instance: str = "") -> str:
    """确保 daemon 在运行。

    默认不自动启动后台 daemon，避免仅加载 skill/导入 SDK 就触发 Chrome 授权弹窗。
    只有调用方明确传入 auto_start=True 时，才会拉起 daemon。
    """
    resolved_instance = _resolve_instance(instance)
    if not _daemon_is_running_for_instance(resolved_instance):
        if not auto_start:
            raise RuntimeError(
                "CDP daemon 未运行。为避免自动触发 Chrome 授权弹窗，"
                "SDK 默认不启动后台 daemon；请先手动运行 daemon.py start/test，"
                "或在本次显式 CDP 操作中传 auto_start=True。"
            )
        _start_daemon(resolved_instance)
    return resolved_instance


# ---------------------------------------------------------------------------
# 公开 API
# ---------------------------------------------------------------------------
def get_all_cookies(auto_start: bool = False, instance: str = "") -> list[dict]:
    """获取浏览器所有 cookie（跨所有域名）。

    返回原始 cookie 列表，每个元素包含 name, value, domain, path 等字段。

    .. warning::
        此函数返回浏览器全量 cookie（通常 500~1000+ 条）。
        直接将全量 cookie 拼接到 HTTP 请求头会导致 Cookie 头体积过大，
        部分服务器（如 nginx）会返回 400 Bad Request。

        **业务代码中请使用 get_cookies(url)** 只获取目标域名的 cookie，
        而不是直接调用此函数。

        此函数仅适用于 CLI 展示、调试分析等内部场景。
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({"action": "get_cookies"}, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"get_cookies failed: {resp.get('error', '?')}")
    return resp.get("cookies", [])


def get_cookies(url: str, auto_start: bool = False, instance: str = "") -> dict[str, str]:
    """获取指定 URL 域名的 cookie。

    :param url: 目标 URL，如 "https://bdp-cn.tuya-inc.com:7799"
    :param auto_start: daemon 未运行时是否显式自动启动
    :return: {cookie_name: cookie_value} 字典
    :raises RuntimeError: daemon 不可用或无匹配 cookie
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({"action": "get_cookies_for_url", "url": url}, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"get_cookies({url}) failed: {resp.get('error', '?')}")
    raw_cookies = resp.get("cookies", [])
    cookies: dict[str, str] = {}
    for item in raw_cookies:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if name:
            cookies[name] = str(item.get("value", ""))

    if not cookies:
        raise RuntimeError(f"未读取到 {url} 的 cookie，请确认已在浏览器登录")
    return cookies


def get_storage(
    key: str = "",
    storage: str = "local",
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """读取指定页面的 localStorage 或 sessionStorage。

    :param key: 为空时返回全部 key；非空时只返回指定 key
    :param storage: "local" 或 "session"
    :param target: 页面标识，默认 active
    :param auto_start: daemon 未运行时是否显式自动启动
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({
        "action": "local_storage_get",
        "key": key,
        "storage": storage,
        "target": target,
    }, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"get_storage({storage}:{key}) failed: {resp.get('error', '?')}")
    return resp


def get_auth_material(
    url: str,
    target: str = "active",
    key_filter: str = "",
    capture_file: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """汇总目标站点的认证材料，SDK 默认返回真实值供业务代码使用。

    返回包含 cookie、localStorage、sessionStorage 和最近抓包认证 header。
    CLI 为安全默认会脱敏；SDK 面向下游 skill，返回 reveal=True 的结果。
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({
        "action": "auth_material",
        "url": url,
        "target": target,
        "key_filter": key_filter,
        "file": capture_file,
        "reveal": True,
    }, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"get_auth_material({url}) failed: {resp.get('error', '?')}")
    return resp


def request_auth_token(
    request_url: str,
    method: str = "GET",
    body: str = "",
    headers: dict[str, Any] | None = None,
    cookie_url: str = "",
    extract: str = "",
    header_templates: list[str] | None = None,
    timeout: int = 30,
    verify: bool = True,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """用浏览器 cookie 请求任意 token 接口，SDK 默认返回真实 token。

    :param request_url: token 接口 URL
    :param method: HTTP 方法
    :param body: JSON 字符串或普通文本请求体
    :param headers: 额外请求头
    :param cookie_url: cookie 读取 URL；为空时使用 request_url 的 origin
    :param extract: dotted path / $.path，例如 data.token
    :param header_templates: 如 ["Authorization=TUYA {token}"]
    :param verify: requests SSL verify 参数
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({
        "action": "auth_token",
        "request_url": request_url,
        "method": method,
        "body": body,
        "headers": headers or {},
        "cookie_url": cookie_url,
        "extract": extract,
        "header_templates": header_templates or [],
        "timeout": timeout,
        "verify": verify,
        "target": target,
        "reveal": True,
    }, timeout=max(15, timeout + 10), instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"request_auth_token({request_url}) failed: {resp.get('error', '?')}")
    return resp


def cdp_call(
    method: str,
    params: dict[str, Any] | None = None,
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """执行任意 CDP 命令。

    :param method: CDP 方法名，如 "Target.getTargets"
    :param params: CDP 方法参数
    :param auto_start: daemon 未运行时是否显式自动启动
    :return: CDP 响应的 result 字段
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    req: dict[str, Any] = {"action": "cdp_call", "method": method}
    if params:
        req["params"] = params
    resp = _send_to_daemon(req, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"cdp_call({method}) failed: {resp.get('error', '?')}")
    return resp.get("result", {})


def daemon_status(instance: str = "") -> dict:
    """查询 daemon 状态。"""
    resolved_instance = _resolve_instance(instance)
    if not _daemon_is_running_for_instance(resolved_instance):
        return {"running": False}
    resp = _send_to_daemon({"action": "ping"}, timeout=3, instance=resolved_instance)
    return {"running": True, **resp}


def stop_daemon(instance: str = "") -> None:
    """停止 daemon。"""
    resolved_instance = _resolve_instance(instance)
    if not _daemon_is_running_for_instance(resolved_instance):
        return
    try:
        _send_to_daemon({"action": "stop"}, timeout=5, instance=resolved_instance)
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
    instance: str = "",
) -> dict:
    """在指定页面上执行 CDP 命令。

    :param target: 目标页面标识（"active" / targetId / "url:keyword"）
    :param method: CDP 方法名
    :param params: CDP 参数
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    req: dict[str, Any] = {
        "action": "page_call",
        "target": target,
        "method": method,
    }
    if params:
        req["params"] = params
    resp = _send_to_daemon(req, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"page_call({method}) failed: {resp.get('error', '?')}")
    return resp.get("result", {})


def _page_action(action: str, auto_start: bool = False, instance: str = "", **kwargs) -> dict:
    """高级页面动作的统一请求封装。"""
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    req: dict[str, Any] = {"action": action, **kwargs}
    resp = _send_to_daemon(req, timeout=30, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"{action} failed: {resp.get('error', '?')}")
    return resp


def snapshot(
    target: str = "active",
    scope: str | None = None,
    include_cursor: bool = False,
    compact: bool = False,
    depth: int | None = None,
    include_urls: bool = False,
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """获取页面可交互元素快照（带 @eN 引用）。"""
    kw: dict[str, Any] = {"target": target}
    if scope:
        kw["scope"] = scope
    if include_cursor:
        kw["include_cursor"] = True
    if compact:
        kw["compact"] = True
    if depth is not None:
        kw["depth"] = depth
    if include_urls:
        kw["include_urls"] = True
    return _page_action("snapshot", auto_start=auto_start, instance=instance, **kw)


def click(
    ref: str, target: str = "active", auto_start: bool = False,
    dblclick: bool = False, right: bool = False,
    at: tuple[int, int] | None = None,
    instance: str = "",
) -> dict:
    """点击元素（CDP 原生鼠标事件）。"""
    kw: dict = {"ref": ref, "target": target}
    if dblclick:
        kw["dblclick"] = True
    if right:
        kw["right"] = True
    if at:
        kw["at"] = list(at)
    return _page_action("click", auto_start=auto_start, instance=instance, **kw)


def click_text(
    text: str, target: str = "active", tag: str = "",
    nth: int = 1, dblclick: bool = False, right: bool = False,
    auto_start: bool = False,
    instance: str = "",
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
    return _page_action("click_text", auto_start=auto_start, instance=instance, **kw)


def activate(target: str = "active", auto_start: bool = False, instance: str = "") -> dict:
    """将指定 tab 切换到前台。"""
    return _page_action("activate", auto_start=auto_start, instance=instance, target=target)


def open_tab(
    url: str,
    wait_ms: int = 3000,
    activate: bool = True,
    alias: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """打开新标签页；alias 非空时同时绑定为 tab:alias，并自动进入固定分组 CDP自动化。"""
    kw: dict[str, Any] = {"url": url, "wait_ms": wait_ms, "activate": activate}
    if alias:
        kw["alias"] = alias
    return _page_action("open_tab", auto_start=auto_start, instance=instance, **kw)


def close_tab(target: str = "active", auto_start: bool = False, instance: str = "") -> dict:
    """关闭标签页。"""
    return _page_action("close_tab", auto_start=auto_start, instance=instance, target=target)


def tab_bind(name: str, target: str = "active", auto_start: bool = False, instance: str = "") -> dict:
    """给标签页绑定别名；后续可用 target='tab:name' 精确引用。"""
    return _page_action("tab_bind", auto_start=auto_start, instance=instance, name=name, target=target)


def tab_get(name: str, auto_start: bool = False, instance: str = "") -> dict:
    """读取单个标签页别名绑定。"""
    return _page_action("tab_get", auto_start=auto_start, instance=instance, name=name)


def tab_list(auto_start: bool = False, instance: str = "") -> dict:
    """列出当前 daemon 内存中的标签页别名绑定。"""
    return _page_action("tab_list", auto_start=auto_start, instance=instance)


def tab_remove(name: str, auto_start: bool = False, instance: str = "") -> dict:
    """删除标签页别名绑定。"""
    return _page_action("tab_remove", auto_start=auto_start, instance=instance, name=name)


def group_create(
    name: str,
    targets: list[str] | None = None,
    color: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """创建标签页群组。"""
    kw: dict = {"name": name, "color": color}
    if targets:
        kw["targets"] = targets
    return _page_action("group_create", auto_start=auto_start, instance=instance, **kw)


def group_add(name: str, targets: list[str], auto_start: bool = False, instance: str = "") -> dict:
    """向群组添加标签页。"""
    return _page_action("group_add", auto_start=auto_start, instance=instance, name=name, targets=targets)


def group_list(name: str = "", auto_start: bool = False, instance: str = "") -> dict:
    """列出群组。"""
    return _page_action("group_list", auto_start=auto_start, instance=instance, name=name)


def group_close(name: str, auto_start: bool = False, instance: str = "") -> dict:
    """关闭群组（关闭所有标签页）。"""
    return _page_action("group_close", auto_start=auto_start, instance=instance, name=name)


def group_delete(name: str, auto_start: bool = False, instance: str = "") -> dict:
    """删除群组定义（不关闭标签页）。"""
    return _page_action("group_delete", auto_start=auto_start, instance=instance, name=name)


def group_activate(name: str, auto_start: bool = False, instance: str = "") -> dict:
    """切换到群组第一个标签页。"""
    return _page_action("group_activate", auto_start=auto_start, instance=instance, name=name)


def group_move(name: str, targets: list[str], auto_start: bool = False, instance: str = "") -> dict:
    """移入群组（从其它组移出）。"""
    return _page_action("group_move", auto_start=auto_start, instance=instance, name=name, targets=targets)


def group_close_tabs(name: str, targets: list[str], auto_start: bool = False, instance: str = "") -> dict:
    """关闭群组内指定标签页。"""
    return _page_action("group_close_tabs", auto_start=auto_start, instance=instance, name=name, targets=targets)


def fill(
    ref: str,
    text: str,
    clear: bool = True,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """填充 input/textarea。"""
    return _page_action(
        "fill", auto_start=auto_start,
        instance=instance,
        ref=ref, text=text, clear=clear, target=target,
    )


def select(
    ref: str,
    value: str,
    by_label: bool = False,
    search_text: str | None = None,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """选择下拉框选项。

    对 searchable 的自定义下拉，可用 search_text 指定先输入的筛选词；
    不传时会在需要时默认回退为输入 value 本身。
    """
    kw: dict[str, Any] = {
        "ref": ref,
        "value": value,
        "by_label": by_label,
        "target": target,
    }
    if search_text is not None:
        kw["search_text"] = search_text
    return _page_action("select", auto_start=auto_start, instance=instance, **kw)


def check(
    ref: str, target: str = "active", auto_start: bool = False, instance: str = ""
) -> dict:
    """勾选 checkbox。"""
    return _page_action("check", auto_start=auto_start, instance=instance, ref=ref, target=target)


def press(
    key: str,
    ref: str | None = None,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """发送按键事件。"""
    kw: dict[str, Any] = {"key": key, "target": target}
    if ref:
        kw["ref"] = ref
    return _page_action("press", auto_start=auto_start, instance=instance, **kw)


def scroll(
    direction: str = "down",
    amount: int = 500,
    selector: str | None = None,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """滚动页面。"""
    kw: dict[str, Any] = {"direction": direction, "amount": amount, "target": target}
    if selector:
        kw["selector"] = selector
    return _page_action("scroll", auto_start=auto_start, instance=instance, **kw)


def wait_for(
    selector: str | None = None,
    text: str | None = None,
    timeout_ms: int = 10000,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """等待元素或文本出现。"""
    kw: dict[str, Any] = {"timeout_ms": timeout_ms, "target": target}
    if selector:
        kw["selector"] = selector
    if text:
        kw["text"] = text
    return _page_action("wait", auto_start=auto_start, instance=instance, **kw)


def get_text(
    ref: str | None = None,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> str:
    """获取元素或页面文本。"""
    kw: dict[str, Any] = {"target": target}
    if ref:
        kw["ref"] = ref
    resp = _page_action("get_text", auto_start=auto_start, instance=instance, **kw)
    return resp.get("text", "")


def get_url(target: str = "active", auto_start: bool = False, instance: str = "") -> str:
    """获取页面 URL。"""
    resp = _page_action("get_url", auto_start=auto_start, instance=instance, target=target)
    return resp.get("url", "")


def get_title(target: str = "active", auto_start: bool = False, instance: str = "") -> str:
    """获取页面标题。"""
    resp = _page_action("get_title", auto_start=auto_start, instance=instance, target=target)
    return resp.get("title", "")


def screenshot(
    path: str = "",
    target: str = "active",
    annotate: bool = False,
    full_page: bool = False,
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """保存页面截图；annotate=True 时叠加 @ref 编号标注。"""
    return _page_action(
        "screenshot",
        auto_start=auto_start,
        instance=instance,
        path=path,
        target=target,
        annotate=annotate,
        full_page=full_page,
    )


# ---------------------------------------------------------------------------
# 网络抓包 API
# ---------------------------------------------------------------------------
def network_capture_start(
    target: str = "active", auto_start: bool = False, instance: str = ""
) -> dict:
    """开始网络抓包，记录页面发出的 API 请求。"""
    return _page_action("network_capture_start", auto_start=auto_start, instance=instance, target=target)


def network_capture_peek(
    target: str = "active", auto_start: bool = False, instance: str = ""
) -> dict:
    """读取当前抓包快照，不停止抓包也不清空缓冲区。"""
    return _page_action("network_capture_peek", auto_start=auto_start, instance=instance, target=target)


def network_capture_stop(
    target: str = "active",
    get_body: bool = False,
    body_mode: str = "",
    wait_ms: int = 0,
    idle_ms: int = 0,
    max_bodies: int = 0,
    max_body_bytes: int = 0,
    method_filter: str = "",
    url_filter: str = "",
    exclude_domain: str = "",
    status_filter: str = "",
    until_match: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """停止网络抓包，返回捕获的 API 请求列表。

    :param get_body: 兼容旧调用；未显式设置 body_mode 时用它决定是否拉取 body
    :param body_mode: body 抓取策略，支持 none / filtered / all
    :param wait_ms: 固定等待窗口，适合已知慢接口
    :param idle_ms: 网络空闲窗口，适合 SPA 动态请求
    :param max_bodies: 限制最多抓取多少个响应 body
    :param max_body_bytes: 限制 body 大小，超过阈值直接跳过
    :param method_filter: stop 阶段预过滤 body 抓取范围
    :param url_filter: stop 阶段预过滤 body 抓取范围
    :param exclude_domain: stop 阶段排除域名
    :param status_filter: stop 阶段按状态码过滤
    :param until_match: 命中关键字后提前结束等待
    :return: {"ok": True, "count": N, "requests": [...]}
    """
    return _page_action(
        "network_capture_stop", auto_start=auto_start,
        instance=instance,
        target=target,
        get_body=get_body,
        body_mode=body_mode,
        wait_ms=wait_ms,
        idle_ms=idle_ms,
        max_bodies=max_bodies,
        max_body_bytes=max_body_bytes,
        method_filter=method_filter,
        url_filter=url_filter,
        exclude_domain=exclude_domain,
        status_filter=status_filter,
        until_match=until_match,
    )


def network_fetch(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    body: str = "",
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
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
        instance=instance,
        target=target, **req,
    )


def network_replay(
    index: int = 1,
    target: str = "active",
    override_url: str = "",
    override_method: str = "",
    override_body: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """重放抓包文件中的第 N 个请求。

    :param index: 请求序号（从 1 开始）
    :return: {"ok": True, "replayed_index": N, "status": 200, "body": ...}
    """
    return _page_action(
        "network_replay", auto_start=auto_start,
        instance=instance,
        target=target, index=index,
        override_url=override_url,
        override_method=override_method,
        override_body=override_body,
    )


# ---------------------------------------------------------------------------
# Monaco / CodeMirror 编辑器操作
# ---------------------------------------------------------------------------

def editor_get(target: str = "active", auto_start: bool = False, instance: str = "") -> dict:
    """读取 Monaco/CodeMirror 编辑器当前内容。

    :return: {"ok": True, "type": "monaco"|"codemirror5"|"codemirror6"|"textarea", "value": "..."}
    """
    return _page_action("editor_get", auto_start=auto_start, instance=instance, target=target)


def editor_set(
    text: str,
    target: str = "active",
    append: bool = False,
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """设置编辑器内容（整段写入）。Ctrl+A 全选后 Input.insertText 替换。

    :param append: True 时追加到末尾，False 时全选后替换
    :return: {"ok": True, "length": N, "append": bool}
    """
    return _page_action(
        "editor_set", auto_start=auto_start,
        instance=instance,
        target=target, text=text, append=append,
    )


def editor_type(
    text: str,
    target: str = "active",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """在编辑器中逐字符输入（模拟真实打字，触发 autocomplete）。

    :return: {"ok": True, "length": N}
    """
    return _page_action(
        "editor_type", auto_start=auto_start,
        instance=instance,
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
    instance: str = "",
) -> dict:
    """通过 title / aria-label / anticon class 搜索图标按钮。

    :return: {"ok": True, "matches": [...], "count": N}
    """
    return _page_action(
        "find_icon", auto_start=auto_start,
        instance=instance,
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
    instance: str = "",
) -> dict:
    """通过图标属性搜索并点击按钮。

    :return: {"ok": True, "tag": "...", "title": "...", "at": [x, y]}
    """
    return _page_action(
        "click_icon", auto_start=auto_start,
        instance=instance,
        target=target, query=query, region=region,
        nth=nth, dblclick=dblclick, right=right,
    )


def scan_tooltips(
    target: str = "active",
    region: str = "",
    scope: str = "",
    auto_start: bool = False,
    instance: str = "",
) -> dict:
    """扫描区域内图标按钮，逐个 hover 收集 tooltip 文字。

    :param region: 九宫格区域限定（top-left / top / top-right / ...）
    :param scope: CSS 选择器限定扫描范围
    :return: {"ok": True, "buttons": [{"tooltip", "icon", "tag", "x", "y", ...}], "count": N}
    """
    return _page_action(
        "scan_tooltips", auto_start=auto_start,
        instance=instance,
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
    instance: str = "",
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
        instance=instance,
        target=target,
        start_x=start_x, start_y=start_y,
        end_x=end_x, end_y=end_y,
        steps=steps, hold_ms=hold_ms,
    )
