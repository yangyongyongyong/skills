"""
jupyterm_cdp — Chrome DevTools Protocol 层。

包含：
  - jscmd daemon 通信
  - 直连 CDP HTTP 探测（fallback）
  - Jupyter 页面 WebSocket 查找
  - Terminal / Notebook 浏览器 tab 切换
  - Notebook 单元格 UI 操作（按钮点击、内容写入、输出读取）
"""

import asyncio
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets.legacy.client import connect as ws_connect

# CDP 默认扫描端口范围（仅 direct HTTP fallback 使用）
DEFAULT_CDP_PORTS = [9222, 9223, 9224, 9225, 9226, 9227, 9228, 9229, 9230]
CDP_HOST_CANDIDATES = ["127.0.0.1", "::1", "localhost"]

# 判定 Jupyter 页面的 URL 关键字
JUPYTER_URL_KEYWORDS = ["/lab", "/tree", "/user/", "jupyter", "notebook"]

# jscmd daemon Unix socket 路径（共享 CDP 网关）
JSCMD_SOCK = os.path.expanduser("~/.jscmd.sock")

# command_id → 工具栏按钮 title 关键字 或特殊处理标记
_NB_CMD_TITLE_MAP = {
    "docmanager:save":                    "Save",
    "notebook:run-cell-and-select-next":  "Run this cell",
    "notebook:interrupt-kernel":          "Interrupt the kernel",
    "notebook:restart-kernel":            "Restart the kernel",
    "notebook:restart-run-all":           "Restart the kernel and run all",
    # insert/delete/cut/copy 都用活跃 cell 自身的 action toolbar 按钮，
    # 避免 _nb_ui_click_button_by_title 误匹配到其他 cell 的同名按钮
    "notebook:insert-cell-below":         "_js_active_cell_btn:Insert a cell below",
    "notebook:insert-cell-above":         "_js_active_cell_btn:Insert a cell above",
    "notebook:delete-cell":               "_js_delete_cell",
    "notebook:cut-cell":                  "_js_active_cell_btn:Cut this cell",
    "notebook:copy-cell":                 "_js_active_cell_btn:Copy this cell",
    "notebook:paste-cell-below":          "Paste this cell",
    "notebook:move-cell-up":              "_keyboard_move_up",
    "notebook:move-cell-down":            "_keyboard_move_down",
    "notebook:change-cell-to-code":       "_select_type_Code",
    "notebook:change-cell-to-markdown":   "_select_type_Markdown",
    "notebook:change-cell-to-raw":        "_select_type_Raw",
}


# ---------------------------------------------------------------------------
# jscmd daemon 通信（共享 CDP 网关）
# ---------------------------------------------------------------------------

def _send_to_jscmd(req: dict, timeout: float = 10.0) -> dict:
    """通过 Unix socket 向 jscmd daemon 发送请求并接收响应。

    @param[in] req     请求 dict（含 cmd 字段）
    @param[in] timeout 超时秒数
    @return 响应 dict
    @raises Exception  daemon 不可用或通信失败
    """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(JSCMD_SOCK)
    sock.sendall(json.dumps(req).encode())
    sock.shutdown(socket.SHUT_WR)
    chunks = []
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        chunks.append(chunk)
    sock.close()
    return json.loads(b"".join(chunks).decode())


def _daemon_cdp_eval(target_id: str, expression: str):
    """通过 jscmd daemon 在指定 page target 上执行 JS 表达式。

    @param[in] target_id  Chrome page target id
    @param[in] expression JS 表达式
    @return 表达式返回值，或 None
    """
    try:
        resp = _send_to_jscmd({
            "cmd": "cdp_eval",
            "target_id": target_id,
            "expression": expression,
        })
        if resp.get("ok"):
            return resp.get("value")
        return None
    except Exception:
        return None


def _detect_via_daemon() -> tuple:
    """通过 jscmd daemon 的 cdp_pages / cdp_eval 发现 Jupyter 页面。

    @return (url, token, status)  status: "ok" | "not_jupyter" | "no_cdp"
    @raises Exception  daemon 不可用
    """
    resp = _send_to_jscmd({"cmd": "cdp_pages"})
    if not resp.get("ok"):
        raise RuntimeError(resp.get("error", "cdp_pages 失败"))

    pages = resp.get("pages", [])
    if not pages:
        raise RuntimeError("daemon 返回 0 个 page target")

    print(f"[jupyterm] 通过 jscmd daemon 发现 {len(pages)} 个页面标签",
          file=sys.stderr)

    # 仅按当前活跃页判断：selected 页必须是 Jupyter，否则直接报 not_jupyter。
    # 不再后台搜索“其他 Jupyter 页”，避免与用户当前焦点不一致。
    selected_page = None
    for page in pages:
        target_id = page.get("id", "")
        if not target_id:
            continue
        state = _daemon_cdp_eval(target_id, "document.visibilityState")
        if state == "visible":
            selected_page = page
            break

    if not selected_page:
        return None, None, "no_cdp"

    page_url = selected_page.get("url", "")
    if not any(kw in page_url for kw in JUPYTER_URL_KEYWORDS):
        return page_url, None, "not_jupyter"

    target_id = selected_page.get("id", "")

    # 从页面 DOM 提取 token
    js_token = ("(() => { "
                "  const el = document.getElementById('jupyter-config-data'); "
                "  if (!el) return ''; "
                "  try { return JSON.parse(el.textContent).token || ''; } "
                "  catch(e) { return ''; } "
                "})()")
    token = _daemon_cdp_eval(target_id, js_token)
    if not token:
        parsed = urllib.parse.urlparse(page_url)
        qs = urllib.parse.parse_qs(parsed.query)
        token = qs.get("token", [""])[0]

    return page_url, token, "ok"


# ---------------------------------------------------------------------------
# CDP 直连探测（fallback：daemon 不可用时使用）
# ---------------------------------------------------------------------------

def _normalize_host_for_url(host: str) -> str:
    """规范化 host 字符串，IPv6 自动补 [] 以便拼接 URL。"""
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _build_http_url(host: str, port: int, path: str) -> str:
    """构造 HTTP URL，统一处理 IPv4/IPv6 host 格式。"""
    return f"http://{_normalize_host_for_url(host)}:{port}{path}"


def _build_ws_url(host: str, port: int, path: str) -> str:
    """构造 WS URL，统一处理 IPv4/IPv6 host 格式。"""
    return f"ws://{_normalize_host_for_url(host)}:{port}{path}"


def _read_json_via_hosts(port: int, path: str, timeout: float = 1.0):
    """按候选 host 轮询读取 JSON，返回 (json_data, host) 或 (None, None)。"""
    for host in CDP_HOST_CANDIDATES:
        try:
            req = urllib.request.Request(
                _build_http_url(host, port, path),
                headers={"Host": "localhost"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read()), host
        except Exception:
            continue
    return None, None


def _normalize_ws_url_with_port(ws_url: str, port: int) -> str:
    """规范化 page/browser websocket URL，缺失端口时自动补齐当前 CDP 端口。"""
    parsed = urllib.parse.urlparse(ws_url)
    if parsed.scheme not in ("ws", "wss") or not parsed.hostname:
        return ws_url
    if parsed.port is not None:
        return ws_url
    path = parsed.path or "/"
    return _build_ws_url(parsed.hostname, port, path)


def _cdp_get_pages(port: int) -> list:
    """获取指定 CDP 端口上的所有 page 类型 targets，失败返回空列表。"""
    targets, _ = _read_json_via_hosts(port, "/json", timeout=1.0)
    if isinstance(targets, list):
        return [t for t in targets if t.get("type") == "page"]
    return []


def _resolve_ws_from_json_version(ws_url: str) -> str | None:
    """当 WS 路径失效(404)时，通过同 host/port 的 /json/version 修复最新地址。"""
    parsed = urllib.parse.urlparse(ws_url)
    if parsed.scheme not in ("ws", "wss") or not parsed.hostname or parsed.port is None:
        return None
    try:
        req = urllib.request.Request(
            _build_http_url(parsed.hostname, parsed.port, "/json/version"),
            headers={"Host": "localhost"}
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            data = json.loads(resp.read())
        recovered = data.get("webSocketDebuggerUrl", "")
        if isinstance(recovered, str) and recovered.startswith("ws://"):
            parsed_recovered = urllib.parse.urlparse(recovered)
            # 某些环境会返回不带端口的 ws://localhost/devtools/...，需要补齐原端口。
            if parsed_recovered.port is None and parsed_recovered.hostname:
                return _build_ws_url(
                    parsed_recovered.hostname,
                    parsed.port,
                    parsed_recovered.path or "/",
                )
            return recovered
    except Exception:
        return None
    return None


async def _cdp_evaluate(ws_url: str, expression: str, timeout: float = 3.0):
    """通过 CDP WebSocket 在指定页面执行 JS 表达式，返回结果值或 None。"""
    async def _eval_once(url: str):
        async with ws_connect(url) as ws:
            msg = json.dumps({
                "id": 1,
                "method": "Runtime.evaluate",
                "params": {"expression": expression, "returnByValue": True}
            })
            await ws.send(msg)
            resp_raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            resp = json.loads(resp_raw)
            result = resp.get("result", {}).get("result", {})
            if result.get("type") in ("string", "boolean", "number"):
                return result.get("value")
            return None

    try:
        return await _eval_once(ws_url)
    except Exception as exc:
        # 关键兼容：DevToolsActivePort 的 ws path 偶尔短时过期，404 时自动修复并重试一次。
        if "HTTP 404" not in str(exc):
            return None
        recovered = _resolve_ws_from_json_version(ws_url)
        if not recovered or recovered == ws_url:
            return None
        try:
            return await _eval_once(recovered)
        except Exception:
            return None


async def _find_selected_jupyter_page(pages: list, port: int) -> tuple:
    """在给定的 page targets 中找到当前 selected 的 Jupyter 页（direct HTTP fallback 用）。

    @return (url, ws_debugger_url, status)  status: "ok" | "not_jupyter" | "no_visible"
    """
    visible_pages = []
    for page in pages:
        ws_url = page.get("webSocketDebuggerUrl", "")
        if not ws_url:
            continue
        ws_url = _normalize_ws_url_with_port(ws_url, port)
        state = await _cdp_evaluate(ws_url, "document.visibilityState")
        if state == "visible":
            page = dict(page)
            page["webSocketDebuggerUrl"] = ws_url
            visible_pages.append(page)

    if not visible_pages:
        return None, None, "no_visible"

    for page in visible_pages:
        url = page.get("url", "")
        if any(kw in url for kw in JUPYTER_URL_KEYWORDS):
            return url, page.get("webSocketDebuggerUrl", ""), "ok"

    return visible_pages[0].get("url", ""), None, "not_jupyter"


async def _extract_token_from_page(ws_url: str, page_url: str) -> str:
    """从页面 DOM 或 URL 参数提取 JupyterLab token（direct HTTP fallback 用）。"""
    js = ("(() => { "
          "  const el = document.getElementById('jupyter-config-data'); "
          "  if (!el) return ''; "
          "  try { return JSON.parse(el.textContent).token || ''; } catch(e) { return ''; } "
          "})()")
    token = await _cdp_evaluate(ws_url, js)
    if token:
        return token
    parsed = urllib.parse.urlparse(page_url)
    qs = urllib.parse.parse_qs(parsed.query)
    return qs.get("token", [""])[0]


def _detect_via_direct_http(ports: list = None) -> tuple:
    """直连 Chrome CDP HTTP 端口发现 Jupyter 页面（daemon 不可用时的 fallback）。

    @param[in] ports 要扫描的端口列表
    @return (url, token, status)
    """
    if ports is None:
        ports = DEFAULT_CDP_PORTS

    async def _run():
        for port in ports:
            pages = _cdp_get_pages(port)
            if not pages:
                continue
            print(f"[jupyterm] 发现 CDP 实例 端口 {port}，共 {len(pages)} 个页面标签",
                  file=sys.stderr)
            url, ws_url, status = await _find_selected_jupyter_page(pages, port)
            if status == "ok":
                token = await _extract_token_from_page(ws_url, url)
                return url, token, "ok"
            elif status == "not_jupyter":
                return url, None, "not_jupyter"
        return None, None, "no_cdp"

    return asyncio.run(_run())


def detect_from_cdp(ports: list = None) -> tuple:
    """发现浏览器当前活动的 Jupyter 标签页，提取 URL 和 token。

    优先通过 jscmd daemon（共享 CDP 网关）探测，daemon 不可用时 fallback
    到直连 Chrome CDP HTTP 端口。

    @param[in] ports fallback 时扫描的端口列表
    @return (url, token, status)  status: "ok" | "not_jupyter" | "no_cdp"
    """
    if os.path.exists(JSCMD_SOCK):
        try:
            print("[jupyterm] 尝试通过 jscmd daemon 探测...", file=sys.stderr)
            return _detect_via_daemon()
        except Exception as e:
            print(f"[jupyterm] daemon 不可用（{e}），回退到直连 HTTP",
                  file=sys.stderr)

    return _detect_via_direct_http(ports)


def build_config_from_url(url: str, token: str) -> dict:
    """根据浏览器页面 URL 和 token 构建配置字典。

    支持任意部署地址，从页面 URL 中提取 scheme://host:port 和 base_path。
    示例：
      http://remote:9999/user/admin/lab  → base_url=http://remote:9999/user/admin
      http://localhost:8889/lab          → base_url=http://localhost:8889
    """
    parsed = urllib.parse.urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    path = parsed.path
    for suffix in ["/lab", "/tree", "/notebooks", "/terminals"]:
        idx = path.find(suffix)
        if idx != -1:
            path = path[:idx]
            break
    base_path = path.rstrip("/")

    base_url = f"{origin}{base_path}"
    ws_scheme = "wss" if parsed.scheme == "https" else "ws"
    ws_base = f"{ws_scheme}://{parsed.netloc}{base_path}"

    return {
        "base_url": base_url,
        "ws_base": ws_base,
        "token": token,
        "source": "browser",
    }


# ---------------------------------------------------------------------------
# 浏览器可见 Terminal / Notebook 探测
# ---------------------------------------------------------------------------

def _find_jupyter_page_ws() -> str:
    """在 CDP 端口上找到 JupyterLab 页面的 page-level WebSocket URL。

    @return WebSocket debugger URL，找不到返回空串
    """
    for port in DEFAULT_CDP_PORTS:
        pages = _cdp_get_pages(port)
        for p in pages:
            url = p.get("url", "")
            if any(kw in url for kw in JUPYTER_URL_KEYWORDS):
                ws = p.get("webSocketDebuggerUrl", "")
                if ws:
                    # 保留 Chrome 返回的原始 websocket 地址，避免破坏 IPv6/host 语义。
                    return _normalize_ws_url_with_port(ws, port)
    return ""


def get_browser_visible_terminals() -> list:
    """通过 CDP 查询浏览器中 JupyterLab 实际打开的 terminal tab（按从左到右 DOM 顺序）。

    @return 有序 terminal 信息列表 [{"name": "12", "current": True}, ...]，
            或 None（CDP 不可用）
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return None

    js = """JSON.stringify(
        Array.from(document.querySelectorAll('li[role="tab"]'))
            .filter(t => /^Terminal\\s+\\d+$/.test(t.textContent.trim()))
            .map(t => ({
                name: t.textContent.trim().replace('Terminal ', ''),
                current: t.classList.contains('jp-mod-current')
                         || t.classList.contains('lm-mod-current')
            }))
    )"""

    async def _run():
        try:
            result = await _cdp_evaluate(ws_url, js, timeout=3.0)
            if result:
                return json.loads(result)
        except Exception:
            pass
        return None

    return asyncio.run(_run())


def switch_to_terminal_tab(position: int) -> bool:
    """通过 CDP 模拟真实鼠标点击切换 JupyterLab 第 position 个 terminal tab（1-based）。

    JupyterLab 使用 Lumino 框架，必须通过 CDP Input.dispatchMouseEvent 发送
    浏览器级鼠标事件，JS 合成事件（element.click()）无效。

    @param[in] position 1-based 位置编号
    @return True 切换成功，False 失败
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return False

    coord_js = f"""(() => {{
        const tabs = Array.from(document.querySelectorAll('li[role="tab"]'))
            .filter(t => /^Terminal\\s+\\d+$/.test(t.textContent.trim()));
        const target = tabs[{position - 1}];
        if (!target) return JSON.stringify({{err:'not_found'}});
        const rect = target.getBoundingClientRect();
        return JSON.stringify({{x: rect.x + rect.width/2, y: rect.y + rect.height/2}});
    }})()"""

    async def _run():
        try:
            async with ws_connect(ws_url) as ws:
                msg_id = 0

                async def cdp_send(method, params=None):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": method}
                    if params:
                        m["params"] = params
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                        if r.get("id") == msg_id:
                            return r

                resp = await cdp_send("Runtime.evaluate",
                                      {"expression": coord_js, "returnByValue": True})
                val = resp.get("result", {}).get("result", {}).get("value", "")
                info = json.loads(val)
                if "err" in info:
                    return False

                x, y = info["x"], info["y"]
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mousePressed", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mouseReleased", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                return True
        except Exception:
            return False

    ok = asyncio.run(_run())
    if ok:
        time.sleep(0.3)
    return ok


def get_browser_visible_notebooks() -> list:
    """通过 CDP 查询浏览器中 JupyterLab 实际打开的 .ipynb tab（按从左到右 DOM 顺序）。

    @return 有序 notebook 信息列表 [{"name": "x.ipynb", "path": "...", "current": True}, ...]，
            或 None（CDP 不可用）
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return None

    js = """JSON.stringify(
        Array.from(document.querySelectorAll('li[role="tab"]'))
            .filter(t => {
                const text = t.textContent.trim();
                const title = t.getAttribute('title') || '';
                return text.endsWith('.ipynb') || title.includes('.ipynb');
            })
            .map(t => {
                const text = t.textContent.trim();
                const title = t.getAttribute('title') || '';
                const pathMatch = title.match(/Path:\\s*(.+?)(?:\\n|$)/);
                const tooltipPath = pathMatch ? pathMatch[1].trim() : text;
                const path = text.endsWith('.ipynb') ? text : tooltipPath;
                return {
                    name: text,
                    path: path,
                    current: t.classList.contains('jp-mod-current')
                             || t.classList.contains('lm-mod-current')
                };
            })
    )"""

    async def _run():
        try:
            result = await _cdp_evaluate(ws_url, js, timeout=3.0)
            if result:
                return json.loads(result)
        except Exception:
            pass
        return None

    return asyncio.run(_run())


def switch_to_notebook_tab(position: int) -> bool:
    """通过 CDP 模拟真实鼠标点击切换 JupyterLab 第 position 个 notebook tab（1-based）。

    @param[in] position 1-based 位置编号
    @return True 切换成功，False 失败
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return False

    coord_js = f"""(() => {{
        const tabs = Array.from(document.querySelectorAll('li[role="tab"]'))
            .filter(t => {{
                const text = t.textContent.trim();
                const title = t.getAttribute('title') || '';
                return text.endsWith('.ipynb') || title.endsWith('.ipynb');
            }});
        const target = tabs[{position - 1}];
        if (!target) return JSON.stringify({{err:'not_found'}});
        const rect = target.getBoundingClientRect();
        return JSON.stringify({{x: rect.x + rect.width/2, y: rect.y + rect.height/2}});
    }})()"""

    async def _run():
        try:
            async with ws_connect(ws_url) as ws:
                msg_id = 0

                async def cdp_send(method, params=None):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": method}
                    if params:
                        m["params"] = params
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=3.0))
                        if r.get("id") == msg_id:
                            return r

                resp = await cdp_send("Runtime.evaluate",
                                      {"expression": coord_js, "returnByValue": True})
                val = resp.get("result", {}).get("result", {}).get("value", "")
                info = json.loads(val)
                if "err" in info:
                    return False

                x, y = info["x"], info["y"]
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mousePressed", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mouseReleased", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                return True
        except Exception:
            return False

    ok = asyncio.run(_run())
    if ok:
        time.sleep(0.3)
    return ok


# ---------------------------------------------------------------------------
# Notebook UI CDP helpers（所见即所得）
# ---------------------------------------------------------------------------

def _nb_ui_run_js(js_code: str, timeout: float = 5.0):
    """在 JupyterLab 页面中执行任意 JS，返回结果字符串。

    通过 CDP Runtime.evaluate 调用，找不到页面时返回 None。

    @param[in] js_code  要执行的 JS 表达式
    @param[in] timeout  超时秒数
    @return JS 返回值字符串，失败返回 None
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return None

    async def _run():
        try:
            return await _cdp_evaluate(ws_url, js_code, timeout=timeout)
        except Exception:
            return None

    return asyncio.run(_run())


def _nb_ui_click_button_by_title(title_substr: str,
                                  ws_url: str = None) -> bool:
    """在 JupyterLab 页面中找到 title 包含 title_substr 的按钮并模拟点击。

    使用 CDP Input.dispatchMouseEvent，确保 Lumino 框架正确响应。
    仅在 jp-button / button / [role=button] 中查找，避免误匹配非按钮元素。

    @param[in] title_substr  按钮 title 属性关键字（部分匹配）
    @param[in] ws_url        页面 WebSocket URL（None 时自动探测）
    @return True 点击成功，False 失败
    """
    if ws_url is None:
        ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return False

    coord_js = f"""(() => {{
        const selectors = 'jp-button, button, [role="button"]';
        const btn = Array.from(document.querySelectorAll(selectors))
            .find(el => {{
                const t = el.getAttribute('title') || '';
                const rect = el.getBoundingClientRect();
                return t.includes({json.dumps(title_substr)}) && rect.width > 0 && rect.height > 0;
            }});
        if (!btn) return JSON.stringify({{err: 'not_found', searched: {json.dumps(title_substr)}}});
        const rect = btn.getBoundingClientRect();
        return JSON.stringify({{x: rect.x + rect.width/2, y: rect.y + rect.height/2, title: btn.getAttribute('title')}});
    }})()"""

    async def _run():
        try:
            async with ws_connect(ws_url) as ws:
                msg_id = 0

                async def cdp_send(method, params=None):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": method}
                    if params:
                        m["params"] = params
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                        if r.get("id") == msg_id:
                            return r

                resp = await cdp_send("Runtime.evaluate",
                                      {"expression": coord_js, "returnByValue": True})
                val = resp.get("result", {}).get("result", {}).get("value", "")
                info = json.loads(val)
                if "err" in info:
                    return False

                x, y = info["x"], info["y"]
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mousePressed", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                await cdp_send("Input.dispatchMouseEvent", {
                    "type": "mouseReleased", "x": x, "y": y,
                    "button": "left", "clickCount": 1
                })
                return True
        except Exception:
            return False

    return asyncio.run(_run())


def _nb_ui_set_select_value(select_title: str, value: str) -> bool:
    """通过 JS 修改 SELECT 元素的值并触发 change 事件（用于 cell 类型切换）。

    @param[in] select_title  SELECT 元素的 title 属性值（部分匹配）
    @param[in] value         目标 option 值（如 "Code", "Markdown", "Raw"）
    @return True 成功，False 失败
    """
    js = f"""(() => {{
        const sel = Array.from(document.querySelectorAll('select[title]'))
            .find(s => s.getAttribute('title').includes({json.dumps(select_title)}));
        if (!sel) return 'not_found';
        const opt = Array.from(sel.options).find(o => o.text === {json.dumps(value)} || o.value === {json.dumps(value)});
        if (!opt) return 'option_not_found';
        sel.value = opt.value;
        sel.dispatchEvent(new Event('change', {{bubbles: true}}));
        return 'ok';
    }})()"""
    return _nb_ui_run_js(js) == "ok"


def nb_ui_exec_command(command_id: str) -> bool:
    """执行 JupyterLab 工具栏命令，通过 CDP 模拟真实浏览器操作（所见即所得）。

    根据 command_id 自动选择执行方式：
    - 大多数命令：找工具栏按钮，CDP 鼠标点击
    - move-cell-up/down：CDP 键盘事件（Ctrl+Shift+↑/↓）
    - change-cell-to-*：修改 cell 类型 SELECT 元素值

    @param[in] command_id  JupyterLab 命令 ID（见 _NB_CMD_TITLE_MAP）
    @return True 执行成功，False 失败
    """
    action = _NB_CMD_TITLE_MAP.get(command_id)
    if action is None:
        return False

    # 通用：点击活跃 cell 自身的 action toolbar 按钮（title 匹配）
    if action.startswith("_js_active_cell_btn:"):
        btn_title = action[len("_js_active_cell_btn:"):]
        cell_btn_js = f"""(function() {{
            var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel'));
            var panel = panels.find(function(p) {{
                return p.classList.contains('jp-mod-current');
            }}) || panels.find(function(p) {{
                return window.getComputedStyle(p).display !== 'none' &&
                       p.getBoundingClientRect().width > 0;
            }});
            if (!panel) return 'no_panel';
            var nb = panel.querySelector('.jp-Notebook');
            if (!nb) return 'no_notebook';
            var cell = nb.querySelector('.jp-Cell.jp-mod-active');
            if (!cell) return 'no_active_cell';
            cell.dispatchEvent(new MouseEvent('mouseenter', {{bubbles:true}}));
            cell.dispatchEvent(new MouseEvent('mousemove',  {{bubbles:true}}));
            var btns = Array.from(cell.querySelectorAll('[title]'));
            var btn = btns.find(function(b) {{
                return b.title && b.title.toLowerCase().includes({json.dumps(btn_title.lower())});
            }});
            if (!btn) return 'no_btn:' + {json.dumps(btn_title)};
            btn.click();
            return 'ok';
        }})()"""
        result = _nb_ui_run_js(cell_btn_js)
        time.sleep(0.2)
        return result == "ok"

    if action == "_js_delete_cell":
        # 找当前激活 cell 内 title 含 "Delete this cell" 的 JP-BUTTON 并点击
        # 先 mouseenter 让 cell toolbar 显示，再 click 删除按钮
        del_js = f"""(function() {{
            var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel'));
            var panel = panels.find(function(p) {{
                return p.classList.contains('jp-mod-current');
            }}) || panels.find(function(p) {{
                return window.getComputedStyle(p).display !== 'none' &&
                       p.getBoundingClientRect().width > 0;
            }});
            if (!panel) return 'no_panel';
            var nb = panel.querySelector('.jp-Notebook');
            if (!nb) return 'no_notebook';
            var cell = nb.querySelector('.jp-Cell.jp-mod-active');
            if (!cell) return 'no_active_cell';
            cell.dispatchEvent(new MouseEvent('mouseenter', {{bubbles:true}}));
            cell.dispatchEvent(new MouseEvent('mousemove',  {{bubbles:true}}));
            var btns = Array.from(cell.querySelectorAll('[title]'));
            var del_btn = btns.find(function(b) {{
                return b.title && b.title.toLowerCase().includes('delete this cell');
            }});
            if (!del_btn) return 'no_delete_btn';
            del_btn.click();
            return 'ok';
        }})()"""
        result = _nb_ui_run_js(del_js)
        time.sleep(0.2)
        return result == "ok"

    if action.startswith("_keyboard_move_"):
        direction = "up" if action.endswith("up") else "down"
        ws_url = _find_jupyter_page_ws()
        if not ws_url:
            return False

        async def _move():
            arrow_key = "ArrowUp" if direction == "up" else "ArrowDown"
            try:
                async with ws_connect(ws_url) as ws:
                    msg_id = 0

                    async def cdp_send(method, params=None):
                        nonlocal msg_id
                        msg_id += 1
                        m = {"id": msg_id, "method": method}
                        if params:
                            m["params"] = params
                        await ws.send(json.dumps(m))
                        while True:
                            r = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                            if r.get("id") == msg_id:
                                return r

                    await cdp_send("Input.dispatchKeyEvent", {
                        "type": "keyDown", "key": arrow_key,
                        "code": arrow_key, "modifiers": 10
                    })
                    await cdp_send("Input.dispatchKeyEvent", {
                        "type": "keyUp", "key": arrow_key,
                        "code": arrow_key, "modifiers": 10
                    })
                    return True
            except Exception:
                return False

        return asyncio.run(_move())

    if action.startswith("_select_type_"):
        cell_type_str = action[len("_select_type_"):]
        return _nb_ui_set_select_value("Select the cell type", cell_type_str)

    # 默认：按 title 找按钮并点击
    return _nb_ui_click_button_by_title(action)


def nb_ui_get_cell_source(idx: int) -> str | None:
    """从浏览器 DOM 读取指定 cell 的当前 source（不依赖文件保存状态）。

    nb-edit 写入后可能尚未持久化到文件，此函数直接读 CodeMirror 编辑器内容，
    确保 nb-exec 执行的是浏览器中实际显示的代码。

    @param[in] idx  0-based cell 索引（当前活跃 notebook 内）
    @return cell 的源码字符串；找不到返回 None
    """
    js = f"""(function() {{
        let panel = document.querySelector('.jp-NotebookPanel.jp-mod-current .jp-Notebook');
        if (!panel) {{
            const panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
            panel = panels.reverse().find(p => p.getBoundingClientRect().width > 0);
        }}
        const cells = panel ? Array.from(panel.querySelectorAll('.jp-Cell'))
                            : Array.from(document.querySelectorAll('.jp-Cell'));
        const cell = cells[{idx}];
        if (!cell) return null;
        const cm = cell.querySelector('.cm-content');
        return cm ? cm.textContent : null;
    }}())"""
    result = _nb_ui_run_js(js)
    if result is None:
        return None
    return str(result)


def nb_ui_get_active_cell_idx() -> int:
    """返回当前激活 cell（.jp-mod-active）在活跃 notebook 内的 0-based 索引。

    nb-add 插入后新 cell 自动激活，用此函数定位，无需依赖执行编号。

    @return 0-based 索引，未找到返回 -1
    """
    js = """(function() {
        let panel = document.querySelector('.jp-NotebookPanel.jp-mod-current .jp-Notebook');
        if (!panel) {
            const panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
            panel = panels.reverse().find(p => p.getBoundingClientRect().width > 0);
        }
        const cells = panel ? Array.from(panel.querySelectorAll('.jp-Cell'))
                            : Array.from(document.querySelectorAll('.jp-Cell'));
        for (let i = 0; i < cells.length; i++) {
            if (cells[i].classList.contains('jp-mod-active')) return i;
        }
        return -1;
    }())"""
    result = _nb_ui_run_js(js)
    try:
        return int(result)
    except (TypeError, ValueError):
        return -1


def nb_ui_find_cell_by_exec_count(exec_count: int) -> int:
    """在当前激活的 notebook 中，通过执行编号 [N] 定位 cell 的 0-based 索引。

    JupyterLab 对滚出视口的 cell 做懒渲染（不渲染 jp-InputArea），
    因此分两阶段：
      1. 快速扫描当前 DOM（已渲染 cell）；
      2. 若未找到，逐格 scrollIntoView 触发渲染后重扫。

    @param[in] exec_count  执行编号（浏览器中显示的 [N] 数字）
    @return 0-based cell 索引，找不到返回 -1
    """
    target = f'[{exec_count}]:'

    # Phase 1: 快速扫描当前已渲染的 cell
    js_quick = f"""(function(){{
        var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
        var panel = panels.slice().reverse().find(function(p){{
            return p.getBoundingClientRect().width > 0;
        }});
        var cells = panel ? Array.from(panel.querySelectorAll('.jp-Cell'))
                          : Array.from(document.querySelectorAll('.jp-Cell'));
        var target = '{target}';
        for (var i = 0; i < cells.length; i++) {{
            var prompt = cells[i].querySelector('.jp-InputArea-prompt');
            if (prompt && prompt.textContent.trim() === target) return i;
        }}
        return -1;
    }}())"""
    result = _nb_ui_run_js(js_quick)
    try:
        idx = int(result)
        if idx >= 0:
            return idx
    except (TypeError, ValueError):
        pass

    # Phase 2: DOM 未找到，fallback 到 REST API 查找 execution_count
    # REST API 的 execution_count 与文件保存状态完全一致，不受滚动/懒渲染影响
    # 同时 DOM 中 cell 顺序与文件顺序一致，所以文件索引 == DOM 索引
    try:
        from jupyterm_config import load_config
        from jupyterm_api import get_notebook, list_sessions
        cfg = load_config()

        # 找当前可见 notebook 的路径
        nb_path = None
        try:
            from jupyterm_cdp import get_browser_visible_notebooks
            nbs = get_browser_visible_notebooks()
            for nb in nbs:
                if nb.get("current"):
                    nb_path = nb.get("path")
                    break
        except Exception:
            pass

        if not nb_path:
            sessions = list_sessions(cfg)
            for s in sessions:
                p = s.get("notebook", s.get("path", ""))
                if isinstance(p, dict):
                    p = p.get("path", "")
                if p.endswith(".ipynb"):
                    nb_path = p
                    break

        if nb_path:
            result = get_notebook(cfg, nb_path)
            cells = result.get("content", result).get("cells", [])
            for i, cell in enumerate(cells):
                if cell.get("execution_count") == exec_count:
                    # 找到后将该 cell 滚动到视口，确保后续操作可见
                    _nb_ui_run_js(f"""(function(){{
                        var panels = Array.from(document.querySelectorAll(
                            '.jp-NotebookPanel .jp-Notebook'));
                        var panel = panels.slice().reverse().find(function(p){{
                            return p.getBoundingClientRect().width > 0;
                        }});
                        if (!panel) return;
                        var cells = panel.querySelectorAll('.jp-Cell');
                        if (cells[{i}]) cells[{i}].scrollIntoView(
                            {{block: 'center', behavior: 'smooth'}});
                    }}())""")
                    return i
    except Exception:
        pass

    return -1


def _active_nb_cells_js() -> str:
    """返回用于查找当前激活 notebook 面板内所有 cell 的 JS 表达式片段。

    多个 notebook 同时打开时，document.querySelectorAll('.jp-Cell') 会返回
    所有 notebook 的 cell。此 helper 将查询范围限定到当前可见/激活的面板。

    @return JS 表达式字符串（结果为 cell NodeList 或 Array）
    """
    return """(() => {
        // 优先找 .jp-mod-current 标记的激活面板
        let panel = document.querySelector('.jp-NotebookPanel.jp-mod-current .jp-Notebook');
        if (!panel) {
            // fallback 1: 找 display 不为 none 的 NotebookPanel（可见面板）
            const panelEls = Array.from(document.querySelectorAll('.jp-NotebookPanel'));
            const visiblePanel = panelEls.find(
                p => window.getComputedStyle(p).display !== 'none' && p.getBoundingClientRect().width > 0
            );
            if (visiblePanel) {
                panel = visiblePanel.querySelector('.jp-Notebook');
            }
        }
        if (!panel) {
            // fallback 2: 找最后一个宽度 > 0 的 .jp-Notebook
            const panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
            panel = panels.reverse().find(p => p.getBoundingClientRect().width > 0);
        }
        return panel ? Array.from(panel.querySelectorAll('.jp-Cell'))
                     : Array.from(document.querySelectorAll('.jp-Cell'));
    })()"""


def nb_ui_set_cell(cell_idx: int, source: str) -> bool:
    """通过 CDP 将指定 cell 内容替换为 source（所见即所得）。

    策略：先点击 cell 进入 command mode，再 dispatch Enter 键进入 edit mode，
    最后用 CodeMirror 6 的 view.dispatch 写入内容（比 execCommand 更可靠）。
    如果 view.dispatch 不可用，fallback 到 execCommand。

    @param[in] cell_idx  0-based cell 索引
    @param[in] source    新的 cell 源码内容
    @return True 写入成功，False 失败
    """
    source_json = json.dumps(source)
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return False

    async def _run():
        try:
            async with ws_connect(ws_url) as ws:
                msg_id = 0

                async def cdp_send(method, params=None):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": method}
                    if params:
                        m["params"] = params
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                        if r.get("id") == msg_id:
                            return r

                async def eval_js(expr, timeout=5.0):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": "Runtime.evaluate",
                         "params": {"expression": expr, "returnByValue": True}}
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
                        if r.get("id") == msg_id:
                            return r.get("result", {}).get("result", {}).get("value")

                # Step 1: 滚到视口并用 JS click 激活（不依赖像素坐标，兼容任意高度输出）
                click_js = f"""(function() {{
                    var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
                    var panel = panels.slice().reverse().find(function(p) {{
                        return p.getBoundingClientRect().width > 0;
                    }});
                    if (!panel) return 'no_panel';
                    var cells = Array.from(panel.querySelectorAll('.jp-Cell'));
                    var cell = cells[{cell_idx}];
                    if (!cell) return 'not_found';
                    cell.scrollIntoView({{block: 'center', behavior: 'instant'}});
                    var target = cell.querySelector('.jp-InputArea-prompt') || cell;
                    target.dispatchEvent(new MouseEvent('mousedown', {{bubbles:true, cancelable:true}}));
                    target.dispatchEvent(new MouseEvent('mouseup',   {{bubbles:true, cancelable:true}}));
                    target.dispatchEvent(new MouseEvent('click',     {{bubbles:true, cancelable:true}}));
                    return 'ok';
                }}())"""
                click_result = await eval_js(click_js)
                if click_result not in ("ok",):
                    return False
                await asyncio.sleep(0.15)

                # Step 2: 用 CodeMirror view API 写入内容（最可靠）
                write_js = f"""(function() {{
                    var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel .jp-Notebook'));
                    var panel = panels.slice().reverse().find(function(p) {{
                        return p.getBoundingClientRect().width > 0;
                    }});
                    if (!panel) return 'err:no_panel';
                    var cells = Array.from(panel.querySelectorAll('.jp-Cell'));
                    var cell = cells[{cell_idx}];
                    if (!cell) return 'err:not_found';

                    // 尝试通过 CodeMirror 6 的 view.dispatch 写入（绕过 DOM，直接改 state）
                    // cmView 挂在 .cm-content 上（不是 .jp-InputArea-editor）
                    var cmContent = cell.querySelector('.cm-content');
                    var cmWrapper = cmContent || cell.querySelector('.jp-InputArea-editor');
                    if (cmWrapper && cmWrapper.cmView && cmWrapper.cmView.view) {{
                        var view = cmWrapper.cmView.view;
                        var doc = view.state.doc;
                        view.dispatch({{
                            changes: {{from: 0, to: doc.length, insert: {source_json}}}
                        }});
                        return 'ok:view_dispatch';
                    }}

                    // fallback: dispatch beforeinput event（模拟用户输入）
                    var editor = cell.querySelector('.cm-content');
                    if (!editor) return 'err:no_editor';
                    editor.focus();
                    document.execCommand('selectAll', false, null);
                    var ev = new InputEvent('beforeinput', {{
                        bubbles: true, cancelable: true,
                        inputType: 'insertText',
                        data: {source_json}
                    }});
                    editor.dispatchEvent(ev);
                    document.execCommand('selectAll', false, null);
                    var ok = document.execCommand('insertText', false, {source_json});
                    return ok ? 'ok:execCommand' : 'err:execCommand_failed';
                }}())"""
                result = await eval_js(write_js, timeout=8.0)
                return isinstance(result, str) and result.startswith("ok")
        except Exception:
            return False

    return asyncio.run(_run())


def nb_ui_set_active_cell(cell_idx: int) -> bool:
    """通过 JS element.click() 将指定 cell 激活为 command mode（不进入编辑模式）。

    改用 JS 直接 dispatchEvent 而非 CDP 像素坐标鼠标事件，彻底避免因 cell
    输出（plot/table）高度变化导致坐标偏移点中错误 cell 的问题。

    @param[in] cell_idx  0-based cell 索引
    @return True 成功，False 失败
    """
    click_js = f"""(function() {{
        var cells = {_active_nb_cells_js()};
        var cell = cells[{cell_idx}];
        if (!cell) return 'not_found';
        cell.scrollIntoView({{block: 'center', behavior: 'instant'}});
        var target = cell.querySelector('.jp-InputArea-prompt') || cell;
        target.dispatchEvent(new MouseEvent('mousedown', {{bubbles: true, cancelable: true}}));
        target.dispatchEvent(new MouseEvent('mouseup',   {{bubbles: true, cancelable: true}}));
        target.dispatchEvent(new MouseEvent('click',     {{bubbles: true, cancelable: true}}));
        return 'ok';
    }}())"""

    result = _nb_ui_run_js(click_js)
    if result == "ok":
        time.sleep(0.15)
        return True
    return False


def nb_ui_read_cell_output(cell_idx: int) -> dict:
    """从浏览器 DOM 读取指定 cell 的执行状态和输出文本（实时）。

    running=True 表示 cell 仍在执行（prompt 显示 [*]）；
    outputs 为已渲染的输出文本列表。

    @param[in] cell_idx  0-based cell 索引
    @return {"running": bool, "outputs": [str], "exec_count": int|None}
    """
    js = f"""(() => {{
        const cells = {_active_nb_cells_js()};
        const cell = cells[{cell_idx}];
        if (!cell) return JSON.stringify({{err: 'not_found'}});
        const prompt = cell.querySelector('.jp-InputArea-prompt');
        const promptText = prompt ? prompt.textContent.trim() : '';
        const running = promptText === '[*]:';
        const execMatch = promptText.match(/\\[(\\d+)\\]:/);
        const execCount = execMatch ? parseInt(execMatch[1]) : null;
        const outputs = Array.from(cell.querySelectorAll('.jp-OutputArea-output'))
            .map(o => o.innerText.trim())
            .filter(t => t.length > 0);
        return JSON.stringify({{running, execCount, outputs}});
    }})()"""
    raw = _nb_ui_run_js(js, timeout=3.0)
    if not raw:
        return {"running": False, "outputs": [], "exec_count": None}
    try:
        return json.loads(raw)
    except Exception:
        return {"running": False, "outputs": [], "exec_count": None}


def file_browser_open_file(path: str) -> bool:
    """通过 CDP 模拟文件浏览器侧边栏双击打开文件（所见即所得）。

    实现步骤：
      1. 检查文件浏览器面板是否可见；若不可见则点击侧边栏 File Browser 图标打开
      2. 刷新文件列表（点击 data-command=filebrowser:refresh）
      3. 若路径含多级目录，依次双击目录名展开
      4. 找到目标文件名并双击（CDP Input.dispatchMouseEvent，clickCount=2）

    @param[in] path  JupyterLab 中的相对文件路径（如 "data/demo.ipynb"）
    @return True 成功，False 失败
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return False

    parts = path.split("/")
    filename = parts[-1]
    dir_parts = parts[:-1]

    async def _run():
        try:
            async with ws_connect(ws_url) as ws:
                msg_id = 0

                async def cdp_send(method, params=None):
                    nonlocal msg_id
                    msg_id += 1
                    m = {"id": msg_id, "method": method}
                    if params:
                        m["params"] = params
                    await ws.send(json.dumps(m))
                    while True:
                        r = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
                        if r.get("id") == msg_id:
                            return r

                async def eval_js(expr):
                    resp = await cdp_send("Runtime.evaluate",
                                          {"expression": expr, "returnByValue": True})
                    return resp.get("result", {}).get("result", {}).get("value", "")

                async def click_xy(x, y, count=1):
                    await cdp_send("Input.dispatchMouseEvent", {
                        "type": "mousePressed", "x": x, "y": y,
                        "button": "left", "clickCount": count
                    })
                    await cdp_send("Input.dispatchMouseEvent", {
                        "type": "mouseReleased", "x": x, "y": y,
                        "button": "left", "clickCount": count
                    })

                # 1. 检查文件浏览器面板是否可见，不可见才点击打开（避免 toggle 关闭）
                fb_visible = await eval_js("""(() => {
                    const fb = document.querySelector('.jp-FileBrowser');
                    if (!fb) return 'false';
                    const r = fb.getBoundingClientRect();
                    return (r.width > 0 && r.height > 0) ? 'true' : 'false';
                })()""")

                if fb_visible != "true":
                    # 侧边栏处于折叠状态，点击打开
                    sidebar_js = """(() => {
                        const btn = Array.from(document.querySelectorAll('.jp-SideBar button, li[title]'))
                            .find(el => (el.getAttribute('title') || '').includes('File Browser')
                                        && el.getBoundingClientRect().width > 0);
                        if (!btn) return JSON.stringify({err:'no_btn'});
                        const r = btn.getBoundingClientRect();
                        return JSON.stringify({x: r.x + r.width/2, y: r.y + r.height/2});
                    })()"""
                    sval = await eval_js(sidebar_js)
                    sinfo = json.loads(sval)
                    if "err" not in sinfo:
                        await click_xy(sinfo["x"], sinfo["y"])
                        await asyncio.sleep(0.5)

                # 2. 刷新文件列表（使用 data-command 定位刷新按钮）
                ref_val = await eval_js("""(() => {
                    const el = document.querySelector("[data-command='filebrowser:refresh']");
                    if (!el) return JSON.stringify({err:'no_refresh'});
                    const r = el.getBoundingClientRect();
                    return JSON.stringify({x: r.x + r.width/2, y: r.y + r.height/2, w: r.width});
                })()""")
                ref_info = json.loads(ref_val)
                if "err" not in ref_info and ref_info.get("w", 0) > 0:
                    await click_xy(ref_info["x"], ref_info["y"])
                    await asyncio.sleep(1.0)

                # 3. 展开各级目录
                for dirname in dir_parts:
                    dir_js = f"""(() => {{
                        const items = Array.from(document.querySelectorAll('.jp-DirListing-item'));
                        const item = items.find(el => {{
                            const span = el.querySelector('.jp-DirListing-itemText');
                            return span && span.textContent.trim() === {json.dumps(dirname)};
                        }});
                        if (!item) return JSON.stringify({{err:'dir_not_found', name:{json.dumps(dirname)}}});
                        const r = item.getBoundingClientRect();
                        return JSON.stringify({{x: r.x + r.width/2, y: r.y + r.height/2}});
                    }})()"""
                    dval = await eval_js(dir_js)
                    dinfo = json.loads(dval)
                    if "err" in dinfo:
                        return False
                    await click_xy(dinfo["x"], dinfo["y"], count=2)
                    await asyncio.sleep(0.5)

                # 4. 找到文件并双击打开
                file_js = f"""(() => {{
                    const items = Array.from(document.querySelectorAll('.jp-DirListing-item'));
                    const item = items.find(el => {{
                        const span = el.querySelector('.jp-DirListing-itemText');
                        return span && span.textContent.trim() === {json.dumps(filename)};
                    }});
                    if (!item) return JSON.stringify({{err:'file_not_found', name:{json.dumps(filename)}}});
                    const r = item.getBoundingClientRect();
                    return JSON.stringify({{x: r.x + r.width/2, y: r.y + r.height/2}});
                }})()"""
                fval = await eval_js(file_js)
                finfo = json.loads(fval)
                if "err" in finfo:
                    return False

                await click_xy(finfo["x"], finfo["y"], count=2)
                return True

        except Exception:
            return False

    ok = asyncio.run(_run())
    if ok:
        time.sleep(0.8)
    return ok
