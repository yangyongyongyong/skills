"""
jupyterm_api — JupyterLab REST API 和 WebSocket 通信层。

包含：
  - Contents API（文件读写、创建）
  - Sessions API
  - Terminals API
  - Terminal WebSocket 执行
  - Kernel WebSocket 执行（Jupyter 消息协议）
"""

import asyncio
import json
import re
import sys
import time
import urllib.parse
import urllib.request

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets.legacy.client import connect as ws_connect

# 命令结束哨兵，确保唯一不冲突
SENTINEL = "__JUPYTERM_DONE_7f3a9b__"


# ---------------------------------------------------------------------------
# 通用 REST helper
# ---------------------------------------------------------------------------

def api_request(cfg: dict, method: str, path: str, data: dict = None):
    """向 JupyterLab REST API 发送请求。

    @param[in] cfg    jupyterm 配置字典（含 base_url / token）
    @param[in] method HTTP 方法（GET / POST / PUT / DELETE）
    @param[in] path   API 路径（如 "/terminals"）
    @param[in] data   请求体 dict，None 表示无 body
    @return 响应 JSON（dict 或 list）
    @raises urllib.error.HTTPError  非 2xx 响应
    """
    url = cfg["base_url"].rstrip("/") + "/api" + path
    headers = {"Authorization": f"token {cfg['token']}",
                "Content-Type": "application/json"}
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        print(f"[jupyterm] API 错误 {e.code}: {err_body}", file=sys.stderr)
        raise


# ---------------------------------------------------------------------------
# Terminal API
# ---------------------------------------------------------------------------

def list_terminals(cfg: dict) -> list:
    """列出所有 Terminal。

    @return Terminal 信息列表
    """
    return api_request(cfg, "GET", "/terminals")


def create_terminal(cfg: dict) -> dict:
    """创建新 Terminal，返回 terminal 信息。

    @return {"name": "1", ...}
    """
    return api_request(cfg, "POST", "/terminals", {})


# ---------------------------------------------------------------------------
# Sessions API
# ---------------------------------------------------------------------------

def get_kernel_status(cfg: dict, kernel_id: str) -> str:
    """查询指定 Kernel 的当前执行状态。

    @param[in] cfg       jupyterm 配置字典
    @param[in] kernel_id kernel UUID
    @return "idle" | "busy" | "starting" | "unknown"
    """
    try:
        result = api_request(cfg, "GET", f"/kernels/{kernel_id}")
        return result.get("execution_state", "unknown")
    except Exception:
        return "unknown"


def wait_kernel_idle(cfg: dict, kernel_id: str, timeout: float = 60.0,
                     poll_interval: float = 0.5) -> bool:
    """等待 Kernel 从 busy 变为 idle（轮询 REST API）。

    用于 CDP 已触发 Run 按钮后，通过状态轮询判断执行完成，
    无需 WebSocket 监听，避免消息竞争问题。

    @param[in] cfg           jupyterm 配置字典
    @param[in] kernel_id     kernel UUID
    @param[in] timeout       最长等待秒数
    @param[in] poll_interval 轮询间隔秒数
    @return True=kernel 变为 idle，False=超时
    """
    deadline = time.monotonic() + timeout
    # 先短暂等待，让 kernel 有机会从 idle 变为 busy（Run 刚触发时）
    time.sleep(0.6)
    while time.monotonic() < deadline:
        status = get_kernel_status(cfg, kernel_id)
        if status == "idle":
            return True
        time.sleep(poll_interval)
    return False


def list_sessions(cfg: dict) -> list:
    """列出所有 Jupyter Session，每条含 kernel id 和关联的 notebook path。

    REST API 不可用时静默返回空列表，不抛异常，保证 CDP 路径仍可继续。

    @return session 列表，元素含 {"id", "kernel": {"id"}, "notebook": {"path"}} 等字段
    """
    try:
        return api_request(cfg, "GET", "/sessions")
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Contents API（文件 / 目录 / Notebook）
# ---------------------------------------------------------------------------

def get_notebook(cfg: dict, path: str) -> dict:
    """通过 Contents API 读取 notebook 文件完整内容（含所有 cell）。

    @param[in] path notebook 在 JupyterLab 中的相对路径，如 "work/analysis.ipynb"
    @return ipynb JSON 对象（含 cells 列表），API 不可用时返回空 {"cells": []}
    """
    encoded = urllib.parse.quote(path, safe="")
    try:
        return api_request(cfg, "GET", f"/contents/{encoded}?content=1")
    except Exception:
        return {"cells": []}


def save_notebook(cfg: dict, path: str, nb_content: dict) -> dict:
    """通过 Contents API 保存修改后的 notebook。

    @param[in] path       notebook 相对路径
    @param[in] nb_content 完整 ipynb JSON（从 get_notebook 取得后修改）
    @return 保存后的 contents 元数据
    """
    encoded = urllib.parse.quote(path, safe="")
    payload = {"type": "notebook", "content": nb_content, "format": "json"}
    return api_request(cfg, "PUT", f"/contents/{encoded}", payload)


def create_directory(cfg: dict, path: str) -> dict:
    """通过 Contents API 在 JupyterLab 中新建目录。

    若路径含多级，JupyterLab 服务端只能创建一级；若需多级请逐级调用。

    @param[in] path  新目录路径（如 "data/results"）
    @return 创建后的目录元数据
    """
    # 先确保父目录存在（单层创建）
    parts = path.rstrip("/").split("/")
    current = ""
    result = None
    for part in parts:
        current = f"{current}/{part}".lstrip("/")
        encoded = urllib.parse.quote(current, safe="")
        try:
            # 尝试先读取，已存在则跳过
            api_request(cfg, "GET", f"/contents/{encoded}")
        except Exception:
            # 不存在，创建
            result = api_request(cfg, "PUT", f"/contents/{encoded}",
                                 {"type": "directory"})
    return result or {}


def create_file(cfg: dict, path: str, file_type: str = "notebook") -> dict:
    """通过 Contents API 创建新文件（notebook / python / text / markdown）。

    @param[in] path      目标路径（如 "work/demo.ipynb"）
    @param[in] file_type 文件类型：notebook | python | text | markdown
    @return 创建后的文件元数据（含实际 path）
    """
    import os
    dir_part = os.path.dirname(path)
    name = os.path.basename(path)
    encoded_dir = urllib.parse.quote(dir_part, safe="") if dir_part else ""
    api_dir_path = f"/contents/{encoded_dir}" if encoded_dir else "/contents"

    if file_type == "notebook":
        payload = {"type": "notebook"}
    else:
        # 文本文件（.py / .txt / .md）
        payload = {"type": "file", "format": "text", "content": ""}

    # 用 POST 在目录下创建（服务端自动生成临时名）
    result = api_request(cfg, "POST", api_dir_path, payload)
    temp_path = result.get("path", "")

    # 若指定了名称且与临时名不同，重命名
    if name and temp_path and os.path.basename(temp_path) != name:
        target_path = f"{dir_part}/{name}".lstrip("/")
        encoded_temp = urllib.parse.quote(temp_path, safe="")
        result = api_request(cfg, "PATCH", f"/contents/{encoded_temp}",
                              {"path": target_path})

    return result


def list_directory(cfg: dict, path: str = "") -> list:
    """列出目录内容。

    @param[in] path  目录路径，空字符串表示根目录
    @return 文件/目录元数据列表
    """
    encoded = urllib.parse.quote(path, safe="") if path else ""
    api_path = f"/contents/{encoded}" if encoded else "/contents"
    result = api_request(cfg, "GET", api_path)
    return result.get("content", [])


# ---------------------------------------------------------------------------
# Terminal 选择 helper
# ---------------------------------------------------------------------------

def get_or_create_terminal(cfg: dict, terminal_id=None) -> str:
    """获取或创建一个可用的 Terminal，返回 terminal name（字符串）。

    优先使用浏览器中实际可见的 terminal（通过 CDP 探测），
    确保命令发到用户正在看的终端里。

    @param[in] cfg         jupyterm 配置字典
    @param[in] terminal_id 指定 terminal name（int 或 str），None 表示自动选择
    @return terminal name 字符串
    """
    from jupyterm_cdp import get_browser_visible_terminals
    terminals = list_terminals(cfg)

    if terminal_id is not None:
        for t in terminals:
            if str(t["name"]) == str(terminal_id):
                return str(t["name"])
        print(f"[jupyterm] Terminal {terminal_id} 不存在，将创建新的", file=sys.stderr)
        t = create_terminal(cfg)
        return str(t["name"])

    browser_tabs = get_browser_visible_terminals()
    if browser_tabs:
        tab_names = {bt["name"] for bt in browser_tabs}
        visible = [t for t in terminals if str(t["name"]) in tab_names]
        if visible:
            for bt in browser_tabs:
                if bt.get("current"):
                    for t in visible:
                        if str(t["name"]) == bt["name"]:
                            print(f"[jupyterm] 使用浏览器活跃 Terminal {bt['name']}", file=sys.stderr)
                            return bt["name"]
            tid = str(visible[-1]["name"])
            print(f"[jupyterm] 使用浏览器可见 Terminal {tid}", file=sys.stderr)
            return tid

    if terminals:
        tid = str(terminals[0]["name"])
        print(f"[jupyterm] 使用已有 Terminal {tid}", file=sys.stderr)
        return tid

    t = create_terminal(cfg)
    tid = str(t["name"])
    print(f"[jupyterm] 已创建 Terminal {tid}", file=sys.stderr)
    return tid


# ---------------------------------------------------------------------------
# Terminal WebSocket 执行
# ---------------------------------------------------------------------------

async def _ws_exec(ws_url: str, command: str, timeout: float,
                   token: str = "") -> str:
    """通过 WebSocket 在 Terminal 中执行命令，返回干净的输出文本。

    @param[in] ws_url   WebSocket URL
    @param[in] command  要执行的命令
    @param[in] timeout  超时秒数
    @param[in] token    认证 token（Authorization header）
    @return 命令输出文本
    """
    full_cmd = f"{command}\necho '{SENTINEL}'\n"

    extra_headers = {}
    if token:
        extra_headers["Authorization"] = f"token {token}"
    async with ws_connect(ws_url, additional_headers=extra_headers) as ws:
        await asyncio.sleep(0.3)
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.2)
        except (asyncio.TimeoutError, Exception):
            pass

        await ws.send(json.dumps(["stdin", full_cmd]))

        parts = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 0.5))
                data = json.loads(msg)
                if isinstance(data, list) and data[0] in ("stdout", "stderr"):
                    parts.append(data[1])
                    if SENTINEL in "".join(parts):
                        break
            except asyncio.TimeoutError:
                combined = "".join(parts)
                if SENTINEL in combined:
                    break
            except Exception as e:
                print(f"[jupyterm] WebSocket 错误: {e}", file=sys.stderr)
                break

    raw = "".join(parts)
    raw = re.sub(r"\x1b\[[0-9;?]*[a-zA-Z]", "", raw)
    raw = re.sub(r"\x1b[()][AB012]", "", raw)
    raw = re.sub(r"\x1b[=>]", "", raw)
    raw = re.sub(r"\x1b\]0;[^\x07]*\x07", "", raw)
    raw = re.sub(r"\r\n", "\n", raw)
    raw = re.sub(r"\r", "\n", raw)

    lines = raw.split("\n")
    result = []
    for line in lines:
        if SENTINEL in line:
            break
        result.append(line)

    text = "\n".join(result)
    cmd_first_line = command.split("\n")[0].strip()
    cleaned = []
    skip_echo = True
    for line in text.split("\n"):
        stripped = line.strip()
        if skip_echo and (not stripped or cmd_first_line in stripped):
            skip_echo = False
            continue
        cleaned.append(line)

    return "\n".join(cleaned).strip()


def exec_command(cfg: dict, command: str,
                 terminal_id: int = None, timeout: float = 30.0) -> str:
    """在 JupyterLab Terminal 中执行命令，返回输出。

    WebSocket 认证统一使用 Authorization header（兼容单机 JupyterLab 和 JupyterHub）。

    @param[in] cfg         jupyterm 配置字典
    @param[in] command     要执行的命令
    @param[in] terminal_id 指定 terminal id，None 表示自动选择
    @param[in] timeout     超时秒数
    @return 命令输出文本
    """
    tid = get_or_create_terminal(cfg, terminal_id)
    ws_url = (cfg["ws_base"].rstrip("/") +
              f"/terminals/websocket/{tid}")
    return asyncio.run(_ws_exec(ws_url, command, timeout, token=cfg["token"]))


async def _ws_send_control(ws_url: str, ctrl_key: str, token: str = "") -> None:
    """向 Terminal WebSocket 发送控制键（仅支持 Ctrl+C）。

    @param[in] ws_url   Terminal websocket 地址
    @param[in] ctrl_key 控制键标识（ctrl-c）
    @param[in] token    认证 token
    """
    key = (ctrl_key or "").strip().lower()
    mapping = {"ctrl-c": "\u0003"}
    if key not in mapping:
        raise ValueError(f"unsupported control key: {ctrl_key}")

    extra_headers = {}
    if token:
        extra_headers["Authorization"] = f"token {token}"

    async with ws_connect(ws_url, additional_headers=extra_headers) as ws:
        # 先短暂清空积压事件，避免前序输出干扰。
        await asyncio.sleep(0.1)
        try:
            while True:
                await asyncio.wait_for(ws.recv(), timeout=0.1)
        except Exception:
            pass
        await ws.send(json.dumps(["stdin", mapping[key]]))


def send_control(cfg: dict, ctrl_key: str, terminal_id: int = None) -> None:
    """向指定 terminal 发送控制键。

    @param[in] cfg         jupyterm 配置字典
    @param[in] ctrl_key    控制键（ctrl-c）
    @param[in] terminal_id terminal id，None 表示自动选择
    """
    tid = get_or_create_terminal(cfg, terminal_id)
    ws_url = (cfg["ws_base"].rstrip("/") +
              f"/terminals/websocket/{tid}")
    asyncio.run(_ws_send_control(ws_url, ctrl_key, token=cfg["token"]))


# ---------------------------------------------------------------------------
# Kernel WebSocket 执行（Jupyter 消息协议）
# ---------------------------------------------------------------------------

async def _kernel_listen_output(ws_url: str, timeout: float,
                               token: str = "") -> dict:
    """监听 Kernel WebSocket，捕获下一次 execute_reply 及其前置输出。

    用于 CDP 已触发 Run 按钮后，只需监听输出而不重发 execute_request 的场景。
    支持 kernel 忙（[*]: 阻塞）时排队等待：会持续监听直到收到最近一次执行的
    execute_reply（status ok/error）。

    @param[in] ws_url  Kernel channels WebSocket URL
    @param[in] timeout 最长等待秒数
    @param[in] token   认证 token
    @return {"ok": bool, "outputs": [str], "error": str|None}
    """
    extra_headers = {}
    if token:
        extra_headers["Authorization"] = f"token {token}"

    outputs = []
    error_msg = None
    ok = False
    # 记录当前正在追踪的 parent_msg_id（取首个 execute_input 确定）
    tracking_id = None

    async with ws_connect(ws_url, additional_headers=extra_headers) as ws:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 1.0))
                msg = json.loads(raw)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break

            msg_type = msg.get("msg_type", "")
            parent_id = msg.get("parent_header", {}).get("msg_id", "")
            content = msg.get("content", {})

            # 通过 execute_input 锁定本次执行的 parent_id
            if msg_type == "execute_input" and tracking_id is None:
                tracking_id = parent_id
                continue

            # 只处理属于本次执行的消息
            if tracking_id and parent_id != tracking_id:
                continue

            if msg_type == "stream":
                outputs.append(content.get("text", ""))
            elif msg_type in ("execute_result", "display_data"):
                data = content.get("data", {})
                text = data.get("text/plain", "")
                if text:
                    outputs.append(text)
            elif msg_type == "error":
                tb = content.get("traceback", [])
                ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
                clean_tb = [ansi_escape.sub("", line) for line in tb]
                error_msg = "\n".join(clean_tb)
                outputs.append(f"ERROR: {content.get('ename')}: {content.get('evalue')}")
            elif msg_type == "execute_reply":
                ok = content.get("status") == "ok"
                break

    return {"ok": ok, "outputs": outputs, "error": error_msg}


async def _kernel_exec_cell(ws_url: str, code: str, timeout: float,
                            token: str = "") -> dict:
    """通过 Jupyter Kernel WebSocket 执行代码，返回输出。

    实现 Jupyter 消息协议：发送 execute_request，收集 stream /
    execute_result / display_data / error 消息，直到 execute_reply。

    @param[in] ws_url  Kernel channels WebSocket URL
    @param[in] code    要执行的代码
    @param[in] timeout 超时秒数
    @param[in] token   认证 token
    @return {"ok": bool, "outputs": [str], "error": str|None}
    """
    import uuid

    msg_id = str(uuid.uuid4())
    execute_request = {
        "header": {
            "msg_id": msg_id,
            "msg_type": "execute_request",
            "username": "jupyterm",
            "session": str(uuid.uuid4()),
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
        },
        "channel": "shell",
    }

    extra_headers = {}
    if token:
        extra_headers["Authorization"] = f"token {token}"

    outputs = []
    error_msg = None
    ok = False

    async with ws_connect(ws_url, additional_headers=extra_headers) as ws:
        await ws.send(json.dumps(execute_request))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 1.0))
                msg = json.loads(raw)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break

            parent_id = msg.get("parent_header", {}).get("msg_id", "")
            if parent_id != msg_id:
                continue

            msg_type = msg.get("msg_type", "")
            content = msg.get("content", {})

            if msg_type == "stream":
                outputs.append(content.get("text", ""))
            elif msg_type in ("execute_result", "display_data"):
                data = content.get("data", {})
                text = data.get("text/plain", "")
                if text:
                    outputs.append(text)
            elif msg_type == "error":
                tb = content.get("traceback", [])
                ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
                clean_tb = [ansi_escape.sub("", line) for line in tb]
                error_msg = "\n".join(clean_tb)
                outputs.append(f"ERROR: {content.get('ename')}: {content.get('evalue')}")
            elif msg_type == "execute_reply":
                ok = content.get("status") == "ok"
                break

    return {"ok": ok, "outputs": outputs, "error": error_msg}
