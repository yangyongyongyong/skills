#!/usr/bin/env python3
"""
jupyterm — 通过 WebSocket 与 JupyterLab Terminal 交互的 CLI 工具。

使用方式：
  jupyterm setup                        # 自动从浏览器当前活动标签探测 token/URL
  jupyterm setup --url <url> --token <tok>  # 手动指定
  jupyterm exec "ls -la"                # 在 Terminal 中执行命令，返回输出
  jupyterm exec -t 2 "pwd"             # 指定 terminal id
  jupyterm exec --timeout 60 "pip list" # 自定义超时（秒）
  jupyterm list                         # 列出所有 Terminal
  jupyterm create                       # 创建新 Terminal
  jupyterm run /path/to/script.sh       # 把文件内容作为多行命令执行
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

# websockets 14+ 改了 API，统一用 connect
try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets.legacy.client import connect as ws_connect

CONFIG_FILE = os.path.expanduser("~/.jupyterm.json")

# 命令结束哨兵，确保唯一不冲突
SENTINEL = "__JUPYTERM_DONE_7f3a9b__"

# CDP 默认扫描端口范围
DEFAULT_CDP_PORTS = [9222, 9223, 9224, 9225, 9226, 9227, 9228, 9229, 9230]

# 判定 Jupyter 页面的 URL 关键字
JUPYTER_URL_KEYWORDS = ["/lab", "/tree", "/user/", "jupyter", "notebook"]


# ---------------------------------------------------------------------------
# 配置读写
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """读取保存的 JupyterLab 连接配置。"""
    if not os.path.exists(CONFIG_FILE):
        sys.exit(
            f"[jupyterm] 未找到配置 {CONFIG_FILE}，请先运行: jupyterm setup"
        )
    with open(CONFIG_FILE) as f:
        return json.load(f)


def save_config(cfg: dict):
    """保存配置到 ~/.jupyterm.json。"""
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)
    print(f"[jupyterm] 配置已保存: {CONFIG_FILE}")
    print(json.dumps(cfg, indent=2))


# ---------------------------------------------------------------------------
# CDP 自动探测：从浏览器当前活动标签读取 JupyterLab 信息
# ---------------------------------------------------------------------------

def _cdp_get_pages(port: int) -> list:
    """获取指定 CDP 端口上的所有 page 类型 targets，失败返回空列表。"""
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/json",
            headers={"Host": "localhost"}
        )
        with urllib.request.urlopen(req, timeout=1) as resp:
            targets = json.loads(resp.read())
        return [t for t in targets if t.get("type") == "page"]
    except Exception:
        return []


async def _cdp_evaluate(ws_url: str, expression: str, timeout: float = 3.0):
    """通过 CDP WebSocket 在指定页面执行 JS 表达式，返回结果值或 None。"""
    try:
        async with ws_connect(ws_url) as ws:
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
    except Exception:
        return None


async def _find_selected_jupyter_page(pages: list) -> tuple:
    """在给定的 page targets 中找到当前 selected（visibilityState=visible）的 Jupyter 页。

    返回 (url, ws_debugger_url, status):
      status: "ok" | "not_jupyter" | "no_visible"
    """
    visible_pages = []

    for page in pages:
        ws_url = page.get("webSocketDebuggerUrl", "")
        if not ws_url:
            continue
        state = await _cdp_evaluate(ws_url, "document.visibilityState")
        if state == "visible":
            visible_pages.append(page)

    if not visible_pages:
        return None, None, "no_visible"

    # 优先选含 Jupyter 关键字的 visible 页
    for page in visible_pages:
        url = page.get("url", "")
        if any(kw in url for kw in JUPYTER_URL_KEYWORDS):
            return url, page.get("webSocketDebuggerUrl", ""), "ok"

    # visible 页不含 Jupyter
    return visible_pages[0].get("url", ""), None, "not_jupyter"


async def _extract_token_from_page(ws_url: str, page_url: str) -> str:
    """从页面 DOM 或 URL 参数提取 JupyterLab token。"""
    # 方法一：从页面内嵌的 jupyter-config-data 读取
    js = ("(() => { "
          "  const el = document.getElementById('jupyter-config-data'); "
          "  if (!el) return ''; "
          "  try { return JSON.parse(el.textContent).token || ''; } catch(e) { return ''; } "
          "})()")
    token = await _cdp_evaluate(ws_url, js)
    if token:
        return token

    # 方法二：从 URL query 参数 ?token= 提取
    parsed = urllib.parse.urlparse(page_url)
    qs = urllib.parse.parse_qs(parsed.query)
    return qs.get("token", [""])[0]


def detect_from_cdp(ports: list = None) -> tuple:
    """扫描本机 CDP 端口，找到浏览器当前活动（selected）的 Jupyter 标签页，提取 URL 和 token。

    返回 (url, token, status):
      status: "ok"          — 成功找到 Jupyter selected 页并提取 token
              "not_jupyter" — selected 页不是 Jupyter，需用户切换标签
              "no_cdp"      — 未找到任何 CDP 实例，需开启 Chrome 远程调试
    """
    if ports is None:
        ports = DEFAULT_CDP_PORTS

    async def _run():
        for port in ports:
            pages = _cdp_get_pages(port)
            if not pages:
                continue
            print(f"[jupyterm] 发现 CDP 实例 端口 {port}，共 {len(pages)} 个页面标签", file=sys.stderr)
            url, ws_url, status = await _find_selected_jupyter_page(pages)
            if status == "ok":
                token = await _extract_token_from_page(ws_url, url)
                return url, token, "ok"
            elif status == "not_jupyter":
                # selected 页不是 Jupyter，明确告知用户，不继续扫描其他端口
                return url, None, "not_jupyter"
            # status == "no_visible"：该端口无可见页，继续下一个端口
        return None, None, "no_cdp"

    return asyncio.run(_run())


def build_config_from_url(url: str, token: str) -> dict:
    """根据浏览器页面 URL 和 token 构建配置字典。

    支持任意部署地址，从页面 URL 中提取 scheme://host:port 和 base_path。
    示例：
      http://remote:9999/user/admin/lab  → base_url=http://remote:9999/user/admin
      http://localhost:8889/lab          → base_url=http://localhost:8889
    """
    parsed = urllib.parse.urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    # 截取 base_path：去掉 /lab、/tree 及其后缀
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
# JupyterLab REST API helpers
# ---------------------------------------------------------------------------

def api_request(cfg: dict, method: str, path: str, data: dict = None):
    """向 JupyterLab REST API 发送请求。"""
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


def list_terminals(cfg: dict) -> list:
    """列出所有 Terminal。"""
    return api_request(cfg, "GET", "/terminals")


def create_terminal(cfg: dict) -> dict:
    """创建新 Terminal，返回 terminal 信息。"""
    return api_request(cfg, "POST", "/terminals", {})


def get_or_create_terminal(cfg: dict, terminal_id: int = None) -> str:
    """获取或创建一个可用的 Terminal，返回 terminal id（字符串）。"""
    terminals = list_terminals(cfg)
    if terminal_id is not None:
        # 检查指定 id 是否存在
        for t in terminals:
            if str(t["name"]) == str(terminal_id):
                return str(t["name"])
        print(f"[jupyterm] Terminal {terminal_id} 不存在，将创建新的", file=sys.stderr)

    if terminals:
        tid = str(terminals[0]["name"])
        print(f"[jupyterm] 使用已有 Terminal {tid}", file=sys.stderr)
        return tid

    t = create_terminal(cfg)
    tid = str(t["name"])
    print(f"[jupyterm] 已创建 Terminal {tid}", file=sys.stderr)
    return tid


# ---------------------------------------------------------------------------
# WebSocket 执行命令
# ---------------------------------------------------------------------------

async def _ws_exec(ws_url: str, command: str, timeout: float) -> str:
    """通过 WebSocket 在 Terminal 中执行命令，返回干净的输出文本。"""
    # 用 sentinel 标记命令结束
    full_cmd = f"{command}\necho '{SENTINEL}'\n"

    async with ws_connect(ws_url) as ws:
        # 清空 welcome 输出（连接后服务端可能发 prompt 等）
        await asyncio.sleep(0.3)
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.2)
        except (asyncio.TimeoutError, Exception):
            pass

        # 发送命令
        await ws.send(json.dumps(["stdin", full_cmd]))

        # 收集输出直到 sentinel 出现
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

    # 后处理：清理 ANSI / VT100 转义序列
    raw = "".join(parts)
    raw = re.sub(r"\x1b\[[0-9;?]*[a-zA-Z]", "", raw)   # CSI 序列（含 [?2004l 等）
    raw = re.sub(r"\x1b[()][AB012]", "", raw)             # 字符集切换
    raw = re.sub(r"\x1b[=>]", "", raw)                    # 小键盘模式
    raw = re.sub(r"\x1b\]0;[^\x07]*\x07", "", raw)       # 标题设置 OSC
    raw = re.sub(r"\r\n", "\n", raw)
    raw = re.sub(r"\r", "\n", raw)

    # 去掉 sentinel 行及其后内容
    lines = raw.split("\n")
    result = []
    for line in lines:
        if SENTINEL in line:
            break
        result.append(line)

    # 去掉第一行（终端 echo 回命令本身）
    text = "\n".join(result)
    # 尝试去除输入命令的 echo：找到第一个实际输出行
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
    """在 JupyterLab Terminal 中执行命令，返回输出。"""
    tid = get_or_create_terminal(cfg, terminal_id)
    ws_url = (cfg["ws_base"].rstrip("/") +
              f"/terminals/websocket/{tid}?token={cfg['token']}")
    return asyncio.run(_ws_exec(ws_url, command, timeout))


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def cmd_setup(args):
    """保存 JupyterLab 连接配置。

    两种模式：
      1. --url + --token  : 手动指定 URL 和 token（适合远程或域名部署）
      2. 无参数（默认）   : 自动扫描本机 Chrome CDP 端口，读取当前活动标签的 Jupyter 信息
    """
    url = getattr(args, "url", None)
    token = getattr(args, "token", None)

    if url:
        # 模式 1：手动指定
        cfg = build_config_from_url(url, token or "")
        save_config(cfg)
        return

    # 模式 2：CDP 自动探测
    cdp_ports_raw = getattr(args, "cdp_ports", None)
    ports = DEFAULT_CDP_PORTS
    if cdp_ports_raw:
        try:
            ports = [int(p.strip()) for p in cdp_ports_raw.split(",")]
        except ValueError:
            print(f"[jupyterm] --cdp-ports 格式错误，应为逗号分隔的端口号，如 9222,9223", file=sys.stderr)
            sys.exit(1)

    print(f"[jupyterm] 正在扫描 CDP 端口 {ports} ...")
    page_url, token, status = detect_from_cdp(ports)

    if status == "ok":
        cfg = build_config_from_url(page_url, token or "")
        save_config(cfg)

    elif status == "not_jupyter":
        print(f"[jupyterm] 当前活动标签不是 Jupyter 页面：{page_url}", file=sys.stderr)
        print("[jupyterm] 请在浏览器中切换到 JupyterLab 标签页后重试。", file=sys.stderr)
        print("[jupyterm] 或手动指定：jupyterm setup --url <url> --token <token>", file=sys.stderr)
        sys.exit(1)

    else:  # no_cdp
        print("[jupyterm] 未发现任何 Chrome CDP 实例。", file=sys.stderr)
        print("[jupyterm] 请确认 Chrome 已开启远程调试，任选其一：", file=sys.stderr)
        print("  Chrome 144+：chrome://inspect/#remote-debugging（按页面允许连接）", file=sys.stderr)
        print("  任意版本：启动 Chrome 时加 --remote-debugging-port=9222", file=sys.stderr)
        print("[jupyterm] 也可手动指定：jupyterm setup --url <url> --token <token>", file=sys.stderr)
        sys.exit(1)


def cmd_list(args):
    """列出所有 Terminal。"""
    cfg = load_config()
    terminals = list_terminals(cfg)
    if not terminals:
        print("[jupyterm] 没有运行中的 Terminal")
    for t in terminals:
        print(f"  Terminal {t['name']}")


def cmd_create(args):
    """创建新 Terminal。"""
    cfg = load_config()
    t = create_terminal(cfg)
    print(f"[jupyterm] 已创建 Terminal {t['name']}")


def cmd_exec(args):
    """在 Terminal 中执行命令并打印输出。"""
    cfg = load_config()
    cmd = args.command
    tid = getattr(args, "terminal", None)
    timeout = getattr(args, "timeout", 30.0)
    output = exec_command(cfg, cmd, terminal_id=tid, timeout=timeout)
    print(output)


def cmd_run(args):
    """把文件内容作为脚本在 Terminal 中执行。"""
    cfg = load_config()
    with open(args.file) as f:
        script = f.read()
    tid = getattr(args, "terminal", None)
    timeout = getattr(args, "timeout", 120.0)
    output = exec_command(cfg, script, terminal_id=tid, timeout=timeout)
    print(output)


def main():
    parser = argparse.ArgumentParser(
        prog="jupyterm",
        description="JupyterLab Terminal WebSocket CLI"
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    # setup
    p_setup = sub.add_parser("setup", help="保存 JupyterLab 连接配置")
    p_setup.add_argument("--url", default=None,
                         help="JupyterLab 页面 URL（手动指定，适合远程/域名部署）")
    p_setup.add_argument("--token", default=None,
                         help="JupyterLab 认证 token（与 --url 配合使用）")
    p_setup.add_argument("--cdp-ports", default=None,
                         help="自定义扫描的 CDP 端口，逗号分隔，如 9222,9223（默认扫描 9222-9230）")
    p_setup.set_defaults(func=cmd_setup)

    # list
    p_list = sub.add_parser("list", help="列出所有 Terminal")
    p_list.set_defaults(func=cmd_list)

    # create
    p_create = sub.add_parser("create", help="创建新 Terminal")
    p_create.set_defaults(func=cmd_create)

    # exec
    p_exec = sub.add_parser("exec", help="在 Terminal 中执行命令")
    p_exec.add_argument("command", help="要执行的命令")
    p_exec.add_argument("-t", "--terminal", type=int, default=None,
                        help="Terminal ID（默认使用第一个可用的）")
    p_exec.add_argument("--timeout", type=float, default=30.0,
                        help="等待输出的超时秒数（默认 30）")
    p_exec.set_defaults(func=cmd_exec)

    # run
    p_run = sub.add_parser("run", help="执行本地脚本文件")
    p_run.add_argument("file", help="脚本文件路径")
    p_run.add_argument("-t", "--terminal", type=int, default=None)
    p_run.add_argument("--timeout", type=float, default=120.0)
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"[jupyterm] 错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
