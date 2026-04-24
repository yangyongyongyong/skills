"""
jupyterm_terminal — Terminal CLI 命令实现。

包含：
  - cmd_setup  / cmd_list / cmd_create / cmd_exec / cmd_run / cmd_term_signal
  - Terminal 解析 helper（#N 语法、server name）
"""

import re
import sys

from jupyterm_cdp import (
    DEFAULT_CDP_PORTS,
    build_config_from_url,
    detect_from_cdp,
    get_browser_visible_terminals,
    switch_to_terminal_tab,
)
from jupyterm_api import (
    create_terminal,
    exec_command,
    list_terminals,
    send_control,
)
from jupyterm_config import load_config, save_config


# ---------------------------------------------------------------------------
# Terminal 解析 helpers
# ---------------------------------------------------------------------------

def _parse_tab_and_cmd(raw_cmd: str, tab_arg) -> tuple:
    """解析命令字符串中的 #N / N# 位置前缀，返回 (position, server_name, clean_cmd)。

    与 jscmd 保持一致的语法。position 为 1-based 浏览器可见位置。

    @param[in] raw_cmd  原始命令字符串（可能含 #N / N# 前缀）
    @param[in] tab_arg  -t 参数（server name，优先级最高），None 表示未指定
    @return (position, server_name, clean_cmd)
    """
    if tab_arg is not None:
        return None, str(tab_arg), raw_cmd

    m = re.match(r"^#(\d+)\s+(.*)", raw_cmd, re.DOTALL)
    if m:
        return int(m.group(1)), None, m.group(2).strip()
    m = re.match(r"^(\d+)#\s+(.*)", raw_cmd, re.DOTALL)
    if m:
        return int(m.group(1)), None, m.group(2).strip()

    return None, None, raw_cmd


def _resolve_terminal(cfg: dict, position: int = None,
                      server_name: str = None) -> tuple:
    """将位置编号或 server name 解析为最终的 (terminal_name, browser_position)。

    @param[in] cfg         jupyterm 配置
    @param[in] position    1-based 浏览器位置（#N 语法），None=未指定
    @param[in] server_name -t 指定的 server terminal name，None=未指定
    @return (terminal_name, browser_position_or_None)
    """
    browser_tabs = get_browser_visible_terminals()

    if server_name is not None:
        terminals = list_terminals(cfg)
        for t in terminals:
            if str(t["name"]) == server_name:
                pos = None
                if browser_tabs:
                    for i, bt in enumerate(browser_tabs):
                        if bt["name"] == server_name:
                            pos = i + 1
                            break
                return server_name, pos
        print(f"[jupyterm] Terminal {server_name} 不存在", file=sys.stderr)
        sys.exit(1)

    if position is not None:
        if not browser_tabs:
            print("[jupyterm] CDP 不可用，无法使用 #N 位置语法", file=sys.stderr)
            sys.exit(1)
        if position < 1 or position > len(browser_tabs):
            print(f"[jupyterm] #{position} 超出范围"
                  f"（浏览器中有 {len(browser_tabs)} 个 Terminal tab）",
                  file=sys.stderr)
            sys.exit(1)
        tab = browser_tabs[position - 1]
        return tab["name"], position

    if browser_tabs:
        for i, bt in enumerate(browser_tabs):
            if bt.get("current"):
                return bt["name"], i + 1
        return browser_tabs[-1]["name"], len(browser_tabs)

    terminals = list_terminals(cfg)
    if terminals:
        return str(terminals[0]["name"]), None

    t = create_terminal(cfg)
    return str(t["name"]), None


# ---------------------------------------------------------------------------
# CLI 命令
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
        cfg = build_config_from_url(url, token or "")
        save_config(cfg)
        return

    cdp_ports_raw = getattr(args, "cdp_ports", None)
    ports = DEFAULT_CDP_PORTS
    if cdp_ports_raw:
        try:
            ports = [int(p.strip()) for p in cdp_ports_raw.split(",")]
        except ValueError:
            print(f"[jupyterm] --cdp-ports 格式错误，应为逗号分隔的端口号，如 9222,9223",
                  file=sys.stderr)
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

    else:
        print("[jupyterm] 未发现任何 Chrome CDP 实例。", file=sys.stderr)
        print("[jupyterm] 请确认 Chrome 已开启远程调试，任选其一：", file=sys.stderr)
        print("  Chrome 144+：chrome://inspect/#remote-debugging（按页面允许连接）", file=sys.stderr)
        print("  任意版本：启动 Chrome 时加 --remote-debugging-port=9222", file=sys.stderr)
        print("[jupyterm] 也可手动指定：jupyterm setup --url <url> --token <token>", file=sys.stderr)
        sys.exit(1)


def cmd_list(args):
    """列出浏览器中实际可见的 Terminal，标注位置编号和活跃状态。

    优先通过 CDP 查询浏览器 DOM，按从左到右顺序显示 #N 位置；
    CDP 不可用时 fallback 到服务端 API 全量列表。
    """
    cfg = load_config()
    terminals = list_terminals(cfg)
    if not terminals:
        print("[jupyterm] 没有运行中的 Terminal")
        return

    browser_tabs = get_browser_visible_terminals()
    if browser_tabs is not None:
        server_names = {str(t["name"]) for t in terminals}
        visible = [bt for bt in browser_tabs if bt["name"] in server_names]
        if visible:
            for i, bt in enumerate(visible):
                marker = "  <-- active" if bt.get("current") else ""
                print(f"  #{i + 1}  Terminal {bt['name']}{marker}")
        else:
            print("[jupyterm] 浏览器中未打开任何 Terminal tab")
    else:
        print("[jupyterm] (CDP 不可用，显示服务端全量列表)", file=sys.stderr)
        for t in terminals:
            print(f"  Terminal {t['name']}")


def cmd_create(args):
    """创建新 Terminal。"""
    cfg = load_config()
    t = create_terminal(cfg)
    print(f"[jupyterm] 已创建 Terminal {t['name']}")


def cmd_exec(args):
    """在 Terminal 中执行命令并打印输出。

    支持 #N / N# 位置语法指定浏览器中从左到右第 N 个 Terminal tab：
      jupyterm exec "#1 pwd"      → 第 1 个可见 Terminal
      jupyterm exec "2# ls -la"   → 第 2 个可见 Terminal
      jupyterm exec -t 12 "pwd"   → 按 server name 指定（向后兼容）
      jupyterm exec "pwd"         → 自动选浏览器当前活跃 Terminal
    """
    cfg = load_config()
    raw_cmd = args.command
    tab_arg = getattr(args, "terminal", None)
    timeout = getattr(args, "timeout", 30.0)

    position, server_name, clean_cmd = _parse_tab_and_cmd(raw_cmd, tab_arg)
    tid, browser_pos = _resolve_terminal(cfg, position, server_name)

    if browser_pos is not None:
        switch_to_terminal_tab(browser_pos)

    output = exec_command(cfg, clean_cmd, terminal_id=tid, timeout=timeout)
    print(output)


def cmd_run(args):
    """把文件内容作为脚本在 Terminal 中执行。支持 -t / #N / N# 位置语法。"""
    cfg = load_config()
    with open(args.file) as f:
        script = f.read()
    tab_arg = getattr(args, "terminal", None)
    timeout = getattr(args, "timeout", 120.0)

    position, server_name, _ = _parse_tab_and_cmd("", tab_arg)
    tid, browser_pos = _resolve_terminal(cfg, position, server_name)

    if browser_pos is not None:
        switch_to_terminal_tab(browser_pos)

    output = exec_command(cfg, script, terminal_id=tid, timeout=timeout)
    print(output)


def cmd_term_signal(args):
    """向 Terminal 发送控制信号（仅 Ctrl+C）。

    支持 #N / N# 与 -t 两种定位方式，默认作用于浏览器当前活跃 Terminal。
    """
    cfg = load_config()
    target = getattr(args, "target", "") or ""
    terminal_arg = getattr(args, "terminal", None)
    signal_name = getattr(args, "signal", "ctrl-c")

    position, server_name, _ = _parse_tab_and_cmd(target, terminal_arg)
    tid, browser_pos = _resolve_terminal(cfg, position, server_name)

    if browser_pos is not None:
        switch_to_terminal_tab(browser_pos)

    send_control(cfg, signal_name, terminal_id=tid)
    print(f"[jupyterm] 已发送 {signal_name} 到 Terminal {tid}")
