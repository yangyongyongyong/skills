#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通过 iTerm2 Python API 在指定会话中执行命令并返回输出。
依赖 Shell Integration（需在目标会话对应的 shell 启动文件中加载）。

官方参考：https://iterm2.com/python-api/examples/runcommand.html
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, List, Optional, Tuple

import iterm2


# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """
    @brief 构建 CLI 参数解析器
    @return 配置好的 ArgumentParser 实例
    """
    parser = argparse.ArgumentParser(
        prog="iterm2_exec",
        description="在 iTerm2 指定会话中执行 shell 命令并输出结果（需 Shell Integration）。",
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    run = sub.add_parser("run", help="在目标会话中执行一条命令")
    run.add_argument(
        "--command", required=True,
        help="要发送给 shell 的完整命令字符串",
    )
    # ── 会话选择（优先级从高到低）──────────────────────────────────────────
    # 1. --session-id：最精确
    run.add_argument("--session-id", default=None, help="目标会话 ID（最精确）")
    # 2. --tab-num：1-based 标签编号，与 iTerm2 ⌘1/⌘2 快捷键一一对应
    #    不指定时默认 1（⌘1），是最常用的选择方式
    run.add_argument(
        "--tab-num", type=int, default=None,
        metavar="N",
        help="标签编号（1=⌘1，2=⌘2，…），不指定则默认 1",
    )
    # 3. 低级索引参数（0 起）：兼容脚本或精确控制场景
    run.add_argument("--window-index", type=int, default=None, help="窗口下标（0 起，默认 0）")
    run.add_argument("--tab-index",    type=int, default=None, help="标签下标（0 起，--tab-num 的底层等价）")
    run.add_argument("--split-index",  type=int, default=None,
                     help="分屏下标（按 session_id 排序后取值，0 起）")
    # ────────────────────────────────────────────────────────────────────────
    run.add_argument(
        "--timeout-seconds", type=float, default=120.0,
        help="等待命令完成的最长时间（秒），默认 120",
    )
    run.add_argument(
        "--json", action="store_true",
        help="以 JSON 格式输出结果（含 stdout / session_id 字段）",
    )
    return parser


# ---------------------------------------------------------------------------
# 会话定位
# ---------------------------------------------------------------------------

def _pick_session_from_tab(tab: iterm2.Tab, split_index: Optional[int], label: str) -> iterm2.Session:
    """
    @brief 从指定 Tab 中按 split_index 或当前会话取出目标 Session
    @param[in] tab         目标标签
    @param[in] split_index 分屏下标（0 起）；为 None 时取当前会话
    @param[in] label       用于错误消息的位置描述（如 "⌘1"）
    @return 目标 Session
    """
    sessions = sorted(tab.sessions, key=lambda s: s.session_id)
    if not sessions:
        raise ValueError(f"标签 {label} 没有可用会话")
    if split_index is not None:
        if split_index < 0 or split_index >= len(sessions):
            raise ValueError(
                f"--split-index {split_index} 越界（标签 {label} 共 {len(sessions)} 个分屏）"
            )
        return sessions[split_index]
    return tab.current_session or sessions[0]


def _resolve_session(app: iterm2.App, args: argparse.Namespace) -> iterm2.Session:
    """
    @brief 按 CLI 参数定位目标 Session，优先级：session-id > tab-num/tab-index > 默认 ⌘1
    @param[in] app  iTerm2 App 实例
    @param[in] args 已解析参数
    @return 目标 Session
    @note --tab-num 是 1-based（⌘N），--tab-index 是 0-based；两者等价，tab-num 优先。
          不传任何选择器时默认使用当前窗口的 ⌘1（第一个标签）。
    """
    # 1. --session-id（最精确）
    if args.session_id:
        session = app.get_session_by_id(args.session_id)
        if session is None:
            raise ValueError(f"找不到 session_id={args.session_id!r}")
        return session

    # 取当前窗口（后续所有分支都需要）
    window: Optional[iterm2.Window] = app.current_window or app.current_terminal_window
    if window is None:
        raise ValueError("无法确定当前 iTerm2 窗口，请确认 iTerm2 正在运行")

    # 如果显式指定了 --window-index，改用该窗口
    if args.window_index is not None:
        windows: List[iterm2.Window] = list(app.windows)
        wi = args.window_index
        if wi < 0 or wi >= len(windows):
            raise ValueError(f"--window-index {wi} 越界（共 {len(windows)} 个窗口）")
        window = windows[wi]

    tabs: List[iterm2.Tab] = list(window.tabs)
    if not tabs:
        raise ValueError("当前窗口没有任何标签")

    # 2. --tab-num（1-based，⌘N）优先于 --tab-index（0-based）
    #    两者皆未指定时默认 tab_num=1（⌘1）
    if args.tab_num is not None:
        tab_num = args.tab_num
        if tab_num < 1 or tab_num > len(tabs):
            raise ValueError(
                f"--tab-num {tab_num} 越界（当前窗口共 {len(tabs)} 个标签，编号 1–{len(tabs)}）"
            )
        ti = tab_num - 1
        label = f"⌘{tab_num}"
    elif args.tab_index is not None:
        ti = args.tab_index
        if ti < 0 or ti >= len(tabs):
            raise ValueError(f"--tab-index {ti} 越界（共 {len(tabs)} 个标签）")
        label = f"⌘{ti + 1}（index {ti}）"
    else:
        # 默认：⌘1（第一个标签）
        ti = 0
        label = "⌘1（默认）"

    return _pick_session_from_tab(tabs[ti], args.split_index, label)


# ---------------------------------------------------------------------------
# 命令执行
# ---------------------------------------------------------------------------

async def _collect_output(session: iterm2.Session, start_y: int, end_y: int) -> str:
    """
    @brief 将会话缓冲区 [start_y, end_y) 行拼接为字符串
    @param[in] session 目标会话
    @param[in] start_y 起始行号（含）
    @param[in] end_y   结束行号（不含）
    @return 拼接后的输出文本
    """
    count = end_y - start_y
    if count <= 0:
        return ""
    lines = await session.async_get_contents(start_y, count)
    buf: List[str] = []
    for line in lines:
        buf.append(line.string)
        if line.hard_eol:
            buf.append("\n")
    return "".join(buf)


def _line_range(li: Any) -> Tuple[int, int]:
    """
    @brief 返回会话缓冲区当前可读的行号范围 [first, last)
    @param[in] li session.async_get_line_info() 返回的 SessionLineInfo
    @return (first_line, end_line) 半开区间
    """
    first = li.overflow
    end = li.overflow + li.scrollback_buffer_height + li.mutable_area_height
    return first, end


_SENTINEL_IDENTITY = "iterm2-exec"

# session_id → True/False 缓存，避免每次命令都探测一次（同一 session 状态不会变）
_SI_CACHE: Dict[str, bool] = {}


async def _find_last_content_line(session: iterm2.Session) -> int:
    """
    @brief 在当前可见区域找到最后一行有内容的绝对行号
    @param[in] session 目标会话
    @return 最后有内容行的绝对行号；若可见区为空则返回 first_visible_line_number
    """
    li = await session.async_get_line_info()
    start = li.first_visible_line_number
    end = li.overflow + li.scrollback_buffer_height + li.mutable_area_height
    if end <= start:
        return start
    lines = await session.async_get_contents(start, end - start)
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].string.strip():
            return start + i
    return start


async def _calc_echo_wrap_lines(
    session: iterm2.Session,
    cmd_text: str,
    cursor_before: int,
) -> int:
    """
    @brief 计算命令 echo（prompt + cmd_text）在终端中实际占用的行数
    @param[in] session       目标会话
    @param[in] cmd_text      实际发送给终端的命令字符串（不含末尾 \\r）
    @param[in] cursor_before 发命令前旧提示符所在的绝对行号
    @return echo 占用的行数（最少 1 行）
    @note prompt 长度通过读取 cursor_before 行内容获得，加上 cmd_text 一起计算 wrap。
    """
    import math
    try:
        cols_str = await session.async_get_variable("columns")
        cols = int(cols_str) if cols_str else 0
    except Exception:
        cols = 0
    if cols <= 0:
        return 2

    try:
        lines = await session.async_get_contents(cursor_before, 1)
        prompt_len = len(lines[0].string) if lines else 0
    except Exception:
        prompt_len = 0

    total_len = prompt_len + len(cmd_text)
    return max(1, math.ceil(total_len / cols))


# ---------------------------------------------------------------------------
# SI 活跃性探测
# ---------------------------------------------------------------------------

async def _detect_si_active(
    connection: iterm2.Connection,
    session: iterm2.Session,
) -> bool:
    """
    @brief 可靠判断目标会话当前 shell 是否有活跃的 Shell Integration
    @param[in] connection 与 iTerm2 的 API 连接
    @param[in] session    目标会话
    @return True = SI 活跃（可用纯 SI 模式）；False = 无 SI（使用哨兵模式）
    @note 通过向 session 发送无副作用的 `true` 命令并等待 PromptMonitor 事件来探测。
          SI 活跃时 PromptMonitor 在 <200ms 内收到 COMMAND_END；无 SI 时 1s 超时。
          结果缓存到 _SI_CACHE[session_id]，同一 session 生命周期内只探测一次。
    @attention 探测会向终端发送一条 `true` 命令（回显可见但无输出）；
               仅在缓存缺失时触发，不影响正常命令执行。
    """
    sid = session.session_id
    if sid in _SI_CACHE:
        return _SI_CACHE[sid]

    MODES = [
        iterm2.PromptMonitor.Mode.COMMAND_START,
        iterm2.PromptMonitor.Mode.COMMAND_END,
        iterm2.PromptMonitor.Mode.PROMPT,
    ]
    M = iterm2.PromptMonitor.Mode
    detected = asyncio.Event()

    async def _probe_watcher(mon: iterm2.PromptMonitor) -> None:
        """
        @brief 监听 PromptMonitor，收到 COMMAND_END 或 COMMAND_START+PROMPT 则置位
        @param[in] mon 已进入上下文的 PromptMonitor
        @return none
        """
        saw_start = False
        while not detected.is_set():
            try:
                kind, _ = await asyncio.wait_for(mon.async_get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            if kind == M.COMMAND_START:
                saw_start = True
            elif kind == M.COMMAND_END:
                detected.set()
                return
            elif kind == M.PROMPT and saw_start:
                detected.set()
                return

    result = False
    async with iterm2.PromptMonitor(connection, session.session_id, MODES) as mon:
        await session.async_send_text("true\r")
        watcher = asyncio.ensure_future(_probe_watcher(mon))
        try:
            await asyncio.wait_for(detected.wait(), timeout=1.0)
            result = True
        except asyncio.TimeoutError:
            result = False
        finally:
            watcher.cancel()

    _SI_CACHE[sid] = result
    return result


# ---------------------------------------------------------------------------
# 模式一：纯 SI 模式（本地 macOS shell，免疫 resize）
# ---------------------------------------------------------------------------

async def _run_command_si(
    connection: iterm2.Connection,
    session: iterm2.Session,
    command: str,
    timeout: float,
) -> str:
    """
    @brief 纯 Shell Integration 模式：发裸命令，用 prompt.output_range 精确截取输出
    @param[in] connection 与 iTerm2 的 API 连接
    @param[in] session    目标会话
    @param[in] command    命令字符串（\\n 自动转 \\r）
    @param[in] timeout    超时秒数；超时后发 Ctrl+C
    @return 命令输出文本
    @note 终端里用户只看到原始命令（`% actual_cmd`），完全无 wrapper 噪声。
          output_range 由 iTerm2 跟踪语义边界，resize 不影响结果。
    """
    MODES = [
        iterm2.PromptMonitor.Mode.COMMAND_START,
        iterm2.PromptMonitor.Mode.COMMAND_END,
        iterm2.PromptMonitor.Mode.PROMPT,
    ]
    M = iterm2.PromptMonitor.Mode

    # 发命令前取当前 prompt 的 unique_id，用于事后精确定位 output_range
    prompt = await iterm2.async_get_last_prompt(connection, session.session_id)

    done_event = asyncio.Event()

    async def _si_watcher(mon: iterm2.PromptMonitor) -> None:
        """
        @brief 等待命令结束的 PromptMonitor 事件
        @param[in] mon 已进入上下文的 PromptMonitor
        @return none
        """
        saw_start = False
        while not done_event.is_set():
            try:
                kind, _ = await asyncio.wait_for(mon.async_get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            if kind == M.COMMAND_START:
                saw_start = True
            elif kind == M.COMMAND_END:
                done_event.set()
                return
            elif kind == M.PROMPT and saw_start:
                done_event.set()
                return

    async with iterm2.PromptMonitor(connection, session.session_id, MODES) as mon:
        cmd_text = command.replace("\n", "\r") + "\r"
        await session.async_send_text(cmd_text)
        watcher = asyncio.ensure_future(_si_watcher(mon))
        try:
            await asyncio.wait_for(done_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            await session.async_send_text("\x03")
            watcher.cancel()
            raise
        finally:
            watcher.cancel()

    if prompt is None:
        return ""
    updated = await iterm2.async_get_prompt_by_id(
        connection, session.session_id, prompt.unique_id
    )
    if updated is None or updated.output_range is None:
        return ""
    r = updated.output_range
    return await _collect_output(session, r.start.y, r.end.y)


# ---------------------------------------------------------------------------
# 模式二：哨兵模式（SSH / Docker / 无 SI 的任意 shell）
# ---------------------------------------------------------------------------

async def _run_command_sentinel(
    connection: iterm2.Connection,
    session: iterm2.Session,
    command: str,
    timeout: float,
) -> str:
    """
    @brief 哨兵模式：用不可见 Custom Escape Sequence 检测命令完成，行号法截取输出
    @param[in] connection 与 iTerm2 的 API 连接
    @param[in] session    目标会话
    @param[in] command    命令字符串（\\n 自动转 \\r）
    @param[in] timeout    超时秒数；超时后发 Ctrl+C
    @return 命令输出文本
    @note 终端显示 `% { actual_cmd; }; printf '...'`（有 wrapper），但无可见标记行。
          输出定位依赖发命令前光标行 + echo wrap 行数，resize 期间执行命令可能偏差。
    """
    import uuid as _uuid
    uid = _uuid.uuid4().hex
    end_id = f"E{uid}"

    def _esc(payload: str) -> str:
        return f"printf '\\033]1337;Custom=id={_SENTINEL_IDENTITY}:{payload}\\007'"

    cmd_body = command.replace(chr(10), chr(13))
    wrapped  = f"{{ {cmd_body}; }}; {_esc(end_id)}"

    cursor_before = await _find_last_content_line(session)
    echo_lines    = await _calc_echo_wrap_lines(session, wrapped, cursor_before)

    MODES = [
        iterm2.PromptMonitor.Mode.COMMAND_START,
        iterm2.PromptMonitor.Mode.COMMAND_END,
        iterm2.PromptMonitor.Mode.PROMPT,
    ]
    M = iterm2.PromptMonitor.Mode
    end_event = asyncio.Event()

    async def _si_watcher(mon: iterm2.PromptMonitor) -> None:
        """
        @brief 竞速监听 PromptMonitor（万一 SI 在探测后切回）
        @param[in] mon PromptMonitor 上下文
        @return none
        """
        saw_start = False
        while not end_event.is_set():
            try:
                kind, _ = await asyncio.wait_for(mon.async_get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            if kind == M.COMMAND_START:
                saw_start = True
            elif kind == M.COMMAND_END:
                end_event.set()
                return
            elif kind == M.PROMPT and saw_start:
                end_event.set()
                return

    async def _ccs_watcher(mon: "iterm2.CustomControlSequenceMonitor") -> None:
        """
        @brief 等待 END 哨兵事件
        @param[in] mon CustomControlSequenceMonitor 上下文
        @return none
        """
        while not end_event.is_set():
            try:
                await asyncio.wait_for(mon.async_get(), timeout=0.2)
                end_event.set()
                return
            except asyncio.TimeoutError:
                continue

    async with iterm2.PromptMonitor(connection, session.session_id, MODES) as si_mon:
        async with iterm2.CustomControlSequenceMonitor(
            connection, _SENTINEL_IDENTITY, f"^{end_id}$", session.session_id
        ) as ccs_mon:
            await session.async_send_text(wrapped + "\r")
            si_task  = asyncio.ensure_future(_si_watcher(si_mon))
            ccs_task = asyncio.ensure_future(_ccs_watcher(ccs_mon))
            try:
                await asyncio.wait_for(end_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                await session.async_send_text("\x03")
                si_task.cancel()
                ccs_task.cancel()
                raise
            finally:
                si_task.cancel()
                ccs_task.cancel()

    # 哨兵比 shell 渲染新提示符快，稍等确保提示符已渲染
    await asyncio.sleep(0.15)

    output_start = cursor_before + echo_lines
    output_end   = await _find_last_content_line(session)
    return await _collect_output(session, output_start, output_end)


# ---------------------------------------------------------------------------
# 统一入口：检测模式 → 分路执行
# ---------------------------------------------------------------------------

async def _run_command(
    connection: iterm2.Connection,
    session: iterm2.Session,
    command: str,
    timeout: float,
) -> str:
    """
    @brief 执行命令的统一入口：自动检测 SI 状态并选择最优模式
    @param[in] connection 与 iTerm2 的 API 连接
    @param[in] session    目标会话
    @param[in] command    命令字符串
    @param[in] timeout    超时秒数
    @return 命令输出文本
    @note SI 活跃（本地 macOS shell）→ _run_command_si：裸命令 + output_range，
          免疫 resize，终端完全干净。
          SI 不活跃（SSH / Docker / 未装 SI）→ _run_command_sentinel：哨兵模式，
          resize 期间执行有偏差风险（已知限制）。
    """
    si_active = await _detect_si_active(connection, session)

    if si_active:
        return await _run_command_si(connection, session, command, timeout)

    print(
        "[iterm2-exec] 当前会话未检测到 Shell Integration，使用哨兵模式。\n"
        "  本地 macOS shell 请安装 Shell Integration 以获得最佳体验：\n"
        "  iTerm2 菜单 → Shell Integration → Install Shell Integration",
        file=sys.stderr,
    )
    return await _run_command_sentinel(connection, session, command, timeout)


# ---------------------------------------------------------------------------
# 异步主流程
# ---------------------------------------------------------------------------

async def _ensure_tab_active(app: iterm2.App, session: iterm2.Session) -> bool:
    """
    @brief 若目标会话所在标签不是当前活动标签，则自动切换过去
    @param[in] app     iTerm2 App 实例
    @param[in] session 目标会话
    @return True 表示发生了切换，False 表示无需切换
    @note 切换后 PromptMonitor 仍工作在目标 session_id 上，不受影响。
    """
    window, tab = app.get_tab_and_window_for_session(session)
    if tab is None or window is None:
        return False
    if window.current_tab is not None and window.current_tab.tab_id == tab.tab_id:
        return False
    await tab.async_activate()
    return True


async def _async_main(connection: iterm2.Connection, args: argparse.Namespace) -> int:
    """
    @brief iTerm2 连接建立后的主逻辑
    @param[in] connection 与 iTerm2 的 API 连接
    @param[in] args       已解析参数
    @return 进程退出码（0 成功 / 2 配置错误 / 124 超时）
    """
    app = await iterm2.async_get_app(connection)

    try:
        session = _resolve_session(app, args)
    except ValueError as exc:
        print(f"会话定位失败：{exc}", file=sys.stderr)
        return 2

    # 若目标标签不是当前活动标签，自动切换（静默，无提示）
    await _ensure_tab_active(app, session)

    try:
        output = await _run_command(
            connection, session, args.command, args.timeout_seconds
        )
    except asyncio.TimeoutError:
        print(
            f"超时（{args.timeout_seconds}s）：命令未在限定时间内结束。\n"
            "请检查 Shell Integration 是否生效，或用 --timeout-seconds 调大限制。",
            file=sys.stderr,
        )
        return 124
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.json:
        result: Dict[str, Any] = {
            "stdout": output,
            "session_id": session.session_id,
            "note": "exit_code_not_guaranteed_by_prompt_range",
        }
        print(json.dumps(result, ensure_ascii=False))
    else:
        sys.stdout.write(output)
        if output and not output.endswith("\n"):
            sys.stdout.write("\n")

    return 0


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> None:
    """
    @brief 脚本入口：解析参数、连接 iTerm2 并执行命令
    @return none
    @note 使用 nonlocal 变量捕获退出码，避免 iterm2.run_until_complete
          不传递协程返回值导致退出码被吞的问题。
    """
    args = _build_parser().parse_args()
    exit_code = 1

    async def _runner(connection: iterm2.Connection) -> None:
        """
        @brief 包装 _async_main，通过闭包捕获退出码
        @param[in] connection 与 iTerm2 的 API 连接
        @return none
        """
        nonlocal exit_code
        exit_code = await _async_main(connection, args)

    try:
        iterm2.run_until_complete(_runner)
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败：{exc}", file=sys.stderr)
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
