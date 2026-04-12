#!/usr/bin/env python3
"""
jscmd — 通过 Chrome CDP 与 JumpServer Web 终端交互的 CLI 工具。

使用方式：
  jscmd daemon start              # 启动后台 daemon（Chrome 弹框一次，1h空闲自动退出）
  jscmd daemon stop               # 停止 daemon
  jscmd daemon status             # 查看 daemon 状态
  jscmd list                      # 列出所有 JumpServer 终端标签
  jscmd exec "ls -la"             # 在活跃 terminal 执行
  jscmd exec "#2 ls -la"         # 在第 2 个标签执行（#N 或 N# 语法）
  jscmd exec "@web ls -la"       # 按 hostname/别名 定位标签执行
  jscmd sessions                  # 查看所有标签的实时 hostname + 别名
  jscmd sessions --refresh        # 强制重新检测所有标签 hostname
  jscmd alias @current web        # 给活跃标签的 hostname 加别名
  jscmd alias #2 db               # 给第 2 个标签的 hostname 加别名
  jscmd alias --remove web        # 删除别名
  jscmd mode python               # 切换活跃标签到 Python REPL 模式
  jscmd mode shell                # 切换回 Shell 模式
  jscmd connect "web-01"          # 在 JumpServer 侧边栏搜索并打开服务器终端

前提：Chrome 需在 chrome://inspect/#remote-debugging 中开启远程调试。
daemon 在首次启动时 Chrome 会弹出一次授权弹窗，之后同一 Chrome 会话内不再弹出。
"""

import argparse
import asyncio
import base64
import glob
import json
import os
import re
import signal
import socket
import sys
import time
import threading
import uuid

# websockets 14+ 改了 API
try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    from websockets.legacy.client import connect as ws_connect

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

DAEMON_SOCK = os.path.expanduser("~/.jscmd.sock")
DAEMON_PID_FILE = os.path.expanduser("~/.jscmd.pid")
ALIASES_FILE = os.path.expanduser("~/.jscmd_aliases.json")
CONFIG_FILE = os.path.expanduser("~/.jscmd_config.json")

LUNA_URL_KEYWORD = "/luna/"
KOKO_URL_KEYWORD = "/koko/connect/"

# ANSI / xterm 转义剥离正则（覆盖 CSI / OSC / DCS / PM / APC / 私有序列）
ANSI_RE = re.compile(
    r'\x1b(?:'
    r'\[[0-9;?<>!]*[a-zA-Z@`]'          # CSI 序列（含私有参数前缀）
    r'|\][^\x07\x1b]*(?:\x07|\x1b\\)'   # OSC 序列
    r'|P[^\x1b]*(?:\x1b\\|$)'           # DCS 序列
    r'|[X^_][^\x1b]*(?:\x1b\\|$)'       # PM / APC 序列
    r'|\([ABJ012]'                       # 字符集切换
    r'|[^[\]PX^_(]'                      # 其他双字节转义
    r')'
    r'|\x08+'                            # backspace（多个连续）
    r'|\x00'                             # NUL 字节
    r'|\r(?!\n)'                         # 孤立 CR
)

# ---------------------------------------------------------------------------
# 配置加载
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "delete_sleep_seconds": 10,
    "extra_block_patterns": [],
    "extra_confirm_patterns": [],
    "disable_safety": False,
    # daemon 服务化配置
    "idle_timeout_seconds": 3600,    # 空闲自关超时（秒），0=禁用
    "cdp_connect_retries": 3,        # Chrome WS 连接重试次数
    "cdp_connect_timeout": 20,       # 每次连接超时（秒）
    # JumpServer Luna 搜索 UI 选择器（可按实际页面覆盖）
    "luna_search_selector": "",      # 空=自动探测
    "luna_asset_item_selector": "",  # 空=自动探测
}


def load_config() -> dict:
    """加载 ~/.jscmd_config.json，不存在时创建默认配置并返回。"""
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        return dict(DEFAULT_CONFIG)
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    merged = dict(DEFAULT_CONFIG)
    merged.update(cfg)
    return merged


def load_aliases() -> dict:
    """加载 ~/.jscmd_aliases.json，返回 {hostname: [alias, ...]} 映射。"""
    if not os.path.exists(ALIASES_FILE):
        return {}
    with open(ALIASES_FILE) as f:
        return json.load(f)


def save_aliases(aliases: dict):
    """保存别名映射到文件。"""
    with open(ALIASES_FILE, "w") as f:
        json.dump(aliases, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# SafetyChecker
# ---------------------------------------------------------------------------

# Level 1: 直接 BLOCK（正则，匹配则拒绝）
_BLOCK_PATTERNS = [
    # 修改系统 Python 解释器
    r"update-alternatives\s+.*python",
    r"ln\s+.*-s.*\bpython[23]?\b",
    r"pyenv\s+global\b",
    # 写入系统关键配置文件
    r">\s*/etc/(passwd|shadow|sudoers|fstab|group|gshadow|hosts)",
    r"tee\s+/etc/(passwd|shadow|sudoers|fstab)",
    r"echo\s+.*>+\s*/etc/(passwd|shadow|sudoers|fstab)",
    # 卸载系统 Python
    r"apt(-get)?\s+(remove|purge)\s+.*python3?",
    r"yum\s+(remove|erase)\s+.*python3?",
    r"dnf\s+(remove|erase)\s+.*python3?",
    r"brew\s+uninstall\s+.*python",
    # 删除系统关键目录
    r"rm\s+.*-[a-z]*[rf][a-z]*\s+(/usr|/lib|/lib64|/bin|/sbin|/etc|/boot|/sys|/proc|/dev)(/|\s|$)",
    r"rm\s+/usr\b",
    r"rm\s+/etc\b",
    r"rm\s+/bin\b",
    # 内核/驱动操作
    r"rmmod\b",
    r"modprobe\s+-r\b",
]

# Level 2: 需要用户确认 + 倒计时（正则）
_CONFIRM_PATTERNS = [
    r"\brm\s+",
    r"\brmdir\b",
    r"\bunlink\b",
    r"\btruncate\b",
    r"\bdd\s+if=",
    r"\bmkfs\b",
    r"\bshred\b",
    r"\bwipefs\b",
    r"\bmkswap\b",
    r"\bfdisk\b",
    r"\bparted\b",
]

# Level 3: WARN（正则）
_WARN_PATTERNS = [
    r"^\s*sudo\b",
    r"\bsystemctl\s+(stop|disable|mask|kill)\b",
    r"\bservice\s+\S+\s+(stop|restart)\b",
    r"\bpip3?\s+uninstall\b",
    r"\bnpm\s+uninstall\s+-g\b",
    r"\bchmod\s+[0-7]*7[0-7][0-7]\s+/",
    r"\bchown\s+\S+\s+/",
]


class SafetyChecker:
    """命令安全三级检查器：BLOCK / CONFIRM+SLEEP / WARN。"""

    LEVEL_OK = "ok"
    LEVEL_WARN = "warn"
    LEVEL_CONFIRM = "confirm"
    LEVEL_BLOCK = "block"

    def __init__(self, config: dict):
        """初始化检查器，加载配置。"""
        self.sleep_seconds = int(config.get("delete_sleep_seconds", 10))
        self.disabled = bool(config.get("disable_safety", False))

        extra_block = config.get("extra_block_patterns", [])
        extra_confirm = config.get("extra_confirm_patterns", [])

        self._block_re = [re.compile(p, re.IGNORECASE) for p in _BLOCK_PATTERNS + extra_block]
        self._confirm_re = [re.compile(p, re.IGNORECASE) for p in _CONFIRM_PATTERNS + extra_confirm]
        self._warn_re = [re.compile(p, re.IGNORECASE) for p in _WARN_PATTERNS]

    def check(self, cmd: str) -> tuple:
        """检查命令安全级别。

        @param[in] cmd 待检查的命令字符串
        @return (level, reason) 其中 level 为 LEVEL_* 常量，reason 为触发原因
        """
        if self.disabled:
            return self.LEVEL_OK, ""

        for pattern in self._block_re:
            if pattern.search(cmd):
                return self.LEVEL_BLOCK, f"匹配危险规则: {pattern.pattern}"

        for pattern in self._confirm_re:
            if pattern.search(cmd):
                return self.LEVEL_CONFIRM, f"检测到删除类操作: {pattern.pattern}"

        for pattern in self._warn_re:
            if pattern.search(cmd):
                return self.LEVEL_WARN, f"检测到高风险操作: {pattern.pattern}"

        return self.LEVEL_OK, ""

    def enforce(self, cmd: str) -> bool:
        """在 CLI 侧强制执行安全策略，返回 True 表示可继续执行。

        @param[in] cmd 待检查的命令字符串
        @return True 允许执行，False 拒绝执行
        """
        level, reason = self.check(cmd)

        if level == self.LEVEL_BLOCK:
            print(f"[BLOCK] 命令已拒绝: {reason}", file=sys.stderr)
            print(f"  命令: {cmd}", file=sys.stderr)
            return False

        if level == self.LEVEL_CONFIRM:
            print(f"[WARN] {reason}", file=sys.stderr)
            print(f"  命令: {cmd}", file=sys.stderr)
            try:
                ans = input("请输入 yes 确认执行（其他输入取消）: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\n已取消。", file=sys.stderr)
                return False
            if ans != "yes":
                print("已取消。", file=sys.stderr)
                return False
            if self.sleep_seconds > 0:
                print(f"[INFO] 将在 {self.sleep_seconds} 秒后执行，Ctrl+C 可取消...", file=sys.stderr)
                try:
                    for remaining in range(self.sleep_seconds, 0, -1):
                        print(f"  {remaining}...", end=" ", flush=True)
                        time.sleep(1)
                    print()
                except KeyboardInterrupt:
                    print("\n已取消。", file=sys.stderr)
                    return False
            return True

        if level == self.LEVEL_WARN:
            print(f"[WARN] {reason}", file=sys.stderr)

        return True


# ---------------------------------------------------------------------------
# Chrome CDP 发现
# ---------------------------------------------------------------------------

def _find_active_ports() -> list:
    """扫描所有 Chrome/Chromium 实例的 DevToolsActivePort 文件。

    @return [(port, ws_path), ...] 列表
    """
    patterns = [
        "~/Library/Application Support/Google/Chrome/DevToolsActivePort",
        "~/Library/Application Support/Google/Chrome/*/DevToolsActivePort",
        "~/Library/Application Support/Chromium/DevToolsActivePort",
        "~/Library/Application Support/Chromium/*/DevToolsActivePort",
        # Linux
        "~/.config/google-chrome/DevToolsActivePort",
        "~/.config/google-chrome/*/DevToolsActivePort",
        "~/.config/chromium/DevToolsActivePort",
    ]
    results = []
    seen_ports = set()
    for pattern in patterns:
        for path in glob.glob(os.path.expanduser(pattern)):
            try:
                lines = open(path).read().strip().split("\n")
                if len(lines) >= 2:
                    port = int(lines[0].strip())
                    ws_path = lines[1].strip()
                    if port not in seen_ports:
                        seen_ports.add(port)
                        results.append((port, ws_path))
            except Exception:
                pass
    return results


# ---------------------------------------------------------------------------
# CDP 辅助
# ---------------------------------------------------------------------------

async def _cdp_send(ws, session_id: str, method: str, params: dict, msg_id: int) -> dict:
    """发送 CDP 消息并等待对应 id 的响应。

    @param[in] ws       WebSocket 连接
    @param[in] session_id CDP session id（空字符串=浏览器级）
    @param[in] method   CDP 方法名
    @param[in] params   方法参数
    @param[in] msg_id   消息 id
    @return 响应的 result 字段（dict）
    """
    msg = {"id": msg_id, "method": method, "params": params}
    if session_id:
        msg["sessionId"] = session_id
    await ws.send(json.dumps(msg))

    deadline = asyncio.get_event_loop().time() + 10
    while asyncio.get_event_loop().time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
        except asyncio.TimeoutError:
            break
        data = json.loads(raw)
        if data.get("id") == msg_id:
            return data.get("result", {})
        # 不是目标响应继续等
    return {}


async def _cdp_eval(ws, session_id: str, expression: str, context_id: int,
                    msg_id: int, timeout: float = 5.0):
    """在指定 contextId 中执行 JS 表达式，返回结果值或 None。

    @param[in] ws          WebSocket 连接
    @param[in] session_id  CDP session id
    @param[in] expression  JS 表达式字符串
    @param[in] context_id  执行上下文 id（0=默认）
    @param[in] msg_id      消息 id
    @param[in] timeout     超时秒数
    @return JS 结果值（string/bool/number）或 None
    """
    params = {
        "expression": expression,
        "returnByValue": True,
    }
    if context_id:
        params["contextId"] = context_id

    msg = {"id": msg_id, "method": "Runtime.evaluate",
           "params": params, "sessionId": session_id}
    await ws.send(json.dumps(msg))

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=min(2.0, timeout))
        except asyncio.TimeoutError:
            break
        data = json.loads(raw)
        if data.get("id") == msg_id:
            result = data.get("result", {}).get("result", {})
            if result.get("type") in ("string", "boolean", "number"):
                return result.get("value")
            return None
    return None


def _strip_ansi(s: str) -> str:
    """剥离 ANSI 转义序列，返回纯文本。

    @param[in] s 含 ANSI 的字符串
    @return 纯文本字符串
    """
    return ANSI_RE.sub("", s)


def _extract_output(raw: str, sentinel: str, cmd_lines: list,
                    mode: str = "shell") -> str:
    """从终端原始输出中提取命令执行结果，剥离回显和哨兵行。

    Shell 模式：
      命令在最后一行末尾追加 printf sentinel；sentinel 回显行触发收集开始。
    解释器模式（python/node/ruby）：
      sentinel 是独立的最后一行；提示符行（>>> / ... / > 等）被过滤；
      收集从第一个非提示符、非哨兵行开始到哨兵前结束。

    @param[in] raw       原始终端输出（含 ANSI）
    @param[in] sentinel  哨兵字符串
    @param[in] cmd_lines 已发送的命令行列表（用于识别回显）
    @param[in] mode      解释器模式（"shell"/"python"/"node"/"ruby"）
    @return 提取出的输出字符串
    """
    clean = _strip_ansi(raw)
    clean = clean.replace("\r\n", "\n").replace("\r", "\n")

    # 找哨兵位置（取最后一次出现，防止输出中偶发相似字符串）
    sentinel_pos = clean.rfind(sentinel)
    if sentinel_pos < 0:
        # 未找到哨兵（超时），返回全部内容并做基本清理
        all_lines = clean.split("\n")
        return "\n".join(ln.rstrip() for ln in all_lines).strip()

    before = clean[:sentinel_pos]
    all_lines = before.split("\n")

    # ---- 解释器模式（Python/Node/Ruby）-------------------------------------
    if mode != "shell":
        cfg = INTERPRETER_CONFIGS.get(mode, INTERPRETER_CONFIGS["python"])
        prompt_re = re.compile(cfg["prompt_re"])
        result_lines = []
        for line in all_lines:
            stripped = line.rstrip()
            # 跳过：解释器提示符行（>>> 1+1 / ... / > 等）
            if prompt_re.match(stripped):
                continue
            # 跳过：含哨兵字符串的行（哨兵的 print 命令回显或输出）
            if sentinel in stripped:
                continue
            result_lines.append(stripped)
        # 去掉首尾多余空行
        while result_lines and not result_lines[0].strip():
            result_lines.pop(0)
        while result_lines and not result_lines[-1].strip():
            result_lines.pop()
        return "\n".join(result_lines)

    # ---- Shell 模式 ---------------------------------------------------------
    # 构建"应跳过"集合：包含哨兵命令片段的行属于命令回显
    skip_markers = [sentinel, "printf '\\n" + sentinel]
    last_cmd_part = cmd_lines[-1].split(";")[0].strip() if cmd_lines else ""

    result_lines = []
    collecting = False

    for line in all_lines:
        stripped_line = line.rstrip()
        is_echo = any(m in line for m in skip_markers)

        if is_echo:
            collecting = True
            continue
        if not collecting:
            continue
        result_lines.append(stripped_line)

    # 如果 collecting 从未触发（单行命令无回显时），退回简单策略
    if not collecting:
        result_lines = [ln.rstrip() for ln in all_lines]
        result_lines = [
            ln for ln in result_lines
            if not re.match(r"^[^\s]*[@%#$][^\s]*.*[$#]\s*$", ln)
        ]

    # 去掉首尾多余空行
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()

    return "\n".join(result_lines)


# ---------------------------------------------------------------------------
# CDP 消息分发器（解决并发 recv 冲突）
# ---------------------------------------------------------------------------

class CDPDispatcher:
    """将单个 WebSocket 连接上的 CDP 消息分发给多个并发等待者。

    后台循环统一负责 recv()；各调用方通过 Future 或 Queue 异步取结果，
    彻底消除"cannot call recv while another coroutine is already running"。
    """

    def __init__(self, ws):
        """初始化分发器。

        @param[in] ws 已建立的 WebSocket 连接对象
        """
        self._ws = ws
        self._pending: dict = {}            # msg_id → asyncio.Future
        self._net_events: asyncio.Queue = asyncio.Queue()  # 终端 WS 帧事件
        self._event_handlers: dict = {}    # method_name → asyncio.Queue
        self._task = None

    async def start(self):
        """启动后台接收循环。

        @return none
        """
        self._task = asyncio.create_task(self._recv_loop())

    async def stop(self):
        """停止后台接收循环。

        @return none
        """
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def add_event_handler(self, method: str, queue: asyncio.Queue):
        """注册额外的事件监听队列，将匹配 method 的消息投入 queue。

        @param[in] method CDP 事件方法名（如 Runtime.executionContextCreated）
        @param[in] queue  接收事件的 asyncio.Queue
        @return none
        """
        self._event_handlers[method] = queue

    def remove_event_handler(self, method: str):
        """取消注册指定方法的事件监听。

        @param[in] method CDP 事件方法名
        @return none
        """
        self._event_handlers.pop(method, None)

    async def _recv_loop(self):
        """后台 WS 接收循环，将消息路由到对应 Future 或事件队列。

        @return none
        """
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                mid = msg.get("id")
                if mid is not None and mid in self._pending:
                    fut = self._pending.pop(mid)
                    if not fut.done():
                        fut.set_result(msg)
                elif msg.get("method") == "Network.webSocketFrameReceived":
                    await self._net_events.put(msg)
                # 将其他事件投给已注册的监听队列
                method = msg.get("method")
                if method and method in self._event_handlers:
                    await self._event_handlers[method].put(msg)
        except Exception as e:
            # 连接断开时通知所有等待者
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(e)

    async def request(self, msg: dict, timeout: float = 10.0) -> dict:
        """发送 CDP 消息并等待对应 id 的响应。

        @param[in] msg     已构建好的 CDP 消息 dict（含 id）
        @param[in] timeout 超时秒数
        @return 响应的 result 字段 dict，超时返回 {}
        """
        mid = msg["id"]
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self._pending[mid] = fut
        await self._ws.send(json.dumps(msg))
        try:
            result = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            return result.get("result", {})
        except asyncio.TimeoutError:
            self._pending.pop(mid, None)
            return {}

    async def send_only(self, msg: dict):
        """发送 CDP 消息，不等待响应（fire-and-forget）。

        @param[in] msg CDP 消息 dict
        @return none
        """
        await self._ws.send(json.dumps(msg))

    async def collect_events(self, sentinel: str, timeout: float) -> tuple:
        """从终端事件队列收集输出直到哨兵出现或超时。

        使用 '\\nSENTINEL' 而非 'SENTINEL' 区分命令回显与真实输出。

        @param[in] sentinel 哨兵字符串
        @param[in] timeout  最大等待秒数
        @return (output_buf: str, timed_out: bool)
        """
        end_marker = "\n" + sentinel
        output_buf = ""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if end_marker in output_buf:
                return output_buf, False
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                event = await asyncio.wait_for(
                    self._net_events.get(),
                    timeout=min(0.4, remaining))
                resp = event["params"]["response"]
                opcode = resp.get("opcode", -1)
                if opcode == 2:
                    decoded = base64.b64decode(
                        resp["payloadData"]).decode("utf-8", errors="replace")
                    output_buf += decoded
                elif opcode == 1:
                    try:
                        d = json.loads(resp["payloadData"])
                        output_buf += d.get("data", "")
                    except Exception:
                        pass
            except asyncio.TimeoutError:
                pass
        return output_buf, True


# ---------------------------------------------------------------------------
# Daemon 核心
# ---------------------------------------------------------------------------

# 特殊键定义：key名 → (DOM key, 控制字符, windowsVirtualKeyCode)
_SPECIAL_KEYS = {
    "ctrl-c":  ("c",    "\x03", 67),
    "ctrl-d":  ("d",    "\x04", 68),
    "ctrl-z":  ("z",    "\x1a", 90),
    "ctrl-l":  ("l",    "\x0c", 76),
    "ctrl-u":  ("u",    "\x15", 85),
    "ctrl-w":  ("w",    "\x17", 87),
    "ctrl-\\": ("\\",   "\x1c", 220),
}

# 解释器模式配置
# append_mode:
#   "suffix" → 哨兵追加到最后一行末尾（bash/shell 模式）
#   "newline" → 哨兵作为独立新行追加（Python/Node/Ruby 模式）
INTERPRETER_CONFIGS = {
    "shell":  {
        "sentinel_cmd": "printf '\\n{s}\\n'",
        "append_mode":  "suffix",       # "; sentinel_cmd" 追加到最后一行
        "trigger_cmds": [],
        "prompt_re":    r"^.*[$#]\s*$", # bash/sh 提示符
    },
    "python": {
        "sentinel_cmd": "print('{s}')",
        "append_mode":  "newline",
        "trigger_cmds": ["python", "python3", "python2", "ipython", "ipython3"],
        "prompt_re":    r"^(>>>|\.\.\.) ",
    },
    "node": {
        "sentinel_cmd": "console.log('{s}')",
        "append_mode":  "newline",
        "trigger_cmds": ["node", "nodejs"],
        "prompt_re":    r"^> ",
    },
    "ruby": {
        "sentinel_cmd": "puts '{s}'",
        "append_mode":  "newline",
        "trigger_cmds": ["irb", "pry"],
        "prompt_re":    r"^(irb|>>|\d+>)",
    },
}

# 执行这些命令后回到 shell 模式
INTERPRETER_EXIT_CMDS = frozenset({
    "exit", "exit()", "quit", "quit()", ".exit", "^D",
    "exit 0", "exit 1",
})


class JscmdDaemon:
    """JumpServer CLI daemon，持久维持 Chrome CDP 连接。"""

    def __init__(self):
        """初始化 daemon 状态。"""
        self.browser_ws = None          # 浏览器级 WS 连接
        self.session_id = None          # Luna 页面 CDP session
        self.luna_target_id = None
        self.iframe_contexts = []       # [(context_id, iframe_idx), ...]，按 DOM 顺序
        self._id_counter = 0
        # 运行时 hostname 缓存（不持久化）: {iframe_idx: hostname}
        self.hostname_cache: dict = {}
        self._loop = None
        # 保存连接参数供重连使用
        self._cdp_port = None
        self._cdp_ws_path = None
        # CDP 消息分发器（setup_monitoring 后启动）
        self._disp: CDPDispatcher = None
        # 当前正在等待的 sentinel（发 Ctrl+C 后需补发）
        self._pending_sentinel: str = None
        self._pending_sentinel_ctx_id: int = None
        # 空闲检测：记录最后一次请求时间（epoch 浮点）
        self._last_active: float = 0.0
        # 解释器模式（per-tab）：{tab_idx: "shell"/"python"/"node"/...}
        self._interpreter_mode: dict = {}

    def _nid(self) -> int:
        """生成递增消息 id。

        @return 下一个消息 id
        """
        self._id_counter += 1
        return self._id_counter

    async def _recv_until(self, target_id: int, timeout: float = 10.0) -> dict:
        """从 browser_ws 接收消息，直到遇到目标 id 的响应。

        @param[in] target_id 目标消息 id
        @param[in] timeout   超时秒数
        @return 匹配的消息 dict，超时返回 {}
        """
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                raw = await asyncio.wait_for(self.browser_ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw)
            if msg.get("id") == target_id:
                return msg
        return {}

    async def connect(self, port: int, ws_path: str) -> bool:
        """连接到指定 Chrome 实例的浏览器级 WS。

        @param[in] port    Chrome debug 端口
        @param[in] ws_path WS 路径（/devtools/browser/<uuid>）
        @return True 连接成功
        """
        url = f"ws://127.0.0.1:{port}{ws_path}"
        print(f"[daemon] 连接 Chrome CDP: {url}", file=sys.stderr)
        self.browser_ws = await ws_connect(
            url, open_timeout=15,
        )
        self._cdp_port = port
        self._cdp_ws_path = ws_path
        print("[daemon] Chrome 连接成功（如弹框请在 Chrome 中点击\u300c\u5141\u8bb8\u300d）", file=sys.stderr)
        return True

    async def reconnect(self) -> bool:
        """断线后重新连接 Chrome 并恢复 Luna session。

        @return True 重连成功
        """
        print("[daemon] 检测到 WS 断线，尝试重连...", file=sys.stderr)
        if self._disp:
            await self._disp.stop()
            self._disp = None
        try:
            if self.browser_ws:
                await self.browser_ws.close()
        except Exception:
            pass
        self.browser_ws = None
        self.session_id = None
        self.iframe_contexts = []
        self.hostname_cache.clear()

        # 重新扫描所有 Chrome 实例
        ports = _find_active_ports()
        for port, ws_path in ports:
            try:
                await self.connect(port, ws_path)
                if await self.find_luna_page():
                    await self.setup_monitoring()
                    print("[daemon] 重连成功", file=sys.stderr)
                    return True
                self.browser_ws = None
            except Exception as e:
                print(f"[daemon] 重连 port={port} 失败: {e}", file=sys.stderr)
        return False

    async def _safe_ws_op(self, coro):
        """执行 WS 操作，遇到连接错误时自动重连后重试一次。

        @param[in] coro 异步协程对象
        @return 协程返回值
        """
        try:
            return await coro
        except Exception as e:
            err_str = str(e).lower()
            if any(k in err_str for k in ("1011", "1006", "close", "connect", "ws")):
                ok = await self.reconnect()
                if ok:
                    return await coro
            raise

    async def find_luna_page(self) -> bool:
        """在已连接的 Chrome 中找到 JumpServer Luna 页面并附加 CDP session。

        @return True 找到并附加成功
        """
        i = self._nid()
        await self.browser_ws.send(json.dumps(
            {"id": i, "method": "Target.getTargets", "params": {}}))
        msg = await self._recv_until(i)
        targets = msg.get("result", {}).get("targetInfos", [])
        luna_pages = [t for t in targets
                      if t.get("type") == "page" and LUNA_URL_KEYWORD in t.get("url", "")]

        if not luna_pages:
            print("[daemon] 未找到 JumpServer Luna 页面，请确认浏览器已打开 JumpServer。",
                  file=sys.stderr)
            return False

        # 选可见的（visibilityState=visible），优先；否则取第一个
        target = luna_pages[0]
        self.luna_target_id = target["targetId"]
        print(f"[daemon] 找到 Luna 页面: {target['url'][:80]}", file=sys.stderr)

        # 附加 session
        i = self._nid()
        await self.browser_ws.send(json.dumps({
            "id": i, "method": "Target.attachToTarget",
            "params": {"targetId": self.luna_target_id, "flatten": True}
        }))
        # attachToTarget 先发 attachedToTarget 事件，再发 id 响应
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            try:
                raw = await asyncio.wait_for(self.browser_ws.recv(), timeout=3)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw)
            if msg.get("method") == "Target.attachedToTarget":
                self.session_id = msg["params"]["sessionId"]
                break
        if not self.session_id:
            print("[daemon] 附加 Luna session 失败。", file=sys.stderr)
            return False
        print(f"[daemon] Luna session: {self.session_id}", file=sys.stderr)
        return True

    async def setup_monitoring(self):
        """启用 Network 和 Runtime 监控，收集 iframe contextId。

        @return none
        """
        def _s(method, params=None):
            return json.dumps({"id": self._nid(), "method": method,
                               "params": params or {}, "sessionId": self.session_id})

        await self.browser_ws.send(_s("Runtime.enable"))
        await self.browser_ws.send(_s("Network.enable"))
        await asyncio.sleep(0.5)

        # 收集 iframe contexts：过滤出空名（非扩展）且非主页面的 context
        contexts_raw = []
        deadline = asyncio.get_event_loop().time() + 3
        while asyncio.get_event_loop().time() < deadline:
            try:
                raw = await asyncio.wait_for(self.browser_ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw)
            if msg.get("method") == "Runtime.executionContextCreated":
                ctx = msg["params"]["context"]
                if (ctx.get("name", "") == "" and
                        ctx.get("auxData", {}).get("frameId") != self.luna_target_id):
                    contexts_raw.append(ctx)

        # 按 DOM 顺序排列：在 Luna 主 context 中读取 iframe 顺序，匹配 contextId
        ordered = await self._order_contexts(contexts_raw)
        self.iframe_contexts = ordered
        print(f"[daemon] 检测到 {len(self.iframe_contexts)} 个 KoKo 终端标签", file=sys.stderr)

        # 启动消息分发器：之后所有 recv 统一由 dispatcher 负责
        if self._disp:
            await self._disp.stop()
        self._disp = CDPDispatcher(self.browser_ws)
        await self._disp.start()

    async def _order_contexts(self, contexts_raw: list) -> list:
        """将 contextId 列表按 DOM iframe 顺序排列。

        @param[in] contexts_raw 原始 context 列表
        @return 按 DOM 顺序排列的 [(context_id, iframe_src), ...] 列表
        """
        if not contexts_raw:
            return []

        # 在每个 context 中查询 location.href 来识别 koko iframe
        koko_contexts = []
        for ctx in contexts_raw:
            ctx_id = ctx["id"]
            result = await self._eval("location.href", ctx_id, timeout=3.0)
            if result and KOKO_URL_KEYWORD in result:
                koko_contexts.append((ctx_id, result))

        # 从 Luna 主页读取 iframe DOM 顺序，用 src 匹配
        js = """
        JSON.stringify(
          Array.from(document.querySelectorAll('iframe'))
               .filter(f => f.src.includes('/koko/connect/'))
               .map((f, idx) => ({idx: idx, src: f.src}))
        )
        """
        result = await self._eval(js, 0, timeout=5.0)
        if not result:
            return [(c[0], c[1]) for c in koko_contexts]

        dom_order = json.loads(result)  # [{idx, src}, ...]

        # 按 DOM 顺序匹配 context
        ordered = []
        for item in dom_order:
            src = item["src"]
            # token 截断匹配（src 可能被截短）
            for ctx_id, ctx_href in koko_contexts:
                if src[:80] in ctx_href or ctx_href[:80] in src:
                    ordered.append((ctx_id, ctx_href))
                    break
        if not ordered:
            ordered = [(c[0], c[1]) for c in koko_contexts]
        return ordered

    async def refresh_contexts(self):
        """重新检测 iframe contexts（标签开关后调用）。

        使用 CDPDispatcher 的事件监听机制，避免与 _recv_loop 冲突。

        @return none
        """
        if self._disp is None:
            # 未启动 dispatcher（setup_monitoring 前），退化到直接 recv
            await self.browser_ws.send(json.dumps({
                "id": self._nid(), "method": "Runtime.enable",
                "params": {}, "sessionId": self.session_id
            }))
            contexts_raw = []
            deadline = asyncio.get_event_loop().time() + 3
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(self.browser_ws.recv(), timeout=0.5)
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                if msg.get("method") == "Runtime.executionContextCreated":
                    ctx = msg["params"]["context"]
                    if (ctx.get("name", "") == "" and
                            ctx.get("auxData", {}).get("frameId") != self.luna_target_id):
                        contexts_raw.append(ctx)
            self.iframe_contexts = await self._order_contexts(contexts_raw)
            self.hostname_cache.clear()
            return

        # 使用 dispatcher 事件监听（CDPDispatcher 已启动时）
        ctx_queue: asyncio.Queue = asyncio.Queue()
        self._disp.add_event_handler("Runtime.executionContextCreated", ctx_queue)
        try:
            # disable 再 enable：强制 Chrome 重新推送所有 context 事件
            disable_msg = self._msg("Runtime.disable", {})
            await self._disp.request(disable_msg, timeout=3.0)
            await asyncio.sleep(0.1)
            enable_msg = self._msg("Runtime.enable", {})
            await self._disp.request(enable_msg, timeout=5.0)

            # 收集事件（最多 4s，连续 1s 没有新事件就退出）
            contexts_raw = []
            deadline = asyncio.get_event_loop().time() + 4
            last_event_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() < deadline:
                remaining = deadline - asyncio.get_event_loop().time()
                idle = asyncio.get_event_loop().time() - last_event_time
                if idle > 1.0 and contexts_raw:
                    break  # 连续 1s 没有新事件，认为已收集完
                try:
                    msg = await asyncio.wait_for(ctx_queue.get(), timeout=min(0.5, remaining))
                    ctx = msg["params"]["context"]
                    last_event_time = asyncio.get_event_loop().time()
                    if (ctx.get("name", "") == "" and
                            ctx.get("auxData", {}).get("frameId") != self.luna_target_id):
                        contexts_raw.append(ctx)
                except asyncio.TimeoutError:
                    continue
        finally:
            self._disp.remove_event_handler("Runtime.executionContextCreated")
            # 重新启用 Network.enable（disable 会影响网络监听）
            try:
                net_msg = self._msg("Network.enable", {})
                await self._disp.request(net_msg, timeout=3.0)
            except Exception:
                pass

        self.iframe_contexts = await self._order_contexts(contexts_raw)
        self.hostname_cache.clear()

    async def detect_active_tab(self) -> int:
        """检测当前活跃（可见）的 terminal 标签索引（0-based）。

        @return 活跃标签的 0-based 索引，未找到返回 0
        """
        js = """JSON.stringify(
          Array.from(document.querySelectorAll('iframe'))
               .filter(f=>f.src.includes('/koko/connect/'))
               .map((f,idx)=>({idx:idx,active:f.offsetWidth>0})))"""
        result = await self._eval(js, 0, timeout=5.0)
        if not result:
            return 0
        try:
            items = json.loads(result)
            for item in items:
                if item.get("active"):
                    return item["idx"]
        except Exception:
            pass
        return 0

    def _msg(self, method: str, params: dict) -> dict:
        """构建带 session_id 的 CDP 消息 dict。

        @param[in] method CDP 方法名
        @param[in] params 方法参数
        @return 消息 dict（含 id 和 sessionId）
        """
        m = {"id": self._nid(), "method": method,
             "params": params, "sessionId": self.session_id}
        return m

    async def _eval(self, expression: str, ctx_id: int,
                    timeout: float = 5.0, fire_and_forget: bool = False):
        """在指定 context 执行 JS，优先使用 dispatcher。

        若 dispatcher 未启动（setup_monitoring 之前），直接使用 browser_ws。

        @param[in] expression      JS 表达式
        @param[in] ctx_id          执行上下文 id（0=主页面）
        @param[in] timeout         超时秒数
        @param[in] fire_and_forget True=不等响应
        @return JS 结果值或 None
        """
        params = {"expression": expression, "returnByValue": True}
        if ctx_id:
            params["contextId"] = ctx_id
        msg = self._msg("Runtime.evaluate", params)

        if fire_and_forget:
            if self._disp:
                await self._disp.send_only(msg)
            else:
                await self.browser_ws.send(json.dumps(msg))
            return None

        if self._disp:
            result = await self._disp.request(msg, timeout=timeout)
            r = result.get("result", {})
        else:
            # dispatcher 未启动（setup_monitoring 之前），直接使用 browser_ws
            await self.browser_ws.send(json.dumps(msg))
            r = {}
            deadline = asyncio.get_event_loop().time() + timeout
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        self.browser_ws.recv(), timeout=min(2.0, timeout))
                except asyncio.TimeoutError:
                    break
                data = json.loads(raw)
                if data.get("id") == msg["id"]:
                    r = data.get("result", {}).get("result", {})
                    break

        if r.get("type") in ("string", "boolean", "number"):
            return r.get("value")
        return None

    async def _focus_xterm(self, ctx_id: int,
                            fire_and_forget: bool = False):
        """聚焦指定 iframe 的 xterm-helper-textarea。

        @param[in] ctx_id          iframe execution context id
        @param[in] fire_and_forget True=不等待聚焦响应（并发场景用）
        @return none
        """
        await self._eval(
            "document.querySelector('.xterm-helper-textarea')?.focus()",
            ctx_id, timeout=3.0, fire_and_forget=fire_and_forget)
        await asyncio.sleep(0.15)

    async def _send_line(self, text: str):
        """向终端发送一行文本并按 Enter（fire-and-forget，支持并发）。

        使用 Input.insertText 一次性插入文本（正确支持 $ | " ' 等特殊字符），
        再用 Input.dispatchKeyEvent 发送 Enter。

        @param[in] text 要发送的文本（不含换行符）
        @return none
        """
        send = self._disp.send_only if self._disp else (
            lambda m: self.browser_ws.send(json.dumps(m)))

        if text:
            await send(self._msg("Input.insertText", {"text": text}))
            await asyncio.sleep(0.05)

        # keyDown + keyUp for Enter
        for ktype in ("keyDown", "keyUp"):
            await send(self._msg("Input.dispatchKeyEvent", {
                "type": ktype,
                "key": "Enter",
                "text": "\r" if ktype == "keyDown" else "",
                "windowsVirtualKeyCode": 13,
                "nativeVirtualKeyCode": 13,
            }))
            await asyncio.sleep(0.04)

    async def _send_ctrl_c(self, ctx_id: int):
        """向终端发送 Ctrl+C 中断信号（SIGINT）。

        @param[in] ctx_id iframe execution context id
        @return none
        """
        await self._send_special_key(ctx_id, "ctrl-c")

    async def _send_special_key(self, ctx_id: int, key_name: str):
        """向终端发送特殊控制键（fire-and-forget，不调 recv，支持并发）。

        只使用 CDP Input.dispatchKeyEvent，避免多种方式叠加导致 shell 混乱。
        若中断了正在等待 sentinel 的 exec，补发 sentinel 让其正常返回。

        @param[in] ctx_id   iframe execution context id
        @param[in] key_name 键名，支持: ctrl-c / ctrl-d / ctrl-z / ctrl-l / ctrl-u / ctrl-w
        @return none
        """
        spec = _SPECIAL_KEYS.get(key_name.lower())
        if not spec:
            print(f"[daemon] 未知特殊键: {key_name}", file=sys.stderr)
            return

        dom_key, ctrl_char, vk_code = spec
        send = self._disp.send_only if self._disp else (
            lambda m: self.browser_ws.send(json.dumps(m)))

        # 聚焦（fire-and-forget）
        await self._focus_xterm(ctx_id, fire_and_forget=True)

        # CDP keyboard event（modifiers=2 → ctrlKey=true → xterm.js 发送控制字符）
        for ktype in ("keyDown", "keyUp"):
            await send(self._msg("Input.dispatchKeyEvent", {
                "type": ktype,
                "key": dom_key,
                "code": f"Key{dom_key.upper()}",
                "text": ctrl_char if ktype == "keyDown" else "",
                "windowsVirtualKeyCode": vk_code,
                "nativeVirtualKeyCode": vk_code,
                "modifiers": 2,
            }))
            await asyncio.sleep(0.05)

        await asyncio.sleep(0.4)

        # 若 ctrl-c 中断了正在运行的 exec（bash 不会再执行后续 printf sentinel），
        # 补发 sentinel 让 exec 的 _collect_until_sentinel 正常返回
        if (key_name == "ctrl-c"
                and self._pending_sentinel
                and self._pending_sentinel_ctx_id == ctx_id):
            sentinel = self._pending_sentinel
            await self._send_line(f"printf '\\n{sentinel}\\n'")

    async def _collect_until_sentinel(
            self, sentinel: str, timeout: float) -> tuple:
        """收集终端输出直到哨兵出现在独立行或超时。

        委托给 CDPDispatcher.collect_events()，与其他并发请求不冲突。

        @param[in] sentinel 哨兵字符串
        @param[in] timeout  最大等待秒数
        @return (output_buf: str, timed_out: bool)
        """
        if self._disp:
            return await self._disp.collect_events(sentinel, timeout)
        # dispatcher 未就绪时的退化版本（仅在 setup 阶段使用）
        end_marker = "\n" + sentinel
        output_buf = ""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if end_marker in output_buf:
                return output_buf, False
            try:
                raw = await asyncio.wait_for(
                    self.browser_ws.recv(), timeout=0.4)
                msg = json.loads(raw)
                if msg.get("method") == "Network.webSocketFrameReceived":
                    resp = msg["params"]["response"]
                    opcode = resp.get("opcode", -1)
                    if opcode == 2:
                        decoded = base64.b64decode(
                            resp["payloadData"]).decode("utf-8", errors="replace")
                        output_buf += decoded
                    elif opcode == 1:
                        try:
                            d = json.loads(resp["payloadData"])
                            output_buf += d.get("data", "")
                        except Exception:
                            pass
            except asyncio.TimeoutError:
                pass
        return output_buf, True

    async def detect_hostname(self, tab_idx: int) -> str:
        """在指定标签中静默检测 hostname。

        @param[in] tab_idx 0-based 标签索引
        @return hostname 字符串，检测失败返回空字符串
        """
        if tab_idx >= len(self.iframe_contexts):
            return ""

        ctx_id = self.iframe_contexts[tab_idx][0]
        sentinel = f"__JSCMD_HN_{uuid.uuid4().hex[:8]}__"

        await self._focus_xterm(ctx_id)

        # 前导空格避免 bash history；用 printf 输出哨兵包裹 hostname，便于提取
        # printf 格式保证实际换行，与 exec_command 的 sentinel 行为一致
        await self._send_line(f" printf '\\n{sentinel}'$(hostname)'{sentinel}\\n'")

        output_buf, _ = await self._collect_until_sentinel(sentinel, timeout=8.0)

        clean = _strip_ansi(output_buf)
        # 终端将 $(hostname) 展开后输出：SENTINEL<hostname>SENTINEL 在同一行
        pattern = re.escape(sentinel) + r"([^\r\n]+?)" + re.escape(sentinel)
        m = re.search(pattern, clean)
        if m:
            hn = m.group(1).strip()
            if hn:
                self.hostname_cache[tab_idx] = hn
                return hn
        # 退化匹配：找换行后的哨兵之前一行
        lines = clean.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        for i, ln in enumerate(lines):
            if sentinel in ln and i > 0:
                candidate = lines[i - 1].strip()
                if candidate:
                    self.hostname_cache[tab_idx] = candidate
                    return candidate
        return ""

    async def get_tab_count(self) -> int:
        """获取当前 KoKo iframe 数量。

        @return iframe 数量
        """
        js = "document.querySelectorAll('iframe[src*=\"/koko/connect/\"]').length"
        result = await self._eval(js, 0, timeout=5.0)
        return int(result or 0)

    async def exec_command(self, tab_idx: int, cmd: str, timeout: float = 30.0) -> str:
        """在指定 iframe 标签中执行命令，返回输出。

        支持多行脚本（\\n 分割逐行发送）、特殊字符（$|"'等）、阻塞命令（超时发 Ctrl+C）。

        @param[in] tab_idx  0-based 标签索引
        @param[in] cmd      要执行的命令字符串（可含 \\n 多行）
        @param[in] timeout  等待输出的超时秒数
        @return 命令输出（已剥离 ANSI 转义），超时时返回已收到的部分输出并标注
        """
        if tab_idx >= len(self.iframe_contexts):
            return (f"[ERROR] 标签 #{tab_idx + 1} 不存在"
                    f"（共 {len(self.iframe_contexts)} 个标签）")

        ctx_id = self.iframe_contexts[tab_idx][0]
        sentinel = f"__JSCMD_{uuid.uuid4().hex[:12]}__"

        # 读取当前标签的解释器模式
        mode = self._interpreter_mode.get(tab_idx, "shell")
        cfg = INTERPRETER_CONFIGS.get(mode, INTERPRETER_CONFIGS["shell"])

        # 清空残留事件（上一次 exec 或 Ctrl+C 后的尾部输出）
        if self._disp:
            await asyncio.sleep(0.15)
            drained = 0
            while not self._disp._net_events.empty():
                try:
                    self._disp._net_events.get_nowait()
                    drained += 1
                except asyncio.QueueEmpty:
                    break
            if drained:
                print(f"[daemon] 清空 {drained} 个残留事件", file=sys.stderr)

        await self._focus_xterm(ctx_id)

        # 统一换行符，按行拆分
        normalized = cmd.replace("\r\n", "\n").replace("\r", "\n")
        lines = normalized.split("\n")

        # 根据解释器模式追加哨兵
        sentinel_cmd = cfg["sentinel_cmd"].format(s=sentinel)
        if cfg["append_mode"] == "suffix":
            # Shell 模式：追加到最后一行末尾（; printf '\nSENTINEL\n'）
            lines[-1] = lines[-1] + f"; {sentinel_cmd}"
        else:
            # 解释器模式：哨兵作为独立的新语句行
            # 对 Python 多行代码块，先追加一个空行关闭 block，再追加哨兵
            if len(lines) > 1 and mode == "python":
                lines.append("")   # 空 Enter 关闭 for/if/def 块
            lines.append(sentinel_cmd)

        # 记录当前 pending sentinel，供并发 send-key ctrl-c 使用
        self._pending_sentinel = sentinel
        self._pending_sentinel_ctx_id = ctx_id

        # 记录原始命令（用于事后检测解释器切换）
        original_cmd = cmd.strip()

        try:
            # 逐行发送（支持多行构造如 for/if/while 的 > 提示符）
            for line in lines:
                await self._send_line(line)
                # 多行脚本行间稍作等待，让 shell / 解释器显示提示符
                if len(lines) > 1:
                    await asyncio.sleep(0.12)

            # 收集输出直到哨兵出现或超时
            output_buf, timed_out = await self._collect_until_sentinel(
                sentinel, timeout)

            # 超时时发送 Ctrl+C 中断阻塞命令，然后补发 sentinel 让收集可以结束
            if timed_out:
                await self._send_ctrl_c(ctx_id)
                await asyncio.sleep(0.4)
                await self._focus_xterm(ctx_id, fire_and_forget=True)
                # 超时补发哨兵：根据模式选择命令
                flush_cmd = sentinel_cmd if mode != "shell" else f"printf '\\n{sentinel}\\n'"
                await self._send_line(flush_cmd)
                extra_buf, _ = await self._collect_until_sentinel(sentinel, 5)
                output_buf += extra_buf
                partial = _extract_output(output_buf, sentinel, lines, mode)
                header = (f"[TIMEOUT] 命令超时（{timeout:.0f}s），已发送 "
                          f"Ctrl+C 中断\n部分输出:\n")
                return header + partial if partial else header.rstrip()
        finally:
            self._pending_sentinel = None
            self._pending_sentinel_ctx_id = None

        output = _extract_output(output_buf, sentinel, lines, mode)

        # 命令执行完毕后更新解释器模式（检测进入/退出子解释器）
        self._update_interpreter_mode(tab_idx, original_cmd, output_buf)

        return output

    def _update_interpreter_mode(self, tab_idx: int,
                                   cmd: str, output_buf: str):
        """根据执行的命令和终端输出更新标签的解释器模式。

        规则（按优先级）：
        1. 命令是退出命令（exit/quit/.exit）→ 回退到 shell
        2. 命令是已知解释器入口（python/node/irb）→ 切换到对应模式
        3. 输出末尾含解释器提示符 → 确认/修正模式

        @param[in] tab_idx   0-based 标签索引
        @param[in] cmd       用户原始命令（无哨兵）
        @param[in] output_buf 原始终端输出
        @return none
        """
        cmd_stripped = cmd.strip().lower()

        # 规则 1：退出命令 → 回到 shell
        if cmd_stripped in INTERPRETER_EXIT_CMDS:
            old = self._interpreter_mode.pop(tab_idx, "shell")
            if old != "shell":
                print(f"[daemon] tab #{tab_idx + 1} 解释器模式: {old} → shell",
                      file=sys.stderr)
            return

        # 规则 2：触发命令 → 切换模式
        # 取命令的第一个单词（忽略路径前缀和参数）
        first_word = cmd_stripped.split()[0].split("/")[-1] if cmd_stripped else ""
        for interp, cfg in INTERPRETER_CONFIGS.items():
            if first_word in cfg["trigger_cmds"]:
                old = self._interpreter_mode.get(tab_idx, "shell")
                self._interpreter_mode[tab_idx] = interp
                if old != interp:
                    print(f"[daemon] tab #{tab_idx + 1} 解释器模式: {old} → {interp}",
                          file=sys.stderr)
                return

        # 规则 3：从输出末尾检测提示符
        clean = _strip_ansi(output_buf).replace("\r\n", "\n").replace("\r", "\n")
        last_lines = [ln for ln in clean.split("\n")[-5:] if ln.strip()]
        last_line = last_lines[-1] if last_lines else ""
        for interp, cfg in INTERPRETER_CONFIGS.items():
            if interp == "shell":
                continue
            if re.match(cfg["prompt_re"], last_line):
                old = self._interpreter_mode.get(tab_idx, "shell")
                self._interpreter_mode[tab_idx] = interp
                if old != interp:
                    print(f"[daemon] tab #{tab_idx + 1} 解释器模式(自动): {old} → {interp}",
                          file=sys.stderr)
                return

    async def search_and_connect(self, server_name: str,
                                   tab_wait: float = 8.0) -> dict:
        """在 JumpServer Luna 侧边栏搜索服务器并打开终端标签。

        流程：
        1. 找到 Luna 页面的主 context（非 KoKo iframe）
        2. 在 #AssetTreeSearchInput 中填入 server_name 并触发 Angular 响应
        3. 轮询 #AssetTree li.level1 a 等待搜索结果
        4. 点击/双击第一个匹配节点
        5. 等待新 KoKo iframe 出现（轮询）
        6. 调用 refresh_contexts() 更新 daemon 标签列表
        7. 返回新标签编号和 iframe URL

        搜索框/节点选择器可在 ~/.jscmd_config.json 中用
        luna_search_selector / luna_asset_item_selector 覆盖。

        @param[in] server_name 要搜索的服务器名称
        @param[in] tab_wait    等待新标签出现的超时秒数（默认 8）
        @return {"ok": True, "tab": N, "hostname": "...", "url": "..."}
        """
        config = load_config()
        search_selector = (config.get("luna_search_selector") or
                           "#AssetTreeSearchInput")
        item_selector   = (config.get("luna_asset_item_selector") or
                           "#AssetTree li.level1 a")

        # Luna 主 context = context_id 为 0 的那个（非 iframe）
        if not self.iframe_contexts:
            return {"ok": False, "error": "未找到 JumpServer 终端标签，请先刷新页面"}

        luna_ctx_id = None
        # 尝试通过 session 执行 JS 直接在 Luna 主框架（ctx_id=0 对应 Luna 页面本身）
        # 先获取 Luna 页面所有 contexts，找到 NOT koko 的那个
        # 实际上我们通过 session_id + no-context eval 就能访问 Luna 主框架
        luna_ctx_id = 0   # sessionId 级别的 eval 默认在主框架

        async def _luna_eval(js: str) -> object:
            """在 Luna 主框架执行 JS，返回 returnByValue 结果。"""
            msg_id = self._nid()
            msg = {
                "id": msg_id,
                "method": "Runtime.evaluate",
                "sessionId": self.session_id,
                "params": {"expression": js, "returnByValue": True},
            }
            return await self._disp.request(msg, timeout=10.0)

        # Step 1：清空旧搜索，填入新搜索词，触发 Angular InputEvent
        js_fill = f"""
(function() {{
  var el = document.querySelector({json.dumps(search_selector)});
  if (!el) return JSON.stringify({{ok: false, error: 'search input not found'}});
  // 清空
  el.value = '';
  el.dispatchEvent(new Event('input', {{bubbles: true}}));
  // 填入搜索词
  el.value = {json.dumps(server_name)};
  el.dispatchEvent(new InputEvent('input', {{data: {json.dumps(server_name)}, bubbles: true}}));
  el.dispatchEvent(new Event('change', {{bubbles: true}}));
  el.focus();
  return JSON.stringify({{ok: true}});
}})()
"""
        resp = await _luna_eval(js_fill)
        # _disp.request 返回 msg["result"] 即外层 result；Runtime.evaluate 结果在 result["result"]
        val = resp.get("result", {}).get("value", "{}")
        try:
            parsed = json.loads(val)
        except Exception:
            parsed = {}
        if not parsed.get("ok"):
            error = parsed.get("error", "填入搜索词失败，请检查 luna_search_selector 配置")
            return {"ok": False, "error": error}

        # Step 2：轮询等待搜索结果出现（最多 5s）
        iframe_count_before = len(self.iframe_contexts)
        matched_node_found = False
        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.4)
            js_find = f"""
(function() {{
  var nodes = Array.from(document.querySelectorAll({json.dumps(item_selector)}));
  return JSON.stringify(nodes.slice(0, 10).map(function(a) {{
    // 取最后一个 span 的文字作为节点名
    var spans = a.querySelectorAll('span');
    var text = spans.length ? spans[spans.length-1].textContent.trim() : a.textContent.trim();
    return {{text: text, id: a.parentElement.id || ''}};
  }}));
}})()
"""
            r = await _luna_eval(js_find)
            node_val = r.get("result", {}).get("value", "[]")
            try:
                nodes = json.loads(node_val)
            except Exception:
                nodes = []

            if nodes:
                matched_node_found = True
                print(f"[daemon] 搜索到 {len(nodes)} 个节点: "
                      f"{[n['text'] for n in nodes[:3]]}",
                      file=sys.stderr)
                break

        if not matched_node_found:
            return {"ok": False,
                    "error": f"未找到匹配 '{server_name}' 的服务器节点，"
                             "请检查服务器名称或 luna_asset_item_selector 配置"}

        # Step 3：双击第一个节点（dblclick 触发 Luna 打开终端）
        js_click = f"""
(function() {{
  var nodes = Array.from(document.querySelectorAll({json.dumps(item_selector)}));
  if (!nodes.length) return JSON.stringify({{ok: false, error: 'no nodes'}});
  var el = nodes[0];
  // 单击选中
  el.click();
  // 双击打开终端
  var evt = new MouseEvent('dblclick', {{bubbles: true, cancelable: true, view: window}});
  el.dispatchEvent(evt);
  return JSON.stringify({{ok: true, text: el.textContent.trim().substring(0, 50)}});
}})()
"""
        r = await _luna_eval(js_click)
        click_val = r.get("result", {}).get("value", "{}")
        try:
            click_result = json.loads(click_val)
        except Exception:
            click_result = {}
        if not click_result.get("ok"):
            return {"ok": False, "error": "双击节点失败: " + str(click_result.get("error"))}

        clicked_name = click_result.get("text", server_name)
        print(f"[daemon] 已双击节点: {clicked_name}", file=sys.stderr)

        # Step 4：等待新 KoKo iframe 出现（轮询 iframe list，最多 tab_wait 秒）
        deadline2 = asyncio.get_event_loop().time() + tab_wait
        new_iframe_url = None
        while asyncio.get_event_loop().time() < deadline2:
            await asyncio.sleep(0.5)
            js_iframes = """
(function() {
  var iframes = Array.from(document.querySelectorAll('iframe'));
  return JSON.stringify(iframes.map(function(f) {
    return {src: f.src, id: f.id};
  }));
})()
"""
            r = await _luna_eval(js_iframes)
            iframe_val = r.get("result", {}).get("value", "[]")
            try:
                iframes_now = json.loads(iframe_val)
            except Exception:
                iframes_now = []
            koko_iframes = [f for f in iframes_now if KOKO_URL_KEYWORD in f.get("src", "")]
            if len(koko_iframes) > iframe_count_before:
                new_iframe_url = koko_iframes[-1]["src"]
                print(f"[daemon] 新终端 iframe: {new_iframe_url[:60]}", file=sys.stderr)
                break

        if not new_iframe_url:
            return {"ok": False,
                    "error": f"等待新终端标签超时（{tab_wait:.0f}s），"
                             "请确认双击了有效的服务器节点"}

        # Step 5：刷新 daemon 的 iframe_contexts 列表
        # 记录刷新前的 ctx IDs，用于识别新标签
        old_ctx_ids = {ctx[0] for ctx in self.iframe_contexts}

        await asyncio.sleep(1.0)  # 给新 iframe 的 JS context 加载一点时间
        await self.refresh_contexts()

        # 找到新增的 ctx（不在旧 ctx 列表中的）
        new_tab_idx = None
        for i, ctx in enumerate(self.iframe_contexts):
            if ctx[0] not in old_ctx_ids:
                new_tab_idx = i
                break

        if new_tab_idx is None:
            # 没有新 ctx，但 iframe 数量增加了，就取最后一个
            if len(self.iframe_contexts) > len(old_ctx_ids):
                new_tab_idx = len(self.iframe_contexts) - 1
            else:
                # 等待再次刷新（最多 5s）
                await asyncio.sleep(2.0)
                await self.refresh_contexts()
                for i, ctx in enumerate(self.iframe_contexts):
                    if ctx[0] not in old_ctx_ids:
                        new_tab_idx = i
                        break
                if new_tab_idx is None:
                    new_tab_idx = len(self.iframe_contexts) - 1

        # Step 6：获取新标签的 hostname
        await asyncio.sleep(1.5)  # 等待 xterm 渲染和 shell 提示符
        try:
            hostname = await self.detect_hostname(new_tab_idx)
        except (IndexError, Exception):
            hostname = "(检测中)"

        return {
            "ok": True,
            "tab": new_tab_idx + 1,
            "hostname": hostname,
            "url": new_iframe_url,
            "clicked_node": clicked_name,
        }

    async def list_tabs(self) -> list:
        """列出所有终端标签的状态信息。

        @return [{idx, hostname, aliases, active}, ...] 列表
        """
        aliases = load_aliases()
        active_idx = await self.detect_active_tab()
        result = []
        for i, (ctx_id, src) in enumerate(self.iframe_contexts):
            hn = self.hostname_cache.get(i, "")
            alias_list = aliases.get(hn, []) if hn else []
            result.append({
                "idx": i + 1,
                "context_id": ctx_id,
                "hostname": hn or "(未检测)",
                "aliases": alias_list,
                "active": (i == active_idx),
            })
        return result

    async def handle_request(self, req: dict) -> dict:
        """处理来自 CLI 的请求，返回响应。

        @param[in] req 请求 dict，含 cmd 字段
        @return 响应 dict
        """
        cmd = req.get("cmd", "")

        if cmd == "ping":
            return {"ok": True, "tabs": len(self.iframe_contexts)}

        if cmd == "list":
            tabs = await self.list_tabs()
            return {"ok": True, "tabs": tabs}

        if cmd == "get_hostname":
            tab_idx = req.get("tab")  # 0-based，None=活跃标签
            if tab_idx is None:
                tab_idx = await self.detect_active_tab()
            hn = await self.detect_hostname(tab_idx)
            return {"ok": True, "hostname": hn, "tab": tab_idx + 1}

        if cmd == "refresh":
            await self.refresh_contexts()
            return {"ok": True, "tabs": len(self.iframe_contexts)}

        if cmd == "exec":
            tab_idx = req.get("tab")   # 0-based，None=自动检测
            command = req.get("command", "")
            timeout = float(req.get("timeout", 30))

            if tab_idx is None:
                tab_idx = await self.detect_active_tab()

            if not command:
                return {"ok": False, "error": "命令为空"}

            output = await self.exec_command(tab_idx, command, timeout)
            return {"ok": True, "output": output, "tab": tab_idx + 1}

        if cmd == "connect":
            server_name = req.get("server_name", "")
            tab_wait = float(req.get("tab_wait", 8.0))
            if not server_name:
                return {"ok": False, "error": "server_name 不能为空"}
            return await self.search_and_connect(server_name, tab_wait)

        if cmd == "set_mode":
            tab_idx = req.get("tab")
            if tab_idx is None:
                tab_idx = await self.detect_active_tab()
            new_mode = req.get("mode", "shell")
            if new_mode not in INTERPRETER_CONFIGS:
                return {"ok": False,
                        "error": f"未知模式: {new_mode}，有效值: "
                                 f"{list(INTERPRETER_CONFIGS.keys())}"}
            old_mode = self._interpreter_mode.get(tab_idx, "shell")
            self._interpreter_mode[tab_idx] = new_mode
            return {"ok": True, "tab": tab_idx + 1,
                    "old_mode": old_mode, "new_mode": new_mode}

        if cmd == "get_mode":
            tab_idx = req.get("tab")
            if tab_idx is None:
                tab_idx = await self.detect_active_tab()
            mode = self._interpreter_mode.get(tab_idx, "shell")
            return {"ok": True, "tab": tab_idx + 1, "mode": mode}

        if cmd == "send_key":
            tab_idx = req.get("tab")   # 0-based，None=活跃标签
            key_name = req.get("key", "ctrl-c")

            if tab_idx is None:
                tab_idx = await self.detect_active_tab()

            if tab_idx >= len(self.iframe_contexts):
                return {"ok": False,
                        "error": f"标签 #{tab_idx + 1} 不存在"}

            ctx_id = self.iframe_contexts[tab_idx][0]
            await self._send_special_key(ctx_id, key_name)
            return {"ok": True, "key": key_name, "tab": tab_idx + 1}

        return {"ok": False, "error": f"未知命令: {cmd}"}

    async def _idle_checker(self):
        """后台空闲检测任务：每 5 分钟检查一次，超过配置阈值则自动退出。

        阈值由 ~/.jscmd_config.json 的 idle_timeout_seconds 控制，0=禁用。

        @return none
        """
        config = load_config()
        idle_timeout = float(config.get("idle_timeout_seconds", 3600))
        if idle_timeout <= 0:
            return  # 禁用

        while True:
            await asyncio.sleep(300)  # 每 5 分钟检查一次
            if self._last_active <= 0:
                continue
            idle = asyncio.get_event_loop().time() - self._last_active
            if idle >= idle_timeout:
                print(
                    f"[daemon] 已空闲 {idle:.0f}s（超过 {idle_timeout:.0f}s），"
                    "自动退出以释放 Chrome 连接。",
                    file=sys.stderr)
                # 清理文件后退出
                for path in (DAEMON_PID_FILE, DAEMON_SOCK):
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                os._exit(0)

    async def run(self):
        """daemon 主循环：监听 Unix socket，处理请求。

        @return none
        """
        # 清理旧 socket
        if os.path.exists(DAEMON_SOCK):
            os.unlink(DAEMON_SOCK)

        server = await asyncio.start_unix_server(
            self._handle_client, path=DAEMON_SOCK)

        # 启动空闲检测后台任务
        asyncio.create_task(self._idle_checker())

        print(f"[daemon] 监听 Unix socket: {DAEMON_SOCK}", file=sys.stderr)
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader,
                              writer: asyncio.StreamWriter):
        """处理单个 socket 客户端连接，遇到 WS 错误时自动重连后重试。

        每次请求都会更新 _last_active 以重置空闲计时器。

        @param[in] reader 异步流读取器
        @param[in] writer 异步流写入器
        @return none
        """
        self._last_active = asyncio.get_event_loop().time()
        try:
            data = await asyncio.wait_for(reader.read(65536), timeout=60)
            req = json.loads(data.decode())
            try:
                resp = await self.handle_request(req)
            except Exception as e:
                # WS 连接断开时尝试重连后重试一次
                err_str = str(e).lower()
                if any(k in err_str for k in
                       ("1011", "1006", "closed", "connect", "websocket")):
                    print(f"[daemon] WS 错误: {e}，尝试重连...", file=sys.stderr)
                    if await self.reconnect():
                        resp = await self.handle_request(req)
                    else:
                        resp = {"ok": False,
                                "error": "Chrome WS 断线且重连失败，请重启 daemon"}
                else:
                    resp = {"ok": False, "error": str(e)}
        except Exception as e:
            resp = {"ok": False, "error": str(e)}
        writer.write(json.dumps(resp).encode())
        await writer.drain()
        writer.close()


# ---------------------------------------------------------------------------
# 守护进程启动/停止
# ---------------------------------------------------------------------------

async def _daemon_main():
    """daemon 异步入口：扫描 Chrome、连接（支持重试）、启动服务。

    Chrome 首次连接会弹出安全授权弹窗，弹窗后会提示用户在 Chrome 中点击「允许」。
    最多重试 cdp_connect_retries 次，每次等待 cdp_connect_timeout 秒。

    @return none
    """
    config = load_config()
    max_retries = int(config.get("cdp_connect_retries", 3))
    connect_timeout = float(config.get("cdp_connect_timeout", 20))

    ports = _find_active_ports()
    if not ports:
        print("[daemon] 未找到 Chrome DevToolsActivePort 文件。\n"
              "  请在 Chrome 中打开 chrome://inspect/#remote-debugging 并启用远程调试。",
              file=sys.stderr)
        sys.exit(1)

    daemon = JscmdDaemon()

    # 扫描找包含 JumpServer Luna 的 Chrome 实例（支持弹窗重试）
    connected = False
    for port, ws_path in ports:
        ws_ok = False
        _url = f"ws://127.0.0.1:{port}{ws_path}"
        for attempt in range(max_retries):
            try:
                daemon.browser_ws = await asyncio.wait_for(
                    ws_connect(_url), timeout=connect_timeout)
                daemon._cdp_port = port
                daemon._cdp_ws_path = ws_path
                print("[daemon] Chrome 连接成功"
                      "（如弹框请在 Chrome 中点击\u300c\u5141\u8bb8\u300d）",
                      file=sys.stderr)
                ws_ok = True
                break
            except Exception as e:
                if attempt == 0:
                    print(
                        "[daemon] 正在等待 Chrome 授权...\n"
                        "  ⚠ 请检查 Chrome 是否弹出安全弹窗，点击「允许」后继续。\n"
                        "  （如未弹窗，请在 Chrome 中打开 "
                        "chrome://inspect/#remote-debugging）",
                        file=sys.stderr)
                elif attempt < max_retries - 1:
                    print(f"[daemon] 重试 {attempt + 1}/{max_retries - 1}..."
                          " 请点击 Chrome 弹窗中的「允许」", file=sys.stderr)
                else:
                    print(
                        f"[daemon] 连接 port={port} 失败（已重试 {max_retries} 次）: {e}\n"
                        "  请确认已在 Chrome 中点击「允许」，"
                        "然后重新运行 'jscmd daemon start'",
                        file=sys.stderr)
                await asyncio.sleep(2)

        if not ws_ok:
            continue

        # 连接成功后检测 Luna 页面
        if await daemon.find_luna_page():
            connected = True
            break
        else:
            print(f"[daemon] port={port} 中未找到 JumpServer Luna 页面",
                  file=sys.stderr)
            daemon.browser_ws = None

    if not connected:
        print("[daemon] 未找到包含 JumpServer 页面的 Chrome 实例。\n"
              "  请确认：\n"
              "  1. Chrome 已在 chrome://inspect/#remote-debugging 开启远程调试\n"
              "  2. JumpServer Luna 页面已在 Chrome 中打开\n"
              "  3. Chrome 安全弹窗已点击「允许」", file=sys.stderr)
        sys.exit(1)

    await daemon.setup_monitoring()

    # 写 PID 文件
    with open(DAEMON_PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    print("[daemon] 启动完成，等待命令...", file=sys.stderr)
    await daemon.run()


def cmd_daemon_start(args):
    """启动 daemon 子进程（后台运行）。

    等待 socket 文件出现（最多 60s），期间每 2s 打印一个点表示进度。
    若超时，提示用户检查 Chrome 是否有待点击的授权弹窗。

    @param[in] args argparse 参数
    @return none
    """
    if _daemon_running():
        print("[jscmd] daemon 已在运行。", file=sys.stderr)
        return

    print("[jscmd] 正在启动 daemon...", file=sys.stderr)
    print("[jscmd] 注意：Chrome 可能弹出安全授权弹窗，请点击「允许」", file=sys.stderr)

    # fork 后台进程
    pid = os.fork()
    if pid > 0:
        # 父进程等待 socket 文件出现（最多 60s，每 2s 一个点）
        print("[jscmd] 等待连接", end="", file=sys.stderr, flush=True)
        for i in range(120):
            time.sleep(0.5)
            if os.path.exists(DAEMON_SOCK):
                print(f"\n[jscmd] daemon 已启动 (pid={pid})")
                return
            if i % 4 == 3:
                print(".", end="", file=sys.stderr, flush=True)
        print(
            "\n[jscmd] daemon 启动超时（60s），请检查：\n"
            "  1. Chrome 是否弹出安全弹窗（点击「允许」）\n"
            "  2. JumpServer Luna 页面是否已在 Chrome 中打开\n"
            "  3. chrome://inspect/#remote-debugging 是否已开启",
            file=sys.stderr)
        return

    # 子进程：运行 daemon
    os.setsid()
    asyncio.run(_daemon_main())


def cmd_daemon_stop(args):
    """停止 daemon 进程。

    @param[in] args argparse 参数
    @return none
    """
    if os.path.exists(DAEMON_PID_FILE):
        pid = int(open(DAEMON_PID_FILE).read().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"[jscmd] daemon (pid={pid}) 已停止。")
        except ProcessLookupError:
            print("[jscmd] daemon 进程已不存在。", file=sys.stderr)
        os.unlink(DAEMON_PID_FILE)
    if os.path.exists(DAEMON_SOCK):
        os.unlink(DAEMON_SOCK)


def _daemon_running() -> bool:
    """检查 daemon 是否在运行。

    @return True 表示 daemon 正在运行
    """
    if not os.path.exists(DAEMON_PID_FILE):
        return False
    try:
        pid = int(open(DAEMON_PID_FILE).read().strip())
        os.kill(pid, 0)  # 不发信号，只检查进程是否存在
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def cmd_daemon_status(args):
    """显示 daemon 状态。

    @param[in] args argparse 参数
    @return none
    """
    if _daemon_running():
        pid = open(DAEMON_PID_FILE).read().strip()
        print(f"[jscmd] daemon 运行中 (pid={pid})")
        resp = _send_to_daemon({"cmd": "ping"})
        if resp.get("ok"):
            print(f"  已连接终端标签数: {resp.get('tabs', '?')}")
    else:
        print("[jscmd] daemon 未运行。使用 'jscmd daemon start' 启动。")


# ---------------------------------------------------------------------------
# CLI 与 daemon 通信
# ---------------------------------------------------------------------------

def _auto_start_daemon() -> bool:
    """尝试自动启动 daemon，等待 socket 就绪（最多 60s）。

    @return True 表示 daemon 已就绪
    """
    if _daemon_running() and os.path.exists(DAEMON_SOCK):
        return True

    print("[jscmd] daemon 未运行，正在自动启动...", file=sys.stderr)
    print("[jscmd] ⚠ Chrome 可能弹出安全授权弹窗，请点击「允许」", file=sys.stderr)

    import subprocess
    subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "daemon", "start"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # 等待 socket 就绪（最多 60s）
    print("[jscmd] 等待 daemon 启动", end="", file=sys.stderr, flush=True)
    for i in range(120):
        time.sleep(0.5)
        if os.path.exists(DAEMON_SOCK):
            print("\n[jscmd] daemon 已就绪", file=sys.stderr)
            return True
        if i % 4 == 3:
            print(".", end="", file=sys.stderr, flush=True)
    print(
        "\n[jscmd] daemon 自动启动失败。\n"
        "  请手动执行 'jscmd daemon start' 并确认 Chrome 弹窗已点击「允许」",
        file=sys.stderr)
    return False


def _send_to_daemon(req: dict, timeout: float = 35.0,
                    auto_start: bool = True) -> dict:
    """通过 Unix socket 向 daemon 发送请求并接收响应。

    若 daemon 未运行且 auto_start=True，会尝试自动启动 daemon（会提示 Chrome 弹窗）。

    @param[in] req        请求 dict
    @param[in] timeout    超时秒数
    @param[in] auto_start 未运行时是否自动启动（默认 True）
    @return 响应 dict，失败返回 {"ok": False, "error": ...}
    """
    if not os.path.exists(DAEMON_SOCK):
        if auto_start:
            if not _auto_start_daemon():
                return {"ok": False,
                        "error": "daemon 启动失败，请手动执行: jscmd daemon start"}
        else:
            return {"ok": False, "error": "daemon 未运行，请先执行: jscmd daemon start"}
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(DAEMON_SOCK)
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
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# 标签参数解析与 @name 解析
# ---------------------------------------------------------------------------

def _parse_tab_and_cmd(raw_cmd: str, tab_arg) -> tuple:
    """解析命令字符串中的 #N / N# / @name 前缀，返回 (tab_spec, clean_cmd)。

    @param[in] raw_cmd  原始命令字符串（可能含前缀）
    @param[in] tab_arg  --tab 参数（优先级最高），None 表示未指定
    @return (tab_spec, clean_cmd)
             tab_spec: None=活跃标签, int=1-based 位置, str=@name
    """
    if tab_arg is not None:
        return int(tab_arg), raw_cmd

    # #N 或 N# 前缀
    m = re.match(r"^#(\d+)\s+(.*)", raw_cmd, re.DOTALL)
    if m:
        return int(m.group(1)), m.group(2).strip()
    m = re.match(r"^(\d+)#\s+(.*)", raw_cmd, re.DOTALL)
    if m:
        return int(m.group(1)), m.group(2).strip()

    # @name 前缀
    m = re.match(r"^@(\S+)\s+(.*)", raw_cmd, re.DOTALL)
    if m:
        return m.group(1), m.group(2).strip()

    return None, raw_cmd


def _resolve_name_to_tab(name: str) -> int:
    """将 @name 解析为实际 tab 索引（0-based），通过实时 hostname 检测。

    @param[in] name 别名或 hostname 字符串
    @return 0-based tab 索引，-1 表示未找到
    """
    aliases = load_aliases()

    # 先找 expected hostname
    expected_hostname = None
    # 精确 hostname 匹配
    if name in aliases:
        expected_hostname = name
    else:
        # 别名匹配
        for hn, alias_list in aliases.items():
            if name in alias_list:
                expected_hostname = hn
                break
        # 子串匹配（hostname 包含 name）
        if not expected_hostname:
            candidates = [hn for hn in aliases if name in hn]
            if len(candidates) == 1:
                expected_hostname = candidates[0]
            elif len(candidates) > 1:
                print(f"[jscmd] @{name} 匹配到多个 hostname: {candidates}", file=sys.stderr)
                return -1

    if not expected_hostname:
        # 如果没有记录，直接用 name 作为 hostname 尝试匹配
        expected_hostname = name

    # 向 daemon 询问 tab 数量
    ping = _send_to_daemon({"cmd": "ping"})
    if not ping.get("ok"):
        print(f"[jscmd] {ping.get('error')}", file=sys.stderr)
        return -1

    tab_count = ping.get("tabs", 0)

    # 对每个 tab 执行 hostname 检测
    for tab_0based in range(tab_count):
        resp = _send_to_daemon({"cmd": "get_hostname", "tab": tab_0based}, timeout=15)
        if resp.get("ok"):
            hn = resp.get("hostname", "")
            if hn == expected_hostname:
                return tab_0based
            # 子串模糊匹配（name 是 hostname 的一部分）
            if name in hn:
                return tab_0based

    print(f"[jscmd] 未找到 hostname 为 '{expected_hostname}' 的标签。", file=sys.stderr)
    print("  使用 'jscmd sessions' 查看当前标签状态。", file=sys.stderr)
    return -1


# ---------------------------------------------------------------------------
# CLI 命令处理
# ---------------------------------------------------------------------------

def cmd_exec(args):
    """执行命令：解析目标标签，安全检查，发送给 daemon。

    @param[in] args argparse 参数
    @return none
    """
    raw_cmd = args.command
    tab_spec, clean_cmd = _parse_tab_and_cmd(raw_cmd, getattr(args, "tab", None))

    if not clean_cmd:
        print("[jscmd] 命令为空。", file=sys.stderr)
        sys.exit(1)

    # 安全检查（CLI 侧）
    config = load_config()
    checker = SafetyChecker(config)
    if not checker.enforce(clean_cmd):
        sys.exit(1)

    # 解析 tab 目标
    tab_0based = None
    if tab_spec is None:
        tab_0based = None  # daemon 自动检测活跃标签
    elif isinstance(tab_spec, int):
        tab_0based = tab_spec - 1
    elif isinstance(tab_spec, str):
        # @name 解析
        tab_0based = _resolve_name_to_tab(tab_spec)
        if tab_0based < 0:
            sys.exit(1)

    req = {
        "cmd": "exec",
        "tab": tab_0based,
        "command": clean_cmd,
        "timeout": getattr(args, "timeout", 30),
    }
    resp = _send_to_daemon(req, timeout=float(req["timeout"]) + 5)
    if resp.get("ok"):
        print(resp.get("output", ""))
    else:
        print(f"[jscmd] 执行失败: {resp.get('error')}", file=sys.stderr)
        sys.exit(1)


def cmd_connect(args):
    """在 JumpServer 侧边栏搜索服务器名称并自动打开终端标签。

    搜索结果按第一个匹配项打开；打开后自动检测 hostname 并输出新标签编号。

    @param[in] args argparse 参数（server_name, --tab-wait）
    @return none
    """
    server_name = args.server_name.strip()
    if not server_name:
        print("[jscmd] server_name 不能为空", file=sys.stderr)
        sys.exit(1)

    tab_wait = getattr(args, "tab_wait", 8.0)
    resp = _send_to_daemon(
        {"cmd": "connect", "server_name": server_name, "tab_wait": tab_wait},
        timeout=float(tab_wait) + 15)

    if resp.get("ok"):
        print(f"[jscmd] 已打开终端标签 #{resp['tab']}")
        print(f"  服务器: {resp.get('clicked_node', server_name)}")
        print(f"  hostname: {resp.get('hostname', '(检测中)')}")
        if resp.get("url"):
            print(f"  URL: {resp['url'][:60]}...")
    else:
        print(f"[jscmd] 连接失败: {resp.get('error')}", file=sys.stderr)
        sys.exit(1)


def cmd_mode(args):
    """查看或设置标签的解释器模式（shell / python / node / ruby）。

    不带 mode 参数时显示当前模式；带 mode 参数时切换。

    @param[in] args argparse 参数
    @return none
    """
    tab_spec, _ = _parse_tab_and_cmd(
        getattr(args, "tab_prefix", "") or "", getattr(args, "tab", None))

    tab_0based = None
    if isinstance(tab_spec, int):
        tab_0based = tab_spec - 1

    new_mode = getattr(args, "mode_name", None)

    if new_mode is None:
        # 查询当前模式
        resp = _send_to_daemon({"cmd": "get_mode", "tab": tab_0based})
        if resp.get("ok"):
            print(f"[jscmd] #{resp['tab']} 当前模式: {resp['mode']}")
        else:
            print(f"[jscmd] {resp.get('error')}", file=sys.stderr)
            sys.exit(1)
    else:
        if new_mode not in INTERPRETER_CONFIGS:
            valid = list(INTERPRETER_CONFIGS.keys())
            print(f"[jscmd] 无效模式: {new_mode}，有效值: {valid}", file=sys.stderr)
            sys.exit(1)
        resp = _send_to_daemon({"cmd": "set_mode", "tab": tab_0based, "mode": new_mode})
        if resp.get("ok"):
            print(f"[jscmd] #{resp['tab']} 模式: {resp['old_mode']} → {resp['new_mode']}")
        else:
            print(f"[jscmd] {resp.get('error')}", file=sys.stderr)
            sys.exit(1)


def cmd_send_key(args):
    """向指定标签发送控制键（Ctrl+C / Ctrl+D 等）。

    @param[in] args argparse 参数
    @return none
    """
    raw_key = args.key.lower()
    if raw_key not in _SPECIAL_KEYS:
        valid = ", ".join(_SPECIAL_KEYS.keys())
        print(f"[jscmd] 不支持的键: {raw_key}，有效值: {valid}", file=sys.stderr)
        sys.exit(1)

    tab_spec, _ = _parse_tab_and_cmd(
        args.tab_prefix or "", getattr(args, "tab", None))

    tab_0based = None
    if isinstance(tab_spec, int):
        tab_0based = tab_spec - 1

    resp = _send_to_daemon(
        {"cmd": "send_key", "key": raw_key, "tab": tab_0based},
        timeout=10)
    if resp.get("ok"):
        print(f"[jscmd] 已向 #{resp['tab']} 发送 {resp['key']}")
    else:
        print(f"[jscmd] 失败: {resp.get('error')}", file=sys.stderr)
        sys.exit(1)


def cmd_list(args):
    """列出所有 JumpServer 终端标签。

    @param[in] args argparse 参数
    @return none
    """
    resp = _send_to_daemon({"cmd": "list"}, timeout=60)
    if not resp.get("ok"):
        print(f"[jscmd] {resp.get('error')}", file=sys.stderr)
        sys.exit(1)
    tabs = resp.get("tabs", [])
    if not tabs:
        print("[jscmd] 当前没有打开的 JumpServer 终端标签。")
        return
    for t in tabs:
        active_mark = " ← 活跃" if t["active"] else ""
        alias_str = f"  aliases={t['aliases']}" if t["aliases"] else ""
        print(f"  #{t['idx']}  host={t['hostname']}{alias_str}{active_mark}")


def cmd_sessions(args):
    """查看所有标签的实时 hostname + 别名状态。

    @param[in] args argparse 参数（含 refresh 标志）
    @return none
    """
    if getattr(args, "refresh", False):
        resp = _send_to_daemon({"cmd": "refresh"})
        if resp.get("ok"):
            print(f"[jscmd] 已刷新，共 {resp['tabs']} 个标签。")
        else:
            print(f"[jscmd] 刷新失败: {resp.get('error')}", file=sys.stderr)
            return

    aliases = load_aliases()

    # 逐个检测 hostname（实时）
    ping = _send_to_daemon({"cmd": "ping"})
    if not ping.get("ok"):
        print(f"[jscmd] {ping.get('error')}", file=sys.stderr)
        sys.exit(1)

    tab_count = ping.get("tabs", 0)
    if tab_count == 0:
        print("[jscmd] 当前没有打开的 JumpServer 终端标签。")
        return

    print(f"[jscmd] 正在检测 {tab_count} 个标签的 hostname...")

    # 获取完整 list（含活跃标记）
    resp = _send_to_daemon({"cmd": "list"}, timeout=60)
    tabs_info = {t["idx"]: t for t in resp.get("tabs", [])}

    for i in range(1, tab_count + 1):
        hn_resp = _send_to_daemon({"cmd": "get_hostname", "tab": i - 1}, timeout=15)
        hn = hn_resp.get("hostname", "(检测失败)") if hn_resp.get("ok") else "(检测失败)"
        alias_list = aliases.get(hn, [])
        active_mark = " ← 活跃" if tabs_info.get(i, {}).get("active") else ""
        alias_str = f"  aliases={alias_list}" if alias_list else "  (无别名)"
        print(f"  #{i}  host={hn}{alias_str}{active_mark}")


def cmd_alias(args):
    """管理 hostname 别名。

    @param[in] args argparse 参数
    @return none
    """
    aliases = load_aliases()

    if getattr(args, "remove", None):
        # 删除别名
        target = args.remove
        removed = False
        for hn, alias_list in aliases.items():
            if target in alias_list:
                alias_list.remove(target)
                removed = True
                print(f"[jscmd] 已从 {hn} 移除别名 '{target}'")
        if not removed:
            print(f"[jscmd] 未找到别名 '{target}'。", file=sys.stderr)
            return
        save_aliases(aliases)
        return

    # 添加别名：jscmd alias <target> <name>
    target = args.target   # @current / #N / hostname
    new_alias = args.name

    # 解析 target 获取 hostname
    hostname = None
    if target == "@current" or target == "current":
        # 检测活跃标签的 hostname
        resp = _send_to_daemon({"cmd": "get_hostname", "tab": None}, timeout=15)
        # tab=None 时 daemon 自动用活跃标签，此处需特殊处理
        # 先获取活跃标签索引
        list_resp = _send_to_daemon({"cmd": "list"}, timeout=30)
        active_tabs = [t for t in list_resp.get("tabs", []) if t["active"]]
        if active_tabs:
            tab_0based = active_tabs[0]["idx"] - 1
            hn_resp = _send_to_daemon({"cmd": "get_hostname", "tab": tab_0based}, timeout=15)
            hostname = hn_resp.get("hostname") if hn_resp.get("ok") else None
    elif target.startswith("#") or re.match(r"^\d+$", target):
        # #N 或数字
        n = int(target.lstrip("#")) - 1
        hn_resp = _send_to_daemon({"cmd": "get_hostname", "tab": n}, timeout=15)
        hostname = hn_resp.get("hostname") if hn_resp.get("ok") else None
    else:
        # 直接视为 hostname
        hostname = target

    if not hostname or hostname == "(未检测)":
        print("[jscmd] 无法获取目标标签的 hostname，请先执行 'jscmd sessions' 检测。",
              file=sys.stderr)
        return

    if hostname not in aliases:
        aliases[hostname] = []
    if new_alias not in aliases[hostname]:
        aliases[hostname].append(new_alias)
        save_aliases(aliases)
        print(f"[jscmd] 已添加: {hostname} → 别名 '{new_alias}'")
    else:
        print(f"[jscmd] 别名 '{new_alias}' 已存在于 {hostname}。")


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """构建 argparse 解析器。

    @return ArgumentParser 实例
    """
    parser = argparse.ArgumentParser(
        prog="jscmd",
        description="JumpServer 终端 CLI — 通过 Chrome CDP 控制 JumpServer Web 终端"
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    # daemon
    p_daemon = sub.add_parser("daemon", help="管理后台 daemon")
    daemon_sub = p_daemon.add_subparsers(dest="daemon_action", required=True)
    daemon_sub.add_parser("start", help="启动 daemon")
    daemon_sub.add_parser("stop", help="停止 daemon")
    daemon_sub.add_parser("status", help="查看 daemon 状态")

    # exec
    p_exec = sub.add_parser("exec", help="执行命令")
    p_exec.add_argument("command", help="命令字符串（支持 #N / N# / @name 前缀）")
    p_exec.add_argument("--tab", "-t", type=int, default=None,
                        help="指定 1-based 标签编号（优先级最高）")
    p_exec.add_argument("--timeout", type=float, default=30,
                        help="等待输出超时秒数（默认 30）")

    # list
    sub.add_parser("list", help="列出所有终端标签")

    # sessions
    p_sess = sub.add_parser("sessions", help="查看标签 hostname/别名状态")
    p_sess.add_argument("--refresh", action="store_true",
                        help="强制重新检测所有标签 hostname")

    # alias
    p_alias = sub.add_parser("alias", help="管理 hostname 别名")
    p_alias.add_argument("target", nargs="?",
                         help="目标：@current / #N / hostname")
    p_alias.add_argument("name", nargs="?", help="要添加的别名")
    p_alias.add_argument("--remove", metavar="ALIAS", help="删除指定别名")

    # connect
    p_connect = sub.add_parser(
        "connect",
        help="在 JumpServer 侧边栏搜索服务器并打开终端",
    )
    p_connect.add_argument(
        "server_name",
        help="服务器名称（支持模糊匹配，取第一个结果）",
    )
    p_connect.add_argument(
        "--tab-wait", type=float, default=8.0,
        help="等待新终端标签出现的超时秒数（默认 8）",
    )

    # mode
    p_mode = sub.add_parser(
        "mode",
        help="查看或设置标签的解释器模式（shell/python/node/ruby）",
    )
    p_mode.add_argument(
        "mode_name", nargs="?", default=None,
        choices=list(INTERPRETER_CONFIGS.keys()),
        help="目标模式，省略则显示当前模式",
    )
    p_mode.add_argument(
        "--tab", "-t", type=int, default=None,
        help="目标标签（1-based），不指定则使用活跃标签",
    )
    p_mode.add_argument(
        "tab_prefix", nargs="?", default="",
        help=argparse.SUPPRESS,
    )

    # send-key
    p_key = sub.add_parser(
        "send-key",
        help="向终端发送控制键（Ctrl+C / Ctrl+D 等）",
        description=(
            "支持的键: " + ", ".join(_SPECIAL_KEYS.keys())
        ),
    )
    p_key.add_argument(
        "key",
        help="要发送的键，如 ctrl-c / ctrl-d / ctrl-z / ctrl-l",
    )
    p_key.add_argument(
        "--tab", "-t", type=int, default=None,
        help="目标标签（1-based），不指定则发给活跃标签",
    )
    p_key.add_argument(
        "tab_prefix", nargs="?", default="",
        help=argparse.SUPPRESS,  # 内部用，接收 #N 前缀语法
    )

    return parser


def main():
    """CLI 主入口。

    @return none
    """
    parser = build_parser()
    args = parser.parse_args()

    if args.subcommand == "daemon":
        if args.daemon_action == "start":
            cmd_daemon_start(args)
        elif args.daemon_action == "stop":
            cmd_daemon_stop(args)
        elif args.daemon_action == "status":
            cmd_daemon_status(args)

    elif args.subcommand == "exec":
        cmd_exec(args)

    elif args.subcommand == "list":
        cmd_list(args)

    elif args.subcommand == "sessions":
        cmd_sessions(args)

    elif args.subcommand == "alias":
        cmd_alias(args)

    elif args.subcommand == "mode":
        cmd_mode(args)

    elif args.subcommand == "send-key":
        cmd_send_key(args)

    elif args.subcommand == "connect":
        cmd_connect(args)


if __name__ == "__main__":
    main()
