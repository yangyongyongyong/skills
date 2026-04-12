# Skill: jumpserver-terminal

通过 **Chrome CDP（Chrome DevTools Protocol）** 与浏览器中打开的 JumpServer Web 终端交互，在 Cursor 中直接对跳板机服务器执行命令、获取输出。

---

## 前提条件

1. **Chrome 远程调试已开启**  
   打开 `chrome://inspect/#remote-debugging`，开启"发现网络目标"（Discover network targets），或点击"开始远程调试"。  
   Chrome 会把 CDP 地址写入：
   ```
   ~/Library/Application Support/Google/Chrome/DevToolsActivePort
   ```
   验证已开启：
   ```bash
   cat "~/Library/Application Support/Google/Chrome/DevToolsActivePort"
   # 正常输出两行：端口号 + WebSocket 路径
   # 例如：
   # 9222
   # /devtools/browser/a1b2c3d4-...
   ```

2. **浏览器已打开 JumpServer**（URL 含 `/luna/`）

3. **依赖安装**（与 jupyterlab-terminal 相同，仅需 websockets）：
   ```bash
   cd ~/.cursor/skills/jumpserver-terminal
   python3 -m venv .venv
   source .venv/bin/activate
   pip install websockets
   ```

---

## 快速上手

### 第一步：启动 daemon

```bash
python3 ~/.cursor/skills/jumpserver-terminal/jscmd.py daemon start
# Chrome 弹出安全确认框 → 点击「允许」（只弹一次）
# 输出: [jscmd] daemon 已启动 (pid=XXXXX)
```

> **daemon 只需启动一次**。它在后台持久维持 Chrome WS 连接，避免 Chrome 反复弹框。  
> **自动启动**：若 daemon 未运行，`exec`/`connect` 等命令会自动启动 daemon（仍需点击 Chrome 弹框）。  
> **自动关闭**：空闲超过 1 小时后 daemon 自动退出，下次使用时重新自动启动。时长可在 `~/.jscmd_config.json` 中配置 `idle_timeout_seconds`。

### 第二步：执行命令

```bash
# 在当前活跃终端执行
python3 jscmd.py exec "ls -la"

# 指定第 2 个标签（两种写法等价）
python3 jscmd.py exec "#2 df -h"
python3 jscmd.py exec "2# df -h"
python3 jscmd.py exec --tab 2 "df -h"

# 按服务器名称执行（@name 语法，自动找到正确标签）
python3 jscmd.py exec "@web-01 ps aux | grep nginx"
```

---

## 完整命令参考

### daemon 管理

| 命令 | 说明 |
|------|------|
| `jscmd daemon start` | 启动后台 daemon（Chrome 弹框一次，1h 空闲自动退出） |
| `jscmd daemon stop` | 停止 daemon |
| `jscmd daemon status` | 查看运行状态及终端数量 |

### 命令执行

```bash
jscmd exec "命令"              # 在活跃终端执行
jscmd exec "#N 命令"           # 指定第 N 个标签（N 从 1 开始）
jscmd exec "N# 命令"           # 同上
jscmd exec --tab N "命令"      # 同上，--tab 优先级最高
jscmd exec "@名称 命令"        # 按 hostname 或别名定位标签
jscmd exec --timeout 60 "命令" # 自定义超时（默认 30 秒）
```

### 标签管理

```bash
jscmd list            # 列出所有终端标签（含活跃标记）
jscmd sessions        # 实时检测每个标签的 hostname + 已知别名
jscmd sessions --refresh  # 强制重新扫描所有标签
```

### 别名管理

```bash
# 给当前活跃标签的服务器加别名
jscmd alias @current web

# 给第 2 个标签的服务器加别名
jscmd alias #2 db

# 直接按 hostname 加别名
jscmd alias web-server-01.example.com web

# 删除别名
jscmd alias --remove web

# 查看所有已定义的别名（存储在 ~/.jscmd_aliases.json）
cat ~/.jscmd_aliases.json
```

### 搜索并连接服务器

在 JumpServer 侧边栏搜索服务器名称并自动打开终端标签：

```bash
# 搜索并打开终端（支持模糊名称，取第一个匹配结果）
jscmd connect "web-01"
jscmd connect "db-master"

# 自定义等待新标签的超时（默认 8 秒）
jscmd connect "slow-server" --tab-wait 15
```

打开后会自动检测 hostname，可用 `jscmd list` 查看新标签。

`connect` 判定「新标签出现」依赖 **Luna 页面上 KoKo iframe 个数增加**，与 shell 登录提示符出现时机无关；iframe 出现后 CDP 再附着终端上下文，若仍超时见下表「connect 超时」。

### 解释器模式

进入 Python/Node.js 等子解释器时，切换模式以正确处理哨兵和输出：

```bash
# 查看当前活跃标签的解释器模式
jscmd mode

# 查看第 2 个标签的模式
jscmd mode --tab 2

# 手动切换到 Python REPL 模式（通常自动检测，无需手动）
jscmd mode python
jscmd mode python --tab 2

# 切回 Shell 模式
jscmd mode shell
```

> **自动检测**：执行 `python` / `python3` 等命令后会自动切换；执行 `exit()` / `quit()` 后自动切回 Shell。

### 控制键

```bash
jscmd send-key ctrl-c          # 发送 Ctrl+C 中断当前命令
jscmd send-key ctrl-d          # 发送 Ctrl+D（EOF / 退出解释器）
jscmd send-key ctrl-z          # 发送 Ctrl+Z（挂起进程）
jscmd send-key ctrl-c --tab 2  # 向第 2 个标签发送 Ctrl+C
```

---

## Hostname 映射机制

- **持久化**（`~/.jscmd_aliases.json`）：只保存 `hostname → [别名...]` 映射，不保存 tab 位置
- **运行时**（daemon 内存）：动态维护 `tab位置 → hostname` 缓存，每次 exec 前会验证
- **安全性**：`@name` 执行前必须实时扫描确认目标 tab 的 hostname 与期望一致，防止标签重排导致在错误服务器执行命令

```bash
# 工作流示例
jscmd sessions                    # 查看 3 个 tab 各连接哪台服务器
# #1  host=web-server-01   (无别名)  ← 活跃
# #2  host=db-master        (无别名)
# #3  host=cache-01         (无别名)

jscmd alias @current web          # 给活跃标签的服务器加别名 "web"
jscmd alias #2 db                 # 给第 2 个标签加别名 "db"

jscmd exec "@web df -h"           # 总是在 web-server-01 执行，不管它在第几个 tab
jscmd exec "@db show databases;"  # 总是在 db-master 执行
```

---

## 安全防护层

所有命令发往终端前自动经过 **SafetyChecker** 三级检查：

### Level 1 — BLOCK（直接拒绝）

以下命令**立即拒绝**，不发往终端：
- 修改系统 Python 解释器（`update-alternatives --set python`、`pyenv global`）
- 写入系统关键文件（`/etc/passwd`、`/etc/shadow`、`/etc/sudoers` 等）
- 卸载系统 Python（`apt remove python3`、`yum remove python`）
- 删除系统目录（`rm -rf /usr`、`rm -rf /etc` 等）

### Level 2 — CONFIRM + SLEEP（需确认 + 倒计时）

删除类命令需用户输入 `yes`，确认后倒计时才执行：
```
[WARN] 检测到删除类操作: rm -rf /tmp/old_logs
请输入 yes 确认执行（其他输入取消）: > yes
[INFO] 将在 10 秒后执行，Ctrl+C 可取消...
  10... 9... 8... 7...
```
触发关键词：`rm`、`rmdir`、`dd if=`、`mkfs`、`shred`、`wipefs` 等

### Level 3 — WARN（打印警告后执行）

- `sudo` 前缀命令
- `systemctl stop/disable/mask`
- `pip uninstall`、`npm uninstall -g`

### 自定义配置

编辑 `~/.jscmd_config.json`（首次运行自动生成）：

```json
{
  "delete_sleep_seconds": 10,
  "extra_block_patterns": ["my-custom-dangerous-cmd"],
  "extra_confirm_patterns": [],
  "disable_safety": false
}
```

---

## Agent 使用指引

当用户要在 JumpServer 终端执行命令时，使用本 skill：

```python
import subprocess, sys

JSCMD = [sys.executable,
         os.path.expanduser("~/.cursor/skills/jumpserver-terminal/jscmd.py")]

def js_exec(cmd, tab=None):
    """在 JumpServer 终端执行命令并返回输出。"""
    args = JSCMD + ["exec"]
    if tab:
        args += ["--tab", str(tab)]
    args.append(cmd)
    r = subprocess.run(args, capture_output=True, text=True)
    return r.stdout.strip()

# 示例
print(js_exec("ls -la"))
print(js_exec("#2 df -h"))
print(js_exec("@web ps aux"))
```

> **注意**：首次使用前需确保 `jscmd daemon start` 已执行，否则 CLI 会提示错误。

---

## 并发执行说明

多个 `exec` 命令同时发往 daemon 时，**daemon 侧串行处理**（后到的命令排队等待），这是预期行为，可防止以下问题：

- 两个命令竞争 Chrome 全局输入焦点，导致内容被打到同一个 terminal
- 两个命令竞争同一个网络事件队列，导致 sentinel 互相"偷"，引发超时

`connect` 命令与 `exec` 也是互斥的：`connect` 在打开新标签后需要刷新 context 列表（`Runtime.disable/enable`），该操作会短暂中断 Network 事件推送。daemon 会在无进行中的 `exec` 时才执行刷新，防止正在收集输出的 exec 丢帧超时。

CLI 层（Shell 工具调用）仍可并发发起，daemon 自动排队、按序执行、各自返回正确输出。

---

## 故障排查

| 问题 | 解决方法 |
|------|----------|
| `daemon 未运行` | 执行 `jscmd daemon start` |
| Chrome 弹框后执行失败 | 确认在 Chrome 中点击了"允许" |
| 未找到 JumpServer 页面 | 确认浏览器已打开 JumpServer（URL 含 `/luna/`） |
| `DevToolsActivePort` 不存在 | 打开 `chrome://inspect/#remote-debugging` 启用远程调试 |
| 命令超时无输出（`[TIMEOUT] 部分输出:`） | 多为并发 exec 竞争旧版本 bug；升级后已修复。若仍出现可增加 `--timeout` 或重启 daemon |
| 命令输出为空 | 适当增加 `--timeout`（默认 30s）|
| 多个命令都打到了同一个 tab | 旧版本并发 bug，升级后已修复；确认使用最新 `jscmd.py` |
| `@name` 找不到标签 | 先执行 `jscmd sessions` 确认 hostname 已检测 |
| `connect` 等待新终端超时 | 先确认侧栏能搜到资产且双击能打开终端。若刚关过标签仍失败，可 `jscmd daemon stop` 后重启 daemon 同步状态；仍不行则你的 Luna 可能在**同一 iframe 内切换会话**而非新增 iframe，需改自动化策略。 |
