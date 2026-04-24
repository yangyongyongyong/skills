# jumpserver-terminal

通过 **Chrome CDP（Chrome DevTools Protocol）** 与浏览器中打开的 **JumpServer Web 终端**交互，在 Cursor Agent 中直接对跳板机服务器执行命令、获取输出。

jscmd daemon 同时作为**通用 Chrome CDP 网关**，提供 `cdp_pages` / `cdp_eval` 供 `jupyterlab-terminal` 等其他 Skill 复用 CDP 连接。

---

## 前置条件

### 1. 开启 Chrome 远程调试（二选一）

**方式 A（推荐）：chrome://inspect 勾选**

正常双击打开 Chrome → 地址栏输入 `chrome://inspect/#remote-debugging` → 勾选 **"Allow remote debugging for this browser instance"**

验证是否成功：
```bash
cat ~/Library/Application\ Support/Google/Chrome/DevToolsActivePort
# 正常输出两行：端口号 + WebSocket 路径
```

**方式 B：命令行启动**

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222
```

验证：
```bash
curl -s http://127.0.0.1:9222/json/version | head -1
# 有 JSON 输出即正常
```

> 两种方式可并存，互不冲突。方式 A 通过 `DevToolsActivePort` 文件通告端口，方式 B 通过 HTTP fallback 探测（默认扫 9222 端口）。

### 2. 安装依赖

```bash
cd ~/.cursor/skills/jumpserver-terminal
python3 -m venv .venv
source .venv/bin/activate
pip install websockets
```

### 3. 浏览器中打开 JumpServer（使用 JumpServer 功能时）

URL 含 `/luna/`，且至少有一个终端标签已打开。

---

## 快速上手

```bash
JSCMD="~/.cursor/skills/jumpserver-terminal/.venv/bin/python3 \
       ~/.cursor/skills/jumpserver-terminal/jscmd.py"

# 1. 启动后台 daemon（Chrome 弹「允许」框 → 点击一次，之后不再弹）
$JSCMD daemon start
# 输出: [jscmd] daemon 已启动 (pid=12345)

# 2. 在当前活跃终端执行命令
$JSCMD exec "hostname"

# 3. 指定第 2 个标签
$JSCMD exec "#2 df -h"

# 4. 列出所有终端标签
$JSCMD list
```

---

## 完整命令参考

### daemon 管理

```bash
jscmd daemon start                  # 启动后台 daemon
jscmd daemon start --idle-timeout 3600   # 1 小时无请求自动退出
jscmd daemon start --idle-timeout 0      # 永久常驻（默认）
jscmd daemon stop                   # 停止 daemon
jscmd daemon status                 # 查看状态（运行时长 / 终端数量）
```

> daemon 未运行时，`exec`/`connect` 等命令会**自动重启 daemon**，但仍需在 Chrome 弹框中点击"允许"。daemon 运行期间不再弹框。

---

### exec — 执行命令

```bash
# 在当前活跃终端执行
jscmd exec "ls -la"
jscmd exec "ps aux | grep nginx"

# 指定标签（三种等价写法）
jscmd exec "#2 df -h"
jscmd exec "2# df -h"
jscmd exec --tab 2 "df -h"

# 按服务器名称 / 别名执行
jscmd exec "@web-01 systemctl status nginx"
jscmd exec "@db show databases;"

# 自定义超时（默认 30 秒）
jscmd exec --timeout 60 "#1 find /var/log -name '*.log' -mtime -1"
jscmd exec --timeout 120 "#2 tar czf backup.tar.gz ~/data/"

# 多行命令（换行分隔，逐行发送）
jscmd exec "#1 cd /var/log\nls -lh"
```

**标签指定优先级**：`--tab N` > `#N / N#` 前缀 > 不指定（活跃 tab）

---

### list / sessions — 查看终端

```bash
# 列出所有终端标签（含活跃标记）
jscmd list
# 输出示例：
#   #1  Terminal 1  <-- active
#   #2  Terminal 2
#   #3  Terminal 3

# 实时检测每个标签连接的 hostname + 已知别名
jscmd sessions
# 输出示例：
#   #1  host=web-server-01   alias=web    ← 活跃
#   #2  host=db-master        alias=db
#   #3  host=cache-01         (无别名)

# 强制重新扫描所有标签（hostname 变化时用）
jscmd sessions --refresh
```

---

### alias — 别名管理

服务器别名存储在 `~/.jscmd_aliases.json`，与 tab 位置解耦，即使标签顺序变化也能正确定位。

```bash
# 给当前活跃标签的服务器加别名
jscmd alias @current web

# 给第 2 个标签的服务器加别名
jscmd alias #2 db

# 直接按 hostname 加别名
jscmd alias web-server-01.example.com web

# 删除别名
jscmd alias --remove web

# 查看所有已定义的别名
cat ~/.jscmd_aliases.json
```

**用别名执行命令**（alias 执行前自动验证 hostname，防止 tab 重排误操作）：

```bash
jscmd exec "@web df -h"
jscmd exec "@db SHOW DATABASES;"
jscmd exec "@cache redis-cli INFO"
```

---

### connect — 搜索并打开服务器终端

在 JumpServer 侧边栏搜索资产并自动打开终端标签：

```bash
# 搜索服务器名称（模糊匹配，取第一个结果）
jscmd connect "web-01"
jscmd connect "db-master"
jscmd connect "prod-api"

# 自定义等待新标签出现的超时（默认 8 秒）
jscmd connect "slow-boot-server" --tab-wait 20
```

打开后可用 `jscmd list` 确认新标签编号，再用 `jscmd alias` 给它起名。

---

### mode — 解释器模式切换

进入 Python / Node.js / Ruby 等子解释器时使用，正确处理哨兵和输出格式：

```bash
# 查看当前活跃标签的模式
jscmd mode

# 查看指定标签的模式
jscmd mode --tab 2

# 切换到 Python REPL 模式（通常自动检测，无需手动）
jscmd mode python
jscmd mode python --tab 2

# 切回 Shell 模式
jscmd mode shell
```

> **自动检测**：执行 `python` / `python3` 等命令后自动切换；执行 `exit()` / `quit()` 后自动切回 Shell 模式。

---

### send-key — 发送控制键

```bash
jscmd send-key ctrl-c           # 中断当前命令（Ctrl+C）
jscmd send-key ctrl-d           # EOF / 退出解释器（Ctrl+D）
jscmd send-key ctrl-z           # 挂起进程（Ctrl+Z）
jscmd send-key ctrl-c --tab 2   # 向第 2 个标签发送 Ctrl+C
```

---

### 通用 CDP 命令（供其他 Skill 复用）

通过 `~/.jscmd.sock` Unix socket 调用，不依赖 JumpServer，只要 Chrome 开启 CDP 即可：

```bash
# 列出所有浏览器页面（jupyterlab-terminal setup 会用到）
echo '{"cmd":"cdp_pages"}' | nc -U ~/.jscmd.sock

# 在指定页面执行 JS 表达式
echo '{"cmd":"cdp_eval","target_id":"ABC123","expression":"document.title"}' | nc -U ~/.jscmd.sock
```

---

## 安全防护

所有命令发往终端前自动经过三级安全检查：

### Level 1 — BLOCK（立即拒绝，不执行）

- 修改系统 Python 解释器：`update-alternatives --set python`、`pyenv global` 等
- 写入系统关键文件：`/etc/passwd`、`/etc/shadow`、`/etc/sudoers` 等
- 卸载系统组件：`apt remove python3`、`yum remove python` 等
- 删除系统目录：`rm -rf /usr`、`rm -rf /etc` 等

### Level 2 — CONFIRM + 倒计时（需用户输入 yes + 等待 10 秒）

```
[WARN] 检测到删除类操作: rm -rf /tmp/old_logs
请输入 yes 确认执行（其他输入取消）: > yes
[INFO] 将在 10 秒后执行，Ctrl+C 可取消...
  10... 9... 8...
```

触发关键词：`rm`、`rmdir`、`dd if=`、`mkfs`、`shred`、`wipefs` 等

### Level 3 — WARN（打印警告后执行）

- `sudo` 前缀命令
- `systemctl stop/disable/mask`
- `pip uninstall`、`npm uninstall -g`

### 自定义安全配置

编辑 `~/.jscmd_config.json`（首次运行自动生成）：

```json
{
  "delete_sleep_seconds": 10,
  "extra_block_patterns": ["my-dangerous-cmd"],
  "extra_confirm_patterns": ["custom-risky-pattern"],
  "disable_safety": false,
  "idle_timeout_seconds": 3600
}
```

---

## Hostname 映射机制

- **持久化**（`~/.jscmd_aliases.json`）：保存 `hostname → [别名...]`，不保存 tab 位置
- **运行时**（daemon 内存）：动态维护 `tab位置 → hostname` 缓存
- **安全验证**：`@name` 执行前实时扫描确认目标 tab 的 hostname 与期望一致，防止标签重排导致在错误服务器执行命令

```bash
# 典型工作流
jscmd sessions               # 确认每个 tab 连接哪台服务器
jscmd alias @current web     # 给活跃标签起名 "web"
jscmd alias #2 db            # 给第 2 个标签起名 "db"
jscmd exec "@web df -h"      # 永远在 web 服务器执行，不管在第几个 tab
jscmd exec "@db 'SHOW TABLES;'"
```

---

## 并发说明

多个 `exec` 命令同时发往 daemon 时，daemon 侧**串行处理**（自动排队），这是预期行为：
- 防止两个命令竞争全局输入焦点，导致内容打到同一个 terminal
- 防止两个哨兵互相"偷"对方的输出，引发超时

CLI 层可并发发起，daemon 自动排队、按序执行、各自返回正确输出。

---

## 已验证兼容性

| 场景 | 状态 | 说明 |
|------|------|------|
| 自建 JumpServer（家庭环境） | ✅ 已验证 | KoKo 使用 `/koko/connect/` 路径 + `Input.insertText` 输入 |
| 企业 JumpServer（公司环境） | ✅ 已验证 | KoKo 使用 `/koko/terminal/` 路径 + `SendTerminalData()` JS API 输入 |

两种 KoKo 版本差异由 daemon 在首次连接时自动检测，无需手动配置。

---

## 故障排查

| 问题 | 解决方法 |
|------|---------|
| `daemon 未运行` | 执行 `jscmd daemon start` |
| Chrome 弹框后执行失败 | 确认在 Chrome 中点击了"允许" |
| 未找到 JumpServer 页面 | 确认浏览器已打开 JumpServer（URL 含 `/luna/`） |
| `DevToolsActivePort` 不存在 | 打开 `chrome://inspect/#remote-debugging` 勾选允许 |
| 命令超时无输出 | 增加 `--timeout` 或重启 daemon（`jscmd daemon stop && jscmd daemon start`） |
| 命令输出为空 | 适当增加 `--timeout`（默认 30s） |
| `@name` 找不到标签 | 先执行 `jscmd sessions` 确认 hostname 已检测；未检测到则 `jscmd sessions --refresh` |
| `connect` 等待新终端超时 | 确认 JumpServer 侧边栏能搜到并双击打开该资产；或 `jscmd daemon stop` 重启后重试 |
| 多个命令都打到同一个 tab | 升级到最新 `jscmd.py`（旧版并发 bug 已修复） |

---

## 文件结构

```
jumpserver-terminal/
├── SKILL.md              # Agent 触发规则与调用规范
├── README.md             # 本文档（用户 & 开发参考）
└── jscmd.py              # CLI + daemon 实现（Chrome CDP + Unix socket）
```

配置文件（自动生成）：
- `~/.jscmd_config.json` — daemon 配置（超时、安全规则等）
- `~/.jscmd_aliases.json` — 服务器别名持久化
- `~/.jscmd.sock` — daemon Unix socket
