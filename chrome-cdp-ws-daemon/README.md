# Chrome CDP WebSocket Daemon

通过 Unix Socket 共享一条持久 CDP WebSocket 连接的守护进程，所有需要 Chrome CDP 的 skill 共用同一连接，用户只需授权一次。

## 架构

```
┌──────────────┐   Unix Socket    ┌──────────────────┐   WebSocket   ┌─────────┐
│  Skill A     │ ────────────────  │  cdp daemon      │ ───────────── │ Chrome  │
│  Skill B     │   (并发安全)     │  (后台常驻)      │  (持久连接)   │ Browser │
│  Skill C     │ ────────────────  │  ~/.chrome-cdp-daemon/cdp.sock   │         │
└──────────────┘                   └──────────────────┘               └─────────┘
```

- daemon 进程可后台常驻，但只在显式启动或显式 CDP 操作时创建
- 所有 skill 通过 Unix Socket 向 daemon 请求 CDP 服务
- 线程安全：多 skill 并发请求互不干扰（RLock + 独立线程）
- 心跳只检测已有连接是否断开，不主动重连 Chrome，避免空闲会话触发授权弹窗
- 首次启动弹一次授权框，后续完全静默

## 文件说明

| 文件 | 说明 |
|------|------|
| `scripts/daemon.py` | 守护进程（CDP 连接管理 + Unix Socket 服务 + CLI） |
| `scripts/cdp_client.py` | 客户端 SDK（cookie / cdp_call / page_call / 高级操作请求封装） |
| `scripts/cdp_actions.py` | 高层动作 SDK（其他 skill 直接 import 使用） |
| `scripts/page_manager.py` | 页面级操作管理器（session 管理 / 元素引用 / JS 注入 / 动作执行） |

## 运行时文件

- `~/.chrome-cdp-daemon/cdp.sock` — Unix Socket 通信文件
- `~/.chrome-cdp-daemon/cdp.pid` — daemon PID 文件
- `~/.chrome-cdp-daemon/cdp.log` — daemon 运行日志

## 安全原则

- 加载 skill、导入 `cdp_client.py`、查询 `daemon_status()` 不会启动 daemon，也不会连接 Chrome
- `cdp_client.py` 默认不自动启动 daemon；如需自动启动，调用方必须显式传 `auto_start=True`
- 心跳只检测已有连接是否断开，不主动重连

## CLI 命令一览

> 以下命令通过 `python daemon.py <command>` 调用，为简洁起见省略前缀。

### 基础管理

| 命令 | 说明 |
|------|------|
| `start` | 显式启动 daemon |
| `stop` | 停止 daemon |
| `restart` | 重启 daemon |
| `status` | 查看 daemon 状态 |
| `test` | 测试 CDP 连接（会自动启动 daemon） |
| `list-pages` | 列出所有打开的 tab |
| `active-page` | 获取用户当前活动的 Chrome tab（macOS only） |

### 快照与元素操作

| 命令 | 说明 |
|------|------|
| `snapshot [-i] [-C] [-s <scope>] [--target <t>] [--json]` | 获取页面可交互元素快照（`@eN` 引用） |
| `click <@ref\|selector> [--dblclick] [--right] [--at x,y] [--target <t>]` | 点击元素 |
| `click-text "文本" [--tag] [--nth N] [--region] [--dblclick] [--right] [--target <t>]` | 通过文本内容查找并点击 |
| `find-text "文本" [--tag] [--region] [--target <t>]` | 搜索包含指定文本的所有元素 |
| `hover <@ref\|(--at x,y)> [--target <t>]` | 鼠标悬浮 |
| `fill <@ref\|selector> "text" [--submit] [--no-native] [--no-clear] [--target <t>]` | 填充表单（默认 native 模式兼容 Vue/React） |
| `select <@ref\|selector> "value" [--by label] [--target <t>]` | 下拉选择（自动兼容原生 select 和 Ant Design/Element UI 等） |
| `check <@ref\|selector> [--target <t>]` | 勾选 checkbox |
| `press <key> [@ref] [--target <t>]` | 发送按键（CDP 原生事件，兼容所有前端框架） |
| `scroll <up\|down\|left\|right> [px] [--at x,y] [@ref] [--target <t>]` | 滚动页面（自动识别滚动容器） |
| `drag <startX,startY> <endX,endY> [--steps N] [--hold-ms N] [--target <t>]` | 鼠标拖拽 |
| `wait --selector <s>\|--text <t> [--timeout-ms N] [--target <t>]` | 等待元素或文本出现 |
| `get-text [@ref\|selector] [--target <t>]` | 获取元素或页面文本 |
| `get-url [--target <t>]` | 获取页面 URL |
| `get-title [--target <t>]` | 获取页面标题 |

### 标签页管理

| 命令 | 说明 |
|------|------|
| `open <url> [--group <name>] [--no-activate] [--wait <ms>]` | 新建标签页 |
| `close [target] [--target <t>]` | 关闭标签页 |
| `activate [target] [--target <t>]` | 激活（切换到）指定 tab |

### Chrome 原生标签组

| 命令 | 说明 |
|------|------|
| `group create <name> [targets...] [--color <c>]` | 创建标签组（颜色: grey/blue/red/yellow/green/pink/purple/cyan/orange） |
| `group add <name> <target1> [target2...]` | 向标签组添加标签页 |
| `group move <name> <target1> [target2...]` | 移入标签组（从其它组移出） |
| `group remove <name> <target1> [target2...]` | 从标签组中移除（不关闭） |
| `group list [name]` | 列出标签组 |
| `group close <name>` | 关闭标签组（关闭所有 tab） |
| `group close-tabs <name> <target1> [target2...]` | 关闭标签组内指定 tab |
| `group delete <name>` | 删除标签组（保留 tab，解散分组） |
| `group activate <name>` | 切换到标签组第一个 tab |

### 网络抓包与重放

| 命令 | 说明 |
|------|------|
| `network-capture start [--follow] [--target <t>]` | 开始网络抓包（`--follow` 自动跟踪新 tab） |
| `network-capture stop [--body] [--target <t>]` | 停止抓包，输出请求列表 |
| `network-capture export [--curl]` | 导出为 Python requests 代码或 curl 命令 |
| `network fetch <url> [--method] [--body] [--target <t>]` | 在页面上下文 fetch（自动带 cookie，绕过 CORS） |
| `network replay [N] [--url] [--method] [--body] [--target <t>]` | 重放抓包的第 N 个请求 |

### Monaco / CodeMirror 编辑器

| 命令 | 说明 |
|------|------|
| `editor-get [--target <t>]` | 读取编辑器内容（自动检测 Monaco/CM5/CM6/textarea） |
| `editor-set "text" [--append] [--target <t>]` | 设置编辑器内容（整段写入或追加） |
| `editor-type "text" [--target <t>]` | 逐字符输入（模拟真实打字，触发 autocomplete） |

### 图标搜索与点击

| 命令 | 说明 |
|------|------|
| `find-icon "query" [--region] [--target <t>]` | 通过 title/aria-label/anticon class 搜索图标按钮 |
| `click-icon "query" [--region] [--nth N] [--dblclick] [--right] [--target <t>]` | 搜索并点击图标按钮 |
| `scan-tooltips [--region] [--scope] [--target <t>]` | 扫描区域内图标按钮，逐个 hover 收集 tooltip 文字 |

### 其他

| 命令 | 说明 |
|------|------|
| `auth-click-test [timeout]` | 授权弹窗自动点击自验 |

### target 参数说明

大部分命令支持 `--target` 指定操作目标页面：
- `active` — 当前活动 tab（默认）
- `targetId` — 如 `C7A52F06`
- `url:keyword` — 按 URL 关键词匹配

### 区域参数 (--region)

快照、文本搜索等支持 `--region` 九宫格区域限定：
`top-left`、`top`、`top-right`、`left`、`center`、`right`、`bottom-left`、`bottom`、`bottom-right`

## SDK 用法

### 方式1：import cdp_client（底层）

```python
import sys
sys.path.insert(0, "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
from cdp_client import get_cookies, cdp_call, page_call

# 获取指定域名的 cookie（默认不自动启动 daemon）
cookies = get_cookies("https://example.com")

# 获取所有 cookie
all_cookies = get_all_cookies()

# 执行任意 CDP 命令
targets = cdp_call("Target.getTargets")

# 页面级 CDP 调用
result = page_call("active", "Runtime.evaluate", {
    "expression": "document.title",
    "returnByValue": True,
})

# 如需自动启动 daemon（显式声明）
targets = cdp_call("Target.getTargets", auto_start=True)
```

### 方式2：import cdp_actions（推荐，高层 API）

```python
import sys
sys.path.insert(0, "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
from cdp_actions import (
    snapshot, click, fill, select, check, press, hover,
    scroll, drag, wait_for, get_text, get_url, get_title,
    list_pages, get_active_page,
    network_capture_start, network_capture_stop,
    network_fetch, network_replay,
    editor_get, editor_set, editor_type,
    find_icon, click_icon, scan_tooltips,
)

# 获取快照
result = snapshot()  # target 默认 "active"
for el in result["elements"]:
    print(f"{el['ref']}  {el['desc']}")

# 用引用操作
fill("@e1", "Hello World")                       # 默认 native 模式
fill("@e1", "搜索词", submit=True)               # 填充后自动 Enter
click("@e3")
click("@e3", dblclick=True)                       # 双击
click("@e3", right=True)                          # 右键

# 鼠标悬浮
hover("@e1")
hover(at=(1361, 24))

# 也可以直接用 CSS 选择器
fill("#my-input", "text value")
click("button[type=submit]")

# 下拉选择（自动兼容原生 <select> 和 Ant Design 等自定义下拉）
select("@e4", "California", by_label=True)

# 滚动（CDP 鼠标滚轮，按坐标定位滚动区域）
scroll("down")
scroll("down", at=(128, 446))

# 网络抓包与 fetch
network_capture_start(target="active")
captured = network_capture_stop(target="active")
resp = network_fetch("https://api.example.com/data", method="GET")
replay = network_replay(index=1)

# Monaco/CodeMirror 编辑器
content = editor_get()
editor_set("SELECT * FROM t1")
editor_set(" LIMIT 10", append=True)
editor_type("sel")

# 图标按钮搜索
matches = find_icon("save")
click_icon("save", region="top-right")
result = scan_tooltips(region="top-right")

# 拖拽
drag(100, 200, 300, 400, steps=15)

# 指定页面
snapshot(target="url:github.com")
fill("@e1", "text", target="url:github.com")

# 获取所有打开的 tab / 当前活动 tab
pages = list_pages()
page = get_active_page()
```

### 方式3：subprocess 调用 CLI

```bash
PYTHON=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 启动 / 测试 / 状态
$PYTHON $SCRIPT start
$PYTHON $SCRIPT test
$PYTHON $SCRIPT status

# 列出所有 tab / 获取活动 tab
$PYTHON $SCRIPT list-pages
$PYTHON $SCRIPT active-page

# 停止 / 重启
$PYTHON $SCRIPT stop
$PYTHON $SCRIPT restart
```

## 增强特性说明

- **snapshot label 关联**：input/select/textarea 元素在 snapshot 输出中自动显示关联的 label 文字（支持 `<label for>` / 祖先包裹 / `aria-labelledby` / Ant Design `.ant-form-item-label`）
- **文本匹配空格标准化**：`find-text` / `click-text` 自动将 `\u00a0`（`&nbsp;`）、全角空格等标准化为普通空格后匹配
- **大 DOM 自动限流**：页面元素 >5000 时 snapshot 自动缩窄到内容根节点，防止超时
- **page_call 30s 超时保护**：单次 CDP 调用超时后自动使 session 失效，返回清晰错误提示
- **下拉选择自动兼容**：原生 `<select>` 和 Ant Design/Element UI/Arco 等自定义下拉均可处理
- **编辑器自动检测**：Monaco/CodeMirror5/CodeMirror6/textarea 自动识别
- **网络抓包跟踪模式**：`--follow` 参数自动包含新开 tab 的请求

## 回归自验用例

### 用例 1：加载/导入不自动启动 daemon

目的：确认普通会话加载 skill 或导入 SDK 不会触发 Chrome 授权弹窗。

```bash
pkill -f "chrome-cdp-ws-daemon/scripts/daemon.py" 2>/dev/null || true
pkill -f "osascript -e" 2>/dev/null || true
rm -f ~/.chrome-cdp-daemon/cdp.sock ~/.chrome-cdp-daemon/cdp.pid

python3 - <<'PY'
import importlib.util
p = '/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/cdp_client.py'
spec = importlib.util.spec_from_file_location('cdp_client', p)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
print(m.daemon_status())
PY

ps -ef | rg "chrome-cdp-ws-daemon/scripts/daemon.py|osascript -e" | rg -v rg || true
```

期望：`daemon_status()` 返回 `{'running': False}`，没有 daemon 进程，没有 `osascript` watcher，Chrome 不弹授权窗口。

### 用例 2：显式首次连接

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py test
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py status
```

期望：`test` 返回 `ok=true`，`status` 返回 `ws_connected=true`，如 Chrome 弹授权框则 AppleScript 自动点击。

### 用例 3：同一 daemon 连接复用

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py status
python3 - <<'PY'
import importlib.util
p = '/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/cdp_client.py'
spec = importlib.util.spec_from_file_location('cdp_client', p)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
print('status1=', m.daemon_status())
print('version=', m.cdp_call('Browser.getVersion').get('product'))
print('status2=', m.daemon_status())
PY
```

期望：`connection_session_id` 不变，`reconnect_count` 不增加，没有新授权弹窗。

### 用例 4：关闭 Chrome 授权后的已连接复用

1. 先执行用例 2，确保 daemon 已连接。
2. 用户在 Chrome 授权弹窗中关闭授权。
3. 再次执行 `daemon.py test` / `daemon.py status`。

期望：如果当前 WebSocket 未被 Chrome 主动断开，`connection_session_id` 保持不变；如果断开则下次显式操作才重连。心跳不主动重连。

### 用例 5：Chrome 重启后不因空闲心跳自动弹窗

1. daemon 已连接。
2. 重启 Chrome。
3. 不执行任何 CDP 操作，等待超过 30 秒。
4. 执行 `daemon.py status`。

期望：心跳只标记断线，不主动重连，不弹授权窗。

### 用例 6：授权弹窗自动点击自验

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py auth-click-test 3
```

期望：没有弹窗时返回 `not_found`，成功点击时返回 `pressed_and_gone`。

### 用例 7：活动页识别

```bash
# daemon 未运行时应报错
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py active-page

# 启动后再测试
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py test
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py list-pages
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py active-page

# SDK 调用
python3 - <<'PY'
import sys
sys.path.insert(0, '/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts')
from cdp_actions import list_pages, get_active_page
print('pages:', len(list_pages()))
print('active:', get_active_page())
PY
```

期望：`active-page` 输出的 url 与浏览器 frontmost 窗口 active tab 一致，切换 tab 后实时更新。

### 排查命令

当授权弹窗堆叠或出现僵尸 sheet 时：

```bash
pkill -f "chrome-cdp-ws-daemon/scripts/daemon.py" 2>/dev/null || true
pkill -f "osascript -e" 2>/dev/null || true
rm -f ~/.chrome-cdp-daemon/cdp.sock ~/.chrome-cdp-daemon/cdp.pid
```