---
name: chrome-cdp-ws-daemon
description: Chrome CDP WebSocket 守护进程；仅在用户明确需要 CDP 连接、cookie 或浏览器操作时使用，不应在会话启动或 skill 加载时自动连接 Chrome。
---

# 使用场景

- 任何 skill 需要通过 Chrome CDP 获取 cookie、操作浏览器页面时，都应该依赖本 skill。
- 避免每个 skill 各自建立 CDP 连接导致用户频繁授权弹窗。

# 触发条件

- 只有当用户明确要求 CDP 连接、读取 cookie、操作 Chrome 页面，或其他 skill 实际执行 CDP 操作时才使用。
- 加载本 skill、导入 `cdp_client.py`、查询 `daemon_status()` 不允许启动 daemon，也不允许连接 Chrome。
- `cdp_client.py` 默认不自动启动 daemon；如需自动启动，调用方必须显式传 `auto_start=True`。
- 手动管理 daemon 时使用 CLI 命令。

# 架构

```
┌──────────────┐   Unix Socket    ┌──────────────────┐   WebSocket   ┌─────────┐
│  Skill A     │ ──────────────── │  cdp daemon      │ ───────────── │ Chrome  │
│  Skill B     │   (并发安全)     │  (后台常驻)      │  (持久连接)   │ Browser │
│  Skill C     │ ──────────────── │  ~/.chrome-cdp-daemon/cdp.sock   │         │
└──────────────┘                  └──────────────────┘               └─────────┘
```

- daemon 进程可后台常驻，但只在显式启动或显式 CDP 操作时创建
- 所有 skill 通过 Unix Socket 向 daemon 请求 CDP 服务
- 线程安全：多 skill 并发请求互不干扰（RLock + 独立线程）
- 心跳只检测已有连接是否断开，不主动重连 Chrome，避免空闲会话触发授权弹窗
- 首次启动弹一次授权框，后续完全静默

# 高级交互（推荐用法）

遵循 **先 snapshot → 再交互 → 再 snapshot** 的模式：

## CLI 方式（对齐 agent-browser 风格）

```bash
DAEMON=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 1. 获取页面可交互元素快照
$DAEMON $SCRIPT snapshot -i
# 输出示例:
# @e1  [input type="text"] "Title"
# @e2  [textarea] "Type your description here…"
# @e3  [button] "Submit new issue"
# --- 3 interactive elements ---

# 2. 用 @ref 引用填充表单（默认 native 模式，兼容 Vue/React）
$DAEMON $SCRIPT fill @e1 "My issue title"
$DAEMON $SCRIPT fill @e2 "Issue description here"
$DAEMON $SCRIPT fill @e1 "搜索内容" --submit         # 填充后自动按 Enter
$DAEMON $SCRIPT fill @e1 "text" --no-native           # 回退到 JS setter 模式
$DAEMON $SCRIPT fill @e1 "text" --no-clear            # 不清空原内容，追加输入

# 3. 点击（支持单击、双击、右键、坐标点击）
$DAEMON $SCRIPT click @e3                              # 普通单击
$DAEMON $SCRIPT click @e3 --dblclick                   # 双击
$DAEMON $SCRIPT click @e3 --right                      # 右键（触发右键菜单）
$DAEMON $SCRIPT click dummy --at 332,156               # 按坐标点击
$DAEMON $SCRIPT click dummy --right --at 332,156       # 按坐标右键

# 4. 下拉选择（自动兼容原生 <select> 和 Ant Design/Element UI/Arco 等）
$DAEMON $SCRIPT select @e4 "option-value"              # 按 value 选择
$DAEMON $SCRIPT select @e4 "显示文本" --by label       # 按显示文本选择
# 对于 Ant Design 等自定义下拉，自动：点击触发 → 等弹出层 → 文本匹配 → 点击选中

# 5. 鼠标悬浮（触发 hover 下拉菜单、tooltip 等）
$DAEMON $SCRIPT hover @e1                              # 悬浮到元素上
$DAEMON $SCRIPT hover --at 1361,24                     # 悬浮到指定坐标

# 6. 按键（CDP 原生 rawKeyDown + char + keyUp，兼容所有前端框架）
$DAEMON $SCRIPT press Enter                            # 回车
$DAEMON $SCRIPT press Enter @e1                        # 在指定元素上按键
$DAEMON $SCRIPT press Tab                              # Tab 切换焦点
$DAEMON $SCRIPT press Escape                           # 关闭弹窗/下拉

# 7. 滚动（CDP 鼠标滚轮，自动识别滚动容器）
$DAEMON $SCRIPT scroll down                            # 视口中心向下滚 500px（主内容区）
$DAEMON $SCRIPT scroll down 800                        # 自定义滚动量
$DAEMON $SCRIPT scroll down --at 128,446               # 在指定坐标滚动（如左侧栏）
$DAEMON $SCRIPT scroll down @e5                        # 在元素所在区域滚动
$DAEMON $SCRIPT scroll up                              # 向上滚动

# 8. 其他
$DAEMON $SCRIPT check @e5                              # 勾选 checkbox
$DAEMON $SCRIPT wait --selector "#result"              # 等待元素出现
$DAEMON $SCRIPT wait --text "成功"                     # 等待文本出现
$DAEMON $SCRIPT get-text @e1                           # 获取元素文本
$DAEMON $SCRIPT get-text                               # 获取整页文本
$DAEMON $SCRIPT get-url                                # 获取当前 URL
$DAEMON $SCRIPT get-title                              # 获取页面标题

# 9. 文本搜索与定位（无需 snapshot）
$DAEMON $SCRIPT find-text "查看血缘"                   # 搜索包含该文本的所有元素
$DAEMON $SCRIPT find-text "查看血缘" --tag button      # 仅搜索 button 标签
$DAEMON $SCRIPT find-text "查看血缘" --region top-right # 限定区域（九宫格）
# 区域: top-left, top, top-right, left, center, right, bottom-left, bottom, bottom-right

# 10. 文本点击（无需 snapshot，一步到位）
$DAEMON $SCRIPT click-text "查看血缘"                  # 找到并点击
$DAEMON $SCRIPT click-text "查看血缘" --tag a          # 仅匹配 <a> 标签
$DAEMON $SCRIPT click-text "提交" --region bottom-right # 限定区域
$DAEMON $SCRIPT click-text "编辑" --nth 2              # 匹配第 2 个

# 11. 标签页管理
$DAEMON $SCRIPT open "https://github.com"              # 新建标签页并激活
$DAEMON $SCRIPT open "https://csdn.net" --group csdn1  # 新建并加入指定标签组
$DAEMON $SCRIPT close                                  # 关闭当前活动页
$DAEMON $SCRIPT close C7A52F06                         # 关闭指定 target
$DAEMON $SCRIPT activate C7A52F06                      # 激活（切换到）指定 tab

# 12. Chrome 原生标签组（显示在 Chrome UI 中的分组）
$DAEMON $SCRIPT group create mygroup --color blue      # 创建空分组
$DAEMON $SCRIPT group add mygroup                      # 将当前页加入分组
$DAEMON $SCRIPT group add mygroup --target "url:csdn"  # 将匹配页加入分组
$DAEMON $SCRIPT group move mygroup                     # 移动当前页到分组（先移出其他组）
$DAEMON $SCRIPT group remove mygroup                   # 从分组中移除当前页（不关闭）
$DAEMON $SCRIPT group close mygroup                    # 关闭整个分组及其所有 tab
$DAEMON $SCRIPT group close-tabs mygroup C7A52F06      # 关闭分组内指定 tab
$DAEMON $SCRIPT group delete mygroup                   # 取消分组（tab 保留，解散分组）
$DAEMON $SCRIPT group activate mygroup                 # 激活分组中第一个 tab
$DAEMON $SCRIPT group list                             # 列出所有分组及其 tab
# 颜色: grey, blue, red, yellow, green, pink, purple, cyan, orange

# 13. 网络抓包与重放
# 抓包
$DAEMON $SCRIPT network-capture start                  # 开始抓包（当前页）
$DAEMON $SCRIPT network-capture start --follow         # 跟踪模式（自动包含新开 tab）
$DAEMON $SCRIPT network-capture stop                   # 停止并输出请求列表
$DAEMON $SCRIPT network-capture stop --body            # 停止并包含响应 body
$DAEMON $SCRIPT network-capture export                 # 导出为 Python requests 代码
$DAEMON $SCRIPT network-capture export --curl          # 导出为 curl 命令

# 页面上下文 fetch（自动带 cookie，绕过 CORS）
$DAEMON $SCRIPT network fetch "https://api.example.com/data"
$DAEMON $SCRIPT network fetch "https://api.example.com/data" --method POST --body '{"key":"val"}'
$DAEMON $SCRIPT network fetch "https://api.example.com/data" --target "url:example"

# 重放抓包的请求
$DAEMON $SCRIPT network replay 1                       # 重放第 1 个请求
$DAEMON $SCRIPT network replay 3 --url "https://..."   # 重放第 3 个，覆盖 URL
$DAEMON $SCRIPT network replay 1 --method POST         # 覆盖 HTTP 方法
$DAEMON $SCRIPT network replay 1 --body '{"new":"data"}'  # 覆盖 body

# 14. Monaco/CodeMirror 编辑器操作
$DAEMON $SCRIPT editor-get                             # 读取编辑器内容（自动检测 Monaco/CM5/CM6/textarea）
$DAEMON $SCRIPT editor-set "SELECT * FROM t"           # 整段替换编辑器内容
$DAEMON $SCRIPT editor-set " LIMIT 10" --append        # 追加到末尾
$DAEMON $SCRIPT editor-type "sel"                      # 逐字符输入（触发 autocomplete）

# 15. 图标按钮搜索与点击
$DAEMON $SCRIPT find-icon "save"                       # 按 title/aria-label/anticon-*/icon-* 搜索
$DAEMON $SCRIPT find-icon "save" --region top-right    # 限定区域
$DAEMON $SCRIPT click-icon "save"                      # 搜索并点击
$DAEMON $SCRIPT click-icon "funnel-plot" --nth 2       # 第 2 个匹配

# 16. tooltip 按钮扫描（悬浮发现）
$DAEMON $SCRIPT scan-tooltips                          # 扫描当前页面所有图标按钮 tooltip
$DAEMON $SCRIPT scan-tooltips --region top-right       # 限定区域
$DAEMON $SCRIPT scan-tooltips --scope ".toolbar"       # 限定 CSS 范围

# 17. 拖拽操作
$DAEMON $SCRIPT drag 100 200 300 400                   # 从 (100,200) 拖到 (300,400)
$DAEMON $SCRIPT drag 100 200 300 400 --steps 20        # 自定义拖拽步数

# 指定操作目标页面（默认 active = 当前活动 tab）
$DAEMON $SCRIPT snapshot -i --target "url:github.com"
$DAEMON $SCRIPT click @e1 --target C7A52F06
```

### 增强特性说明

- **snapshot label 关联**：input/select/textarea 元素在 snapshot 输出中自动显示关联的 label 文字（支持 `<label for>` / 祖先包裹 / `aria-labelledby` / Ant Design `.ant-form-item-label`）
- **文本匹配空格标准化**：`find-text` / `click-text` 自动将 `\u00a0`（`&nbsp;`）、全角空格等标准化为普通空格后匹配，解决 Ant Design 按钮中 "确 定" 类文字的匹配问题
- **大 DOM 自动限流**：页面元素 >5000 时 snapshot 自动缩窄到内容根节点，防止超时
- **page_call 30s 超时保护**：单次 CDP 调用超时后自动使 session 失效，返回清晰错误提示

## 典型场景示例

```bash
# 搜索框输入并搜索（一条命令）
$DAEMON $SCRIPT fill @e90 "llm" --submit

# hover 触发区域切换下拉 → 点击选项
$DAEMON $SCRIPT hover --at 1361,24 --target <id>
$DAEMON $SCRIPT click dummy --at 1367,79 --target <id>

# 右键菜单操作
$DAEMON $SCRIPT click dummy --right --at 332,156       # 右键弹出菜单
$DAEMON $SCRIPT click dummy --at 386,212               # 点击菜单项

# 双击编辑
$DAEMON $SCRIPT click @e5 --dblclick

# 多区域滚动（如 CSDN 左侧栏 vs 主内容）
$DAEMON $SCRIPT scroll down --at 128,446               # 滚动左侧栏
$DAEMON $SCRIPT scroll down                            # 滚动主内容（视口中心）
```

## SDK 方式（其他 skill import 使用）

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
fill("@e2", "Description", native=False)          # JS setter 模式
click("@e3")
click("@e3", dblclick=True)                       # 双击
click("@e3", right=True)                          # 右键

# 鼠标悬浮
hover("@e1")
hover(at=(1361, 24))

# 也可以直接用 CSS 选择器
fill("#my-input", "text value")
click("button[type=submit]")

# 等待元素
wait_for(selector="#success-msg", timeout_ms=5000)

# 下拉选择（自动兼容原生 <select> 和 Ant Design 等自定义下拉）
select("@e4", "California", by_label=True)

# 滚动（CDP 鼠标滚轮，按坐标定位滚动区域）
scroll("down")                                    # 视口中心
scroll("down", at=(128, 446))                     # 指定坐标（如左侧栏）

# 网络抓包与 fetch
network_capture_start(target="active")            # 开始抓包
# ... 执行页面操作 ...
captured = network_capture_stop(target="active")  # 停止，获取请求列表

# 在页面上下文 fetch（自动带 cookie）
resp = network_fetch("https://api.example.com/data", method="GET")
print(resp["status"], resp["body"])

# 重放抓包的请求
replay = network_replay(index=1)                  # 重放第 1 个
print(replay["status"], replay["body"])

# Monaco/CodeMirror 编辑器
content = editor_get()                            # 读取编辑器内容
editor_set("SELECT * FROM t1")                    # 整段替换
editor_set(" LIMIT 10", append=True)              # 追加到末尾
editor_type("sel")                                # 逐字符输入（触发 autocomplete）

# 图标按钮搜索
matches = find_icon("save")                       # 搜索图标按钮
click_icon("save", region="top-right")            # 限定区域点击

# tooltip 扫描（发现鼠标悬浮才显示文字的按钮）
result = scan_tooltips(region="top-right")        # 扫描右上角
for btn in result["buttons"]:
    print(f"{btn['tooltip']} at ({btn['x']},{btn['y']}) icon={btn.get('icon','')}")

# 拖拽
drag(100, 200, 300, 400, steps=15)                # 从 (100,200) 拖到 (300,400)

# 指定页面
snapshot(target="url:github.com")
fill("@e1", "text", target="url:github.com")

# 页面级 CDP 调用（需要更底层控制时）
from cdp_client import page_call
result = page_call("active", "Runtime.evaluate", {
    "expression": "document.title",
    "returnByValue": True,
})
```

## 快照参数

```bash
# -i          交互模式（默认）
# -C          包含 cursor-interactive 元素（带 onclick 的 div 等）
# -s <scope>  限定 CSS 范围，如 -s "#main-content"
# --json      输出 JSON 格式
# --target    指定页面: active | targetId | url:keyword
```

# 基础操作

## 方式1：直接 import client SDK

```python
import sys
sys.path.insert(0, "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
from cdp_client import get_cookies, cdp_call
from cdp_actions import list_pages, get_active_page

# 获取指定域名的 cookie
cookies = get_cookies("https://bdp-cn.tuya-inc.com:7799")

# 执行任意 CDP 命令
targets = cdp_call("Target.getTargets")

# 获取所有打开的 tab
pages = list_pages()

# 获取用户当前活动的 Chrome tab（macOS only）
page = get_active_page()

# 如需自动启动 daemon
targets = cdp_call("Target.getTargets", auto_start=True)
```

## 方式2：subprocess 调用 CLI

```bash
# 显式启动 daemon
/Users/luca/miniforge3/envs/py311/bin/python /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py start

# 测试连接
... daemon.py test

# 查看状态
... daemon.py status

# 列出所有 tab
... daemon.py list-pages

# 获取活动 tab（macOS only）
... daemon.py active-page

# 停止 / 重启
... daemon.py stop
... daemon.py restart
```

# 文件说明

- `scripts/daemon.py` — 守护进程（CDP 连接管理 + Unix Socket 服务 + CLI）
- `scripts/cdp_client.py` — 客户端 SDK（cookie / cdp_call / 高级操作请求封装）
- `scripts/cdp_actions.py` — 高层动作 SDK（其他 skill 直接 import）
- `scripts/page_manager.py` — 页面级操作管理器（session 管理 / 元素引用 / JS 注入 / 动作执行）

# 运行时文件

- `~/.chrome-cdp-daemon/cdp.sock` — Unix Socket 通信文件
- `~/.chrome-cdp-daemon/cdp.pid` — daemon PID 文件
- `~/.chrome-cdp-daemon/cdp.log` — daemon 运行日志
