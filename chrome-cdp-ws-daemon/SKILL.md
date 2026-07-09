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

# SPA 接口发现（标准流程）

用于 Tone / BDP 等前端分页、下拉筛选场景；**以抓包为准**，不要靠扫静态 JS bundle 猜 API。

```bash
DAEMON=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 方式 A：一条龙快路径（推荐）
$DAEMON $SCRIPT discover-api \
  --target "url:tone.tuya-inc.com/devops/app/list" \
  --url "https://tone.tuya-inc.com:7799/devops/app/list" \
  --fast \
  --idle-ms 800 \
  --filter-url "/api/" \
  --body-mode filtered \
  --max-bodies 6 \
  --export-python-client

# 方式 B：基于当前页后续动作抓接口（不 reload）
$DAEMON $SCRIPT discover-api \
  --target tab:tone-app \
  --no-nav \
  --do "click-text 构建" \
  --do "press Enter" \
  --until-match "launcher/l/api" \
  --body-mode filtered \
  --max-bodies 4

# 方式 C：手动分步
$DAEMON $SCRIPT tab bind tone-app --target "url:tone.tuya-inc.com/devops/app/list"
$DAEMON $SCRIPT network-capture start --target tab:tone-app
$DAEMON $SCRIPT reload --target tab:tone-app          # 或 navigate <url>
$DAEMON $SCRIPT network-capture stop \
  --target tab:tone-app \
  --idle-ms 800 \
  --filter-url "launcher/l/api" \
  --body-mode filtered \
  --max-bodies 6
$DAEMON $SCRIPT network-capture filter --url launcher/l/api
$DAEMON $SCRIPT network-capture summary
$DAEMON $SCRIPT network-capture export --python-client
```

`body-mode` 说明：
- `none`：完全不拉响应体，最快，适合先找接口名
- `filtered`：只给过滤命中的请求拉 body，推荐默认用法
- `all`：全量拉 body，最慢，只在明确需要完整响应时使用

# 下游 skill 鉴权（cookie 优先）

业务 CLI（如 `tuya-bigdata`）应 **import `cdp_client.get_cookies(url)`**，不要用 subprocess 读 `local-storage`，除非确认 token 只在 storage 中。

```bash
# 按域名导出 cookie（给 Python requests）
$DAEMON $SCRIPT cookies get "https://tone.tuya-inc.com:7799" --header
$DAEMON $SCRIPT cookies inspect "https://tone.tuya-inc.com:7799" --json
$DAEMON $SCRIPT cookies validate "https://tone.tuya-inc.com:7799" --expect OPS_USER_TOKEN --json

# 从抓包看业务 header 名（值已脱敏）
$DAEMON $SCRIPT auth-template tone.tuya-inc.com

# 通用认证材料汇总（cookie + token-like storage + 抓包认证 header）
$DAEMON $SCRIPT auth material "https://example.com" --target active --json
$DAEMON $SCRIPT auth material "https://example.com" --key token --json
$DAEMON $SCRIPT auth material "https://example.com" --reveal --json  # 显式输出真实值

# 通用 cookie 换 token；不内置任何业务域名、cookie 名或接口路径
$DAEMON $SCRIPT auth token "https://example.com/api-token-auth/" \
  --method POST \
  --body '{}' \
  --cookie-url "https://example.com" \
  --extract data.token \
  --header-template "Authorization=TUYA {token}" \
  --reveal
```

`network fetch` 受 CORS 限制；逆向完成后请用 **cookies + requests** 调 API。

SDK 中优先使用 `cdp_client.get_cookies()`、`get_storage()`、`get_auth_material()`、`request_auth_token()`，避免业务 skill 复制 subprocess 调用和 token 换取样板。

# 高级交互（推荐用法）

遵循 **先 snapshot → 再交互 → 再 snapshot** 的模式：

## CLI 方式（对齐 agent-browser 风格）

```bash
DAEMON=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 1. 获取页面可交互元素快照
$DAEMON $SCRIPT snapshot -i
$DAEMON $SCRIPT snapshot -T                            # -T / --with-tooltips：对无文字图标按钮自动补全 tooltip（⚡ 新增）
$DAEMON $SCRIPT snapshot -i -c --json                  # compact JSON：只保留 ref/desc/value 等 LLM 必要字段
$DAEMON $SCRIPT snapshot -i -u                         # 输出链接 href，默认文本输出不展开 URL
$DAEMON $SCRIPT snapshot -i -d 8                       # 仅保留 DOM 深度 <= 8 的交互元素（显式限流）
$DAEMON $SCRIPT snapshot -i --max-output 12000         # 输出最多 12000 字符，防止 token 爆炸
$DAEMON $SCRIPT snapshot -i --content-boundaries       # 用边界标记包裹页面内容，降低 prompt injection 风险
$DAEMON $SCRIPT diff snapshot                          # 对比当前快照与上次 snapshot baseline，只输出变化
$DAEMON $SCRIPT diff snapshot -u --target "url:github.com"  # diff 时包含 href
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
$DAEMON $SCRIPT select @e4 "dwd_trade_topic" --search dwd --by label
# 对于 Ant Design 等自定义下拉，自动：点击触发 → 等弹出层 → 文本匹配 → 点击选中
# 若是 searchable 下拉（需先输入字符才出候选），会自动尝试输入；也可用 --search 显式指定筛选词
# 在 BDP 建表页这类场景里，可直接用于“所属数据库”这类动态下拉
# 若 SQL 为标准 Hive DDL，优先走顶部“导入sql”自动解析；分区字段应由平台自动识别，无需手工删除字段再新增 dt 分区

# 5. 鼠标悬浮（触发 hover 下拉菜单、tooltip 等）
$DAEMON $SCRIPT hover @e1                              # 悬浮到元素上
$DAEMON $SCRIPT hover --at 1361,24                     # 悬浮到指定坐标

# 6. 按键（CDP 原生 rawKeyDown + char + keyUp，兼容所有前端框架）
$DAEMON $SCRIPT press Enter                            # 回车
$DAEMON $SCRIPT press Enter @e1                        # 在指定元素上按键
$DAEMON $SCRIPT press Tab                              # Tab 切换焦点
$DAEMON $SCRIPT press Escape                           # 关闭弹窗/下拉
# 组合键（⚡ 新增）— 支持 Meta/Ctrl/Alt/Shift 任意组合
$DAEMON $SCRIPT press Meta+S                           # Cmd+S（Mac 保存）
$DAEMON $SCRIPT press Ctrl+Shift+P                     # Ctrl+Shift+P（命令面板）
$DAEMON $SCRIPT press Alt+F4                           # Alt+F4

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
$DAEMON $SCRIPT get-url --decode-param panes           # 提取 URL 中的 JSON 参数并格式化（⚡ 新增）
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
$DAEMON $SCRIPT open "https://github.com"              # 新建标签页并激活（强制进入固定分组 CDP自动化）
$DAEMON $SCRIPT open "https://csdn.net" --group csdn1  # 自定义 group 会被忽略；无法进入固定组时 open 失败并关闭新页
$DAEMON $SCRIPT open "https://bdp-cn.tuya-inc.com:7799/apps/metadata/applicationLibraryForm" --alias bdp-form
$DAEMON $SCRIPT tab bind current-bdp --target active    # 给当前页绑定 alias
$DAEMON $SCRIPT tab get bdp-form                        # 查看绑定到的 targetId/url/title
$DAEMON $SCRIPT tab list                                # 列出所有 alias
$DAEMON $SCRIPT target list --json                      # 列出可解析 target
$DAEMON $SCRIPT target resolve host:bdp-cn.tuya-inc.com # host 唯一命中才返回
$DAEMON $SCRIPT target resolve title:数据开发           # title 唯一命中才返回
$DAEMON $SCRIPT target resolve url-strict:bdp,develop   # URL 严格唯一命中才返回
$DAEMON $SCRIPT click @e1 --target tab:bdp-form        # 后续动作都可直接精确绑定到 tab:别名
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
$DAEMON $SCRIPT network-capture stop                   # 停止并输出请求列表（默认不抓 body，最快）
$DAEMON $SCRIPT network-capture stop --idle-ms 800     # 等网络空闲后停止，适合 SPA
$DAEMON $SCRIPT network-capture stop --body-mode filtered --filter-url /api/ --max-bodies 6
$DAEMON $SCRIPT network-capture stop --body-mode all --wait-ms 6000   # 明确需要全量 body 时再开
$DAEMON $SCRIPT network-capture stop --until-match "launcher/l/api" --idle-ms 300
$DAEMON $SCRIPT network-capture load-file /path/capture.json  # 载入已有抓包
$DAEMON $SCRIPT reload --target tab:myapp              # 刷新页面（勿用 eval location.reload）
$DAEMON $SCRIPT navigate "https://tone.tuya-inc.com:7799/devops/app/list"
$DAEMON $SCRIPT discover-api --target tab:myapp --fast --filter-url /api/
$DAEMON $SCRIPT discover-api --target tab:myapp --no-nav --do "click-text 保存" --idle-ms 800
$DAEMON $SCRIPT auth-template tone.tuya-inc.com        # 认证 header 模板（脱敏）
$DAEMON $SCRIPT network-capture filter --method GET --url "datafactory" --exclude-domain cdn  # 过滤（⚡ 新增）
$DAEMON $SCRIPT network-capture filter --status 200   # 按状态码过滤
$DAEMON $SCRIPT network-capture summary               # 摘要请求、通用认证头、请求/响应 JSON 结构（⚡ 新增）
$DAEMON $SCRIPT network-capture summary --json --out /tmp/cdp_summary.json  # 写出结构化摘要
$DAEMON $SCRIPT network-capture diff-body             # 分析写请求体与相关 GET 响应差异（⚡ 新增）
$DAEMON $SCRIPT network-capture infer-crud            # 推断 GET detail + PUT full payload 等流程（⚡ 新增）
$DAEMON $SCRIPT network-capture summary --filtered    # 基于上次 filter 结果摘要
$DAEMON $SCRIPT network-capture infer-crud --json     # JSON 输出，便于其他 skill 解析
$DAEMON $SCRIPT network-capture export                 # 导出为 Python requests 代码
$DAEMON $SCRIPT network-capture export --filtered      # 基于上次 filter 结果导出
$DAEMON $SCRIPT network-capture export --curl          # 导出为 curl 命令
$DAEMON $SCRIPT network-capture export --python-client # 导出完整可运行客户端（含 cookie 获取 + 认证头）（⚡ 新增）

# 动作级抓包：一条命令完成“开始抓包 → 执行动作 → 停止抓包 → 摘要/推断/导出”
$DAEMON $SCRIPT capture-action \
  --target "url:bdp-cn.tuya-inc.com/apps/develop" \
  --do "editor-set aaa" \
  --do "press Meta+S" \
  --filter-url datafactory/job \
  --body-mode filtered \
  --idle-ms 800 \
  --export-python-client

# capture-action 默认会给支持 target 的动作自动追加 --target；同一资源的整包 PUT 会输出并发写风险提示。
# 如果只是确认请求是否发出，优先用 --body-mode none 或 filtered，不要默认 all。

# 默认入口：先整段抓包，再决定是否需要回退到分阶段
$DAEMON $SCRIPT capture-flow start \
  --target tab:myapp \
  --goal "获取 metric 链路" \
  --filter-url /api/ \
  --idle-ms 800 \
  --body-mode filtered \
  --json

# 用户完成整段操作流后，agent 结束整段抓包并查看是否已经清晰
$DAEMON $SCRIPT capture-flow stop --session <session_id> --json
$DAEMON $SCRIPT capture-flow analyze --session <session_id> --json
$DAEMON $SCRIPT capture-flow export --session <session_id> --candidate-group 1 --python-client --json
$DAEMON $SCRIPT capture-flow abort --session <session_id> --json

# 如果 capture-flow 返回 unclear，再进入 agent 对话驱动的分阶段抓包（无交互 CLI）
# 1. agent 根据 recommended_phases 创建 capture-guide 会话，再把 next_prompt 转述给用户
$DAEMON $SCRIPT capture-guide start \
  --target tab:myapp \
  --step "点击按钮A" \
  --step "下拉框B选择xxx" \
  --step "勾选单选框C" \
  --step "点击底部按钮D" \
  --filter-url /api/ \
  --idle-ms 800 \
  --body-mode filtered \
  --json

# 2. 用户在浏览器手动做完当前步骤后，agent 调用 ack 推进
$DAEMON $SCRIPT capture-guide ack --session <session_id> --json

# 3. 其他会话操作
$DAEMON $SCRIPT capture-guide status --session <session_id> --json
$DAEMON $SCRIPT capture-guide skip --session <session_id> --json
$DAEMON $SCRIPT capture-guide retry --session <session_id> --json
$DAEMON $SCRIPT capture-guide retry --session <session_id> --previous --json
$DAEMON $SCRIPT capture-guide analyze --session <session_id> --json
$DAEMON $SCRIPT capture-guide export --session <session_id> --step 4 --python-client --json
$DAEMON $SCRIPT capture-guide export --session <session_id> --final-write-only --curl --json
$DAEMON $SCRIPT capture-guide abort --session <session_id> --json

# 页面上下文 fetch（自动带 cookie，绕过 CORS）
$DAEMON $SCRIPT network fetch "https://api.example.com/data"
$DAEMON $SCRIPT network fetch "https://api.example.com/data" --method POST --body '{"key":"val"}'
$DAEMON $SCRIPT network fetch "https://api.example.com/data" --headers '{"uid":"usr123","X-Token":"tok"}'  # 自定义 header（⚡ 新增）
$DAEMON $SCRIPT network fetch "https://api.example.com/data" --target "url:example"
# ⚠️ 注意：network fetch 在页面 JS 上下文执行，受浏览器 CORS 策略限制。
#   - 同域 API（如 Grafana 内部接口）：可用
#   - 跨域 API（需要 Access-Control-Allow-Origin）：可能被拦截
#   - 推荐做法：使用 cookies get + Python requests（不受 CORS 限制，支持 SSL verify=False）

# 17. localStorage / sessionStorage（⚡ 新增）
$DAEMON $SCRIPT local-storage get                              # 列出所有 localStorage 键值
$DAEMON $SCRIPT local-storage get "grafanaUserPreferences"     # 读取指定 key
$DAEMON $SCRIPT local-storage get "grafanaUserPreferences" --json  # JSON 格式化输出
$DAEMON $SCRIPT local-storage get "token" --session            # 读取 sessionStorage
$DAEMON $SCRIPT local-storage set "myKey" "myValue"            # 写入 localStorage
$DAEMON $SCRIPT local-storage remove "myKey"                   # 删除
# 典型场景：获取 Grafana deviceId、OAuth token、用户设置等非 cookie 认证信息

# 14. eval-js — 在页面上下文执行 JS（⚡ 新增）
$DAEMON $SCRIPT eval-js "document.title"                        # 获取页面标题（`eval` 为别名）
$DAEMON $SCRIPT eval-js 'document.querySelector("meta[name=csrf-token]")?.content'  # 获取 csrf-token
$DAEMON $SCRIPT eval-js "1 + 1"                                 # 算术表达式
$DAEMON $SCRIPT eval-js "fetch('/api').then(r=>r.json())" --await  # await Promise

# 15. capture-headers — 实时监听请求 header（⚡ 新增）
$DAEMON $SCRIPT capture-headers --wait 10                        # 监听 10 秒，输出所有请求 header
$DAEMON $SCRIPT capture-headers --url-filter "datafactory" --wait 15  # 只输出含 datafactory 的请求

# 16. scan-shortcuts — 扫描页面快捷键提示（⚡ 新增）
$DAEMON $SCRIPT scan-shortcuts                                   # 扫描全页可见快捷键
$DAEMON $SCRIPT scan-shortcuts --target "url:example"           # 指定页面

# 18. 页面渲染诊断（通用，不绑定具体平台）
$DAEMON $SCRIPT diagnose-page /tmp/page.png --target active --json
# 输出包含 screenshot、像素统计、可见文本块、数值文本块、空内容容器候选、图表候选 DOM 和资源尾部摘要。

# 19. 导出浏览器登录 cookie（给 requests/curl 复用）
$DAEMON $SCRIPT cookies get "https://example.com"               # 默认 JSON 输出 {name: value}
$DAEMON $SCRIPT cookies get "https://example.com" --json        # 显式 JSON
$DAEMON $SCRIPT cookies get "https://example.com" --header      # Cookie: a=1; b=2（直接用于 curl -H）
$DAEMON $SCRIPT cookies get "https://example.com" --raw         # 原始列表（含 domain/path/httpOnly）

# 重放抓包的请求
$DAEMON $SCRIPT network replay 1                       # 重放第 1 个请求
$DAEMON $SCRIPT network replay 3 --url "https://..."   # 重放第 3 个，覆盖 URL
$DAEMON $SCRIPT network replay 1 --method POST         # 覆盖 HTTP 方法
$DAEMON $SCRIPT network replay 1 --body '{"new":"data"}'  # 覆盖 body

# 14. 代码编辑器操作（Monaco / CodeMirror 5/6 / Ace Editor / textarea）
$DAEMON $SCRIPT editor-get                             # 读取编辑器内容（自动检测类型，输出 Type: monaco/codemirror5/codemirror6/ace/textarea）
$DAEMON $SCRIPT editor-set "SELECT * FROM t"           # 整段替换编辑器内容（Ace 优先走 JS API，其他走 insertText）
$DAEMON $SCRIPT editor-set " LIMIT 10" --append        # 追加到末尾
$DAEMON $SCRIPT editor-type "sel"                      # 逐字符输入（触发 autocomplete）
# 注意：DMS、StarRocks 等平台使用 Ace Editor，editor-get/set 已原生支持

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

# 18. 截图（视觉定位）
$DAEMON $SCRIPT screenshot /tmp/page.png               # 保存当前视口截图
$DAEMON $SCRIPT screenshot /tmp/page.png --full        # 保存整页截图
$DAEMON $SCRIPT screenshot /tmp/page.png --annotate    # 标注交互元素编号，[N] 对应 @eN，可直接继续 click @eN
$DAEMON $SCRIPT screenshot --annotate --target "url:github.com"  # 不传路径时保存到 /tmp/cdp_screenshot_*.png

# 指定操作目标页面（默认 active = 当前活动 tab）
$DAEMON $SCRIPT snapshot -i --target "url:github.com"
$DAEMON $SCRIPT click @e1 --target C7A52F06

# 批处理：一次 CLI 调用执行多步，减少 agent 往返和重复启动开销
$DAEMON $SCRIPT batch --bail \
  "snapshot -i -c" \
  "click @e1" \
  "snapshot -i -c --max-output 8000"

# JSON 批处理（stdin 或参数均可）
printf '[["status"],["snapshot","-i","-c","--json"]]' | $DAEMON $SCRIPT batch --json --bail
```

### 增强特性说明

- **snapshot label 关联**：input/select/textarea 元素在 snapshot 输出中自动显示关联的 label 文字（支持 `<label for>` / 祖先包裹 / `aria-labelledby` / Ant Design `.ant-form-item-label`）
- **文本匹配空格标准化**：`find-text` / `click-text` 自动将 `\u00a0`（`&nbsp;`）、全角空格等标准化为普通空格后匹配，解决 Ant Design 按钮中 "确 定" 类文字的匹配问题
- **searchable 下拉自动回退**：`select` 在自定义下拉中如果初次看不到候选，会自动探测搜索输入框并输入筛选词；也支持 `--search` / `search_text=` 显式覆盖
- **tab alias 精确绑定**：`open --alias foo`、`tab bind foo --target active` 后，可用 `--target tab:foo` 精确指向同一标签页，避免按标题/URL 模糊匹配误操作
- **CDP 新开页固定分组**：凡是 `open` 创建的新标签页，都会强制进入 Chrome 原生固定分组 `CDP自动化`；自定义 `--group` 只保留兼容并会被忽略，无法加入固定组时命令失败并关闭刚创建的新页
- **大 DOM 自动限流**：页面元素 >5000 时 snapshot 自动缩窄到内容根节点，防止超时
- **page_call 30s 超时保护**：单次 CDP 调用超时后自动使 session 失效，返回清晰错误提示
- **agent-browser 风格 token 优化**：`snapshot -c` 输出紧凑字段，`-d` 按 DOM 深度显式限流，`--max-output` 截断超长页面输出
- **增量 diff**：`diff snapshot` 对比当前快照与上次 `snapshot` baseline，仅输出变化，适合动作后验证
- **动作级抓包**：`capture-action` 将开始抓包、执行多条动作、停止抓包、摘要、CRUD 推断和导出客户端合并为一次调用
- **全流程优先抓包**：`capture-flow` 是默认入口，先整段监听并自动判断链路是否已经清晰
- **对话驱动分阶段抓包**：`capture-guide` 只在 `capture-flow` 判定不清晰时回退使用，不等待 stdin，也不要求用户直接面对 CLI
- **抓包分析**：`network-capture summary/diff-body/infer-crud` 可快速识别请求体关键字段、GET detail + PUT 整包回写模式，以及同资源并发写风险
- **标注截图**：`screenshot --annotate` 会在截图上叠加 `[N]` 编号，并输出 `[N] -> @eN` 图例，解决纯文本 snapshot 看不到的视觉问题
- **页面内容边界标记**：`--content-boundaries` 或 `CDP_DAEMON_CONTENT_BOUNDARIES=1` 会用 nonce 包裹页面来源内容，帮助 LLM 区分不可信页面文本与系统指令
- **批处理执行**：`batch` 支持命令字符串和 JSON stdin，可配合 `--bail` 在失败时中止
- **域名白名单**：`CDP_DAEMON_ALLOWED_DOMAINS="example.com,*.example.com"` 会限制 `open` 和 `network fetch` 的目标域名
- **动作策略**：`CDP_DAEMON_ACTION_POLICY=/path/policy.json` 支持 `{"default":"deny","allow":["snapshot","get-*"],"deny":["click","fill"]}` 这类静态策略
- **敏感动作确认**：`CDP_DAEMON_CONFIRM_ACTIONS="click,fill,eval-js"` 会要求显式确认；非交互环境默认拒绝，确认本次执行可在命令末尾加 `--yes`

## 典型场景示例

```bash
# 推荐流程：先整段抓包，再决定是否回退到 capture-guide
$DAEMON $SCRIPT capture-flow start --target tab:myapp --goal "获取 metric 链路" --json
# 用户完成整段操作
$DAEMON $SCRIPT capture-flow stop --session <session_id> --json

# 搜索框输入并搜索（一条命令）
$DAEMON $SCRIPT fill @e90 "llm" --submit

# 若整段抓包仍不清晰，再创建 capture-guide 会话，把 next_prompt 转述给用户；用户每完成一阶段，agent 再调用 ack
$DAEMON $SCRIPT capture-guide start --target tab:myapp --step "点击保存" --step "确认弹窗" --json
$DAEMON $SCRIPT capture-guide ack --session <session_id> --json
$DAEMON $SCRIPT capture-guide ack --session <session_id> --json
$DAEMON $SCRIPT capture-guide analyze --session <session_id> --json

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
select("@e4", "dwd_trade_topic", by_label=True, search_text="dwd")

# 滚动（CDP 鼠标滚轮，按坐标定位滚动区域）
scroll("down")                                    # 视口中心
scroll("down", at=(128, 446))                     # 指定坐标（如左侧栏）

# 网络抓包与 fetch
network_capture_start(target="active")            # 开始抓包
# ... 执行页面操作 ...
captured = network_capture_stop(
    target="active",
    body_mode="filtered",
    idle_ms=800,
    url_filter="/api/",
    max_bodies=6,
)  # 停止，按过滤条件拉少量 body

# 在页面上下文 fetch（自动带 cookie）
resp = network_fetch("https://api.example.com/data", method="GET")
print(resp["status"], resp["body"])

# 重放抓包的请求
replay = network_replay(index=1)                  # 重放第 1 个
print(replay["status"], replay["body"])

# Monaco/CodeMirror/Ace 编辑器（自动探测类型）
content = editor_get()                            # 返回 {ok, type, value, language}
editor_set("SELECT * FROM t1")                    # 整段替换（Ace 用 JS API，其他用 insertText）
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
# -c/--compact  只输出 ref/desc/value/checked/depth（配合 --json 最省 token）
# -d/--depth N  仅保留 DOM 深度 <= N 的交互元素
# -u/--urls     文本输出中附带 href；compact JSON 中保留 href
# --json      输出 JSON 格式
# --target    指定页面: active | targetId | url:keyword
# --max-output N          限制输出字符数，也可用 CDP_DAEMON_MAX_OUTPUT
# --content-boundaries    页面内容加边界标记，也可用 CDP_DAEMON_CONTENT_BOUNDARIES=1
```

## CLI 配置与安全护栏

配置加载优先级：`~/.chrome-cdp-daemon/config.json` < 当前目录 `chrome-cdp-daemon.json` / `cdp-daemon.json` < 环境变量 < 命令行参数。

```json
{
  "max_output": 50000,
  "content_boundaries": true,
  "allowed_domains": ["tuya-inc.com", "*.tuya-inc.com"],
  "action_policy": "/Users/luca/.chrome-cdp-daemon/policy.json",
  "confirm_actions": ["click", "fill", "eval-js"],
  "confirm_interactive": false,
  "default_instance": "chrome-profile-9222"
}
```

策略文件示例：

```json
{
  "default": "allow",
  "deny": ["cookies", "eval-js"],
  "allow": ["snapshot", "get-*", "find-*", "click-text"]
}
```

敏感动作确认示例：

```bash
# 非交互环境会拒绝
CDP_DAEMON_CONFIRM_ACTIONS=click $DAEMON $SCRIPT click @e1

# 本次显式放行
CDP_DAEMON_CONFIRM_ACTIONS=click $DAEMON $SCRIPT click @e1 --yes

# 仅在 TTY 中启用交互确认，必须输入 yes
CDP_DAEMON_CONFIRM_ACTIONS=click CDP_DAEMON_CONFIRM_INTERACTIVE=1 $DAEMON $SCRIPT click @e1
```

注意：`status`、`daemon_status()`、导入 SDK 仍不会启动 daemon，也不会连接 Chrome；这些增强只影响实际 CLI 操作阶段。

# 基础操作

## 方式1：直接 import client SDK

```python
import sys
sys.path.insert(0, "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
from cdp_client import get_cookies, cdp_call
from cdp_actions import list_pages, get_active_page

# 获取指定域名的 cookie（推荐：自动过滤，防止 Cookie 头过大）
cookies = get_cookies("https://bdp-cn.tuya-inc.com:7799")
# ⚠️ 警告：不要在业务请求中使用 get_all_cookies()！
# get_all_cookies() 返回全量 cookie（500~1000+ 条），直接拼接会导致 nginx 返回 400。
# 业务代码请始终使用 get_cookies(url) 按域名过滤。

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
... daemon.py test --instance chrome-profile-9222

# 多实例时先列出实例，再显式选择
... daemon.py instance list
... daemon.py instance list --json
... daemon.py status --instance chrome-profile-9222

# 实例选择优先级：--instance / CHROME_CDP_INSTANCE > config.json 的 default_instance > 唯一实例
# 同一个浏览器若被两种探测方式（DevToolsActivePort 文件 / 进程命令行）各登记一次，
# instance list 会按浏览器身份（ws_url，退化用 host:port）自动去重，保留有 pid 的一条为主，
# 另一条 instance_id 作为 alias 仍可显式选择，因此单浏览器场景不会再误报“检测到多个实例”。

# 查看状态
... daemon.py status

# 当前版本的 start/restart 会额外等待 CDP 真正 ready，并在后台预热连接；
# 正常情况下，后续首个 active-page/list-pages/network-capture 不再承担完整冷启动成本。

# 列出所有 tab
... daemon.py list-pages

# 获取活动 tab（macOS only）
... daemon.py active-page

# 页面渲染诊断：输出 DOM 文本块、空内容容器候选、canvas/svg 数量、截图像素白屏统计
... daemon.py diagnose-page /tmp/page_diagnose.png --target tab:page-under-test --wait-ms 3000 --json

# 停止 / 重启（安全方式：只杀 daemon PID，不影响 Chrome）
... daemon.py stop          # 停止 daemon（等价于 stop-daemon）
... daemon.py stop-daemon   # 同上，名称更明确
... daemon.py restart       # 重启（等价于 restart-daemon）
... daemon.py restart-daemon

# ⚠️ 警告：永远不要用 `lsof -ti :9222 | xargs kill -9` 来重启 daemon
# 9222 端口是 Chrome 的远程调试端口，上述命令会把 Chrome 本身杀掉
```

# 文件说明

- `scripts/daemon.py` — 守护进程（CDP 连接管理 + Unix Socket 服务 + CLI）
- `scripts/cdp_client.py` — 客户端 SDK（cookie / cdp_call / 高级操作请求封装）
- `scripts/cdp_actions.py` — 高层动作 SDK（其他 skill 直接 import）
- `scripts/page_manager.py` — 页面级操作管理器（session 管理 / 元素引用 / JS 注入 / 动作执行）

`diagnose-page` 用于通用页面渲染验证：当截图白屏但 DOM 有文本时，会提示优先检查 full-page 截图模式或遮罩层；当发现可见空容器、数值文本缺失或没有 canvas/svg 时，会输出通用候选信息供上层 skill 继续判断。

# 运行时文件

- `~/.chrome-cdp-daemon/cdp.sock` — Unix Socket 通信文件
- `~/.chrome-cdp-daemon/cdp.pid` — daemon PID 文件
- `~/.chrome-cdp-daemon/cdp.log` — daemon 运行日志
