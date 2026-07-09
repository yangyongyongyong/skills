# Chrome CDP Daemon 回归自验用例

本文档记录本 skill 的手工回归测试场景。每次改动 `scripts/daemon.py` 或 `scripts/cdp_client.py` 后，可按需执行这些用例，避免再次出现自动弹窗、重复授权、旧 daemon 残留等问题。

## 基础约定

- 默认不允许仅因加载 skill、导入 `cdp_client.py`、查询状态而启动 daemon 或连接 Chrome。
- 只有用户明确要求 CDP 连接、cookie 读取、浏览器操作，或显式执行 `daemon.py test/start` 时，才允许连接 Chrome。
- 通用鉴权能力只汇总 cookie、storage 和抓包 header，不内置业务域名、token 接口或 header 名。
- `connection_session_id` 用于判断是否复用同一条 CDP WebSocket 连接：同一连接不变，重连后变化。
- `cdp_mode` 用于区分连接来源：`scheme1_devtools_active_port` 表示 `chrome://inspect/#remote-debugging` / `DevToolsActivePort`，`scheme2_remote_debugging_port` 表示 `--remote-debugging-port`。

## 用例 1：加载/导入不自动启动 daemon

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

期望：

- `daemon_status()` 返回 `{'running': False}`。
- 没有 daemon 进程。
- 没有 `osascript` watcher。
- Chrome 不弹授权窗口。

## 用例 2：显式首次连接

目的：用户明确要求 CDP 测试时，允许启动 daemon 并建立连接。

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py test
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py status
```

期望：

- `Test` 返回 `ok=true`。
- `status` 返回 `ws_connected=true`。
- 返回 `cdp_mode`。
- 返回非空 `connection_session_id`。
- 如 Chrome 弹授权框，AppleScript 自动点击后不应堆叠多个弹窗。

## 用例 3：同一 daemon 连接复用

目的：已连接后再次调用不应重新弹窗，且应复用同一连接。

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

期望：

- `status1.connection_session_id == status2.connection_session_id`。
- `reconnect_count` 不增加。
- 没有新授权弹窗。
- 没有新的 `osascript` watcher 长时间残留。

## 用例 4：关闭 Chrome 授权后的已连接复用

目的：验证用户关闭/撤销授权弹窗后，当前已建立 WebSocket 是否仍可用。

步骤：

1. 先执行用例 2，确保 daemon 已连接并记录 `connection_session_id`。
2. 用户在 Chrome 授权弹窗或相关授权 UI 中关闭授权。
3. 执行：

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py test
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py status
```

期望：

- 如果当前 WebSocket 未被 Chrome 主动断开，`connection_session_id` 保持不变，说明仍在复用旧连接。
- 如果 Chrome 主动断开旧 WebSocket，下一次显式 CDP 操作才允许重连，且 `connection_session_id` 应变化。
- 心跳线程不得在用户空闲聊天期间主动重连或弹授权。
- 失败时不得连续堆叠多个授权弹窗。

当前实测记录：关闭授权后，当前已建立 WebSocket 未失效，`connection_session_id` 保持 `fbe40527b5224c108528e44af1a03228`，`reconnect_count=0`。

## 用例 5：Chrome 重启后不因空闲心跳自动弹窗

目的：确认 Chrome 重启后，daemon 心跳只标记断线，不主动重连。

步骤：

1. daemon 已连接。
2. 重启 Chrome。
3. 不执行 `test/cdp_call/get_cookies`，等待超过 30 秒。
4. 执行：

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py status
```

期望：

- 不应因心跳自动弹授权窗。
- `status` 可显示 `ws_connected=false` 或 `last_error`，但不得主动重连 Chrome。
- 只有后续显式执行 `daemon.py test` 或业务 CDP 调用时，才允许重新连接。

## 用例 6：授权弹窗自动点击自验

目的：确认 AppleScript 点击授权后，会验证弹窗是否消失。

```bash
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py auth-click-test 3
```

期望：

- 没有弹窗时返回 `not_found`。
- 成功点击且弹窗消失时返回 `pressed_and_gone ...`。
- 若返回 `pressed_but_still_present ...` 或 `found_remote_debug_sheet_without_buttons`，说明 Chrome sheet 可能已进入异常/僵尸状态，应停止 daemon/watcher，并由用户手动关闭或重启 Chrome。

## 用例 7：旧 daemon 残留排查

目的：避免其他 skill 的旧 CDP daemon 持续触发授权弹窗。

```bash
ps -ef | rg -i "chrome-cdp-ws-daemon/scripts/daemon.py|tuya-bigdata/scripts/cdp_daemon.py|osascript -e" | rg -v "rg -i" || true
lsof -nP -iTCP:9222 2>/dev/null || true
```

期望：

- 除当前预期的 `chrome-cdp-ws-daemon ... __run_daemon__` 外，不应有其他 CDP daemon。
- 不应有长期残留的 `osascript -e` watcher。
- 如果存在 `/Users/luca/.cursor/skills/tuya-bigdata/scripts/cdp_daemon.py start` 等旧进程，应先停止，否则会独立触发 Chrome 授权弹窗。

## 用例 8：活动页识别

目的：验证 `active-page` 命令返回的 url/title 与浏览器当前 active tab 一致。

```bash
# 1. daemon 未运行时应报错而不是自动拉起
pkill -f "chrome-cdp-ws-daemon/scripts/daemon.py" 2>/dev/null || true
rm -f ~/.chrome-cdp-daemon/cdp.sock ~/.chrome-cdp-daemon/cdp.pid
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py active-page
# 期望：打印 "CDP daemon not running"，退出码 1

# 2. 启动 daemon
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py test

# 3. list-pages 列出所有 tab
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py list-pages

# 4. active-page 返回与浏览器当前 tab 一致
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py active-page

# 5. 切换到另一个 Chrome tab 后再次调用，结果应更新
python3 /Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py active-page

# 6. SDK 调用
python3 - <<'PY'
import sys
sys.path.insert(0, '/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts')
from cdp_actions import list_pages, get_active_page
print('pages:', len(list_pages()))
print('active:', get_active_page())
PY
```

期望：

- 步骤 1：daemon 未运行时 CLI 直接报错，不拉起 daemon，不弹 Chrome 授权窗口。
- 步骤 3：`list-pages` 输出包含当前浏览器所有打开的 tab（url + title + targetId 前 8 位）。
- 步骤 4：`active-page` 输出的 `url` 与浏览器 frontmost 窗口 active tab 的 url 一致。
- 步骤 5：切换 tab 后再次调用，url/title 实时更新（验证 AppleScript 路径准确）。
- 步骤 6：SDK 调用 `get_active_page()` 结果与 CLI 一致，且 `targetId` 非空。
- 全程：不出现新的 Chrome 授权弹窗，`reconnect_count` 不增加。

当授权弹窗堆叠或出现僵尸 sheet 时，先停止所有触发源：

```bash
pkill -f "chrome-cdp-ws-daemon/scripts/daemon.py" 2>/dev/null || true
pkill -f "tuya-bigdata/scripts/cdp_daemon.py" 2>/dev/null || true
pkill -f "osascript -e" 2>/dev/null || true
rm -f ~/.chrome-cdp-daemon/cdp.sock ~/.chrome-cdp-daemon/cdp.pid
```

如果 Chrome 授权 sheet 仍无法关闭，重启 Chrome 后再继续测试。

## 用例 9：agent-browser 风格 CLI 增强回归

目的：验证 `snapshot` token 优化、内容边界、批处理、域名白名单与敏感动作确认。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 1. 重启 daemon，确保加载当前代码
$PY "$SCRIPT" restart
$PY "$SCRIPT" test

# 2. 打开独立测试页，避免 active tab 的大 DOM 干扰
open_out=$($PY "$SCRIPT" open https://example.com/ --wait 1000)
printf '%s\n' "$open_out"
target=$(printf '%s\n' "$open_out" | awk '/Opened:/ {print $2; exit}')

# 3. 基础页面信息
$PY "$SCRIPT" get-url --target "$target"
$PY "$SCRIPT" get-title --target "$target"

# 4. snapshot token 优化
$PY "$SCRIPT" snapshot -i --target "$target" --max-output 1200
$PY "$SCRIPT" snapshot -i -c --json --target "$target" --max-output 1000
$PY "$SCRIPT" snapshot -i -d 4 --json --target "$target" --max-output 1000
$PY "$SCRIPT" snapshot -i -u --target "$target" --max-output 1200

# 5. 页面内容边界标记
$PY "$SCRIPT" snapshot -i --content-boundaries --target "$target" --max-output 800

# 6. JSON 批处理
printf '[["get-url","--target","%s"],["snapshot","-i","-c","--json","--target","%s","--max-output","800"]]' "$target" "$target" \
  | $PY "$SCRIPT" batch --json --bail

# 7. 域名白名单应拒绝非白名单 open
CDP_DAEMON_ALLOWED_DOMAINS=example.com $PY "$SCRIPT" open https://evil.com

# 8. 敏感动作确认：非交互环境应拒绝
CDP_DAEMON_CONFIRM_ACTIONS=click $PY "$SCRIPT" click @e1 --target "$target"

# 9. 清理测试 tab，daemon 应继续运行
$PY "$SCRIPT" close "$target"
$PY "$SCRIPT" status
```

期望：

- `test` 返回 `ok=true`，并显示 Chrome 版本与 `connection_session_id`。
- `open https://example.com/ --wait 1000` 返回 URL 不应停留在 `about:blank`。
- 普通 `snapshot -i` 输出 `@e1 [a] "Learn more"`。
- `snapshot -i -c --json` 只保留紧凑字段，如 `desc/depth/ref`。
- `snapshot -i -d 4` 在 example.com 上返回 0 个元素，证明 depth 过滤生效。
- `snapshot -i -u` 输出 `href=https://iana.org/domains/example`。
- `--content-boundaries` 输出 `CDP_DAEMON_PAGE_CONTENT` 与 `END_CDP_DAEMON_PAGE_CONTENT`，nonce 一致。
- `batch --json --bail` 输出合法 JSON，且两个子命令 `code=0`。
- 域名白名单返回 `domain 'evil.com' is not in allowed_domains: example.com`。
- 敏感动作确认返回 `action 'click' requires confirmation`。
- `close` 后 `status` 仍显示 `Running`，不应留下 socket/pid 丢失的 orphan daemon。

## 用例 10：orphan daemon 自愈

目的：验证 `stop/restart` 能处理 pid/socket 文件丢失但 daemon 进程仍存活的异常状态。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

$PY "$SCRIPT" start
pid=$(pgrep -f "chrome-cdp-ws-daemon/.*/daemon.py __run_daemon__" | head -1)
rm -f ~/.chrome-cdp-daemon/cdp.sock ~/.chrome-cdp-daemon/cdp.pid

$PY "$SCRIPT" status
$PY "$SCRIPT" restart
$PY "$SCRIPT" status
```

期望：

- socket/pid 文件丢失时，`status` 能提示 orphan daemon PID，而不是误报健康。
- `restart` 只终止本 skill 的 orphan daemon，不影响 Chrome。
- `restart` 后重新生成 `cdp.sock/cdp.pid`，`status` 返回 `Running`。

## 用例 11：diff snapshot 与标注截图

目的：验证增量快照 diff 和视觉标注截图能力。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

$PY "$SCRIPT" restart
$PY "$SCRIPT" test

open_out=$($PY "$SCRIPT" open https://example.com/ --wait 1000)
target=$(printf '%s\n' "$open_out" | awk '/Opened:/ {print $2; exit}')

# 第一次 snapshot 会保存 baseline
$PY "$SCRIPT" snapshot -i --target "$target" --max-output 1000

# 页面未变化时应无 diff
$PY "$SCRIPT" diff snapshot --target "$target" --max-output 1000

# 参数变化时应输出 unified diff
$PY "$SCRIPT" diff snapshot --target "$target" -u --max-output 1000

# 标注截图，输出路径与 [N] -> @eN 图例
shot=/tmp/cdp_example_annotated.png
rm -f "$shot"
$PY "$SCRIPT" screenshot "$shot" --annotate --target "$target"
ls -lh "$shot"

$PY "$SCRIPT" close "$target"
$PY "$SCRIPT" status
```

期望：

- `diff snapshot` 在无变化时输出 `No snapshot changes.`。
- `diff snapshot -u` 输出 `--- previous-snapshot` / `+++ current-snapshot` 的 unified diff。
- `screenshot --annotate` 保存 PNG 文件，并输出类似 `[1] @e1 [a] "Learn more"` 的图例。
- 截图后 `@eN` 引用仍可继续用于交互，直到页面变化或引用过期。
- 清理测试 tab 后 daemon 仍为 `Running`。

## 用例 12：searchable 下拉回归

目的：验证 `select` 能处理“先输入字符再出现候选”的动态下拉，如 BDP 建表页的“所属数据库”。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 先自行打开目标页面并拿到对应 @ref，这里只验证命令形态
$PY "$SCRIPT" snapshot -i -c --target active --max-output 3000

# 显式给筛选词
$PY "$SCRIPT" select @e_db "dwd_trade_topic" --by label --search dwd

# 不给 --search 时，组件支持搜索的话会自动回退为输入目标值本身
$PY "$SCRIPT" select @e_db "dwd_trade_topic" --by label
```

期望：

- 初次打开下拉看不到目标候选时，`select` 会自动探测搜索输入框并输入筛选词。
- 返回结果中 `searched=true` 时，表示走过 searchable 回退路径。
- 对标准 Hive DDL 的建表流程，应优先走页面顶部“导入sql”自动解析；分区字段无需再手工删除字段并补录 `dt`。

## 用例 13：tab alias 绑定回归

目的：验证打开或当前激活页可绑定 alias，后续动作用 `tab:alias` 精确命中同一标签页。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

$PY "$SCRIPT" open "https://example.com" --alias ex1 --wait 1000
$PY "$SCRIPT" tab get ex1
$PY "$SCRIPT" tab list
$PY "$SCRIPT" get-url --target tab:ex1
$PY "$SCRIPT" close tab:ex1
$PY "$SCRIPT" tab list
```

期望：

- `open --alias ex1` 返回 `Opened: <targetId> ... [alias=tab:ex1]`。
- `tab get ex1` 返回稳定的 `target_id/url/title`。
- 后续所有支持 `--target` 的动作都能用 `tab:ex1` 精确指定页面。
- 关闭页面后，对应 alias 会自动移除，不再残留到失效 target。

## 用例 14：CDP 新开页强制分组

目的：验证所有通过 `open` 创建的标签页都会强制进入固定 Chrome 分组 `CDP自动化`，自定义分组会被忽略。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

$PY "$SCRIPT" open "https://example.com" --alias auto-group-1 --wait 1000
$PY "$SCRIPT" open "https://example.com" --group ignored-name --alias auto-group-2 --wait 1000
$PY "$SCRIPT" group list
$PY "$SCRIPT" close tab:auto-group-1
$PY "$SCRIPT" close tab:auto-group-2
```

期望：

- `open` 输出中包含 `→ group 'CDP自动化'`。
- 传入 `--group ignored-name` 时输出 `[ignored-group=ignored-name]`，实际仍进入 `CDP自动化`。
- `group list` 能看到固定分组 `CDP自动化`，且新开的 tab 已加入该组。
- 如果无法加入固定组，`open` 会失败并关闭刚创建的新页，避免留下未分组的自动化 tab。

## 用例 15：capture-guide 对话驱动手动抓包

目的：验证 `capture-guide` 适合 skill/agent 多轮对话驱动，不要求用户直接在 CLI 中按快捷键。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 1. agent 创建会话，把 next_prompt 转述给用户
$PY "$SCRIPT" capture-guide start \
  --target tab:myapp \
  --step "点击按钮A" \
  --step "下拉框B选择xxx" \
  --step "点击底部按钮D" \
  --filter-url /api/ \
  --idle-ms 800 \
  --body-mode filtered \
  --json

# 2. 用户在浏览器完成当前步骤后，agent 调用 ack 推进
$PY "$SCRIPT" capture-guide ack --session <session_id> --json
$PY "$SCRIPT" capture-guide status --session <session_id> --json

# 3. 全流程完成后做分析与导出
$PY "$SCRIPT" capture-guide analyze --session <session_id> --json
$PY "$SCRIPT" capture-guide export --session <session_id> --final-write-only --python-client --json
```

期望：

- `start` 返回固定结构化 JSON，包含 `session_id/current_step/next_prompt/baseline_summary`。
- `ack` 不读 stdin，只依赖当前会话状态推进到下一步；若当前是最后一步，会自动停止抓包并返回 `capture_file/filtered_capture_file/summary/crud`。
- `status` 能在任意时刻返回当前步骤、最近一步摘要和下一条提示。
- `capture-guide` 最终仍会落 `/tmp/cdp_network_capture.json` 与 `/tmp/cdp_network_capture_filtered.json`，旧的 `network-capture summary/export` 可以继续复用。
- 该模式的交互应由 agent 对话驱动，CLI 本身不要求用户按回车、输入 `r/q` 等快捷键。

## 用例 16：capture-flow 全流程优先，必要时回退到阶段化抓包

目的：验证新的默认策略是“先整段抓包，再判断是否需要回退到 `capture-guide`”。

```bash
PY=/Users/luca/miniforge3/envs/py311/bin/python
SCRIPT=/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

# 1. agent 开始整段抓包
$PY "$SCRIPT" capture-flow start \
  --target tab:myapp \
  --goal "获取 metric 链路" \
  --filter-url /api/ \
  --idle-ms 800 \
  --body-mode filtered \
  --json

# 2. 用户自行完成一整段页面操作流

# 3. agent 停止整段抓包并查看分析结果
$PY "$SCRIPT" capture-flow stop --session <session_id> --json
$PY "$SCRIPT" capture-flow analyze --session <session_id> --json
$PY "$SCRIPT" capture-flow export --session <session_id> --candidate-group 1 --python-client --json
```

期望：

- `capture-flow start` 返回固定结构化 JSON，包含 `session_id/goal/target/baseline_summary/message`。
- `capture-flow stop` 返回 `clarity_status`、`candidate_requests`、`candidate_groups`、`recommended_next_action`。
- 当整段流程已经能看出核心链路时，`clarity_status=clear`，agent 直接使用 `candidate_requests/candidate_groups` 即可。
- 当整段流程没有新增请求时，返回 `clear_no_network`，明确告诉 agent 这大概率是纯前端行为。
- 当候选链路混杂时，返回 `clarity_status=unclear`，并产出 `recommended_phases`，由 agent 再创建 `capture-guide` 会话进行分阶段抓包。
- `capture-flow` 最终仍会落 `/tmp/cdp_network_capture.json` 与 `/tmp/cdp_network_capture_filtered.json`，并且 `status/analyze/export` 在 daemon 不在线时也能读取已完成会话。
