# Chrome CDP Daemon 回归自验用例

本文档记录本 skill 的手工回归测试场景。每次改动 `scripts/daemon.py` 或 `scripts/cdp_client.py` 后，可按需执行这些用例，避免再次出现自动弹窗、重复授权、旧 daemon 残留等问题。

## 基础约定

- 默认不允许仅因加载 skill、导入 `cdp_client.py`、查询状态而启动 daemon 或连接 Chrome。
- 只有用户明确要求 CDP 连接、cookie 读取、浏览器操作，或显式执行 `daemon.py test/start` 时，才允许连接 Chrome。
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
- 如果存在 `/Users/luca/.cc-switch/skills/tuya-bigdata/scripts/cdp_daemon.py start` 等旧进程，应先停止，否则会独立触发 Chrome 授权弹窗。

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
