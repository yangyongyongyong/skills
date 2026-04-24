# CDP 静默后台操作完全指南

> 目标：通过 Chrome DevTools Protocol (CDP) 实现**完全后台、不影响用户**的浏览器自动化操作。

---

## 一、CDP 能做什么

> 所有操作均复用同一 WebSocket 连接，格式：`ws.send({"id": N, "method": "Domain.method", "params": {...}})`

| 能力分类 | 典型操作 | 简易调用示例（一行） |
|---------|---------|-------------------|
| 页面导航 | 跳转 URL | `{"method":"Page.navigate","params":{"url":"https://example.com"}}` |
| 页面截图 | 截 PNG | `{"method":"Page.captureScreenshot","params":{"format":"png"}}` |
| PDF 打印 | 输出 PDF | `{"method":"Page.printToPDF","params":{"printBackground":true}}` |
| JS 执行 | 执行脚本 | `{"method":"Runtime.evaluate","params":{"expression":"document.title"}}` |
| DOM 查询 | 找节点 | `{"method":"DOM.querySelector","params":{"nodeId":1,"selector":"#btn"}}` |
| DOM 修改 | 改属性 | `{"method":"DOM.setAttributeValue","params":{"nodeId":5,"name":"value","value":"hi"}}` |
| Cookie 读取 | 拿 Cookie | `{"method":"Network.getCookies","params":{"urls":["https://example.com"]}}` |
| Cookie 写入 | 设 Cookie | `{"method":"Network.setCookie","params":{"name":"k","value":"v","domain":".example.com"}}` |
| 请求拦截 | 开启拦截 | `{"method":"Fetch.enable","params":{"patterns":[{"urlPattern":"*/api/*"}]}}` |
| 请求放行 | 继续请求 | `{"method":"Fetch.continueRequest","params":{"requestId":"xxx"}}` |
| 请求伪造 | 返回假数据 | `{"method":"Fetch.fulfillRequest","params":{"requestId":"xxx","responseCode":200,"body":"e30="}}` |
| 鼠标点击 | 点坐标 | `{"method":"Input.dispatchMouseEvent","params":{"type":"mousePressed","x":100,"y":200,"button":"left","clickCount":1}}` |
| 键盘输入 | 输字符 | `{"method":"Input.dispatchKeyEvent","params":{"type":"char","text":"a"}}` |
| 文件上传 | 设文件 | `{"method":"DOM.setFileInputFiles","params":{"nodeId":9,"files":["/tmp/a.txt"]}}` |
| 设备模拟 | 改分辨率 | `{"method":"Emulation.setDeviceMetricsOverride","params":{"width":375,"height":812,"deviceScaleFactor":2,"mobile":true}}` |
| UA 模拟 | 改 UA | `{"method":"Emulation.setUserAgentOverride","params":{"userAgent":"Mozilla/5.0 (iPhone)"}}` |
| 地理位置 | 改坐标 | `{"method":"Emulation.setGeolocationOverride","params":{"latitude":31.2,"longitude":121.5}}` |
| 清除缓存 | 清缓存 | `{"method":"Network.clearBrowserCache","params":{}}` |
| 断网模拟 | 模拟离线 | `{"method":"Network.emulateNetworkConditions","params":{"offline":true,"latency":0,"downloadThroughput":0,"uploadThroughput":0}}` |
| 性能指标 | 拿指标 | `{"method":"Performance.getMetrics","params":{}}` |
| **当前活动页** | **获取 selected Tab** | `Runtime.evaluate(document.visibilityState + hasFocus)`（或事件缓存）→ 见「获取当前活动页专栏」 |
| 多 Tab 列表 | 列所有 Tab | `{"method":"Target.getTargets","params":{}}` |
| 新开 Tab | 创建 Tab | `{"method":"Target.createTarget","params":{"url":"about:blank"}}` |
| 关闭 Tab | 关 Tab | `{"method":"Target.closeTarget","params":{"targetId":"xxx"}}` |
| 对话框处理 | 自动确认 | `{"method":"Page.handleJavaScriptDialog","params":{"accept":true}}` |
| **页面结构探索** | **列所有 iframe** | `{"method":"Runtime.evaluate","params":{"expression":"[...document.querySelectorAll('iframe')].map(f=>f.src)"}}` → 见「页面探索专栏」 |
| **网络接口探索** | **监听所有 XHR** | `{"method":"Network.enable","params":{}}` → 监听 `requestWillBeSent` 事件 → 见「页面探索专栏」 |
| **WebSocket 探索** | **捕获 WS 消息** | `{"method":"Network.enable","params":{}}` → 监听 `webSocketFrameSent/Received` 事件 → 见「页面探索专栏」 |

---

## 一补、获取用户当前活动页（Selected Tab）专栏

> **结论先行**：`Target.getTargets` 和 `/json/list` 的顺序都不等于“用户当前选中 Tab”，不应作为活动页判定依据。要“快且准”，请用事件驱动缓存；按需查询时用 `visibilityState + hasFocus` 双条件。

### 方案对比（修订）

| 方案 | 原理 | 速度 | 准确度 | 需要条件 | 平台 |
|------|------|------|--------|---------|------|
| 1. 事件驱动缓存（推荐） | 监听 target 生命周期 + 页内 `visibilitychange/focus/blur`，实时更新活动页 | 最快 | 高 | CDP 长连接 | 全平台 |
| 2. 按需快照查询（推荐） | 遍历 page target，执行 JS 检查 `visibilityState` 与 `hasFocus` | 中 | 高 | CDP 端口 | 全平台 |
| 3. AppleScript（非纯 CDP） | 系统 API 读取前台窗口 active tab | 快 | 最高 | macOS 权限 | 仅 macOS |
| 4. Chrome Extension（非纯 CDP） | `chrome.tabs.query({active:true,currentWindow:true})` | 快 | 最高 | 安装扩展 | 全平台 |

### 反例声明（请勿使用）

- `Target.getTargets` 第一个 page 并不保证是当前选中页。
- `/json/list` 第一个 page 也不保证是当前选中页。
- `attached == true` 只能说明会话附着，不代表用户正看该页。

### 方案 1：事件驱动缓存（纯 CDP，速度与准确度最佳）

思路：启动时全量扫描一次，后续靠事件实时更新，不需要每次全量遍历。

```python
import json
import websocket

def is_active_from_state(state: str, has_focus: bool) -> bool:
    """根据页面可见性和焦点状态判断当前 target 是否为活动页。"""
    return state == "visible" and has_focus is True

# 说明：
# 1) 通过 Target.setDiscoverTargets + Target.setAutoAttach(flatten=true) 监听 page target 变化
# 2) 在每个 page 注入监听：
#    document.addEventListener('visibilitychange', report)
#    window.addEventListener('focus', report)
#    window.addEventListener('blur', report)
# 3) report 上报 document.visibilityState + document.hasFocus() 到本地缓存
# 4) 本地缓存中满足 is_active_from_state(...) 的 target 即“当前活动页”
```

### 方案 2：按需快照查询（纯 CDP，易落地）

当你没有常驻进程时，临时查询一次可以用这个方案。

```python
import json, urllib.request, websocket

def get_active_tab_snapshot(cdp_port: int = 9222) -> dict | None:
    """
    按需查询当前活动页：
    1) 枚举所有 page target
    2) 对每个页面执行 JS，检查 visibilityState 与 hasFocus
    3) 返回命中的 target 基础信息
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{cdp_port}/json/list") as f:
        targets = json.loads(f.read())

    pages = [t for t in targets if t.get("type") == "page" and t.get("webSocketDebuggerUrl")]
    expr = "JSON.stringify({state: document.visibilityState, focus: document.hasFocus()})"

    for target in pages:
        ws = websocket.create_connection(target["webSocketDebuggerUrl"], suppress_origin=True, timeout=3)
        try:
            ws.send(json.dumps({
                "id": 1,
                "method": "Runtime.evaluate",
                "params": {"expression": expr}
            }))
            resp = json.loads(ws.recv())
            raw = resp.get("result", {}).get("result", {}).get("value", "{}")
            info = json.loads(raw)
            if info.get("state") == "visible" and info.get("focus") is True:
                return {
                    "url": target.get("url"),
                    "title": target.get("title"),
                    "targetId": target.get("id"),
                    "wsUrl": target.get("webSocketDebuggerUrl"),
                }
        except Exception:
            pass
        finally:
            ws.close()

    return None
```

### 补充说明

- 浏览器最小化或失焦时，可能所有页都不是 `visible + focus=true`，应允许返回 `None`。
- 生产建议使用“方案 1 + 方案 2 校准”的混合模式：平时事件缓存，异常时全量快照纠偏。

## 一补2、CDP 探索页面结构与接口专栏

> **核心用途**：拿到一个未知内网页面，用 CDP 系统性地摸清它的 DOM 结构、网络接口、WebSocket 通信协议，最终实现自动化操控——这正是 `jumpserver-terminal` skill 的诞生过程。

---

### 探索思路总览

```
未知页面
  │
  ├─ Step 1：DOM 结构探索
  │     ├── 列出所有 iframe（嵌套页面）
  │     ├── 找关键 DOM 节点（编辑器/终端/按钮）
  │     └── 附加到 iframe 的独立 Target
  │
  ├─ Step 2：网络接口探索
  │     ├── 监听所有 XHR/Fetch 请求
  │     ├── 捕获请求 URL + Header + Body
  │     └── 整理成可 replay 的接口文档
  │
  ├─ Step 3：WebSocket 协议探索
  │     ├── 找到页面建立的 WebSocket 连接
  │     ├── 监听双向消息帧
  │     └── 分析协议格式，找到输入/输出消息结构
  │
  └─ Step 4：自动化操控
        ├── JS 注入直接写 WebSocket（终端类）
        ├── JS 注入操作编辑器对象（Monaco/xterm）
        └── 直接 replay HTTP 接口（API 类）
```

---

### Step 1：DOM 结构探索

#### 1.1 列出所有 iframe（找嵌套子页面）

```python
import json, websocket, urllib.request

def explore_iframes(ws_url: str) -> list[dict]:
    """列出页面所有 iframe 的 src、id、class，找到关键嵌套页面。"""
    ws = websocket.create_connection(ws_url, suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {
        "expression": """
JSON.stringify([...document.querySelectorAll('iframe')].map((f, i) => ({
    index: i,
    src: f.src,
    id: f.id,
    className: f.className,
    width: f.offsetWidth,
    height: f.offsetHeight,
})))
"""
    }}))
    resp = json.loads(ws.recv())
    ws.close()
    return json.loads(resp["result"]["result"]["value"])

# JumpServer 示例输出：
# [{"index":0,"src":"https://js.example.com/luna/","id":"","className":"koko-iframe","width":1200,"height":800}]
```

#### 1.2 附加到 iframe 的独立 Target（进入子页面上下文）

iframe 在 CDP 中是独立的 `Target`，需要单独 attach 才能操作其内部 DOM/JS：

```python
def attach_to_iframe_target(cdp_port=9222) -> list[dict]:
    """
    列出所有 Target（含 iframe），找到嵌套页面的 webSocketDebuggerUrl。
    iframe 的 type 通常也是 'page'，但 URL 与主页面不同。
    """
    with urllib.request.urlopen(f"http://127.0.0.1:{cdp_port}/json/list") as f:
        return json.loads(f.read())
    # 筛选：[t for t in targets if 'luna' in t.get('url','')]
```

#### 1.3 JS 探索关键 DOM 节点（编辑器/终端/按钮）

```python
def find_key_elements(ws_url: str) -> str:
    """用 JS 在页面里找可能的编辑器、终端、输入框、按钮。"""
    ws = websocket.create_connection(ws_url, suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {
        "expression": """
JSON.stringify({
    // 终端类
    xterm:   !!window.Terminal || !!document.querySelector('.xterm'),
    xtermWs: window.__xtermWs ? 'found' : 'not found',

    // 编辑器类
    monaco:  !!window.monaco,
    ace:     !!window.ace,
    cm:      !!window.CodeMirror,

    // 关键元素
    iframes: document.querySelectorAll('iframe').length,
    inputs:  [...document.querySelectorAll('input')].map(i=>({type:i.type,id:i.id,name:i.name})),
    buttons: [...document.querySelectorAll('button')].map(b=>b.innerText.trim()).filter(Boolean).slice(0,10),

    // 全局对象（找暴露的 API 入口）
    globalKeys: Object.keys(window).filter(k =>
        !['chrome','location','history','document','navigator','performance'].includes(k)
        && typeof window[k] === 'object' && window[k] !== null
    ).slice(0, 20),
})
"""
    }}))
    resp = json.loads(ws.recv())
    ws.close()
    return resp["result"]["result"]["value"]
```

---

### Step 2：网络接口探索（监听 XHR/Fetch）

```python
import json, websocket, urllib.request, threading, time

def sniff_network_requests(ws_url: str, duration_seconds=15) -> list[dict]:
    """
    监听页面在指定时间内发出的所有网络请求。
    在此期间手动在页面操作一次目标功能，即可捕获对应接口。
    """
    captured = []
    ws = websocket.create_connection(ws_url, suppress_origin=True)

    # 开启网络监听
    ws.send(json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
    ws.recv()

    print(f"[*] 开始监听 {duration_seconds}s，请在浏览器中操作目标功能...")
    deadline = time.time() + duration_seconds
    msg_id = 2

    while time.time() < deadline:
        ws.settimeout(1.0)
        try:
            msg = json.loads(ws.recv())
        except Exception:
            continue

        if msg.get("method") == "Network.requestWillBeSent":
            req = msg["params"]["request"]
            captured.append({
                "url":     req["url"],
                "method":  req["method"],
                "headers": dict(list(req.get("headers", {}).items())[:5]),  # 只取前5个header
                "body":    req.get("postData", "")[:500],  # body 截断
            })
            print(f"  → {req['method']} {req['url'][:80]}")

    ws.close()
    return captured

# 典型输出示例（JumpServer 登录后的接口）：
# → POST https://js.example.com/api/v1/authentication/login/
# → GET  https://js.example.com/api/v1/assets/hosts/?limit=20
# → GET  https://js.example.com/luna/   (iframe 加载)
```

---

### Step 3：WebSocket 协议探索（最关键——终端/实时系统的核心）

许多内网系统（JumpServer 终端、JupyterLab、在线 IDE）的核心交互不走 HTTP，而是走 **WebSocket**。CDP 可以完整捕获所有 WS 帧。

```python
def sniff_websocket_messages(ws_url: str, duration_seconds=30) -> list[dict]:
    """
    监听页面建立的所有 WebSocket 连接及其消息帧。
    在此期间在终端里输入一条命令，即可看到对应的协议格式。
    """
    captured = []
    ws_connections = {}  # requestId -> url

    ws = websocket.create_connection(ws_url, suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
    ws.recv()

    print(f"[*] 监听 WebSocket {duration_seconds}s，请在页面中操作...")
    deadline = time.time() + duration_seconds

    while time.time() < deadline:
        ws.settimeout(1.0)
        try:
            msg = json.loads(ws.recv())
        except Exception:
            continue

        method = msg.get("method", "")
        params = msg.get("params", {})

        if method == "Network.webSocketCreated":
            req_id = params.get("requestId")
            url = params.get("url", "")
            ws_connections[req_id] = url
            print(f"  [WS建立] {url}")

        elif method == "Network.webSocketFrameSent":
            req_id = params.get("requestId")
            payload = params.get("response", {}).get("payloadData", "")
            captured.append({"direction": "SEND", "ws_url": ws_connections.get(req_id), "data": payload[:200]})
            print(f"  [SEND→] {payload[:80]}")

        elif method == "Network.webSocketFrameReceived":
            req_id = params.get("requestId")
            payload = params.get("response", {}).get("payloadData", "")
            captured.append({"direction": "RECV", "ws_url": ws_connections.get(req_id), "data": payload[:200]})
            print(f"  [←RECV] {payload[:80]}")

    ws.close()
    return captured

# JumpServer 终端 WS 协议示例输出：
# [WS建立] wss://js.example.com/koko/ws/terminal/?target_id=xxx
# [SEND→] {"id":"term1","type":"TERMINAL_DATA","data":"ls\r"}      ← 输入命令
# [←RECV] {"id":"term1","type":"TERMINAL_DATA","data":"total 32\r\ndrwxr-xr-x..."}  ← 输出
```

---

### Step 4：自动化操控（探索完成后的实施）

#### 4.1 JS 直接写 WebSocket（终端类最优解）

探索到 WS 协议后，用 JS 注入直接找到页面持有的 WebSocket 对象并写入：

```python
def send_to_terminal_via_ws_injection(ws_url: str, command: str, session_id: str) -> str:
    """
    通过 JS 注入，找到页面持有的 xterm WebSocket 对象，直接发送命令。
    比模拟键盘输入更稳定，不依赖焦点。

    探索阶段已确认协议格式为：{"id": "<session_id>", "type": "TERMINAL_DATA", "data": "<cmd>\\r"}
    """
    payload = json.dumps({"id": session_id, "type": "TERMINAL_DATA", "data": command + "\r"})
    escaped = payload.replace('"', '\\"')

    ws = websocket.create_connection(ws_url, suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {
        "expression": f"""
(() => {{
    // 找到页面上所有活跃的 WebSocket 连接（xterm 通常挂在 window 或组件实例上）
    const sockets = window.__activeSockets || [];
    if (sockets.length > 0) {{
        sockets[0].send("{escaped}");
        return "sent via __activeSockets";
    }}
    // 回退：遍历全局对象找 WebSocket 实例
    for (const key of Object.keys(window)) {{
        if (window[key] instanceof WebSocket && window[key].readyState === 1) {{
            window[key].send("{escaped}");
            return "sent via window." + key;
        }}
    }}
    return "no active WebSocket found";
}})()
"""
    }}))
    resp = json.loads(ws.recv())
    ws.close()
    return resp["result"]["result"]["value"]
```

#### 4.2 JS 操控 xterm.js 终端对象（更精准）

JumpServer 使用 xterm.js，可以通过 JS 直接调用其 API：

```python
def write_to_xterm(ws_url: str, command: str) -> None:
    """通过 JS 找到 xterm Terminal 实例，直接调用 _core._inputHandler.input() 写入。"""
    ws = websocket.create_connection(ws_url, suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate", "params": {
        "expression": f"""
(() => {{
    // xterm.js 4.x+: Terminal 实例通常挂在 DOM 节点的 __proto__ 链或全局变量上
    const termEl = document.querySelector('.xterm');
    if (termEl && termEl._xterm) {{
        termEl._xterm._core._inputHandler.input({json.dumps(command + chr(13))}, false);
        return 'ok via _xterm';
    }}
    return 'xterm not found';
}})()
"""
    }}))
    ws.recv()
    ws.close()
```

---

### 完整探索流程示例（以 JumpServer 为例）

```
第 1 轮：DOM 探索
  → 发现 .koko-iframe，src 指向 /luna/ 子页面
  → attach 到 luna iframe target

第 2 轮：DOM 探索（luna iframe 上下文）
  → 发现 window.Terminal（xterm.js 已加载）
  → 发现 .xterm 节点（终端渲染区域）
  → globalKeys 里看到 __activeSockets = [WebSocket]

第 3 轮：WS 协议探索
  → 手动在终端输 "ls"，捕获到：
    SEND: {"id":"term_abc","type":"TERMINAL_DATA","data":"ls\r"}
    RECV: {"id":"term_abc","type":"TERMINAL_DATA","data":"...(ls输出)..."}
  → 协议格式确认！

第 4 轮：实现自动化
  → 直接用 JS 注入写 window.__activeSockets[0].send(...)
  → 监听 Network.webSocketFrameReceived 收集输出
  → 封装为 jscmd exec "ls"
```

**这就是 `jumpserver-terminal` skill 的完整诞生路径。**

---

| 方案 | 需要 CDP 端口 | 需要 Chrome 运行 | 是否操作页面 | 用户可见 | 推荐度 |
|------|-------------|----------------|------------|---------|--------|
| A：Headless 独立 Profile | 是（自己开） | 否（自己启动） | 操作页面 | 完全不可见 | ⭐⭐⭐⭐⭐ |
| B：复制 Profile 后 Headless | 是（自己开） | 否（自己启动） | 操作页面 | 完全不可见 | ⭐⭐⭐⭐ |
| C：`connect_over_cdp` 新开 Tab | 是（用户开） | 是 | 操作页面 | Tab 标题可见 | ⭐⭐⭐ |
| D：CDP 偷 Cookie + 直接调 API | 是（用户开） | 是 | **不操作页面** | 完全不可见 | ⭐⭐⭐⭐⭐ |
| E：`browser_cookie3` 读磁盘 Cookie | **否** | **否** | **不操作页面** | 完全不可见 | ⭐⭐⭐⭐ |
| F：AppleScript + JS 注入（macOS） | **否** | 是（前台即可） | 操作页面 | 用户有感知 | ⭐⭐⭐ |
| G：CDP 网络拦截（Fetch.enable） | 是（用户开） | 是 | **不操作页面** | 完全不可见 | ⭐⭐⭐⭐ |
| H：长驻后台 CDP 守护进程 | 是（自己开） | 否（自己启动） | 按需 | 完全不可见 | ⭐⭐⭐⭐⭐ |
| 直接附加到用户现有 Tab | 是（用户开） | 是 | 操作页面 | 完全可见 | ❌ |

---

## 三、各方案详解

### 方案 A：Headless 独立 Profile（推荐生产）

**原理**：维护一个专属 Agent Profile 目录，第一次有界面登录，之后永远 headless 复用。

```bash
# 第一次：有界面启动，手动完成 SSO 登录，Cookie 保存到 profile
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --user-data-dir=/Users/luca/.chrome-agent-profile \
  --remote-debugging-port=9222

# 后续：headless 复用，完全静默
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --headless=new \
  --user-data-dir=/Users/luca/.chrome-agent-profile \
  --remote-debugging-port=9222 \
  --no-sandbox \
  --disable-gpu
```

**优点**：
- 用户完全感知不到
- Cookie / localStorage / Session 完整保留
- 适合定时任务、Agent 自动化

**限制**：
- 同一 profile 目录不能同时被两个 Chrome 进程使用（文件锁）

---

### 方案 B：复制现有 Profile 后 Headless

**适用场景**：想复用用户当前 Chrome 的登录态，但用户 Chrome 正在运行。

```bash
# 1. 复制 Default profile（Chrome 运行中也可复制，但建议先关闭）
cp -r "$HOME/Library/Application Support/Google/Chrome/Default" \
      /tmp/chrome-agent-profile/Default

# 2. headless 使用副本
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --headless=new \
  --user-data-dir=/tmp/chrome-agent-profile \
  --remote-debugging-port=9222
```

**注意**：
- Chrome Cookie 在 macOS 上使用 AES 加密，存储在 `Default/Cookies`（SQLite）
- 复制后可直接使用，Chrome headless 进程会自行解密
- 副本不会随用户浏览器更新 Cookie，需要定期重新复制

---

### 方案 C：connect_over_cdp 连接已运行 Chrome

**适用场景**：用户 Chrome 正在运行且已登录，不想维护独立 profile。

```bash
# 用户 Chrome 需要开启调试端口（加到启动参数，或重启时带上）
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9222
```

```python
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    # 连接到用户已运行的 Chrome
    browser = p.chromium.connect_over_cdp("http://localhost:9222")
    context = browser.contexts[0]   # 复用现有 context（含完整 Cookie）

    # 新开 Tab，用户可能看到标签，但不会自动切换过去
    page = context.new_page()
    page.goto("https://internal-system/query")
    page.fill("#keyword", "搜索词")
    page.click("#btn-search")
    page.wait_for_selector("#results")
    data = page.inner_text("#results")
    page.close()   # 操作完关闭 Tab，几乎无感知
```

**优点**：零配置，直接复用用户登录态  
**缺点**：用户 Chrome 必须以 `--remote-debugging-port` 启动；新 Tab 标题对用户可见

---

### 方案 D：CDP 偷 Cookie + 直接调 API（最轻量，推荐内网系统）

**原理分两步**：

**第一步（一次性准备）：抓包分析接口**

在浏览器里手动操作一次目标功能，用 DevTools Network 面板或 CDP `Network.requestWillBeSent` 事件捕获实际发出的 HTTP 请求，记录下：
- 接口 URL（如 `POST /api/v1/mysql_query/`）
- 请求头（`Authorization`、`Content-Type` 等）
- 请求体结构（JSON 字段名）
- 认证流程（如先用 Cookie 换 Token，再带 Token 调业务接口）

```python
# 用 CDP 监听一次页面操作，自动记录所有接口请求（一次性逆向）
ws.send({"method": "Network.enable", "params": {}})
# ... 然后手动在页面点击查询按钮，监听 requestWillBeSent 事件输出
```

**第二步（每次调用）：CDP 偷 Cookie + 直接 replay 接口**

拿到接口结构后，后续每次调用只需用 CDP 读取最新 Cookie，然后用 `requests` 直接重放这个请求，**全程不打开、不切换、不操作任何页面**。

**前提条件**：
- 已通过抓包分析出目标系统的接口 URL 和请求结构
- 用户 Chrome 已开启 `--remote-debugging-port=9222`
- 用户已在 Chrome 中登录过目标系统（Cookie 存在即可，页面不需要开着）

```python
import json
import urllib.request
import websocket
import requests

CDP_PORT = 9222
# 以下 URL 和结构均来自第一步抓包分析的结果
TARGET_WEB = "https://internal-system.tuya-inc.com"
TOKEN_API  = f"{TARGET_WEB}/api/v1/api-token-auth/"   # 抓包得：POST，用 Cookie 换业务 Token
QUERY_API  = f"{TARGET_WEB}/api/v1/mysql_query/"      # 抓包得：POST，带 Token 执行查询


def steal_cookies_via_cdp(url: str) -> dict[str, str]:
    """通过 CDP Network.getCookies 从已运行的 Chrome 读取指定域名的 Cookie。"""
    with urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/list") as f:
        targets = json.loads(f.read())
    page = next(t for t in targets if t.get("type") == "page" and t.get("webSocketDebuggerUrl"))

    ws = websocket.create_connection(page["webSocketDebuggerUrl"], suppress_origin=True)
    ws.send(json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
    ws.recv()
    ws.send(json.dumps({"id": 2, "method": "Network.getCookies", "params": {"urls": [url]}}))
    resp = json.loads(ws.recv())
    ws.close()

    return {c["name"]: c["value"] for c in resp.get("result", {}).get("cookies", [])}


def run_api_with_cdp_cookie(sql: str) -> dict:
    """
    用 CDP 偷来的 Cookie replay 抓包得到的接口，不操作任何页面。

    接口来源：打开 DevTools → Network → 手动点击一次查询 → 右键请求 → Copy as cURL
    """
    cookies = steal_cookies_via_cdp(TARGET_WEB)

    session = requests.Session()
    session.cookies.update(cookies)

    # 抓包发现：先 POST /api-token-auth/ 用 Cookie 换业务 Token
    token_resp = session.post(TOKEN_API, json={})
    token = token_resp.json()["data"]["token"]

    # 抓包发现：再 POST /mysql_query/ 带 Authorization 头执行 SQL
    result = session.post(
        QUERY_API,
        headers={"Authorization": f"TUYA {token}"},
        json={
            "sql": sql,
            "database_name": "tuya_algorithm",   # 抓包得到的字段名
            "cluster_name": "default",            # 抓包得到的字段名
            "limit": 50,
        },
    )
    return result.json()
```

**如何快速抓包获取接口结构：**

```
1. 打开 Chrome DevTools（F12）→ Network 面板
2. 在页面上手动操作一次目标功能（如点击查询按钮）
3. 在 Network 面板找到对应 XHR/Fetch 请求
4. 右键 → "Copy as cURL"，即可得到完整的 URL、Header、Body
5. 将 cURL 转换为 Python requests（工具：https://curlconverter.com）
```

**方案 D 的优点**：
- 完全不操作页面，用户零感知
- 不需要 headless Chrome、不需要 Playwright
- Cookie 每次实时从 CDP 读取，自动感知用户重新登录
- 比方案 A/B/C 更轻量，依赖更少（仅 `websocket-client` + `requests`）

**限制**：
- 需要提前花 5 分钟抓包分析接口结构（一次性工作）
- 目标系统接口变更时需重新抓包
- 用户 Chrome 必须开启 `--remote-debugging-port`
- Cookie 依赖用户维持登录态，用户登出后需重新登录

**真实案例**：`tuya-starrocks-query-platform` skill 的 `run_starrocks_sql_silent` 工具即采用此方案，完整实现见 `~/.codex/mcp/tuya_starrocks_sql_platform/starrocks_sql_reader.py`。

---

### 方案 E：`browser_cookie3` 直接读磁盘 Cookie（无需 CDP 端口）

**原理**：完全绕过 Chrome 进程，直接从磁盘的 SQLite 文件读取加密 Cookie，解密后注入 requests session。**既不需要 Chrome 开 CDP 端口，也不需要 Chrome 正在运行**。

```python
import browser_cookie3
import requests

def get_cookies_from_disk(domain: str) -> dict[str, str]:
    """直接从 Chrome 磁盘 Cookie 文件读取，无需 Chrome 运行。"""
    cookies = {}
    for cookie in browser_cookie3.chrome():
        if domain in cookie.domain:
            cookies[cookie.name] = cookie.value
    return cookies

def run_api_without_chrome(sql: str) -> dict:
    """无需 Chrome 进程，直接读磁盘 Cookie 调 API。"""
    cookies = get_cookies_from_disk("starrocks-dms-cn.tuya-inc.com")
    session = requests.Session()
    session.cookies.update(cookies)
    return session.post("https://starrocks-dms-cn.tuya-inc.com/api/v1/query/", json={"sql": sql}).json()
```

**优点**：
- 最轻量：**无需 Chrome 运行、无需 CDP 端口**，纯离线读文件
- 适合脚本、定时任务、CI 环境
- `browser_cookie3` 自动处理 macOS Keychain/Linux Secret Service 解密

**限制**：
- Chrome 运行时 Cookie 文件被锁定（可先 `cp` 副本再读）
- macOS 上需要 Keychain 权限，首次运行会弹系统授权弹框
- Cookie 是磁盘快照，不感知实时刷新（适合长效 SSO Token）

```bash
pip install browser-cookie3
```

---

### 方案 F：AppleScript + JS 注入（macOS 专属，读写前台 Tab）

**原理**：通过 macOS AppleScript 控制 Chrome，在**前台活动 Tab** 执行 JS 读取页面内容或 Cookie，或通过剪贴板中转操作编辑器。**不需要 CDP 端口**，但需要页面已经打开。

```python
import subprocess, json

def execute_js_in_active_tab(js_code: str) -> str:
    """在 Chrome 前台 Tab 执行 JS，返回结果。"""
    script = f'''
tell application "Google Chrome"
  return execute active tab of front window javascript "{js_code}"
end tell
'''
    return subprocess.check_output(["osascript"], input=script, text=True).strip()

def get_cookies_via_applescript(domain: str) -> dict:
    """通过 JS 读取当前页 document.cookie（仅限非 HttpOnly）。"""
    raw = execute_js_in_active_tab("JSON.stringify(document.cookie)")
    return dict(pair.split("=", 1) for pair in json.loads(raw).split("; ") if "=" in pair)

def read_page_data_via_clipboard() -> str:
    """Cmd+A + Cmd+C 全选复制，再 pbpaste 读取——适合编辑器内容。"""
    subprocess.run(["osascript", "-e",
        'tell application "Google Chrome" to activate\n'
        'tell application "System Events" to keystroke "a" using command down\n'
        'tell application "System Events" to keystroke "c" using command down'
    ])
    import time; time.sleep(0.2)
    return subprocess.check_output(["pbpaste"], text=True)
```

**优点**：
- 无需 CDP 端口，无需 Playwright
- 可以读写任意编辑器（Monaco/Ace/CodeMirror）内容
- macOS 原生，稳定可靠

**限制**：
- **仅限 macOS**
- 需要辅助功能权限（系统偏好 → 隐私 → 辅助功能）
- 操作前台 Tab，执行时会短暂激活 Chrome 窗口（对用户略有感知）
- JS 注入只能读 non-HttpOnly Cookie

**真实案例**：`tuya-starrocks-query-platform` 的 `read_current_starrocks_sql` / `edit_current_starrocks_sql` 即此方案。

---

### 方案 G：CDP 网络拦截 `Fetch.enable`（零页面操作读响应）

**原理**：连接到已运行 Chrome，开启 `Fetch.enable` 拦截指定域名的所有请求/响应，**完全不需要新开 Tab、不需要导航页面**，像代理一样旁路监听 Chrome 的网络流量。

```python
import json, websocket, urllib.request, threading

CDP_PORT = 9222

def intercept_network_responses(target_url_pattern: str, on_response):
    """监听 Chrome 网络响应，不操作任何页面。"""
    with urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json/list") as f:
        targets = json.loads(f.read())
    page = next(t for t in targets if t.get("type") == "page")

    ws = websocket.create_connection(page["webSocketDebuggerUrl"])

    # 开启网络拦截
    ws.send(json.dumps({"id": 1, "method": "Fetch.enable", "params": {
        "patterns": [{"urlPattern": target_url_pattern, "requestStage": "Response"}]
    }}))

    msg_id = 2
    while True:
        msg = json.loads(ws.recv())
        if msg.get("method") == "Fetch.requestPaused":
            params = msg["params"]
            # 获取响应体
            ws.send(json.dumps({"id": msg_id, "method": "Fetch.getResponseBody",
                                "params": {"requestId": params["requestId"]}}))
            body_msg = json.loads(ws.recv())
            body = body_msg.get("result", {}).get("body", "")
            on_response(params["request"]["url"], body)

            # 放行请求
            ws.send(json.dumps({"id": msg_id + 1, "method": "Fetch.continueRequest",
                                "params": {"requestId": params["requestId"]}}))
            msg_id += 2
```

**适用场景**：
- 想捕获 Chrome 在某个页面上发出的所有 API 请求/响应（逆向接口）
- 拦截并修改请求参数（如自动注入额外字段）
- 监控特定接口的实时数据（无需轮询）

**优点**：完全被动旁路，不影响用户任何操作  
**限制**：需要用户 Chrome 开 CDP 端口；`Fetch.enable` 会拦截所有匹配请求（需谨慎匹配模式）

---

### 方案 H：长驻后台 CDP 守护进程（多任务复用，生产推荐）

**原理**：将 headless Chrome 作为**常驻后台服务**启动，Agent 每次直接连接复用，省去冷启动开销（Chrome 启动约 1-2s）。适合高频调用场景。

```python
import subprocess, time, atexit, requests
from playwright.sync_api import sync_playwright, Browser

AGENT_PROFILE = "/Users/luca/.chrome-agent-profile"
CDP_PORT = 9223
_chrome_proc = None
_browser: Browser | None = None


def ensure_chrome_running() -> Browser:
    """确保后台 Chrome 已启动，返回可复用的 browser 实例。"""
    global _chrome_proc, _browser

    # 检查是否已在运行
    try:
        requests.get(f"http://localhost:{CDP_PORT}/json/version", timeout=1)
    except Exception:
        _chrome_proc = subprocess.Popen([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "--headless=new",
            f"--user-data-dir={AGENT_PROFILE}",
            f"--remote-debugging-port={CDP_PORT}",
            "--no-sandbox", "--disable-gpu",
        ])
        time.sleep(1.5)

    if _browser is None or not _browser.is_connected():
        p = sync_playwright().start()
        _browser = p.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")

    return _browser


def run_task(url: str) -> str:
    """复用已有 browser，每次只新开一个 Tab，用完即关。"""
    browser = ensure_chrome_running()
    context = browser.contexts[0] if browser.contexts else browser.new_context()
    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle")
        return page.content()
    finally:
        page.close()


# 进程退出时自动清理
atexit.register(lambda: _chrome_proc and _chrome_proc.terminate())
```

**优点**：
- 多次调用共享同一 Chrome 进程，**消除冷启动延迟**
- Cookie/Session 在进程生命周期内持续有效
- 适合 MCP Server、定时任务调度器等长期运行场景

**限制**：需要管理进程生命周期；进程异常退出需要自动重启（可用 supervisord）

---

## 四、最优方案（推荐）

### 场景判断树

```
需要操作涂鸦内网系统（需 SSO 登录）？
│
├── 是 → 目标系统有后端 REST API 可直接调用？
│   │
│   ├── 是（有 API）
│   │   ├── Chrome 是否需要运行？
│   │   │   ├── 可以不运行 → 方案 E（browser_cookie3 读磁盘，最简单）
│   │   │   └── 已在运行且开了 CDP → 方案 D（CDP 偷 Cookie + 直接调 API）
│   │   └── 高频调用 → 方案 H（长驻守护进程，复用连接）
│   │
│   └── 否（纯页面操作，无 API）
│       ├── macOS + 页面已打开 → 方案 F（AppleScript + JS，无需 CDP 端口）
│       ├── 用户 Chrome 已开 CDP → 方案 C（connect_over_cdp + new_page）
│       ├── 想监听网络响应而非操作 DOM → 方案 G（Fetch.enable 网络拦截）
│       └── 都不满足 → 方案 A（独立 profile，首次手动登录后 headless 永久复用）
│
└── 否（无需登录态）→ 方案 A headless 直接启动，无需 user-data-dir
```

### 推荐：方案 A 完整实现（Playwright Python）

```python
#!/usr/bin/env python3
"""
静默后台 CDP 操作模板
- 使用独立 Agent Profile，首次手动登录后复用 Cookie
- 完全 headless，不影响用户
"""
import subprocess
import time
from playwright.sync_api import sync_playwright

AGENT_PROFILE = "/Users/luca/.chrome-agent-profile"
CDP_PORT = 9223   # 避免与用户 Chrome 的 9222 冲突


def launch_headless_chrome():
    """启动 headless Chrome，复用 agent profile 的登录态。"""
    proc = subprocess.Popen([
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "--headless=new",
        f"--user-data-dir={AGENT_PROFILE}",
        f"--remote-debugging-port={CDP_PORT}",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-extensions",
    ])
    time.sleep(1.5)   # 等待启动
    return proc


def run_silent_task(url: str, keyword: str) -> str:
    """
    静默执行：打开页面、填写关键词、点击查询、返回结果。

    Args:
        url: 目标页面 URL
        keyword: 搜索关键词

    Returns:
        查询结果文本
    """
    proc = launch_headless_chrome()
    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")
            page.fill("#keyword", keyword)
            page.click("#btn-search")
            page.wait_for_selector("#results", timeout=15000)
            result = page.inner_text("#results")
            page.close()
            browser.close()
            return result
    finally:
        proc.terminate()


if __name__ == "__main__":
    result = run_silent_task("https://internal-system/", "查询词")
    print(result)
```

---

## 五、首次登录初始化流程

```bash
# Step 1：有界面启动，手动完成 SSO
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --user-data-dir=/Users/luca/.chrome-agent-profile

# Step 2：在浏览器中访问目标系统，完成登录（Cookie 自动保存到 profile）

# Step 3：关闭 Chrome

# Step 4：验证 headless 是否能保持登录态
python3 -c "
from playwright.sync_api import sync_playwright
import subprocess, time
subprocess.Popen([
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '--headless=new', '--user-data-dir=/Users/luca/.chrome-agent-profile',
    '--remote-debugging-port=9223', '--no-sandbox'
])
time.sleep(2)
with sync_playwright() as p:
    b = p.chromium.connect_over_cdp('http://localhost:9223')
    pg = b.new_page()
    pg.goto('https://internal-system/')
    print(pg.title())   # 若非登录页则说明 Cookie 有效
    b.close()
"
```

---

## 六、注意事项

- **文件锁**：同一 `--user-data-dir` 不能被两个 Chrome 进程同时使用。Agent 使用独立 profile 可彻底避免此问题。
- **Cookie 过期**：SSO Token 通常有有效期，过期后需重新执行首次登录流程。
- **端口冲突**：Agent 用 `9223`，避免与用户 Chrome 默认的 `9222` 冲突。
- **headless=new**：Chrome 112+ 推荐使用新 headless 模式，旧版 `--headless` 行为有差异。
- **macOS 权限**：首次运行可能需要在「系统偏好设置 → 安全性」中允许 Chrome 被脚本调用。
- **依赖安装**：`pip install playwright && playwright install chromium`
