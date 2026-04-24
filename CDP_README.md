# Chrome CDP 三种场景说明（防混淆）

本文用于区分 Chrome CDP 的三种常见运行场景，避免把“传统 `--remote-debugging-port` 模式”和 Chrome 146 新特性（`chrome://inspect/#remote-debugging` 勾选 **Allow remote debugging for this browser instance**）混为一谈。

## 1. 三种场景总览

| 场景 | 启动方式 | 常见监听地址 | 发现入口 | 连接建议 |
|---|---|---|---|---|
| A. 传统命令行模式 | 显式 `--remote-debugging-port=9222` | 常见 `127.0.0.1:9222`（也可能含 localhost） | `http://<host>:9222/json/version`、`/json` | 先 `/json/version` 取 `webSocketDebuggerUrl`，再连 WS |
| B. 新特性 + IPv4 | `chrome://inspect/#remote-debugging` 勾选 Allow（Chrome 146+） | `127.0.0.1:9222` | **优先** `DevToolsActivePort` | 先读 `DevToolsActivePort` 得 browser WS，再做 CDP |
| C. 新特性 + IPv6 | 同上（Chrome 146+） | `[::1]:9222` | **优先** `DevToolsActivePort` | 先读 `DevToolsActivePort`，并兼容 IPv6 URL（`[::1]`） |

## 2. 关键差异（必须记住）

1. 场景 A 是“命令行显式端口模式”，HTTP 发现端点（`/json*`）通常可直接使用。  
2. 场景 B/C 是“Chrome 146 新特性模式”，`DevToolsActivePort` 才是权威入口。  
3. 新特性模式下，`/json` 可能不可用、延迟可用，或与真实 browser WS 不一致。  
4. 新特性模式下会出现 IPv4/IPv6 两种监听形态，**不能写死 `127.0.0.1`**。  
5. browser WS path（`/devtools/browser/<uuid>`）可能短时过期，连接 404 时要支持自恢复。

## 3. 三种场景的连接方式

### A. 传统命令行模式（示例）

```bash
nohup "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --remote-debugging-port=9222 \
  --remote-allow-origins=http://127.0.0.1:9222 \
  --user-data-dir=/Users/luca/chrome-profile \
  >/tmp/chrome-cdp.log 2>&1 &
```

注意（重要）：
1. 命令行启动时**必须指定自定义 profile 路径**（`--user-data-dir=/你的路径`）。  
2. 不要复用默认 profile（如日常 Chrome 用户目录）；在默认 profile 下，Chrome 出于安全策略常导致远程调试端口不可用或行为异常。  
3. 推荐为自动化/CDP 固定单独目录，例如 `/Users/luca/chrome-profile`，并与日常浏览器会话隔离。

推荐流程：
1. 访问 `/json/version` 获取 `webSocketDebuggerUrl`。  
2. 连接该 WS（browser-level）。  
3. 再发 `Browser.getVersion`、`Target.getTargets` 等命令。

### B/C. Chrome 146 新特性模式（inspect 勾选）

入口：`chrome://inspect/#remote-debugging` 勾选 **Allow remote debugging for this browser instance**。

推荐流程：
1. 读取 Chrome 用户目录下 `DevToolsActivePort`。  
2. 解析端口与 browser ws path。  
3. 按 host 候选重试连接：`127.0.0.1` -> `[::1]` -> `localhost`。  
4. 若连接报 `HTTP 404`，用同 host/port 的 `/json/version` 获取最新 `webSocketDebuggerUrl` 并重试一次。

## 4. 实现层面的统一建议（给脚本/Agent）

1. 不要把 host 写死为 `127.0.0.1`。  
2. 统一支持 host 候选：`127.0.0.1`、`::1`、`localhost`。  
3. 统一做 IPv6 URL 规范化：`::1` 组装 URL 时要变成 `[::1]`。  
4. 如果 `webSocketDebuggerUrl` 缺失端口（如 `ws://localhost/devtools/...`），自动补回当前 CDP 端口。  
5. 新特性模式优先 `DevToolsActivePort`，`/json*` 仅作为探测/修复兜底。

## 5. 快速判定当前属于哪种场景

1. 如果是你手工/脚本显式加了 `--remote-debugging-port` 启动 Chrome，优先按场景 A 处理。  
2. 如果是从 `chrome://inspect/#remote-debugging` 勾选打开，按场景 B/C 处理。  
3. 看到 `Server running at: [::1]:9222` 就是新特性 + IPv6（场景 C）。  
4. 看到 `Server running at: 127.0.0.1:9222` 就是新特性 + IPv4（场景 B）。
