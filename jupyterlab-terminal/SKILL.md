# Skill: JupyterLab Terminal WebSocket CLI

## 用途

通过 WebSocket 直接与 JupyterLab Terminal 交互，执行命令并获取输出——无需截图、无需 MCP，速度比浏览器自动化快 10 倍。

适用场景：
- 在 Jupyter Notebook Pod 内运行 Python 脚本、shell 命令
- 验证组件连通性（Kafka、MinIO、StarRocks、Spark 等）
- 快速调试大数据环境配置

## 核心行为规则（最高优先级）

### 禁止使用任何浏览器 MCP（绝对禁止，无例外）

`user-chrome-devtools`、`plugin-browse-browser` 等浏览器 MCP **会启动新的受控 Chrome 实例**，
不能读取用户现有浏览器，**永远不得调用**，包括 `list_pages`、`new_page`、`navigate_page` 等所有方法。

setup 由 `jupyterm setup`（无参数）自动完成，直接扫描本机 CDP 端口读取 selected 标签，无需任何 MCP。

### 手动触发 Skill 的执行流程

1. 检查 `~/.jupyterm.json` 是否存在且可用（`jupyterm list` 无报错）
   - **可用** → 直接执行命令
   - **不可用** → 运行 `jupyterm setup`（无参数），自动探测
2. 若 setup 报错"当前活动标签不是 Jupyter 页面"：
   - 停止，提示用户："请在浏览器切换到 JupyterLab 标签后重试 `jupyterm setup`"
   - 不猜 URL，不修改代码，不继续执行
3. 执行命令：
   - `jupyterm list` 取最大编号 terminal
   - `jupyterm exec -t <id> "<command>"`

## 工具位置

```
~/.cursor/skills/jupyterlab-terminal/
├── jupyterm          # bash wrapper（可直接执行）
├── jupyterm.py       # Python 实现
└── SKILL.md          # 本文档
```

## 前置条件

1. **Chrome 已开启 CDP 远程调试**：`jupyterm setup` 通过 `http://127.0.0.1:{port}/json` 直接读取已有 Chrome 实例，不借助任何 MCP。
   - **Chrome 144+**：在地址栏打开 `chrome://inspect/#remote-debugging`，按提示允许远程调试（官方说明见 [Chrome 博客](https://developer.chrome.com/blog/chrome-devtools-mcp-debug-your-browser-session?hl=zh-cn)）。
   - **通用**：启动 Chrome 时加 `--remote-debugging-port=9222`（或你自定义的端口，配合 `jupyterm setup --cdp-ports`）。
2. **浏览器 selected（当前激活）标签已打开 JupyterLab**
3. **JupyterLab 内至少开着一个 Terminal**（或用 `jupyterm create` 创建）

## 使用方式

### 初始化配置（首次使用 / Jupyter 地址变更后）

```bash
JUPYTERM=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 自动探测：扫描本机 CDP 端口，读取 selected 标签的 Jupyter URL 和 token
$JUPYTERM setup

# 手动指定（远程服务器 / 域名部署）
$JUPYTERM setup --url http://jupyter.example.com/user/admin/lab --token abc123

# 自定义 CDP 扫描端口（多 Chrome 实例场景）
$JUPYTERM setup --cdp-ports 9222,9223
```

setup 内部流程：
1. 扫描端口 9222–9230，`GET /json` 取所有 page targets
2. 对每个 page 执行 CDP `Runtime.evaluate("document.visibilityState")`
3. 找到 `"visible"`（selected）的页面
4. 判断 URL 是否含 jupyter——否则停止提示用户切换标签
5. 读取 `jupyter-config-data` 中的 token，保存 `~/.jupyterm.json`

### 执行命令

```bash
$JUPYTERM exec "ls ~/demos/"             # 简单命令
$JUPYTERM exec "python3 -c 'import kafka; print(kafka.__version__)'"
$JUPYTERM exec --timeout 60 "pip list"  # 长命令加大超时
$JUPYTERM exec -t 2 "pwd"              # 指定 terminal id
```

### 列出 / 创建 Terminal

```bash
$JUPYTERM list                          # 列出所有 Terminal
$JUPYTERM create                        # 在 JupyterLab 中创建新 Terminal
```

### 执行本地脚本文件

```bash
$JUPYTERM run /tmp/test_kafka.sh        # 把本地脚本发送到容器内执行
$JUPYTERM run /tmp/test_kafka.sh --timeout 120
```

## Agent 使用规范

### 核心原则

用户提到"在 jupyter 执行"、"jupyter 里运行"、"notebook 容器内执行"等任何类似含义时，**必须且只能使用此 skill**，理由：

1. **用户可见**：命令通过 WebSocket 发到浏览器里的 Terminal，用户能实时看到输入和输出
2. **远程兼容**：Jupyter 可能部署在远程服务器，本地 shell 不一定能通
3. **禁止替代**：不得使用任何浏览器 MCP、截图、本地 shell 代替执行

### Terminal ID 规则

- 执行前**必须先 `list`**，找到编号最大的 Terminal（即用户最近打开、正在看的那个）
- 用 `-t <id>` 指定，确保命令出现在用户**当前可见**的 Terminal 里
- 禁止不加 `-t` 直接执行（默认 Terminal 1 用户看不到）

```bash
JUPYTERM=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 正确：先 list，再指定最新 terminal
$JUPYTERM list
$JUPYTERM exec -t 3 "pwd"

# 错误：不指定 terminal id（用户看不到执行过程）
$JUPYTERM exec "pwd"
```

### 完整工作流

```bash
JUPYTERM=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 0. 首次使用或配置失效时：自动探测（浏览器须已切到 Jupyter 标签）
$JUPYTERM setup

# 1. 查看用户当前打开的 terminal
$JUPYTERM list
# 输出示例：Terminal 1 / Terminal 2 / Terminal 3 → 取最大编号，如 3

# 2. 在用户可见的 terminal 执行命令
$JUPYTERM exec -t 3 "ls ~/demos/"
$JUPYTERM exec -t 3 --timeout 60 "python3 ~/demos/01_kafka_demo.py"
```

## 注意事项

- **Token 过期**：Pod 重启后 token 会变，重新运行 `jupyterm setup` 即可
- **Port-forward 断开**：若 8888 连不上，检查 port-forward 进程是否存活
- **输出截断**：默认超时 30 秒，长命令需传 `--timeout N`
- **并发限制**：同一 Terminal 不支持并发命令，多条命令请串行执行
