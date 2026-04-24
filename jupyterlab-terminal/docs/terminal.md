# Terminal 命令参考

> CLI 路径：`~/.cursor/skills/jupyterlab-terminal/jupyterm`
> 以下示例中 `$J` = `~/.cursor/skills/jupyterlab-terminal/jupyterm`

---

## setup — 初始化 / 重新探测配置

```bash
# 最常用：浏览器切到 JupyterLab 标签后，自动探测 URL + token
$J setup

# 手动指定 URL 和 token（远程服务器 / 域名部署）
$J setup --url http://jupyter.example.com/user/admin/lab --token abc123

# 自定义 CDP 扫描端口（本机同时跑多个 Chrome 时）
$J setup --cdp-ports 9222,9223,9224

# 仅更新 token，URL 不变（token 过期最常见场景）
$J setup --token newtoken456
```

**失败处理**：若报错"当前活动标签不是 Jupyter 页面"，需在浏览器手动切到 JupyterLab 标签后重试，不要改代码、不要猜 URL。

---

## list — 列出浏览器中可见的 Terminal tab

```bash
# 列出所有可见 terminal（带位置编号和活跃标记）
$J list
# 输出示例：
#   #1  Terminal 1
#   #2  Terminal 3  <-- active
#   #3  Terminal 5
```

**使用场景**：执行命令前先 `list` 确认编号，再用 `#N` 指定目标。

---

## create — 新建 Terminal

```bash
# 在 JupyterLab 中创建新 Terminal（通过 REST API）
$J create
# 输出示例：[jupyterm] 已创建 terminal: 4
```

---

## exec — 在指定 Terminal 执行命令

### 按位置 `#N` 指定（推荐）

```bash
# 第 1 个 Terminal 执行
$J exec "#1 pwd"
$J exec "#1 ls -la"

# 第 2 个 Terminal 执行（两种等价写法）
$J exec "2# echo hello"
$J exec "#2 echo hello"

# 自动选浏览器当前活跃的 Terminal（不加 #N）
$J exec "hostname"
$J exec "python3 --version"
```

### 按服务端 terminal name 指定（向后兼容）

```bash
$J exec -t 3 "pwd"
$J exec -t 1 "ls ~/demos/"
```

### 超时控制

```bash
# 默认超时 30 秒，长命令必须加 --timeout
$J exec "#1 pip install pandas" --timeout 120
$J exec "#2 python3 train.py" --timeout 600
$J exec --timeout 60 "#1 find / -name '*.log' 2>/dev/null | head -20"
```

### 典型命令示例

```bash
# 查环境信息
$J exec "#1 python3 -c 'import sys; print(sys.version)'"
$J exec "#1 conda info --envs"
$J exec "#1 pip list | grep pandas"

# 查数据文件
$J exec "#1 ls -lh ~/data/"
$J exec "#1 wc -l ~/data/*.csv"

# 验证服务连通
$J exec "#1 curl -s http://kafka:9092"
$J exec "#1 python3 -c 'import kafka; print(kafka.__version__)'"

# 查进程 / 资源
$J exec "#1 ps aux | grep python"
$J exec "#1 free -h"
$J exec "#1 df -h"
```

### 优先级（从高到低）

| 指定方式 | 示例 | 说明 |
|---------|------|------|
| `-t server_name` | `exec -t 3 "pwd"` | 按 Jupyter 服务端 terminal name |
| `#N` / `N#` 位置 | `exec "#1 pwd"` | 浏览器从左到右第 N 个可见 terminal tab |
| 不指定 | `exec "pwd"` | 自动选浏览器当前活跃（selected）的 terminal |

---

## run — 执行本地脚本文件

```bash
# 将本地脚本发送到 Terminal 执行（脚本内容通过 WebSocket 逐行发送）
$J run /tmp/test_kafka.sh

# 加大超时（脚本运行时间较长时）
$J run /tmp/benchmark.sh --timeout 300

# 指定目标 Terminal（默认用活跃 terminal）
$J run /tmp/init_env.sh -t 2
```

**适用场景**：本地写好的 shell 脚本需要在远程 Jupyter 容器内执行，无需手动复制。
