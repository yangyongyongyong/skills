---
name: chrome-cdp-ws-daemon
description: Chrome CDP WebSocket 守护进程——所有需要 CDP 的 skill 共用一个持久连接，用户只需授权一次。
---

# 使用场景

- 任何 skill 需要通过 Chrome CDP 获取 cookie、操作浏览器页面时，都应该依赖本 skill。
- 避免每个 skill 各自建立 CDP 连接导致用户频繁授权弹窗。

# 触发条件

- 其他 skill 通过 `cdp_client.py` 自动依赖，通常不需要用户手动触发。
- 手动管理 daemon 时使用 CLI 命令。

# 架构

```
┌──────────────┐   Unix Socket    ┌──────────────────┐   WebSocket   ┌─────────┐
│  Skill A     │ ──────────────── │  cdp daemon      │ ───────────── │ Chrome  │
│  Skill B     │   (并发安全)     │  (后台常驻)      │  (持久连接)   │ Browser │
│  Skill C     │ ──────────────── │  ~/.chrome-cdp-daemon/cdp.sock   │         │
└──────────────┘                  └──────────────────┘               └─────────┘
```

- daemon 进程后台常驻，持有一个到 Chrome 的持久 WebSocket 连接
- 所有 skill 通过 Unix Socket 向 daemon 请求 CDP 服务
- 线程安全：多 skill 并发请求互不干扰（RLock + 独立线程）
- 心跳保活：每 30 秒自动检测连接健康，断线自动重连
- 首次启动弹一次授权框，后续完全静默

# 其他 skill 如何使用

## 方式1：直接 import client SDK

```python
import sys
sys.path.insert(0, "/Users/luca/.claude/skills/chrome-cdp-ws-daemon/scripts")
from cdp_client import get_cookies, cdp_call

# 获取指定域名的 cookie
cookies = get_cookies("https://bdp-cn.tuya-inc.com:7799")

# 执行任意 CDP 命令
targets = cdp_call("Target.getTargets")
```

## 方式2：subprocess 调用 CLI

```bash
# 启动 daemon（通常自动完成）
/Users/luca/miniforge3/envs/py311/bin/python /Users/luca/.claude/skills/chrome-cdp-ws-daemon/scripts/daemon.py start

# 查看状态
... daemon.py status

# 停止
... daemon.py stop

# 重启
... daemon.py restart
```

# 文件说明

- `scripts/daemon.py` — 守护进程（CDP 连接管理 + Unix Socket 服务 + CLI）
- `scripts/cdp_client.py` — 客户端 SDK（其他 skill import 使用）

# 运行时文件

- `~/.chrome-cdp-daemon/cdp.sock` — Unix Socket 通信文件
- `~/.chrome-cdp-daemon/cdp.pid` — daemon PID 文件
- `~/.chrome-cdp-daemon/cdp.log` — daemon 运行日志
