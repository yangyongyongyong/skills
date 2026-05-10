# cursor-skills

个人 [Cursor Agent Skills](https://cursor.com/docs/skills) 合集——扩展 Cursor Agent 在本机的自动化能力。

Cursor、Codex CLI、Claude Code 均支持相同的 SKILL.md 格式。

---

## Skills 一览

### `chrome-cdp-ws-daemon` — Chrome CDP WebSocket 守护进程

所有需要 CDP 的 skill 共用一个持久连接，用户只需授权一次。避免每个 skill 各自建立 CDP 连接导致频繁授权弹窗。

```
┌──────────────┐   Unix Socket    ┌──────────────────┐   WebSocket   ┌─────────┐
│  Skill A     │ ──────────────── │  cdp daemon      │ ───────────── │ Chrome  │
│  Skill B     │   (并发安全)     │  (后台常驻)      │  (持久连接)   │ Browser │
│  Skill C     │ ──────────────── │  ~/.chrome-cdp-daemon/cdp.sock   │         │
└──────────────┘                  └──────────────────┘               └─────────┘
```

**特性**：
- daemon 后台常驻，持有持久 WebSocket 连接
- 所有 skill 通过 Unix Socket 请求 CDP 服务，线程安全
- 心跳保活：每 30 秒检测连接健康，断线自动重连
- 首次启动弹一次授权框，后续完全静默

```bash
DAEMON=~/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py

python $DAEMON start    # 启动 daemon
python $DAEMON status   # 查看状态
python $DAEMON stop     # 停止
```

→ **[详细文档](chrome-cdp-ws-daemon/SKILL.md)**：架构说明、其他 skill 如何集成、运行时文件

---

### `iterm2-exec` — 向 iTerm2 会话发命令

让 Agent 向本机 iTerm2 中**已有的标签页**（含 SSH、docker exec 等远端会话）发送命令并取回输出，无需新建连接，无需在远端安装任何工具。

![iterm2-exec 演示](assets/iterm2_skill.gif)

**两种执行模式（自动切换）**：
- **SI 模式**（本地 shell 已装 Shell Integration）：直发命令，`output_range` 精确截取，完全免疫 resize 干扰
- **哨兵模式**（SSH / Docker / 未装 SI）：命令前后插入不可见 Escape Sequence 作为边界，远端只需有 `printf`

```bash
ITERM=~/.cursor/skills/iterm2-exec/.venv/bin/python
SCRIPT=~/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py

$ITERM $SCRIPT run --command 'echo OK'           # 默认 1号标签
$ITERM $SCRIPT run --tab-num 2 --command 'ls'    # 指定 2号标签
$ITERM $SCRIPT run --tab-num 2 --command 'pip install pandas' --timeout-seconds 120
```

→ **[详细文档](iterm2-exec/README.md)**：完整参数、所有执行模式、多窗口/分屏、故障排查

---

### `jumpserver-terminal` — 操作 JumpServer Web 终端

让 Agent 通过 Chrome CDP 直接向浏览器里打开的 **JumpServer Web 终端**发命令，无需 SSH 密钥，无需在服务器安装任何工具。jscmd daemon 同时作为通用 Chrome CDP 网关供其他 Skill 复用。

![jumpserver-terminal 演示](assets/jumpserver_skill.gif)

```bash
JSCMD="~/.cursor/skills/jumpserver-terminal/.venv/bin/python3 \
       ~/.cursor/skills/jumpserver-terminal/jscmd.py"

$JSCMD daemon start           # 启动后台 daemon（Chrome 弹框一次）
$JSCMD exec "hostname"        # 当前活跃终端
$JSCMD exec "#2 df -h"        # 指定第 2 个标签
$JSCMD exec "@web-01 ps aux"  # 按服务器别名
$JSCMD connect "db-master"    # 搜索并自动打开终端
```

→ **[详细文档](jumpserver-terminal/README.md)**：完整命令参考、别名管理、安全防护三级机制、故障排查

---

### `jupyterlab-terminal` — 操作 JupyterLab Terminal & Notebook

让 Agent 通过 WebSocket 直接向 JupyterLab Terminal 发命令、对 Notebook 进行 WYSIWYG 编辑（等价于用户手动点击工具栏按钮），速度比截图方式快 10 倍以上。

![jupyterlab-terminal 演示](assets/jupyter_skill.gif)

```bash
J=~/.cursor/skills/jupyterlab-terminal/jupyterm

$J setup                              # 自动探测（浏览器切到 Jupyter 标签）
$J exec "#1 python3 --version"        # Terminal 执行命令
$J nb-read                            # 读取当前 notebook
$J nb-add --cell "[5]"                # 在 [5] 下插入 cell
$J nb-edit --cell active --source "print(999)"   # 写入代码
$J nb-exec --cell active              # 执行
```

→ **[详细文档](jupyterlab-terminal/README.md)** / [Terminal](jupyterlab-terminal/docs/terminal.md) / [Notebook](jupyterlab-terminal/docs/notebook.md) / [文件浏览器](jupyterlab-terminal/docs/files.md) / [工作流](jupyterlab-terminal/docs/workflows.md)

---

## 共享基础设施：Chrome CDP

`jumpserver-terminal` 和 `jupyterlab-terminal` 共用同一个 **jscmd daemon** 作为 CDP 网关。

### 开启 Chrome 远程调试（二选一）

| 方式 | 操作 | 适用场景 |
|------|------|---------|
| **方式 A（推荐）** | 打开 `chrome://inspect/#remote-debugging` → 勾选 Allow | 日常使用，正常启动 Chrome |
| **方式 B** | `chrome --remote-debugging-port=9222` 启动 | 自动化脚本场景 |

验证：
```bash
curl -s http://127.0.0.1:9222/json/version | head -1   # 有输出即正常
```

---

## 安装与使用

### 安装目录

| 工具 | Skills 目录 |
|------|------------|
| [Cursor](https://cursor.com) | `~/.cursor/skills/` |
| [Codex CLI](https://github.com/openai/codex) | `~/.codex/skills/` |
| [Claude Code](https://docs.anthropic.com/claude/docs/claude-code) | `~/.claude/skills/` |

> Cursor 还有一个 **内置** Skills 目录（`~/.cursor/skills-cursor/`），由产品维护，**不要往这里安装第三方 Skill**。

### 安装方式（推荐：只复制需要的 Skill）

```bash
SKILLS_DIR=~/.cursor/skills   # 其他工具替换路径

git clone https://github.com/yangyongyongyong/skills /tmp/cursor-skills-repo

# 只复制需要的 Skill
cp -r /tmp/cursor-skills-repo/chrome-cdp-ws-daemon "$SKILLS_DIR/"
cp -r /tmp/cursor-skills-repo/iterm2-exec "$SKILLS_DIR/"
cp -r /tmp/cursor-skills-repo/jupyterlab-terminal "$SKILLS_DIR/"
cp -r /tmp/cursor-skills-repo/jumpserver-terminal "$SKILLS_DIR/"

rm -rf /tmp/cursor-skills-repo
```

或软链接方式（便于 `git pull` 统一更新）：

```bash
git clone https://github.com/yangyongyongyong/skills ~/projects/cursor-skills
ln -s ~/projects/cursor-skills/iterm2-exec ~/.cursor/skills/iterm2-exec
```

### Codex 使用坑（重要）

在 Codex Desktop / Codex CLI 中，很多人会遇到“目录里有 `SKILL.md`，但会话里看不到 Skill”的问题。  
常见原因是：仅放在 `~/.cursor/skills`、使用了软链接、或会话未刷新。

建议按下面步骤处理：

1. 把 Skill 放到 **真实目录**：`~/.codex/skills/<skill-name>/`（不要只用 symlink）。
2. 确保 `SKILL.md` 顶部有标准 frontmatter（至少 `name`、`description`）。
3. 在 `~/.codex/config.toml` 中显式注册并启用：

```toml
[[skills.config]]
path = "/Users/你的用户名/.codex/skills/iterm2-exec/SKILL.md"
enabled = true

[[skills.config]]
path = "/Users/你的用户名/.codex/skills/jumpserver-terminal/SKILL.md"
enabled = true

[[skills.config]]
path = "/Users/你的用户名/.codex/skills/jupyterlab-terminal/SKILL.md"
enabled = true
```

4. **完全退出** Codex 进程后重新打开（仅关闭窗口通常不够）。
5. 新建一个**全新会话**再验证（旧会话里的技能列表可能是启动时快照，不会热更新）。

---

## 目录结构

```
~/.cursor/skills/
├── README.md
├── assets/
│   ├── iterm2_skill.gif
│   ├── jumpserver_skill.gif
│   └── jupyter_skill.gif
├── chrome-cdp-ws-daemon/
│   ├── SKILL.md              # Agent 触发规则
│   └── scripts/
│       ├── daemon.py         # 守护进程
│       └── cdp_client.py     # 客户端 SDK
├── iterm2-exec/
│   ├── SKILL.md              # Agent 触发规则
│   ├── README.md             # 详细参数与案例文档
│   └── scripts/iterm2_exec.py
├── jumpserver-terminal/
│   ├── SKILL.md              # Agent 触发规则
│   ├── README.md             # 详细命令与安全文档
│   └── jscmd.py
└── jupyterlab-terminal/
    ├── SKILL.md              # Agent 触发规则
    ├── README.md             # 导航入口
    ├── jupyterm              # bash wrapper
    ├── jupyterm*.py          # 各模块实现
    └── docs/
        ├── terminal.md       # Terminal 命令完整参考
        ├── notebook.md       # Notebook 命令完整参考
        ├── files.md          # 文件浏览器命令完整参考
        └── workflows.md      # 典型工作流
```

---

## License

MIT
