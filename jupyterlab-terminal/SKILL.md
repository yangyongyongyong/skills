---
name: jupyterlab-terminal
description: 通过 Chrome CDP 直接操作 JupyterLab 的 Terminal 与 Notebook，实现所见即所得的自动化。
---

# Skill: JupyterLab Terminal & Notebook CLI

通过 CDP（Chrome DevTools Protocol）直接与 JupyterLab Terminal 和 Notebook 交互，
执行命令并获取输出——无需截图、无需 MCP，WYSIWYG（所见即所得）。

## 详细文档

| 文档 | 内容 |
|------|------|
| [Terminal 命令](docs/terminal.md) | setup / list / create / exec / run，含所有参数案例 |
| [Notebook 命令](docs/notebook.md) | nb-read/edit/exec/add/del/move 等，含 `--cell [N]` / `active` / 索引三种定位方式 |
| [文件浏览器命令](docs/files.md) | file-new / file-new-dir / file-open / file-list，含完整工作流 |
| [典型工作流](docs/workflows.md) | 数据探查、修复重跑、结构整理、Terminal+Notebook 联动等 7 个场景 |

---

## 快速上手

```bash
J=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 首次使用：浏览器切到 JupyterLab 标签后
$J setup

# Terminal：在第 1 个 terminal 执行命令
$J exec "#1 python3 --version"

# Notebook：读取当前活跃 notebook
$J nb-read

# Notebook：在 [3] 下新增 cell，写入代码并执行
$J nb-add --cell "[3]"
$J nb-edit --cell active --source "print('hello')"
$J nb-exec --cell active
```

---

## 核心行为规则（Agent 必读）

### 禁止使用任何浏览器 MCP

`plugin-browse-browser`、`user-chrome-devtools` 等 **会启动新 Chrome 实例，无法读取用户现有浏览器**，永远不得调用。

### 触发流程

1. 检查 `~/.jupyterm.json` 是否存在（`$J list` 无报错）
   - 可用 → 直接执行命令
   - 不可用 → 运行 `$J setup`（浏览器须切到 JupyterLab 标签）
2. 若 setup 报错"当前活动标签不是 Jupyter 页面"：提示用户切换标签后重试，**不猜 URL，不修改代码**
3. `term-signal` 仅支持 `ctrl-c` 用于中断阻塞命令；`ctrl-d` 已禁用，避免误关闭 Terminal 标签页

### 定位语法

```
Terminal：  #1 / #2 / ...      （浏览器左到右第 N 个 terminal tab）
Notebook：  nb#1 / nb#2 / ...  （浏览器左到右第 N 个 .ipynb tab）
            不指定 → 自动用当前活跃 tab
Cell：      --cell 0            （0-based 索引）
            --cell "[N]"        （执行编号，只有执行过的 cell 才有）
            --cell active       （当前选中的 cell，nb-add 后常用）
```

---

## 工具文件结构

```
~/.cursor/skills/jupyterlab-terminal/
├── jupyterm              # bash wrapper（直接执行）
├── jupyterm.py           # CLI 入口
├── jupyterm_config.py    # 配置读写（~/.jupyterm.json）
├── jupyterm_cdp.py       # CDP 层：daemon/直连、tab 切换、Notebook UI、文件浏览器
├── jupyterm_api.py       # REST API 层：Terminal/Sessions/Contents/Kernel WebSocket
├── jupyterm_terminal.py  # Terminal CLI 命令
├── jupyterm_notebook.py  # Notebook CLI 命令
├── jupyterm_files.py     # 文件浏览器 CLI 命令
├── SKILL.md              # 本文档（主入口）
└── docs/
    ├── terminal.md       # Terminal 命令完整参考
    ├── notebook.md       # Notebook 命令完整参考（含 --cell 三种定位）
    ├── files.md          # 文件浏览器命令完整参考
    └── workflows.md      # 典型工作流（7 个场景）
```

---

## 前置条件

1. Chrome 已开启 CDP 远程调试（`chrome://inspect` 勾选，或 `--remote-debugging-port=9222` 启动）
2. 浏览器当前选中标签已打开 JupyterLab
3. JupyterLab 内有 Terminal（或用 `$J create` 创建）

---

## 已验证兼容性

| 部署方式 | 状态 |
|---------|------|
| JupyterHub 启动的 JupyterLab（家庭自建） | ✅ 已验证 |
| JupyterHub 启动的 JupyterLab（公司环境） | ✅ 已验证 |
| 独立启动的 JupyterLab（`jupyter lab`） | ⚠️ 未测试 |
