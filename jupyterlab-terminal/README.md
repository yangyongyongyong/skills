# jupyterlab-terminal

通过 **Chrome CDP + JupyterLab WebSocket** 与 JupyterLab Terminal 和 Notebook 交互，执行命令并获取输出——无需截图、无需 MCP，**WYSIWYG（所见即所得）**，速度比浏览器自动化快 10 倍。

---

## 前置条件

1. Chrome 已开启 CDP 远程调试（`chrome://inspect` 勾选，或 `--remote-debugging-port=9222` 启动）
2. 浏览器当前选中标签已打开 JupyterLab
3. JupyterLab 内有 Terminal（或用 `jupyterm create` 创建）

---

## 快速上手

```bash
J=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 首次使用：自动探测配置
$J setup

# Terminal：第 1 个 terminal 执行命令
$J exec "#1 python3 --version"

# Notebook：读取当前活跃 notebook
$J nb-read

# Notebook：在 [3] 下新增 cell，写代码并执行
$J nb-add --cell "[3]"
$J nb-edit --cell active --source "print('hello')"
$J nb-exec --cell active
```

---

## 详细文档

| 文档 | 内容 |
|------|------|
| [Terminal 命令](docs/terminal.md) | setup / list / create / exec / run，含所有参数和典型案例 |
| [Notebook 命令](docs/notebook.md) | nb-read/edit/exec/add/del/move/cell-type 等 13 个命令，含 `[N]` / `active` / 索引三种 cell 定位方式的完整示例 |
| [文件浏览器命令](docs/files.md) | file-new / file-new-dir / file-open / file-list，含完整创建→打开→执行工作流 |
| [典型工作流](docs/workflows.md) | 数据探查、修复重跑、结构整理、Terminal+Notebook 联动、批量执行等 7 个完整场景 |

---

## Cell 定位（--cell 参数）速查

```
--cell 0          # 第 1 个 cell（0-based 索引）
--cell "[3]"      # 执行编号为 [3] 的 cell（执行过才有编号）
--cell active     # 当前浏览器高亮的 cell（nb-add 后常用）
```

---

## 已验证兼容性

| 部署方式 | 状态 |
|---------|------|
| JupyterHub 启动的 JupyterLab（家庭自建） | ✅ 已验证 |
| JupyterHub 启动的 JupyterLab（公司环境） | ✅ 已验证 |
| 独立启动的 JupyterLab（`jupyter lab`） | ⚠️ 未测试 |

---

## 文件结构

```
jupyterlab-terminal/
├── SKILL.md              # Agent 触发规则与核心行为规范
├── README.md             # 本文档（导航入口）
├── jupyterm              # bash wrapper（直接执行）
├── jupyterm.py           # CLI 入口，注册所有子命令
├── jupyterm_config.py    # 配置读写（~/.jupyterm.json）
├── jupyterm_cdp.py       # CDP 层：panel 识别、cell 操作、文件浏览器
├── jupyterm_api.py       # REST API 层：Terminal / Sessions / Contents / Kernel WebSocket
├── jupyterm_terminal.py  # Terminal CLI 命令实现
├── jupyterm_notebook.py  # Notebook CLI 命令实现
├── jupyterm_files.py     # 文件浏览器 CLI 命令实现
└── docs/
    ├── terminal.md       # Terminal 命令完整参考
    ├── notebook.md       # Notebook 命令完整参考
    ├── files.md          # 文件浏览器命令完整参考
    └── workflows.md      # 典型工作流（7 个场景）
```
