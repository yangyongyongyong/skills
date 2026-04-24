---
name: iterm2-exec
description: >-
  通过本机 iTerm2 的 Python API 在指定会话中执行 shell 命令并取回输出；
---

# iTerm2 会话命令执行

## 何时使用本 Skill

- 用户希望 **不新开 SSH**，而是让 Agent **向本机 iTerm2 里已有的标签页/分屏会话** 发送命令。
- 典型背景：你在 iTerm2 里已 `ssh` 到生产/跳板机，希望 Cursor 操作该会话，相当于操作线上 shell。

## 硬前提（只有 1 条）

**本机 iTerm2 已运行**，且允许 Python API 连接（首次连接需在 iTerm2 中授权一次）。

## 执行模式（自动检测，智能切换）

每次执行命令前，CLI 会向目标会话发送无副作用的 `true` 命令来探测 Shell Integration（SI）是否活跃，并自动选择最优模式。同一会话的检测结果被缓存，不会每次重复探测。

| 维度 | 纯 SI 模式 | 哨兵模式 |
|------|-----------|---------|
| **适用场景** | 本地 macOS shell（zsh/bash 已装 SI） | SSH、docker exec、未装 SI 的任意 shell |
| **终端显示** | `% actual_cmd`（完全干净） | `% { actual_cmd; }; printf '...'`（有 wrapper） |
| **resize 稳定性** | ✅ 完全免疫（output_range 跟踪语义边界） | ⚠️ resize 期间执行命令可能偏差（已知限制） |
| **输出精度** | prompt.output_range 精确截取 | 行号计算，偶有偏差 |
| **远端安装要求** | 需在目标 shell 装 Shell Integration | 仅需 `printf`（POSIX 标准，无需额外安装） |

### 纯 SI 模式（本地 macOS shell）

- 发送**裸命令**，无任何包裹，终端里只看到 `% your_command`
- 由 iTerm2 Shell Integration 追踪命令语义边界，`output_range` 精确截取输出
- 若当前会话**未检测到 SI**，CLI 会在 stderr 打印安装提示并自动降级到哨兵模式

> 安装 Shell Integration：iTerm2 菜单 → **Shell Integration → Install Shell Integration**

### 哨兵模式（SSH / Docker / 未装 SI 的 shell）

- 命令被包裹为：
  ```bash
  { 你的命令; }; printf '\033]1337;Custom=id=iterm2-exec:<uuid>\007'
  ```
- `printf` 发出不可见的 Custom Escape Sequence → 由**本机 iTerm2** 截获，判断命令已结束
- 远端服务器只需有 `printf`（任何 POSIX 系统均满足），**无需在远端安装任何工具**
- ⚠️ **已知限制**：执行命令期间调整终端窗口大小，可能导致输出行号计算偏差，建议命令运行中不要 resize

## 不提供的能力（避免误解）

- 本方案**不保证**拿到进程的真实 **exit code**（只截取文本输出）。

## 标准调用方式（请用绝对路径）

依赖：`iterm2` PyPI 包（见 [iTerm2 Python API](https://iterm2.com/python-api/)）。在 macOS 自带「受管 Python」（PEP 668）环境下，推荐直接使用本 skill 目录下已创建好的虚拟环境解释器：

`/Users/thomas990p/.cursor/skills/iterm2-exec/.venv/bin/python`

若你删除了 `.venv`，可按下述方式一键重建：

```bash
python3 -m venv /Users/thomas990p/.cursor/skills/iterm2-exec/.venv
/Users/thomas990p/.cursor/skills/iterm2-exec/.venv/bin/pip install iterm2
```

```bash
/Users/thomas990p/.cursor/skills/iterm2-exec/.venv/bin/python \
  /Users/thomas990p/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py run \
  --command '你的命令' \
  [--timeout-seconds 120] \
  [--json]
```

### 会话选择（优先级从高到低）

### 标签编号说明

以下写法含义完全相同，Agent 理解用户意图时应统一映射到 `--tab-num N`：

| 用户自然语言 | 对应参数 |
|-------------|---------|
| 1号标签 / 1#标签 / ⌘1 / 第一个标签 | `--tab-num 1` |
| 2号标签 / 2#标签 / ⌘2 / 第二个标签 | `--tab-num 2` |
| N 号标签 / N# 标签 | `--tab-num N` |

### 参数一览

| 参数 | 含义 | 备注 |
|------|------|------|
| `--session-id <id>` | 指定会话 ID | 最精确，适合脚本固定绑定 |
| `--tab-num N` | **标签编号（1-based，⌘N）** | **最常用**；1号标签/1#标签均映射到此 |
| `--window-index W` | 窗口下标（0 起） | 多窗口时配合 `--tab-num` 使用 |
| `--tab-index T` | 标签下标（0 起） | `--tab-num` 的底层等价（`T = N-1`） |
| `--split-index S` | 分屏下标（0 起，按 session_id 排序） | 有分屏时使用 |
| 什么都不传 | **默认 ⌘1**（1号标签） | 最常见场景，无需额外参数 |

### 自动切换标签

**指定的标签若不是当前活动标签，CLI 会自动切换过去**，再执行命令，无需手动切换。

### 典型用法

```bash
# 默认：向 1号标签（⌘1）发命令
python3 .../iterm2_exec.py run --command 'echo OK'

# 向 2号标签（⌘2）发命令，若当前在 1号会自动切过去
python3 .../iterm2_exec.py run --tab-num 2 --command 'hostname'

# 多窗口：第二个窗口的 1号标签
python3 .../iterm2_exec.py run --window-index 1 --tab-num 1 --command 'pwd'

# 有分屏：2号标签里的第 0 个分屏
python3 .../iterm2_exec.py run --tab-num 2 --split-index 0 --command 'ps aux'
```

## Agent 使用约定

1. **默认用绝对路径** 调用脚本，避免工作目录变化导致找不到文件。
2. 对生产环境：**先确认** 用户意图与命令范围；**拒绝**未确认的破坏性操作（如大范围 `rm`、`kubectl delete`、`DROP TABLE` 等）。
3. **禁止**在命令行参数中传入密码、Token、私钥；必要时让用户在 iTerm2 内手动输入或使用已存在的 ssh agent。
4. 长耗时命令必须显式传入足够大的 **`--timeout-seconds`**，否则可能超时。
5. iterm2可能连接了线上服务器,默认禁止执行危险操作 如:修改profile 删除文件 覆盖文件等.  可通过和用户交互识别当前操作权限

## 故障排查

| 现象 | 可能原因 |
|------|----------|
| 连接失败 / 无法附着 iTerm2 | iTerm2 未运行或未授权 Python API |
| stderr 提示"未检测到 Shell Integration，使用哨兵模式" | 当前会话未安装 SI；本地 shell 建议安装以获得最佳体验（见上方安装指引） |
| 超时无输出（哨兵模式） | 远端没有 `printf`（极罕见）；或命令本身阻塞（加大 `--timeout-seconds`） |
| 超时无输出（SI 模式） | 命令阻塞、PromptMonitor 事件被过滤，或 SI 刚安装未生效（需重开 shell） |
| 哨兵模式输出内容偏差 | resize 期间执行了命令（已知限制）；等命令完成后再调整窗口 |
| 本地 shell 也被判为哨兵模式 | SI 缓存是进程级的；如刚装完 SI 需重启 shell 后再调用 CLI |
| 想强制切换模式 | 暂不支持手动指定，由 SI 探测自动决定；安装/卸载 SI 后重开 shell 即可切换 |

## 维护说明

- CLI 实现位置：[`scripts/iterm2_exec.py`](scripts/iterm2_exec.py)
- 若本机用户名或 home 路径不同，请相应替换上文中的绝对路径前缀。
