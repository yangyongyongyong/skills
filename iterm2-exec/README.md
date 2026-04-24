# iterm2-exec

让 Cursor Agent 向本机 **iTerm2 中已有的标签页**（含 SSH、docker exec 等远端会话）发送命令并取回输出，无需新建连接，无需在远端安装任何工具。

---

## 前置要求

- macOS，iTerm2 已运行
- iTerm2 已开启 Python API：**Preferences → General → Magic → Enable Python API**
- 首次运行时 iTerm2 会弹出授权弹框，点击「允许」即可（仅弹一次）

---

## 安装依赖

```bash
# 在 skill 目录创建独立 venv（仅首次）
python3 -m venv ~/.cursor/skills/iterm2-exec/.venv
~/.cursor/skills/iterm2-exec/.venv/bin/pip install iterm2
```

---

## 快速上手

```bash
ITERM=~/.cursor/skills/iterm2-exec/.venv/bin/python
SCRIPT=~/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py

# 向默认标签（⌘1）发命令
$ITERM $SCRIPT run --command 'echo OK'

# 向 2号标签发命令
$ITERM $SCRIPT run --tab-num 2 --command 'hostname'

# 带超时（秒），默认 30s
$ITERM $SCRIPT run --tab-num 2 --command 'pip install pandas' --timeout-seconds 120
```

---

## 执行模式

CLI 每次自动探测目标会话是否安装了 iTerm2 Shell Integration（SI），并选择最优模式，**无需手动配置**。

| 模式 | 触发条件 | 终端显示 | 输出精度 | resize 稳定性 |
|------|---------|---------|---------|-------------|
| **SI 模式** | 本地 shell 已安装 Shell Integration | `% your_command`（干净） | ✅ 精确（`output_range`） | ✅ 完全免疫 |
| **哨兵模式** | SSH / Docker / 未装 SI 的任意 shell | `% { your_cmd; }; printf '...'` | ⚠️ 行号计算，偶有偏差 | ⚠️ resize 期间有偏差 |

**SI 模式安装**：iTerm2 菜单 → **Shell Integration → Install Shell Integration**（本地 shell 才有效，无需在远端安装）

> 哨兵模式要求远端只有 `printf`（任何 POSIX 系统均满足），无需在服务器上安装额外工具。

---

## 完整参数参考

```
python iterm2_exec.py run [OPTIONS]
```

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `--command` | str | **必填** | 要执行的 shell 命令 |
| `--tab-num` | int | `1` | 标签编号（1-based，对应 ⌘1 / ⌘2 ...）**最常用** |
| `--session-id` | str | — | iTerm2 Session ID（最精确，适合脚本固定绑定） |
| `--window-index` | int | `0` | 窗口下标（0-based，多窗口时配合 `--tab-num` 用） |
| `--tab-index` | int | — | 标签下标（0-based），与 `--tab-num` 互为等价（`index = num - 1`） |
| `--split-index` | int | `0` | 分屏下标（0-based，按 session_id 排序） |
| `--timeout-seconds` | int | `30` | 等待命令完成的超时（秒） |
| `--json` | flag | off | 以 JSON 格式输出结果（含 `stdout`、`exit_info` 等字段） |

### 会话选择优先级（从高到低）

1. `--session-id`（直接绑定 session）
2. `--window-index` + `--tab-num`（多窗口+多标签）
3. `--tab-num`（单窗口多标签，**最常用**）
4. 不传任何参数 → 默认 ⌘1（1号标签）

---

## 典型用法示例

### 基础用法

```bash
ITERM=~/.cursor/skills/iterm2-exec/.venv/bin/python
SCRIPT=~/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py

# 向 1号标签（默认）执行
$ITERM $SCRIPT run --command 'echo OK'
$ITERM $SCRIPT run --command 'pwd'
$ITERM $SCRIPT run --command 'ls -lh ~/data/'

# 向 2号标签执行
$ITERM $SCRIPT run --tab-num 2 --command 'hostname'
$ITERM $SCRIPT run --tab-num 2 --command 'df -h'
$ITERM $SCRIPT run --tab-num 2 --command 'ps aux | grep python'

# 向 3号标签执行，60秒超时
$ITERM $SCRIPT run --tab-num 3 --command 'python3 train.py' --timeout-seconds 600
```

### 长命令 / 需要更长超时

```bash
# pip 安装（120秒）
$ITERM $SCRIPT run --tab-num 2 --command 'pip install pandas matplotlib scikit-learn' --timeout-seconds 120

# 运行测试（300秒）
$ITERM $SCRIPT run --tab-num 1 --command 'pytest tests/' --timeout-seconds 300

# 查大目录（60秒）
$ITERM $SCRIPT run --command 'find ~/data -name "*.parquet" | xargs ls -lh' --timeout-seconds 60
```

### 多窗口场景

```bash
# 第 2 个窗口的 1号标签
$ITERM $SCRIPT run --window-index 1 --tab-num 1 --command 'pwd'

# 第 2 个窗口的 3号标签，有分屏时指定第 0 个分屏
$ITERM $SCRIPT run --window-index 1 --tab-num 3 --split-index 0 --command 'ls'
```

### SSH / Docker 远端会话（哨兵模式）

```bash
# 向已经 ssh 到远端服务器的 2号标签发命令（无需在服务器上安装任何工具）
$ITERM $SCRIPT run --tab-num 2 --command 'cat /etc/os-release'
$ITERM $SCRIPT run --tab-num 2 --command 'free -h && df -h'
$ITERM $SCRIPT run --tab-num 2 --command 'systemctl status nginx'

# docker exec 的 tab 也一样用
$ITERM $SCRIPT run --tab-num 3 --command 'env | grep JAVA'
```

### JSON 格式输出

```bash
# 输出结构：{"stdout": "...", "exit_info": {...}, "mode": "sentinel/si"}
$ITERM $SCRIPT run --command 'ls -la' --json
```

---

## 标签编号说明

用户说的自然语言与参数的对应关系：

| 用户自然语言 | 对应参数 |
|------------|---------|
| 1号标签 / ⌘1 / 第一个标签 / 1#标签 | `--tab-num 1` |
| 2号标签 / ⌘2 / 第二个标签 / 2#标签 | `--tab-num 2` |
| N 号标签 | `--tab-num N` |

指定的标签若不是当前活动标签，**CLI 会自动切换过去**再执行，无需用户手动切换。

---

## 故障排查

| 现象 | 可能原因 | 解决方法 |
|------|---------|---------|
| `ConnectionRefusedError` 或无法连接 | iTerm2 未运行或未开启 Python API | 打开 iTerm2 → Preferences → General → Magic → 勾选 Enable Python API |
| 首次运行无输出 | iTerm2 弹出授权弹框未点击 | 切到 iTerm2，点击弹框里的「允许」 |
| 提示"未检测到 Shell Integration，使用哨兵模式" | 本地 shell 未安装 SI（SSH 会话则正常） | 如需 SI 模式：iTerm2 菜单 → Shell Integration → Install Shell Integration，重开 shell |
| 哨兵模式输出内容偏差 | 命令执行期间调整了终端窗口大小 | 等命令完成后再 resize |
| 超时无输出（哨兵模式） | 命令阻塞 / 远端无 `printf`（极罕见） | 增加 `--timeout-seconds` |
| 超时无输出（SI 模式） | 命令阻塞 / SI 刚装完未生效 | 重开 shell 后重试；或加大 `--timeout-seconds` |
| 本地 shell 被判为哨兵模式 | SI 缓存是进程级，刚装完 SI 需重开 shell | 重开 shell 后重试 |

---

## 文件结构

```
iterm2-exec/
├── SKILL.md              # Agent 触发规则与行为规范
├── README.md             # 本文档（用户 & 开发参考）
└── scripts/
    └── iterm2_exec.py    # CLI 实现（基于 iTerm2 Python API）
```

---

## 注意事项

- **只传绝对路径**：避免工作目录变化导致找不到脚本
- **不在参数里传密码/Token**：密码请让用户在 iTerm2 中手动输入，或使用 ssh-agent
- **长命令必须加 `--timeout-seconds`**：默认 30 秒，运行时间长的命令（如模型训练、pip 安装）需显式设置
- **不保证拿到 exit code**：只截取文本输出，不保证进程的真实退出码
- **生产环境谨慎操作**：该 tab 可能连接线上服务器，执行破坏性命令（rm、覆盖文件等）前需与用户确认
