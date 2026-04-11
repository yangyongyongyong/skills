# cursor-skills

个人 [Cursor Agent Skills](https://cursor.com/docs/skills) 合集——用于扩展 Cursor Agent 在本机的自动化能力。

每个 Skill 是一个独立目录，包含一份 `SKILL.md`（Agent 行为指引）和可选的辅助脚本。Agent 会根据上下文自动判断是否触发对应 Skill。

## 安装

将本仓库克隆或复制到 `~/.cursor/skills/`：

```bash
git clone https://github.com/yangyongyongyong/skills ~/.cursor/skills
```

若目录已存在，将各 Skill 子目录移入即可，Cursor 会自动发现。

---

## Skills 一览

### `iterm2-exec` — 通过 iTerm2 在本机/远端会话执行命令

**核心场景**：你已在 iTerm2 里打开了一个 SSH 会话连接到服务器，希望让 Cursor Agent 直接向该会话发送命令并取回输出——相当于让 Agent 操作线上环境，无需额外建立 SSH 连接。

**工作原理**：借助 [iTerm2 Python API](https://iterm2.com/python-api/) 连接本机运行中的 iTerm2，通过 Shell Integration 的 Prompt 元数据精确截取「本次命令」的输出区间，杜绝全屏抓取的噪声与歧义。

**支持的场景**：

| 场景 | 说明 |
|------|------|
| 默认 ⌘1 | 不传任何选择器，自动操作 iTerm2 第一个标签（⌘1） |
| 按 ⌘N 编号 | `--tab-num 2` 即 ⌘2，与 iTerm2 快捷键一一对应，最直观 |
| 按低级索引 | `--window-index` / `--tab-index` / `--split-index` 精确到某个分屏（0 起） |
| 按 Session ID | `--session-id` 最精确，适合脚本固定绑定某个会话 |
| 多行命令 | `for` 循环、`if` 语句等含 `\n` 的命令，自动转换为终端回车 |
| 特殊字符 | 双引号、单引号、`$变量`、管道、反引号、中文/emoji 均可透传 |
| JSON 输出 | `--json` 返回 `{ stdout, session_id }` 结构，方便 Agent 解析 |
| 阻塞命令识别 | 超时（`--timeout-seconds`）后自动发送 `Ctrl+C`，shell 立即恢复可用 |
| SSH 远端操作 | 目标会话已 SSH 到服务器时，命令在远端执行，输出返回本地 |

**前置要求**：

- macOS，iTerm2 已运行并在 Preferences → General → Magic 中开启 **Python API**
- 目标会话（本机或 SSH 远端）已安装 [iTerm2 Shell Integration](https://iterm2.com/documentation-shell-integration.html)

**快速开始**：

```bash
# 1. 创建虚拟环境并安装依赖（仅首次）
python3 -m venv ~/.cursor/skills/iterm2-exec/.venv
~/.cursor/skills/iterm2-exec/.venv/bin/pip install iterm2

# 2. 验证
~/.cursor/skills/iterm2-exec/.venv/bin/python \
  ~/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py run \
  --command 'echo OK'
```

**局限**：输出通过 prompt 区间截取，不等价于远端进程的真实 exit code；无 Shell Integration 时直接报错而非降级为全屏抓取。

---

## 目录结构

```
~/.cursor/skills/
├── README.md                 # 本文件
└── iterm2-exec/
    ├── SKILL.md              # Agent 触发规则、调用示例、安全约束
    └── scripts/
        └── iterm2_exec.py    # CLI 实现（单文件，基于 iTerm2 Python API）
```

---

## 贡献 / 添加新 Skill

1. 在本目录新建子目录，名称用 `kebab-case`。
2. 在其中创建 `SKILL.md`，frontmatter 包含 `name` 与 `description`。
3. 将辅助脚本放在 `scripts/` 子目录。
4. 更新本 README 的「Skills 一览」表格。

Cursor Skill 格式参考：[Agent Skills 文档](https://cursor.com/docs/skills)。

---

## License

MIT
