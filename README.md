# cursor-skills

个人 [Cursor Agent Skills](https://cursor.com/docs/skills) 合集——用于扩展 Cursor Agent 在本机的自动化能力。

每个 Skill 是一个独立目录，包含一份 `SKILL.md`（Agent 行为指引）和可选的辅助脚本。Agent 会根据上下文自动判断是否触发对应 Skill。

## 安装与使用

Cursor、Codex、Claude Code 均支持相同的 SKILL.md 格式，使用步骤完全一致，**仅安装目录不同**：

| 工具 | Skills 目录 |
|------|------------|
| [Cursor](https://cursor.com) | `~/.cursor/skills/` |
| [Codex CLI](https://github.com/openai/codex) | `~/.codex/skills/` |
| [Claude Code](https://docs.anthropic.com/claude/docs/claude-code) | `~/.claude/skills/` |

**Skills 目录通常已有其他内容，请勿直接 `git clone` 到该目录（会报错或覆盖）。** 推荐按下方任一方式安装：

### 方式一：只复制 Skill 子目录（最安全，推荐）

```bash
# 以 Cursor 为例，其他工具替换目录即可
SKILLS_DIR=~/.cursor/skills   # Codex: ~/.codex/skills  Claude: ~/.claude/skills

git clone https://github.com/yangyongyongyong/skills /tmp/cursor-skills-repo
cp -r /tmp/cursor-skills-repo/iterm2-exec "$SKILLS_DIR/"
rm -rf /tmp/cursor-skills-repo
```

### 方式二：克隆到独立目录，再软链接各 Skill

```bash
git clone https://github.com/yangyongyongyong/skills ~/projects/cursor-skills

# 按需链接到各工具的 Skills 目录
ln -s ~/projects/cursor-skills/iterm2-exec ~/.cursor/skills/iterm2-exec
# ln -s ~/projects/cursor-skills/iterm2-exec ~/.codex/skills/iterm2-exec
```

方式二的好处：后续 `git pull` 一次即可更新所有工具的 Skill。

### 方式三：目录为空时直接克隆

仅在目录**不存在或为空**时适用：

```bash
git clone https://github.com/yangyongyongyong/skills ~/.cursor/skills
```

---

## Skills 一览

### `iterm2-exec` — 通过 iTerm2 在本机/远端会话执行命令

让 Cursor Agent 直接向 iTerm2 中已有的标签页（包括 SSH、docker exec 等远端会话）发送命令并取回输出，无需额外建立连接。

**核心特性**：

- **智能双模式**：自动探测目标会话是否安装了 iTerm2 Shell Integration
  - 本地 shell（已装 SI）→ 纯 SI 模式：直发裸命令，`output_range` 精确截取，完全免疫 resize
  - SSH / Docker / 未装 SI → 哨兵模式：不可见 Custom Escape Sequence 检测完成，远端只需有 `printf`，无需安装任何工具
- **标签自动切换**：`--tab-num N` 对应 ⌘N，指定非当前标签会自动切过去
- **阻塞保护**：超时自动发 `Ctrl+C`，shell 立即恢复

**前置要求**：macOS，iTerm2 已运行并开启 Python API（Preferences → General → Magic）。

**快速开始**：

```bash
# 安装依赖（仅首次）
python3 -m venv ~/.cursor/skills/iterm2-exec/.venv
~/.cursor/skills/iterm2-exec/.venv/bin/pip install iterm2

# 向默认标签（⌘1）发命令
~/.cursor/skills/iterm2-exec/.venv/bin/python \
  ~/.cursor/skills/iterm2-exec/scripts/iterm2_exec.py run \
  --command 'echo OK'
```

完整参数说明、执行模式细节、故障排查见 [`iterm2-exec/SKILL.md`](iterm2-exec/SKILL.md)。

---

## 目录结构

```
~/.cursor/skills/
├── README.md
└── iterm2-exec/
    ├── SKILL.md              # Agent 触发规则、调用示例、安全约束
    └── scripts/
        └── iterm2_exec.py    # CLI 实现（单文件，基于 iTerm2 Python API）
```

---

## 添加新 Skill

1. 新建 `kebab-case` 命名的子目录。
2. 创建 `SKILL.md`，frontmatter 包含 `name` 与 `description`。
3. 辅助脚本放在 `scripts/` 子目录。
4. 更新本 README 的「Skills 一览」。

格式参考：[Cursor Agent Skills 文档](https://cursor.com/docs/skills)。

---

## License

MIT
