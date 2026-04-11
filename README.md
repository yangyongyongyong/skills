# cursor-skills

个人 [Cursor Agent Skills](https://cursor.com/docs/skills) 合集——用于扩展 Cursor Agent 在本机的自动化能力。

每个 Skill 是一个独立目录，包含一份 `SKILL.md`（Agent 行为指引）和可选的辅助脚本。Agent 会根据上下文自动判断是否触发对应 Skill。

## 安装

```bash
git clone https://github.com/yangyongyongyong/skills ~/.cursor/skills
```

若目录已存在，将各 Skill 子目录移入即可，Cursor 会自动发现。

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
