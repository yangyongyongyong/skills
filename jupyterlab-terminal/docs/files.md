# 文件浏览器命令参考

> CLI 路径：`~/.cursor/skills/jupyterlab-terminal/jupyterm`
> 以下示例中 `$J` = `~/.cursor/skills/jupyterlab-terminal/jupyterm`
>
> 所有文件操作均通过 CDP 操控 JupyterLab 文件浏览器 UI（WYSIWYG），或通过 REST API 创建文件。

---

## file-list — 列出目录内容

```bash
# 列出根目录
$J file-list

# 列出指定目录
$J file-list "work"
$J file-list "myproject/data"
$J file-list "."          # 当前目录（同根目录）
```

**输出示例**：

```
work/
  analysis.ipynb   notebook  2024-01-15
  data.parquet     file      2024-01-14
  results/         directory
```

---

## file-new-dir — 新建目录

```bash
# 新建单层目录
$J file-new-dir "data"
$J file-new-dir "myproject"

# 新建多级目录（逐层自动创建）
$J file-new-dir "data/results"
$J file-new-dir "projects/demo/output"
$J file-new-dir "work/2024/q1"
```

---

## file-new — 创建新文件

文件类型按扩展名自动推断，也可用 `--type` 显式指定。

```bash
# 创建 notebook（.ipynb）
$J file-new "analysis.ipynb"
$J file-new "work/demo.ipynb" --type notebook
$J file-new "myproject/explore.ipynb"

# 创建 Python 脚本（.py）
$J file-new "script.py"
$J file-new "work/utils.py" --type python

# 创建 Markdown 文档（.md）
$J file-new "README.md"
$J file-new "docs/notes.md" --type markdown

# 创建普通文本文件
$J file-new "config.txt" --type text
$J file-new "data/schema.json" --type text

# 创建后查看提示
# 输出示例：
#   [jupyterm] 已创建文件: work/demo.ipynb
#   类型: notebook
#   提示: 使用 jupyterm file-open "work/demo.ipynb" 在浏览器中打开
```

---

## file-open — 在浏览器中打开文件

通过 CDP 模拟双击文件浏览器侧边栏打开文件，等价于用户手动双击。

```bash
# 打开 notebook
$J file-open "analysis.ipynb"
$J file-open "work/demo.ipynb"
$J file-open "myproject/explore.ipynb"

# 打开 Python 文件（在 JupyterLab 编辑器中打开）
$J file-open "work/utils.py"

# 打开 Markdown 文件
$J file-open "README.md"

# 打开子目录中的文件
$J file-open "data/results/report.ipynb"
```

**内部流程**：
1. 检查文件浏览器侧边栏是否可见，不可见则点击展开
2. 点击"Refresh"刷新文件列表
3. 逐层导航到目标目录
4. 双击目标文件

**打开后处理 Kernel 选择弹窗**：首次打开新建的 notebook 时会弹出"Select Kernel"对话框，CLI 会自动点击"Select"确认默认 kernel。

---

## 完整创建 → 打开 → 编写 → 执行工作流

```bash
JUPYTERM=~/.cursor/skills/jupyterlab-terminal/jupyterm

# 1. 新建项目目录
$JUPYTERM file-new-dir "myproject"

# 2. 创建 notebook
$JUPYTERM file-new "myproject/analysis.ipynb"

# 3. 在浏览器中打开（自动处理 kernel 选择弹窗）
$JUPYTERM file-open "myproject/analysis.ipynb"

# 4. 确认 notebook 已出现在 tab 列表
$JUPYTERM nb-list
#   nb#1  analysis.ipynb  <-- active

# 5. 写入代码并执行
$JUPYTERM nb-edit --cell 0 --source "print('Hello from new notebook!')"
$JUPYTERM nb-exec --cell 0
```

---

## 常见场景

### 在容器里新建数据分析项目

```bash
$J file-new-dir "analysis_20240115"
$J file-new "analysis_20240115/01_explore.ipynb"
$J file-new "analysis_20240115/02_clean.ipynb"
$J file-open "analysis_20240115/01_explore.ipynb"
```

### 查看容器中有哪些 parquet 文件

```bash
$J file-list "."
# 配合 terminal exec 列出文件大小
$J exec "#1 find . -name '*.parquet' -exec ls -lh {} \;"
```
