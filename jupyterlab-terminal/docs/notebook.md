# Notebook 命令参考

> CLI 路径：`~/.cursor/skills/jupyterlab-terminal/jupyterm`
> 以下示例中 `$J` = `~/.cursor/skills/jupyterlab-terminal/jupyterm`
>
> **所有操作均为 WYSIWYG（所见即所得）**：通过 CDP 直接操控浏览器 UI，浏览器内容实时更新。

---

## Cell 定位方式（--cell 参数）

所有需要定位 cell 的命令都支持以下三种 `--cell` 写法：

| 格式 | 示例 | 说明 |
|------|------|------|
| 整数（0-based） | `--cell 0` | 第 1 个 cell，不依赖执行状态 |
| `[N]` 执行编号 | `--cell "[3]"` | DOM 中 prompt 显示为 `[3]:` 的 cell（只有执行过的 cell 才有编号，每次执行后编号会变） |
| `active` | `--cell active` | 当前浏览器中高亮的 cell（nb-add 后新 cell 自动激活） |

> **注意**：`[N]` 依赖浏览器前端的实际显示状态，与文件中保存的执行编号可能不一致（用户执行了但未保存时）。CLI 操作前会自动触发保存以同步状态。

---

## Notebook 定位方式（首个位置参数）

| 格式 | 示例 | 说明 |
|------|------|------|
| `nb#N` | `nb-read "nb#1"` | 浏览器从左到右第 N 个可见 .ipynb tab |
| 路径 | `nb-read "work/test.ipynb"` | 按 JupyterLab 中的相对路径 |
| `-n path` | `nb-read -n work/test.ipynb` | 同路径，优先级最高 |
| 不指定 | `nb-read` | 自动选浏览器当前活跃的 .ipynb tab（最常用） |

---

## nb-list — 列出可见 Notebook tab

```bash
$J nb-list
# 输出示例：
#   nb#1  Untitled1.ipynb  <-- active
#   nb#2  analysis.ipynb
#   nb#3  work/report.ipynb
```

---

## nb-read — 读取 Notebook 内容

读取所有 cell 的类型、源码、已有输出（从 REST API 获取文件内容）。

```bash
# 读取当前活跃的 notebook（最常用）
$J nb-read

# 按 tab 位置
$J nb-read "nb#1"
$J nb-read "nb#2"

# 按路径
$J nb-read "work/analysis.ipynb"
$J nb-read -n "myproject/data_explore.ipynb"
```

**输出格式**：

```
Notebook: analysis.ipynb  (5 cells)
============================================================
[0] code  [3]
----------------------------------------
  import pandas as pd
  df = pd.read_parquet('./data.parquet')
  --- output ---
  shape: (1000, 5)

[1] markdown  [ ]
----------------------------------------
  # Data Overview

[2] code  [ ]
----------------------------------------
  (empty)
```

---

## nb-cell-read — 精确读取单个或全部 Cell

与 `nb-read` 相比更灵活：支持 `--cell` 精确定位单个 cell，支持 `--json` 结构化输出（适合 Agent 程序化解析），支持 `--input-only` / `--output-only` 过滤。

底层使用 `nbformat` 库解析（支持 `image/png`、`application/json` 等所有 output 类型），比手写解析更稳定。

```bash
# 读取全部 cell（含输入和输出）
$J nb-cell-read

# 读取单个 cell（三种定位方式）
$J nb-cell-read --cell 0            # 0-based 索引
$J nb-cell-read --cell "[3]"        # 执行编号 [3]
$J nb-cell-read --cell active       # 当前活跃 cell

# 指定 notebook
$J nb-cell-read "nb#1" --cell "[5]"
$J nb-cell-read "work/analysis.ipynb" --cell 2

# 只看输入（source）
$J nb-cell-read --cell "[3]" --input-only

# 只看输出（outputs）
$J nb-cell-read --cell "[3]" --output-only

# JSON 格式输出（Agent 解析推荐）
$J nb-cell-read --cell "[3]" --json
$J nb-cell-read --json              # 全部 cell 的 JSON 数组
```

**默认格式输出**：

```
cell[5]  code  [17]
--- source ---
  print(999)
--- output ---
  999
```

**--json 格式输出（单 cell）**：

```json
{
  "index": 5,
  "cell_type": "code",
  "exec_count": 17,
  "source": "print(999)",
  "outputs": ["999"]
}
```

**--json 格式输出（全部 cell，不传 --cell）**：

```json
[
  {"index": 0, "cell_type": "code", "exec_count": 9, "source": "import pandas as pd\n...", "outputs": ["shape: (1884790, 6)\n..."]},
  {"index": 1, "cell_type": "markdown", "exec_count": null, "source": "# Title", "outputs": []},
  ...
]
```

**image/png 输出标注**：

```
cell[2]  code  [4]
--- source ---
  plt.plot([1,2,3])
  plt.show()
--- output ---
  [image/png output]
```

---

## nb-edit — 修改 Cell 源码

等价于用户手动在 cell 里编辑内容后按 Ctrl+S 保存。

```bash
# 修改 cell[0]（0-based 索引）
$J nb-edit --cell 0 --source "print('hello world')"

# 修改执行编号为 [3] 的 cell
$J nb-edit --cell "[3]" --source "x = 42"

# 修改当前活跃的 cell
$J nb-edit --cell active --source "print('updated')"

# 多行代码（shell heredoc 方式）
$J nb-edit --cell 0 --source "$(cat <<'EOF'
import pandas as pd
import numpy as np

df = pd.read_parquet('./data.parquet')
print(df.head(3))
EOF
)"

# 修改指定路径 notebook 的 cell
$J nb-edit "work/analysis.ipynb" --cell 2 --source "df.describe()"

# 指定 nb#N tab
$J nb-edit "nb#2" --cell 0 --source "# New cell content"
```

---

## nb-exec — 执行 Cell

等价于点击工具栏 Run 按钮，输出同时显示在浏览器和 CLI。

```bash
# 执行当前活跃 notebook 的 cell[0]
$J nb-exec --cell 0

# 执行执行编号为 [5] 的 cell
$J nb-exec --cell "[5]"

# 执行当前活跃的 cell（nb-add 后常用）
$J nb-exec --cell active

# 执行所有 code cell（按顺序）
$J nb-exec --all

# 加大超时（默认 60s，长计算时必须设置）
$J nb-exec --cell 2 --timeout 120
$J nb-exec --all --timeout 300

# 指定 notebook
$J nb-exec "nb#1" --cell 3
$J nb-exec "work/analysis.ipynb" --cell 0 --timeout 60
```

**输出格式**：

```
==================================================
[0] code
  df = pd.read_parquet('./data.parquet')
  print(df.head(3))
------------------------------
   dev_id  dp_value_new  ...
0  abc123         258.0
1  abc123         492.0
2  abc123         310.0
```

---

## nb-add — 插入新 Cell

等价于点击工具栏 ＋ 按钮，新 cell 插入后自动成为 active。

```bash
# 在当前活跃 cell 下方插入（最常用）
$J nb-add

# 在 cell[2] 下方插入
$J nb-add --cell 2

# 在执行编号 [5] 的 cell 下方插入
$J nb-add --cell "[5]"

# 在 cell[2] 上方插入
$J nb-add --cell 2 --above

# 在执行编号 [5] 的 cell 上方插入
$J nb-add --cell "[5]" --above

# 插入后立即写入内容（两步操作）
$J nb-add --cell "[3]"
$J nb-edit --cell active --source "print('new cell')"

# 插入后写入并执行（三步操作）
$J nb-add --cell "[3]"
$J nb-edit --cell active --source "x = 1 + 1; print(x)"
$J nb-exec --cell active
```

---

## nb-del — 删除单个 Cell

删除后自动触发保存，无需手动保存。

```bash
# 按 0-based 索引删除
$J nb-del --cell 0
$J nb-del --cell 5

# 按执行编号删除（推荐，用户视角更直观）
$J nb-del --cell "[3]"
$J nb-del --cell "[17]"

# 删除当前活跃的 cell
$J nb-del --cell active

# 指定 notebook
$J nb-del "nb#1" --cell "[5]"
```

> **安全提示**：删除操作不可恢复，删前可先 `nb-read` 确认 cell 内容。

---

## nb-save — 保存 Notebook

```bash
# 保存当前活跃 notebook
$J nb-save

# 保存指定 notebook
$J nb-save "nb#1"
$J nb-save "work/analysis.ipynb"
```

---

## nb-interrupt — 中断 Kernel

等价于点击工具栏 ■ 按钮，中断正在执行的 cell。

```bash
$J nb-interrupt
$J nb-interrupt "nb#1"
```

---

## nb-restart — 重启 Kernel

等价于点击工具栏 ↺ 按钮，重启 kernel（自动确认弹窗）。

```bash
# 重启当前 notebook 的 kernel
$J nb-restart

# 重启指定 notebook 的 kernel
$J nb-restart "nb#1"
$J nb-restart "work/analysis.ipynb"
```

---

## nb-restart-all — 重启 Kernel 并执行全部 Cell

等价于点击工具栏 ↠ 按钮（Restart Kernel and Run All Cells）。

```bash
$J nb-restart-all
$J nb-restart-all "nb#1"
```

---

## nb-cut / nb-copy / nb-paste — 剪切 / 复制 / 粘贴

```bash
# 剪切 cell[2]
$J nb-cut --cell 2
$J nb-cut --cell "[5]"

# 复制 cell[1]
$J nb-copy --cell 1
$J nb-copy --cell "[3]"

# 粘贴到当前 cell 下方
$J nb-paste
$J nb-paste "nb#1"

# 典型用法：把 cell[1] 移到 cell[4] 下方
$J nb-cut --cell 1
$J nb-paste        # 粘贴到当前选中 cell 下方
```

---

## nb-move — 上移 / 下移 Cell

等价于 cell action toolbar 的 ↑ / ↓ 按钮。

```bash
# 上移 cell[3]
$J nb-move --cell 3 --up
$J nb-move --cell "[5]" --up

# 下移 cell[2]
$J nb-move --cell 2 --down
$J nb-move --cell "[3]" --down

# 下移多步（一次移动 3 格）
$J nb-move --cell 0 --down --steps 3
$J nb-move --cell "[1]" --up --steps 2
```

---

## nb-cell-type — 切换 Cell 类型

等价于工具栏 Code / Markdown / Raw 下拉菜单。

```bash
# 改为 markdown
$J nb-cell-type --cell 2 --type markdown
$J nb-cell-type --cell "[3]" --type markdown

# 改为 code
$J nb-cell-type --cell 1 --type code

# 改为 raw
$J nb-cell-type --cell 0 --type raw

# 配合 nb-edit：先改类型再写内容
$J nb-cell-type --cell "[4]" --type markdown
$J nb-edit --cell "[4]" --source "# Section Title"
```

---

## nb-delete-all — 清空 Notebook 所有 Cell

> ⚠️ **危险操作，不可恢复**，必须加 `--confirm` 才能执行。

```bash
# 必须加 --confirm，防止误操作
$J nb-delete-all --confirm
$J nb-delete-all "nb#1" --confirm
```

---

## 常见工作流片段

### 在指定位置插入并执行代码

```bash
# [5] 下面新增 cell，写入 print(999) 并执行
$J nb-add --cell "[5]"
$J nb-edit --cell active --source "print(999)"
$J nb-exec --cell active
```

### 修改已有 cell 并重新执行

```bash
# 把执行编号 [3] 的 cell 改为新代码并执行
$J nb-edit --cell "[3]" --source "df.groupby('name').mean()"
$J nb-exec --cell "[3]"
```

### 读取输出确认执行结果

```bash
# 执行后读取 notebook 查看所有输出
$J nb-exec --cell 0
$J nb-read | grep -A 10 "^\[0\]"
```

### 批量清空重跑

```bash
# 删除所有 cell，重新写入，全部执行
$J nb-delete-all "nb#1" --confirm
$J nb-add                          # 新建 cell[0]
$J nb-edit --cell 0 --source "import pandas as pd; df = pd.read_csv('./data.csv'); print(df.shape)"
$J nb-exec --cell 0
```
