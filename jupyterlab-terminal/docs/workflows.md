# 典型工作流 & 案例

> CLI 路径：`~/.cursor/skills/jupyterlab-terminal/jupyterm`
> 以下示例中 `$J` = `~/.cursor/skills/jupyterlab-terminal/jupyterm`

---

## 工作流 1：新建 Notebook 做数据探查

适用：拿到一个新 parquet/csv 文件，快速了解数据结构和分布。

```bash
# 1. 新建并打开 notebook
$J file-new "explore.ipynb"
$J file-open "explore.ipynb"

# 2. cell[0]：读取数据 + 基本信息
$J nb-edit --cell 0 --source "$(cat <<'EOF'
import pandas as pd
df = pd.read_parquet('./data.parquet')
print('shape:', df.shape)
print('\ndtypes:\n', df.dtypes)
print('\nmissing:\n', df.isnull().sum())
df.head(3)
EOF
)"
$J nb-exec --cell 0

# 3. 在 [1] 下插入 cell：统计描述
$J nb-add --cell "[1]"
$J nb-edit --cell active --source "df.describe()"
$J nb-exec --cell active

# 4. 再插入 cell：某列分布折线图
$J nb-add --cell active
$J nb-edit --cell active --source "$(cat <<'EOF'
import matplotlib.pyplot as plt
col = df.select_dtypes(include='number').columns[0]
plt.figure(figsize=(8, 3))
plt.plot(df[col].head(50).values)
plt.title(f'{col} - first 50 rows')
plt.tight_layout()
plt.savefig('plot.png', dpi=120)
plt.show()
EOF
)"
$J nb-exec --cell active

# 5. 保存
$J nb-save
```

---

## 工作流 2：修复并重跑指定 Cell

适用：某个 cell 报错或结果不对，需要修改代码并重新执行。

```bash
# 1. 查看当前 notebook 状态
$J nb-read

# 2. 找到出错的 cell（假设是 [7]），修改代码
$J nb-edit --cell "[7]" --source "result = df.groupby('name')['value'].mean().reset_index()"

# 3. 重新执行该 cell
$J nb-exec --cell "[7]"

# 4. 查看结果（读取输出）
$J nb-read | grep -A 15 "^\[4\]"   # [7] 在文件中是 cell[4]
```

---

## 工作流 3：在 Terminal 验证环境并写入 Notebook

适用：先在 Terminal 快速验证库是否可用，再写到 notebook 里正式执行。

```bash
# 1. Terminal 验证环境
$J exec "#1 python3 -c 'import pandas, matplotlib, sklearn; print(\"OK\")}'"
$J exec "#1 ls *.parquet"

# 2. 打开目标 notebook
$J nb-list   # 确认 nb#1 是目标文件

# 3. 写入完整代码
$J nb-edit "nb#1" --cell 0 --source "$(cat <<'EOF'
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler

df = pd.read_parquet('./data.parquet')
print(df.shape)
EOF
)"
$J nb-exec "nb#1" --cell 0
```

---

## 工作流 4：整理 Notebook 结构

适用：清理不需要的 cell、调整顺序、给 cell 加说明。

```bash
# 查看当前结构
$J nb-read

# 删除不需要的 cell（假设 [12] 是临时调试用的）
$J nb-del --cell "[12]"

# 在 cell[0] 上方插入标题 cell
$J nb-add --cell 0 --above
$J nb-cell-type --cell active --type markdown
$J nb-edit --cell active --source "# Data Analysis Report\n\n数据来源：data.parquet"

# 把某个 cell 移到更合适的位置（下移 2 格）
$J nb-move --cell 3 --down --steps 2

# 保存
$J nb-save
```

---

## 工作流 5：在已有 Notebook 中追加分析

适用：notebook 已有若干执行结果，在末尾追加新分析。

```bash
# 1. 读取当前 notebook，确认最后一个 cell 的执行编号
$J nb-read
# 假设最后是 [16]

# 2. 在 [16] 下新增 cell
$J nb-add --cell "[16]"
$J nb-edit --cell active --source "$(cat <<'EOF'
# 新增：按时间聚合分析
import matplotlib.pyplot as plt
daily = df.groupby('dt')['dp_value_new'].mean()
plt.figure(figsize=(10, 4))
daily.plot()
plt.title('Daily Average')
plt.tight_layout()
plt.savefig('daily_avg.png', dpi=120)
plt.show()
EOF
)"
$J nb-exec --cell active --timeout 60
```

---

## 工作流 6：批量执行多个 Notebook

适用：需要按顺序跑多个 notebook（数据管道）。

```bash
# 分别打开并执行每个 notebook
$J file-open "01_load.ipynb"
$J nb-exec --all --timeout 120

$J file-open "02_clean.ipynb"
$J nb-exec --all --timeout 120

$J file-open "03_analysis.ipynb"
$J nb-exec --all --timeout 300

# 最后查看结果
$J nb-read "03_analysis.ipynb"
```

---

## 工作流 7：快速调试 Terminal 脚本

适用：写了一个 shell 脚本需要在容器内测试。

```bash
# 在容器里创建脚本（通过 terminal exec 写文件）
$J exec "#1 cat > /tmp/check_env.sh << 'EOF'
#!/bin/bash
echo \"Python: \$(python3 --version)\"
echo \"Conda envs:\"
conda env list
echo \"Disk:\"
df -h /
EOF"

# 执行脚本
$J exec "#1 bash /tmp/check_env.sh"

# 或者用本地脚本文件
$J run /tmp/check_env.sh --timeout 30
```

---

## 常见错误处理

### Kernel 未启动

```bash
# 症状：nb-exec 报 "没有活跃的 Kernel"
# 解决：先打开 notebook，点击 Switch Kernel 选择一个
$J file-open "analysis.ipynb"
# 然后重试 nb-exec
$J nb-exec --cell 0
```

### 配置失效（token 过期 / Pod 重启）

```bash
# 症状：任何命令报认证错误或连接失败
# 解决：重新 setup（浏览器须切到 JupyterLab 标签）
$J setup
```

### Cell 编号 [N] 找不到

```bash
# 症状：nb-del/nb-edit --cell "[5]" 报错"未找到执行编号 [5]"
# 原因：该 cell 尚未执行（没有 [N] 编号），或编号已因重新执行而改变
# 解决方案 1：用 nb-read 查看当前执行编号
$J nb-read | grep '^\['
# 解决方案 2：用 0-based 索引代替
$J nb-del --cell 4   # 用索引而非 [N]
```

### nb-add 后 cell 位置不对

```bash
# 原因：before auto-save 后 DOM 中的执行编号变了
# 解决：CLI 操作前会自动 save，操作后用 nb-read 验证
$J nb-read | grep '^\['
```
