"""
jupyterm_files — 文件浏览器操作 CLI 命令。

包含：
  - cmd_file_new_dir  新建目录
  - cmd_file_new      创建新文件（notebook / python / text / markdown）
  - cmd_file_open     通过 CDP 双击文件浏览器侧边栏打开文件
  - cmd_file_list     列出目录内容（辅助命令）
"""

import sys

from jupyterm_cdp import file_browser_open_file
from jupyterm_api import create_directory, create_file, list_directory
from jupyterm_config import load_config


# 文件类型 → 扩展名映射（用于验证路径）
_TYPE_EXTS = {
    "notebook": ".ipynb",
    "python": ".py",
    "text": ".txt",
    "markdown": ".md",
}


def cmd_file_new_dir(args):
    """通过 Contents API 新建目录。

    若路径含多级，逐级创建（父目录不存在时自动创建）。

    @example jupyterm file-new-dir "data/results"
    """
    cfg = load_config()
    path = args.path.strip("/")

    try:
        create_directory(cfg, path)
        print(f"[jupyterm] 已创建目录: {path}")
    except Exception as e:
        print(f"[jupyterm] 创建目录失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_file_new(args):
    """通过 Contents API 创建新文件，支持 notebook / python / text / markdown。

    创建后文件存在于 JupyterLab 服务端，需用 jupyterm file-open 打开。

    @example jupyterm file-new "work/demo.ipynb" --type notebook
    @example jupyterm file-new "work/script.py" --type python
    @example jupyterm file-new "notes.md" --type markdown
    """
    cfg = load_config()
    path = args.path
    file_type = getattr(args, "type", "notebook")

    # 根据扩展名自动推断类型（若未指定 --type）
    if not file_type:
        lower = path.lower()
        if lower.endswith(".ipynb"):
            file_type = "notebook"
        elif lower.endswith(".py"):
            file_type = "python"
        elif lower.endswith(".md"):
            file_type = "markdown"
        else:
            file_type = "text"

    if file_type not in _TYPE_EXTS:
        print(f"[jupyterm] 不支持的文件类型 {file_type!r}，"
              f"可选: {', '.join(_TYPE_EXTS)}", file=sys.stderr)
        sys.exit(1)

    try:
        result = create_file(cfg, path, file_type)
        actual_path = result.get("path", path)
        print(f"[jupyterm] 已创建文件: {actual_path}")
        print(f"  类型: {file_type}")
        print(f"  提示: 使用 jupyterm file-open \"{actual_path}\" 在浏览器中打开")
    except Exception as e:
        print(f"[jupyterm] 创建文件失败: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_file_open(args):
    """通过 CDP 模拟双击文件浏览器侧边栏打开文件（所见即所得）。

    实现步骤：
      1. 确保 File Browser 面板可见
      2. 若路径含多级，依次双击目录展开
      3. 双击目标文件打开

    @example jupyterm file-open "work/demo.ipynb"
    @example jupyterm file-open "data/results/report.md"
    """
    path = args.path

    ok = file_browser_open_file(path)
    if ok:
        print(f"[jupyterm] 已在浏览器中打开: {path}")
    else:
        print(f"[jupyterm] 打开失败（CDP 不可用或文件不在文件浏览器中可见）: {path}",
              file=sys.stderr)
        print("  提示: 请确保文件存在且 JupyterLab 文件浏览器侧边栏已展开",
              file=sys.stderr)
        sys.exit(1)


def cmd_file_list(args):
    """列出目录内容（辅助命令）。

    @example jupyterm file-list           # 列出根目录
    @example jupyterm file-list "work"    # 列出 work 目录
    """
    cfg = load_config()
    path = getattr(args, "path", "") or ""

    try:
        items = list_directory(cfg, path)
        label = path or "(root)"
        print(f"目录: {label}  ({len(items)} 项)")
        for item in sorted(items, key=lambda x: (x.get("type") != "directory", x.get("name", ""))):
            item_type = item.get("type", "file")
            name = item.get("name", "")
            size = item.get("size")
            size_str = f"  {size} bytes" if size is not None and item_type != "directory" else ""
            icon = "📁" if item_type == "directory" else "📄"
            print(f"  {icon} {name}{size_str}")
    except Exception as e:
        print(f"[jupyterm] 列出目录失败: {e}", file=sys.stderr)
        sys.exit(1)
