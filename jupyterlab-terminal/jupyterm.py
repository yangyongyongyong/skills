#!/usr/bin/env python3
"""
jupyterm — 通过 WebSocket 与 JupyterLab Terminal 交互的 CLI 工具。

使用方式：
  jupyterm setup                        # 自动从浏览器当前活动标签探测 token/URL
  jupyterm setup --url <url> --token <tok>  # 手动指定
  jupyterm exec "ls -la"                # 在 Terminal 中执行命令，返回输出
  jupyterm exec "#1 pwd"               # 指定浏览器第 1 个可见 Terminal
  jupyterm exec --timeout 60 "pip list" # 自定义超时（秒）
  jupyterm term-signal ctrl-c "#1"     # 向第 1 个 Terminal 发送 Ctrl+C
  jupyterm list                         # 列出所有 Terminal
  jupyterm create                       # 创建新 Terminal
  jupyterm run /path/to/script.sh       # 把文件内容作为多行命令执行
  jupyterm nb-list                      # 列出浏览器中可见的 notebook
  jupyterm nb-exec "nb#1" --cell 0     # 执行 notebook 第 1 个 cell
  jupyterm file-new-dir "data"          # 新建目录
  jupyterm file-new "data/demo.ipynb"   # 新建 notebook
  jupyterm file-open "data/demo.ipynb"  # 双击打开文件
"""

import argparse
import sys

from jupyterm_terminal import (
    cmd_setup,
    cmd_list,
    cmd_create,
    cmd_exec,
    cmd_run,
    cmd_term_signal,
)
from jupyterm_notebook import (
    cmd_nb_list,
    cmd_nb_read,
    cmd_nb_cell_read,
    cmd_nb_edit,
    cmd_nb_exec,
    cmd_nb_save,
    cmd_nb_interrupt,
    cmd_nb_restart,
    cmd_nb_restart_all,
    cmd_nb_add,
    cmd_nb_del,
    cmd_nb_cut,
    cmd_nb_copy,
    cmd_nb_paste,
    cmd_nb_move,
    cmd_nb_cell_type,
    cmd_nb_delete_all,
)
from jupyterm_files import (
    cmd_file_new_dir,
    cmd_file_new,
    cmd_file_open,
    cmd_file_list,
)


def main():
    """CLI 入口，注册所有子命令并分派执行。"""
    parser = argparse.ArgumentParser(
        prog="jupyterm",
        description="JupyterLab Terminal / Notebook / Files CLI"
    )
    sub = parser.add_subparsers(dest="subcommand", required=True)

    # ------------------------------------------------------------------
    # Terminal 命令
    # ------------------------------------------------------------------

    # setup
    p_setup = sub.add_parser("setup", help="保存 JupyterLab 连接配置")
    p_setup.add_argument("--url", default=None,
                         help="JupyterLab 页面 URL（手动指定，适合远程/域名部署）")
    p_setup.add_argument("--token", default=None,
                         help="JupyterLab 认证 token（与 --url 配合使用）")
    p_setup.add_argument("--cdp-ports", default=None,
                         help="自定义扫描的 CDP 端口，逗号分隔，如 9222,9223（默认扫描 9222-9230）")
    p_setup.set_defaults(func=cmd_setup)

    # list
    p_list = sub.add_parser("list", help="列出所有 Terminal")
    p_list.set_defaults(func=cmd_list)

    # create
    p_create = sub.add_parser("create", help="创建新 Terminal")
    p_create.set_defaults(func=cmd_create)

    # exec
    p_exec = sub.add_parser("exec", help="在 Terminal 中执行命令")
    p_exec.add_argument("command",
                        help='要执行的命令（支持 "#1 pwd" / "2# ls" 位置语法）')
    p_exec.add_argument("-t", "--terminal", default=None,
                        help="按 server name 指定 Terminal（优先级高于 #N）")
    p_exec.add_argument("--timeout", type=float, default=30.0,
                        help="等待输出的超时秒数（默认 30）")
    p_exec.set_defaults(func=cmd_exec)

    # run
    p_run = sub.add_parser("run", help="执行本地脚本文件")
    p_run.add_argument("file", help="脚本文件路径")
    p_run.add_argument("-t", "--terminal", type=int, default=None)
    p_run.add_argument("--timeout", type=float, default=120.0)
    p_run.set_defaults(func=cmd_run)

    # term-signal
    p_sig = sub.add_parser("term-signal",
                           help="向 Terminal 发送控制信号（仅 ctrl-c）")
    p_sig.add_argument("signal", choices=["ctrl-c"],
                       help="控制信号类型")
    p_sig.add_argument("target", nargs="?", default="",
                       help='可选目标："#1" / "2#"；不传表示当前活跃 Terminal')
    p_sig.add_argument("-t", "--terminal", default=None,
                       help="按 server name 指定 Terminal（优先级高于 #N）")
    p_sig.set_defaults(func=cmd_term_signal)

    # ------------------------------------------------------------------
    # Notebook 命令
    # ------------------------------------------------------------------

    # nb-list
    p_nb_list = sub.add_parser("nb-list", help="列出浏览器中可见的 notebook（.ipynb）")
    p_nb_list.set_defaults(func=cmd_nb_list)

    # nb-read
    p_nb_read = sub.add_parser("nb-read", help="读取 notebook 所有 cell（类型/源码/输出）")
    p_nb_read.add_argument("notebook", nargs="?", default=None,
                           help='notebook 路径或 nb#N 位置（如 "nb#1"、"work/test.ipynb"）')
    p_nb_read.add_argument("-n", default=None,
                           help="按路径指定 notebook（优先级高于位置）")
    p_nb_read.set_defaults(func=cmd_nb_read)

    # nb-cell-read
    p_nb_cr = sub.add_parser("nb-cell-read",
                              help="读取单个或全部 cell 的输入和输出（支持 --json）")
    p_nb_cr.add_argument("notebook", nargs="?", default=None,
                         help='notebook 路径或 nb#N 位置')
    p_nb_cr.add_argument("-n", default=None,
                         help="按路径指定 notebook（优先级高于位置）")
    p_nb_cr.add_argument("--cell", default=None,
                         help="[N]/active/0-based 索引；不传则读全部 cell")
    p_nb_cr.add_argument("--input-only", action="store_true",
                         help="只显示 source，不显示 outputs")
    p_nb_cr.add_argument("--output-only", action="store_true",
                         help="只显示 outputs，不显示 source")
    p_nb_cr.add_argument("--json", action="store_true",
                         help="以 JSON 格式输出（适合程序解析）")
    p_nb_cr.set_defaults(func=cmd_nb_cell_read)

    # nb-edit
    p_nb_edit = sub.add_parser("nb-edit", help="修改 notebook 指定 cell 源码并保存")
    p_nb_edit.add_argument("notebook", nargs="?", default=None,
                           help='notebook 路径或 nb#N 位置（如 "nb#1"、"work/test.ipynb"）')
    p_nb_edit.add_argument("-n", default=None,
                           help="按路径指定 notebook（优先级高于位置）")
    p_nb_edit.add_argument("--cell", required=True,
                           help="cell 位置：整数（0-based）或 [N]（浏览器执行编号，如 [2]）")
    p_nb_edit.add_argument("--source", required=True,
                           help="新的 cell 源码内容")
    p_nb_edit.set_defaults(func=cmd_nb_edit)

    # nb-exec
    p_nb_exec = sub.add_parser("nb-exec", help="执行 notebook cell（优先通过浏览器 UI Run 按钮）")
    p_nb_exec.add_argument("notebook", nargs="?", default=None,
                           help='notebook 路径或 nb#N 位置（如 "nb#1"、"work/test.ipynb"）')
    p_nb_exec.add_argument("-n", default=None,
                           help="按路径指定 notebook（优先级高于位置）")
    p_nb_exec.add_argument("--cell", default=None,
                           help="cell 位置：整数（0-based）或 [N]（浏览器执行编号），与 --all 互斥")
    p_nb_exec.add_argument("--all", action="store_true",
                           help="执行所有 code cell（顺序执行）")
    p_nb_exec.add_argument("--timeout", type=float, default=60.0,
                           help="每个 cell 的执行超时秒数（默认 60）")
    p_nb_exec.set_defaults(func=cmd_nb_exec)

    # nb-save
    p_nb_save = sub.add_parser("nb-save", help="保存 notebook（工具栏 💾）")
    p_nb_save.add_argument("notebook", nargs="?", default=None,
                           help='notebook 路径或 nb#N 位置')
    p_nb_save.add_argument("-n", default=None)
    p_nb_save.set_defaults(func=cmd_nb_save)

    # nb-interrupt
    p_nb_int = sub.add_parser("nb-interrupt", help="中断 kernel（工具栏 ■）")
    p_nb_int.add_argument("notebook", nargs="?", default=None)
    p_nb_int.add_argument("-n", default=None)
    p_nb_int.set_defaults(func=cmd_nb_interrupt)

    # nb-restart
    p_nb_restart = sub.add_parser("nb-restart", help="重启 kernel（工具栏 ↺）")
    p_nb_restart.add_argument("notebook", nargs="?", default=None)
    p_nb_restart.add_argument("-n", default=None)
    p_nb_restart.set_defaults(func=cmd_nb_restart)

    # nb-restart-all
    p_nb_rall = sub.add_parser("nb-restart-all", help="重启 kernel 并运行全部 cell（工具栏 ↠）")
    p_nb_rall.add_argument("notebook", nargs="?", default=None)
    p_nb_rall.add_argument("-n", default=None)
    p_nb_rall.set_defaults(func=cmd_nb_restart_all)

    # nb-add
    p_nb_add = sub.add_parser("nb-add", help="插入新 cell（工具栏 ＋）")
    p_nb_add.add_argument("notebook", nargs="?", default=None)
    p_nb_add.add_argument("-n", default=None)
    p_nb_add.add_argument("--cell", default=None,
                          help="cell 位置：整数（0-based）或 [N]（执行编号）")
    p_nb_add.add_argument("--above", action="store_true",
                          help="在当前 cell 上方插入（默认下方）")
    p_nb_add.set_defaults(func=cmd_nb_add)

    # nb-del
    p_nb_del = sub.add_parser("nb-del",
                              help="删除单个 cell（支持 [N] 执行编号 / active / 0-based 索引）")
    p_nb_del.add_argument("notebook", nargs="?", default=None)
    p_nb_del.add_argument("-n", default=None)
    p_nb_del.add_argument("--cell", default=None,
                          help="[N] 执行编号 / active / 0-based 索引（必填）")
    p_nb_del.set_defaults(func=cmd_nb_del)

    # nb-cut
    p_nb_cut = sub.add_parser("nb-cut", help="剪切 cell（工具栏 ✂）")
    p_nb_cut.add_argument("notebook", nargs="?", default=None)
    p_nb_cut.add_argument("-n", default=None)
    p_nb_cut.add_argument("--cell", default=None,
                          help="cell 位置：整数（0-based）或 [N]（执行编号）")
    p_nb_cut.set_defaults(func=cmd_nb_cut)

    # nb-copy
    p_nb_copy = sub.add_parser("nb-copy", help="复制 cell（工具栏 Copy）")
    p_nb_copy.add_argument("notebook", nargs="?", default=None)
    p_nb_copy.add_argument("-n", default=None)
    p_nb_copy.add_argument("--cell", default=None,
                           help="cell 位置：整数（0-based）或 [N]（执行编号）")
    p_nb_copy.set_defaults(func=cmd_nb_copy)

    # nb-paste
    p_nb_paste = sub.add_parser("nb-paste", help="粘贴 cell（工具栏 Paste）")
    p_nb_paste.add_argument("notebook", nargs="?", default=None)
    p_nb_paste.add_argument("-n", default=None)
    p_nb_paste.set_defaults(func=cmd_nb_paste)

    # nb-move
    p_nb_move = sub.add_parser("nb-move", help="移动 cell（工具栏 Move Up/Down）")
    p_nb_move.add_argument("notebook", nargs="?", default=None)
    p_nb_move.add_argument("-n", default=None)
    p_nb_move.add_argument("--cell", default=None,
                           help="cell 位置：整数（0-based）或 [N]（执行编号）")
    direction = p_nb_move.add_mutually_exclusive_group(required=True)
    direction.add_argument("--up", action="store_true", help="上移")
    direction.add_argument("--down", action="store_true", help="下移")
    p_nb_move.add_argument("--steps", type=int, default=1,
                           help="移动步数（默认 1）")
    p_nb_move.set_defaults(func=cmd_nb_move)

    # nb-cell-type
    p_nb_ct = sub.add_parser("nb-cell-type", help="切换 cell 类型（工具栏类型下拉）")
    p_nb_ct.add_argument("notebook", nargs="?", default=None)
    p_nb_ct.add_argument("-n", default=None)
    p_nb_ct.add_argument("--cell", required=True,
                         help="cell 位置：整数（0-based）或 [N]（执行编号）")
    p_nb_ct.add_argument("--type", required=True,
                         choices=["code", "markdown", "raw"],
                         help="目标类型：code / markdown / raw")
    p_nb_ct.set_defaults(func=cmd_nb_cell_type)

    # nb-delete-all
    p_nb_da = sub.add_parser("nb-delete-all",
                             help="删除当前 notebook 所有 cell（需要 --confirm）")
    p_nb_da.add_argument("notebook", nargs="?", default=None,
                         help="可选：notebook 位置（nb#N 或路径），默认当前激活")
    p_nb_da.add_argument("-n", default=None)
    p_nb_da.add_argument("--confirm", action="store_true",
                         help="确认执行危险的删除全部操作")
    p_nb_da.set_defaults(func=cmd_nb_delete_all)

    # ------------------------------------------------------------------
    # 文件浏览器命令
    # ------------------------------------------------------------------

    # file-new-dir
    p_fndir = sub.add_parser("file-new-dir", help="通过 REST API 新建目录（支持多级）")
    p_fndir.add_argument("path", help='目录路径（如 "data/results"）')
    p_fndir.set_defaults(func=cmd_file_new_dir)

    # file-new
    p_fnew = sub.add_parser("file-new", help="通过 REST API 创建新文件")
    p_fnew.add_argument("path", help='文件路径（如 "work/demo.ipynb"）')
    p_fnew.add_argument("--type", default=None,
                        choices=["notebook", "python", "text", "markdown"],
                        help="文件类型（默认按扩展名推断）")
    p_fnew.set_defaults(func=cmd_file_new)

    # file-open
    p_fopen = sub.add_parser("file-open", help="通过 CDP 双击文件浏览器侧边栏打开文件")
    p_fopen.add_argument("path", help='文件路径（如 "data/demo.ipynb"）')
    p_fopen.set_defaults(func=cmd_file_open)

    # file-list
    p_flist = sub.add_parser("file-list", help="列出目录内容")
    p_flist.add_argument("path", nargs="?", default="",
                         help='目录路径（默认为根目录）')
    p_flist.set_defaults(func=cmd_file_list)

    # ------------------------------------------------------------------
    # 解析并执行
    # ------------------------------------------------------------------
    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"[jupyterm] 错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
