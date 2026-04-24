"""
jupyterm_notebook — Notebook CLI 命令实现。

包含：
  - nb-list / nb-read / nb-cell-read / nb-edit / nb-exec
  - nb-save / nb-interrupt / nb-restart / nb-restart-all
  - nb-add / nb-cut / nb-copy / nb-paste / nb-move / nb-cell-type
  - Notebook 路径/位置解析 helper
"""

import asyncio
import json
import re
import sys
import time

try:
    import nbformat as _nbformat
    _HAS_NBFORMAT = True
except ImportError:
    _HAS_NBFORMAT = False

from jupyterm_cdp import (
    _find_jupyter_page_ws,
    _nb_ui_click_button_by_title,
    _nb_ui_run_js,
    get_browser_visible_notebooks,
    nb_ui_exec_command,
    nb_ui_find_cell_by_exec_count,
    nb_ui_get_active_cell_idx,
    nb_ui_get_cell_source,
    nb_ui_read_cell_output,
    nb_ui_set_active_cell,
    nb_ui_set_cell,
    switch_to_notebook_tab,
)
from jupyterm_api import (
    _kernel_exec_cell,
    get_notebook,
    get_kernel_status,
    list_sessions,
    save_notebook,
    wait_kernel_idle,
)
from jupyterm_config import load_config


# ---------------------------------------------------------------------------
# Auto-save helper
# ---------------------------------------------------------------------------

def _auto_save():
    """在执行任何读写操作前先触发浏览器保存，确保 DOM 状态与文件同步。

    用户可能在浏览器中执行了 cell 但未点击保存，此时文件内容与浏览器状态不一致。
    所有 notebook 命令在操作前都应调用此函数。

    @return none
    """
    ws_url = _find_jupyter_page_ws()
    if not ws_url:
        return
    try:
        _nb_ui_run_js("""
        (function() {
            // 找可见的 notebook panel（jp-mod-current 或 display 不为 none）
            var panels = Array.from(document.querySelectorAll('.jp-NotebookPanel'));
            var panel = panels.find(function(p) {
                return p.classList.contains('jp-mod-current');
            }) || panels.find(function(p) {
                return window.getComputedStyle(p).display !== 'none' &&
                       p.getBoundingClientRect().width > 0;
            });
            if (!panel) return 'no_panel';
            // 查找 toolbar 中 data-command="docmanager:save" 的元素（JP-BUTTON 或 button）
            var saveBtn = panel.querySelector('[data-command="docmanager:save"]');
            if (!saveBtn) {
                // fallback: title 包含 save
                var all = Array.from(panel.querySelectorAll('.jp-Toolbar [title]'));
                saveBtn = all.find(function(b) {
                    return (b.title || '').toLowerCase().includes('save');
                });
            }
            if (saveBtn) { saveBtn.click(); return 'save_clicked'; }
            return 'no_save_btn';
        })()
        """)
        import time as _time
        _time.sleep(0.8)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Notebook 解析 helpers
# ---------------------------------------------------------------------------

def _parse_nb_arg(raw_arg, tab_arg=None) -> tuple:
    """解析 notebook 参数中的 nb#N 位置前缀，返回 (position, path)。

    语法规则：
      nb#N  → 浏览器中第 N 个 .ipynb tab（1-based）
      其他  → 视为 notebook 路径

    @param[in] raw_arg  原始参数（可能含 nb#N 前缀或为 notebook 路径）
    @param[in] tab_arg  -n 参数（直接指定 path），优先级最高
    @return (position_or_None, path_or_None)
    """
    if tab_arg is not None:
        return None, tab_arg

    if raw_arg is None:
        return None, None

    m = re.match(r"^nb#(\d+)$", raw_arg.strip(), re.IGNORECASE)
    if m:
        return int(m.group(1)), None

    return None, raw_arg.strip() if raw_arg.strip() else None


def _resolve_cell_idx(raw_cell) -> int:
    """将 --cell 参数解析为 0-based cell 索引。

    支持两种格式：
      整数（0-based）   → 直接作为索引，如 --cell 2 表示第 3 个 cell
      [N] / N 且含括号  → 按浏览器显示的执行编号从 DOM 中定位，
                          如 --cell "[2]" 表示 prompt 为 [2]: 的 cell

    优先从浏览器 DOM 查找（保证与未保存状态一致）；CDP 不可用时 fallback 到
    将 N 直接作为 0-based index（警告用户）。

    @note 在定位前自动触发浏览器保存，确保执行编号与文件内容一致。
    @param[in] raw_cell  argparse 传入的原始值（int 或 str）
    @return 0-based cell 索引；无法解析返回原值（int）
    """
    if raw_cell is None:
        return None

    # 解析前先保存，确保浏览器状态与文件同步
    _auto_save()

    # 已经是整数且无 [N] 格式 → 直接返回
    if isinstance(raw_cell, int):
        return raw_cell

    s = str(raw_cell).strip()
    import re as _re

    # "active" → 从浏览器 DOM 查找当前激活的 cell（nb-add 后新 cell 自动激活）
    if s.lower() == "active":
        idx = nb_ui_get_active_cell_idx()
        if idx >= 0:
            return idx
        print("[jupyterm] 浏览器中未找到激活的 cell，"
              "请确认 notebook 已打开且有 cell 被选中", file=sys.stderr)
        sys.exit(1)

    # "[N]" → 按执行编号从 DOM 定位（已执行过的 cell 才有编号）
    m = _re.match(r"^\[(\d+)\]$", s)
    if m:
        exec_count = int(m.group(1))
        idx = nb_ui_find_cell_by_exec_count(exec_count)
        if idx >= 0:
            return idx
        print(f"[jupyterm] 浏览器中未找到执行编号 [{exec_count}] 的 cell，"
              f"请确认该 cell 已执行", file=sys.stderr)
        sys.exit(1)

    # 纯数字字符串 → 作为 0-based index
    if s.isdigit():
        return int(s)

    print(f"[jupyterm] 无法解析 --cell 参数: {raw_cell!r}，"
          f"支持格式: 整数（0-based）、[N]（执行编号）或 active（当前激活）",
          file=sys.stderr)
    sys.exit(1)


def _resolve_notebook(cfg: dict, position: int = None,
                      path: str = None) -> tuple:
    """将 nb#N 位置编号或路径解析为 (notebook_path, kernel_id)。

    优先级：path 参数 > nb#N 浏览器位置 > 当前活跃 .ipynb tab > sessions 第一条。
    kernel_id 通过 sessions API 匹配 path 获取，若 notebook 未运行 kernel 则为 None。

    @param[in] cfg      jupyterm 配置字典
    @param[in] position 1-based 浏览器位置（nb#N 语法），None=未指定
    @param[in] path     notebook 显式路径，None=未指定
    @return (notebook_path, kernel_id_or_None)
    """
    sessions = list_sessions(cfg)
    nb_tabs = get_browser_visible_notebooks()

    def _find_kernel(nb_path):
        """根据 notebook path 在 sessions 中查找 kernel id。"""
        for s in sessions:
            sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
            if sp == nb_path or sp.endswith(nb_path) or nb_path.endswith(sp):
                return s.get("kernel", {}).get("id")
        return None

    if path is not None:
        return path, _find_kernel(path)

    if position is not None:
        if not nb_tabs:
            print("[jupyterm] CDP 不可用，无法使用 #N 位置语法", file=sys.stderr)
            sys.exit(1)
        if position < 1 or position > len(nb_tabs):
            print(f"[jupyterm] #{position} 超出范围"
                  f"（浏览器中有 {len(nb_tabs)} 个 notebook tab）",
                  file=sys.stderr)
            sys.exit(1)
        tab = nb_tabs[position - 1]
        nb_path_hint = tab.get("path") or tab.get("name", "")
        for s in sessions:
            sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
            if sp == nb_path_hint or sp.endswith(nb_path_hint) or nb_path_hint.endswith(sp):
                return sp, s.get("kernel", {}).get("id")
        return nb_path_hint, None

    if nb_tabs:
        for tab in nb_tabs:
            if tab.get("current"):
                nb_path_hint = tab.get("path") or tab.get("name", "")
                for s in sessions:
                    sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
                    if sp == nb_path_hint or sp.endswith(nb_path_hint) or nb_path_hint.endswith(sp):
                        return sp, s.get("kernel", {}).get("id")
                return nb_path_hint, None
        tab = nb_tabs[-1]
        nb_path_hint = tab.get("path") or tab.get("name", "")
        for s in sessions:
            sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
            if sp == nb_path_hint or sp.endswith(nb_path_hint) or nb_path_hint.endswith(sp):
                return sp, s.get("kernel", {}).get("id")
        return nb_path_hint, None

    if sessions:
        s = sessions[0]
        sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
        return sp, s.get("kernel", {}).get("id")

    print("[jupyterm] 未找到任何打开的 notebook", file=sys.stderr)
    sys.exit(1)


def _format_cell_output(cell: dict) -> list:
    """将 notebook cell 的 outputs 字段格式化为可读文本列表。

    支持所有标准 output_type：stream / execute_result / display_data / error。
    image/png 等二进制数据输出标注类型而不展开。

    @param[in] cell  notebook cell 字典（原始 JSON 或 nbformat NotebookNode）
    @return 可读输出文本列表
    """
    result = []
    for output in cell.get("outputs", []):
        ot = output.get("output_type", "")
        if ot == "stream":
            text = output.get("text", "")
            if isinstance(text, list):
                text = "".join(text)
            result.append(text.rstrip())
        elif ot in ("execute_result", "display_data"):
            data = output.get("data", {})
            text = data.get("text/plain", "")
            if isinstance(text, list):
                text = "".join(text)
            if text:
                result.append(text.rstrip())
            if "image/png" in data:
                result.append("[image/png output]")
            if "image/jpeg" in data:
                result.append("[image/jpeg output]")
            if "application/json" in data:
                try:
                    result.append("[application/json: " +
                                  json.dumps(data["application/json"])[:200] + "]")
                except Exception:
                    result.append("[application/json output]")
        elif ot == "error":
            ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
            tb = output.get("traceback", [])
            clean = [ansi_escape.sub("", line) for line in tb]
            result.append(f"ERROR: {output.get('ename')}: {output.get('evalue')}")
            if clean:
                result.append("\n".join(clean[:5]))
    return result


def _nb_get_cells(cfg: dict, nb_path: str) -> tuple:
    """读取 notebook 文件并返回 (cells, nb_dict)。

    优先用 nbformat 解析（更稳定），不可用时 fallback 到原始 JSON。
    操作前由调用方负责触发 _auto_save()。

    @param[in] cfg      Jupyter 配置字典
    @param[in] nb_path  notebook 相对路径
    @return (cells list, raw nb dict)
    """
    result = get_notebook(cfg, nb_path)
    nb = result.get("content", result)
    if _HAS_NBFORMAT:
        try:
            nb_str = json.dumps(nb)
            parsed = _nbformat.reads(nb_str, as_version=4)
            return list(parsed.cells), nb
        except Exception:
            pass
    return nb.get("cells", []), nb


# ---------------------------------------------------------------------------
# 通用工具栏命令执行器
# ---------------------------------------------------------------------------

def _nb_ui_cmd(args, command_id: str, need_cell: bool = False,
               cell_repeat: int = 1):
    """通用工具栏按钮执行器。

    解析 notebook 位置、切换 tab、（可选）激活 cell，然后执行指定命令。
    所有操作前自动触发浏览器保存，确保 DOM 状态与文件同步。

    @param[in] args        argparse 命名空间
    @param[in] command_id  JupyterLab 命令 ID
    @param[in] need_cell   是否需要先激活 --cell 指定的 cell
    @param[in] cell_repeat 对同一 cell 重复执行几次（用于 move-up/down 多步）
    """
    _auto_save()
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    position, path = _parse_nb_arg(raw, tab_arg)
    _resolve_notebook(cfg, position=position, path=path)

    if position is not None:
        switch_to_notebook_tab(position)

    if need_cell:
        raw_cell = getattr(args, "cell", None)
        cell_idx = _resolve_cell_idx(raw_cell)
        if cell_idx is not None:
            nb_ui_set_active_cell(cell_idx)
            time.sleep(0.1)

    ok = False
    for _ in range(cell_repeat):
        ok = nb_ui_exec_command(command_id)

    if ok:
        print(f"[jupyterm] 已执行: {command_id}")
    else:
        print(f"[jupyterm] 执行失败（CDP 不可用或按钮未找到）: {command_id}",
              file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Notebook CLI 命令
# ---------------------------------------------------------------------------

def cmd_nb_list(args):
    """列出浏览器中实际可见的 .ipynb notebook tab，标注位置和活跃状态。

    优先通过 CDP 查询浏览器 DOM，CDP 不可用时 fallback 到 sessions API。
    """
    cfg = load_config()
    nb_tabs = get_browser_visible_notebooks()

    if nb_tabs is not None:
        if nb_tabs:
            for i, tab in enumerate(nb_tabs):
                marker = "  <-- active" if tab.get("current") else ""
                print(f"  #{i + 1}  {tab['name']}{marker}")
        else:
            print("[jupyterm] 浏览器中未打开任何 notebook tab")
    else:
        print("[jupyterm] (CDP 不可用，显示 sessions 列表)", file=sys.stderr)
        sessions = list_sessions(cfg)
        if sessions:
            for i, s in enumerate(sessions):
                sp = s.get("path", "") or (s.get("notebook") or {}).get("path", "")
                kid = s.get("kernel", {}).get("id", "")[:8]
                print(f"  [{i + 1}] {sp}  kernel={kid}")
        else:
            print("[jupyterm] 没有运行中的 notebook session")


def cmd_nb_read(args):
    """读取 notebook 内容，打印每个 cell 的类型、源码和已有输出。

    支持 nb#N 位置语法、notebook 路径、以及自动选当前活跃 notebook。
    @note 读取前自动触发浏览器保存，确保输出与执行状态最新。
    """
    _auto_save()
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    position, path = _parse_nb_arg(raw, tab_arg)

    nb_path, _ = _resolve_notebook(cfg, position=position, path=path)

    if position is not None:
        switch_to_notebook_tab(position)

    cells, _ = _nb_get_cells(cfg, nb_path)
    print(f"Notebook: {nb_path}  ({len(cells)} cells)")
    print("=" * 60)

    for i, cell in enumerate(cells):
        cell_type = cell.get("cell_type", "unknown")
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)

        exec_count = cell.get("execution_count")
        ec_str = f"[{exec_count}]" if exec_count is not None else "[ ]"

        print(f"\n[{i}] {cell_type}  {ec_str}")
        print("-" * 40)
        if source.strip():
            for line in source.splitlines():
                print(f"  {line}")
        else:
            print("  (empty)")

        if cell_type == "code":
            out_texts = _format_cell_output(cell)
            if out_texts:
                print("  --- output ---")
                for t in out_texts:
                    for line in t.splitlines():
                        print(f"  {line}")
            else:
                print("  --- output: (none) ---")


def cmd_nb_cell_read(args):
    """读取指定 cell 或全部 cell 的输入（source）和输出（outputs）。

    与 nb-read 相比，支持 --cell 精确定位单个 cell，并提供 --json 结构化输出，
    适合 Agent 程序化消费。

    @example jupyterm nb-cell-read              读取全部 cell
    @example jupyterm nb-cell-read --cell "[3]" 读取执行编号 [3] 的 cell
    @example jupyterm nb-cell-read --cell 2     读取 cell[2]（0-based 索引）
    @example jupyterm nb-cell-read --cell active 读取当前活跃 cell
    @example jupyterm nb-cell-read --cell 2 --json   JSON 格式输出
    @example jupyterm nb-cell-read --cell 2 --input-only  只看 source
    @example jupyterm nb-cell-read --cell 2 --output-only 只看输出
    """
    _auto_save()
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    position, path = _parse_nb_arg(raw, tab_arg)

    nb_path, _ = _resolve_notebook(cfg, position=position, path=path)

    if position is not None:
        switch_to_notebook_tab(position)

    cells, _ = _nb_get_cells(cfg, nb_path)

    raw_cell = getattr(args, "cell", None)
    input_only = getattr(args, "input_only", False)
    output_only = getattr(args, "output_only", False)
    as_json = getattr(args, "json", False)

    # 确定目标 cell 索引列表
    if raw_cell is None:
        indices = list(range(len(cells)))
    else:
        idx = _resolve_cell_idx(raw_cell)
        if idx is None or idx < 0 or idx >= len(cells):
            print(f"[jupyterm] cell 索引超出范围（共 {len(cells)} 个 cell）",
                  file=sys.stderr)
            sys.exit(1)
        indices = [idx]

    results = []
    for i in indices:
        cell = cells[i]
        cell_type = cell.get("cell_type", "unknown")
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)
        exec_count = cell.get("execution_count")
        out_texts = _format_cell_output(cell) if cell_type == "code" else []

        if as_json:
            results.append({
                "index": i,
                "cell_type": cell_type,
                "exec_count": exec_count,
                "source": source,
                "outputs": out_texts,
            })
        else:
            ec_str = f"[{exec_count}]" if exec_count is not None else "[ ]"
            print(f"cell[{i}]  {cell_type}  {ec_str}")
            if not output_only:
                print("--- source ---")
                if source.strip():
                    for line in source.splitlines():
                        print(f"  {line}")
                else:
                    print("  (empty)")
            if not input_only and cell_type == "code":
                print("--- output ---")
                if out_texts:
                    for t in out_texts:
                        for line in t.splitlines():
                            print(f"  {line}")
                else:
                    print("  (none)")
            if len(indices) > 1:
                print()

    if as_json:
        out = results[0] if len(results) == 1 else results
        print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_nb_edit(args):
    """修改 notebook 指定 cell 的源码，优先通过浏览器 UI（所见即所得）写入并保存。

    优先路径（CDP 可用时）：通过 CDP 直接写入浏览器中的 cell（实时可见）并保存。
    Fallback（CDP 不可用）：通过 Contents API 写文件（用户需关闭重开才可见）。

    @note 操作前自动触发浏览器保存，确保 cell 位置与执行编号和文件一致。
    @example jupyterm nb-edit "nb#1" --cell 2 --source "print('hello')"
    """
    _auto_save()
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    new_source = args.source
    position, path = _parse_nb_arg(raw, tab_arg)

    nb_path, _ = _resolve_notebook(cfg, position=position, path=path)

    if position is not None:
        switch_to_notebook_tab(position)

    # 解析 cell 索引（支持整数和 [N] 执行编号，从浏览器 DOM 定位）
    cell_idx = _resolve_cell_idx(args.cell)

    ws_url = _find_jupyter_page_ws()
    if ws_url:
        ok = nb_ui_set_cell(cell_idx, new_source)
        if ok:
            nb_ui_exec_command("docmanager:save")
            print(f"[jupyterm] 已在浏览器中更新 cell[{cell_idx}] 并保存")
            print(f"  内容: {new_source[:80]}{'...' if len(new_source) > 80 else ''}")
            return
        else:
            print("[jupyterm] CDP 写入失败（cell 索引超范围或 notebook 未激活），"
                  "尝试 fallback 到 Contents API", file=sys.stderr)

    # Fallback：Contents API
    result = get_notebook(cfg, nb_path)
    nb = result.get("content", result)
    cells = nb.get("cells", [])

    if cell_idx < 0 or cell_idx >= len(cells):
        print(f"[jupyterm] cell 索引 {cell_idx} 超出范围（共 {len(cells)} 个 cell）",
              file=sys.stderr)
        sys.exit(1)

    old_source = cells[cell_idx].get("source", "")
    if isinstance(old_source, list):
        old_source = "".join(old_source)

    cells[cell_idx]["source"] = new_source
    save_notebook(cfg, nb_path, nb)

    print(f"[jupyterm] (fallback) 已通过文件 API 更新 {nb_path} cell[{cell_idx}]")
    print(f"  旧: {old_source[:80]}{'...' if len(old_source) > 80 else ''}")
    print(f"  新: {new_source[:80]}{'...' if len(new_source) > 80 else ''}")


def cmd_nb_exec(args):
    """执行 notebook 中指定/全部 cell，优先通过浏览器 UI（所见即所得）。

    优先路径（CDP 可用时）：激活 cell → 点击 Run 按钮 → 轮询 DOM 读取输出。
    Fallback（CDP 不可用）：通过 Kernel WebSocket 执行并收集输出。

    @note 执行前自动触发浏览器保存，确保 cell 内容为浏览器最新状态。
    @example jupyterm nb-exec "nb#1" --cell 2        执行第 2 个 cell
    @example jupyterm nb-exec "nb#1" --all           执行所有 code cell
    @example jupyterm nb-exec "work/test.ipynb"      按路径指定 notebook
    """
    _auto_save()
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    raw_cell = getattr(args, "cell", None)
    run_all = getattr(args, "all", False)
    timeout = getattr(args, "timeout", 60.0)
    position, path = _parse_nb_arg(raw, tab_arg)

    # 解析 cell 索引（支持整数和 [N] 执行编号）
    cell_idx = _resolve_cell_idx(raw_cell)

    nb_path, kernel_id = _resolve_notebook(cfg, position=position, path=path)

    if not kernel_id:
        print(f"[jupyterm] {nb_path} 没有活跃的 Kernel（未运行），"
              "请先在 JupyterLab 中打开并启动 Kernel", file=sys.stderr)
        sys.exit(1)

    if position is not None:
        switch_to_notebook_tab(position)

    result = get_notebook(cfg, nb_path)
    nb = result.get("content", result)
    cells = nb.get("cells", [])

    if cell_idx is not None:
        if cell_idx < 0 or cell_idx >= len(cells):
            print(f"[jupyterm] cell 索引 {cell_idx} 超出范围（共 {len(cells)} 个 cell）",
                  file=sys.stderr)
            sys.exit(1)
        targets = [(cell_idx, cells[cell_idx])]
    elif run_all:
        targets = [(i, c) for i, c in enumerate(cells) if c.get("cell_type") == "code"]
    else:
        print("[jupyterm] 请指定 --cell N 或 --all", file=sys.stderr)
        sys.exit(1)

    use_cdp = bool(_find_jupyter_page_ws())
    ws_kernel_url = cfg["ws_base"].rstrip("/") + f"/api/kernels/{kernel_id}/channels"

    for idx, cell in targets:
        source = cell.get("source", "")
        if isinstance(source, list):
            source = "".join(source)
        cell_type = cell.get("cell_type", "code")

        # CDP 模式：从浏览器 DOM 读取实际 source，覆盖文件中可能的旧版本
        # （nb-edit 写入浏览器后保存可能有延迟，DOM 是最新状态）
        if use_cdp:
            dom_source = nb_ui_get_cell_source(idx)
            if dom_source is not None:
                source = dom_source

        print(f"\n{'=' * 50}")
        print(f"[{idx}] {cell_type}")
        for line in source.splitlines():
            print(f"  {line}")
        print("-" * 30)

        if cell_type != "code" or not source.strip():
            print("  (skip: markdown or empty)")
            continue

        if use_cdp:
            # CDP 路径：
            #   1. 激活 cell（浏览器可见）
            #   2. 点击 Run 按钮（浏览器实时显示执行过程）
            #   3. 轮询 Kernel status REST API 等待 idle
            #   4. 从 REST API 读取输出（已保存到 ipynb）
            nb_ui_set_active_cell(idx)
            time.sleep(0.1)
            nb_ui_exec_command("notebook:run-cell-and-select-next")

            idle = wait_kernel_idle(cfg, kernel_id, timeout=timeout)
            if not idle:
                print(f"  [超时 {timeout}s，cell 可能仍在执行]", file=sys.stderr)
                continue

            # 触发保存，确保 REST API 拿到最新输出
            nb_ui_exec_command("docmanager:save")
            time.sleep(0.5)

            # 优先从 DOM 读输出（最实时，不受文件同步延迟影响）
            # nb_ui_read_cell_output 返回 {"running": bool, "outputs": [str], "exec_count": int|None}
            dom_result = nb_ui_read_cell_output(idx)
            dom_outputs = dom_result.get("outputs", []) if isinstance(dom_result, dict) else []
            if dom_outputs:
                for text in dom_outputs:
                    for line in text.splitlines():
                        print(f"  {line}")
            else:
                print("  (no output)")
        else:
            # 无 CDP：直接通过 Kernel WebSocket 执行并收集输出
            res = asyncio.run(_kernel_exec_cell(ws_kernel_url, source, timeout,
                                                token=cfg["token"]))
            if res["outputs"]:
                for text in res["outputs"]:
                    for line in text.splitlines():
                        print(f"  {line}")
            else:
                print("  (no output)")
            if not res["ok"]:
                print(f"\n[jupyterm] cell[{idx}] 执行失败", file=sys.stderr)
                if res.get("error"):
                    print(res["error"], file=sys.stderr)


def cmd_nb_delete_all(args):
    """删除当前 notebook 所有 cell（危险操作，需要 --confirm 参数）。

    流程：
      1. 校验 --confirm 标志，防止误调用
      2. 从浏览器识别当前激活的 notebook 路径
      3. 通过 REST API 将 cells 清空（写入空列表）
      4. 触发浏览器 docmanager:reload 刷新显示
      5. JupyterLab 重载后自动保留 1 个空 cell（正常行为）

    @note 必须显式传入 --confirm 才会执行，防止误删。
    @note REST API 写入后浏览器会弹出"文件已在外部修改，是否重载"确认框，
          reload 命令会自动确认。
    """
    if not getattr(args, "confirm", False):
        print("[jupyterm] 危险操作：nb-delete-all 需要加 --confirm 参数才能执行")
        print("[jupyterm] 示例：jupyterm nb-delete-all --confirm")
        sys.exit(1)

    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    position, path = _parse_nb_arg(raw, tab_arg)

    nb_path, _ = _resolve_notebook(cfg, position=position, path=path)

    # 读取原 notebook 保留 metadata / nbformat
    result = get_notebook(cfg, nb_path)
    nb = result.get("content", result)
    original_count = len(nb.get("cells", []))
    print(f"[jupyterm] {nb_path}：共 {original_count} 个 cell，正在清空…")

    nb_empty = {
        "cells": [],
        "metadata": nb.get("metadata", {}),
        "nbformat": nb.get("nbformat", 4),
        "nbformat_minor": nb.get("nbformat_minor", 5),
    }
    save_notebook(cfg, nb_path, nb_empty)
    print(f"[jupyterm] 已清空所有 cell（REST API 写入）")

    # 触发浏览器重载：直接点击 data-command='docmanager:reload' 按钮（最可靠）
    time.sleep(0.3)
    _nb_ui_run_js("""(function(){
        var btn = document.querySelector(\"[data-command='docmanager:reload']\");
        if(btn) { btn.click(); return; }
        // fallback：菜单 File > Reload
        var items = Array.from(document.querySelectorAll(\".lm-Menu-itemLabel\"));
        var r = items.find(function(i){ return i.textContent.trim() === 'Reload Notebook'; });
        if(r) r.closest(\".lm-Menu-item\").click();
    }())""")
    time.sleep(1.0)
    # 确认重载弹框（如有）
    _nb_ui_run_js("""(function(){
        var dialog = document.querySelector('.jp-Dialog');
        if(!dialog) return;
        var btns = Array.from(dialog.querySelectorAll('button'));
        var ok = btns.find(function(b){
            var t = b.textContent.trim().toLowerCase();
            return t === 'reload' || t === 'revert' || t === 'ok' || t.includes('reload');
        });
        if(ok) ok.click();
    }())""")
    print(f"[jupyterm] 浏览器已刷新，notebook 现在为空")


def cmd_nb_del(args):
    """删除指定单个 cell（支持 [N] 执行编号 / active / 0-based 索引）。

    操作前自动保存，确保浏览器与文件状态一致。

    @example jupyterm nb-del --cell "[3]"   删除执行编号为 [3] 的 cell
    @example jupyterm nb-del --cell active  删除当前激活的 cell
    @example jupyterm nb-del --cell 2       删除第 3 个 cell（0-based）
    """
    _auto_save()
    raw_cell = getattr(args, "cell", None)
    if raw_cell is None:
        print("[jupyterm] 请指定 --cell [N] / active / 索引", file=sys.stderr)
        sys.exit(1)

    cell_idx = _resolve_cell_idx(raw_cell)
    if cell_idx is None:
        print("[jupyterm] 无法解析 --cell 参数", file=sys.stderr)
        sys.exit(1)

    nb_ui_set_active_cell(cell_idx)
    time.sleep(0.1)
    ok = nb_ui_exec_command("notebook:delete-cell")
    if ok:
        _auto_save()
        print(f"[jupyterm] 已删除 cell[{cell_idx}]（原 --cell {raw_cell}）")
    else:
        print("[jupyterm] 删除失败（CDP 不可用）", file=sys.stderr)
        sys.exit(1)


def cmd_nb_save(args):
    """保存当前 notebook（等价于点击工具栏 💾 保存按钮）。"""
    _nb_ui_cmd(args, "docmanager:save")


def cmd_nb_interrupt(args):
    """中断正在运行的 kernel（等价于点击工具栏 ■ 按钮）。"""
    _nb_ui_cmd(args, "notebook:interrupt-kernel")


def cmd_nb_restart(args):
    """重启 kernel（等价于点击工具栏 ↺ 按钮，会弹出确认对话框）。

    @note JupyterLab 会弹出确认框，命令会自动确认（等价于点击 Restart）。
    """
    cfg = load_config()
    raw = getattr(args, "notebook", None)
    tab_arg = getattr(args, "n", None)
    position, path = _parse_nb_arg(raw, tab_arg)
    _resolve_notebook(cfg, position=position, path=path)
    if position is not None:
        switch_to_notebook_tab(position)

    ok = _nb_ui_click_button_by_title("Restart the kernel")
    if not ok:
        print("[jupyterm] 执行失败（未找到 Restart 按钮）", file=sys.stderr)
        sys.exit(1)

    time.sleep(0.8)
    confirm_js = """(() => {
        const btn = Array.from(document.querySelectorAll('button'))
            .find(b => /restart/i.test(b.textContent) &&
                       (b.closest('.jp-Dialog') || b.closest('[class*="dialog"]') || b.closest('[role="dialog"]')));
        if (btn) { btn.click(); return 'confirmed'; }
        return 'no_dialog';
    })()"""
    confirm = _nb_ui_run_js(confirm_js, timeout=3.0)
    if confirm == "confirmed":
        print("[jupyterm] 已重启 kernel（已确认对话框）")
    else:
        print("[jupyterm] 已发送 restart 请求（未检测到确认对话框，可能已直接重启）")


def cmd_nb_restart_all(args):
    """重启 kernel 并运行全部 cell（等价于点击工具栏 ↠ 按钮）。"""
    _nb_ui_cmd(args, "notebook:restart-run-all")


def cmd_nb_add(args):
    """在当前 cell 下方（或上方）插入新 cell（等价于点击工具栏 ＋ 按钮）。

    @example jupyterm nb-add "nb#1"          在当前活跃 cell 下方插入
    @example jupyterm nb-add "nb#1" --above  在当前活跃 cell 上方插入
    @example jupyterm nb-add "nb#1" --cell 2 在 cell[2] 下方插入
    """
    above = getattr(args, "above", False)
    cmd_id = "notebook:insert-cell-above" if above else "notebook:insert-cell-below"
    _nb_ui_cmd(args, cmd_id, need_cell=True)


def cmd_nb_cut(args):
    """剪切指定 cell（等价于点击工具栏 ✂ 按钮）。

    @example jupyterm nb-cut "nb#1" --cell 2
    """
    _nb_ui_cmd(args, "notebook:cut-cell", need_cell=True)


def cmd_nb_copy(args):
    """复制指定 cell（等价于点击工具栏 Copy 按钮）。

    @example jupyterm nb-copy "nb#1" --cell 2
    """
    _nb_ui_cmd(args, "notebook:copy-cell", need_cell=True)


def cmd_nb_paste(args):
    """将剪贴板中的 cell 粘贴到当前 cell 下方（等价于点击工具栏 Paste 按钮）。

    @example jupyterm nb-paste "nb#1"
    """
    _nb_ui_cmd(args, "notebook:paste-cell-below")


def cmd_nb_move(args):
    """上移或下移指定 cell（等价于点击工具栏 Move Up/Down 按钮）。

    @example jupyterm nb-move "nb#1" --cell 2 --up
    @example jupyterm nb-move "nb#1" --cell 2 --down --steps 3
    """
    down = getattr(args, "down", False)
    steps = max(1, getattr(args, "steps", 1))
    cmd_id = "notebook:move-cell-down" if down else "notebook:move-cell-up"
    _nb_ui_cmd(args, cmd_id, need_cell=True, cell_repeat=steps)


def cmd_nb_cell_type(args):
    """切换指定 cell 的类型（code / markdown / raw），等价于工具栏下拉菜单。

    @example jupyterm nb-cell-type "nb#1" --cell 2 --type markdown
    """
    type_map = {
        "code": "notebook:change-cell-to-code",
        "markdown": "notebook:change-cell-to-markdown",
        "raw": "notebook:change-cell-to-raw",
    }
    cell_type = args.type.lower()
    if cell_type not in type_map:
        print(f"[jupyterm] 不支持的类型 {args.type!r}，可选: code / markdown / raw",
              file=sys.stderr)
        sys.exit(1)
    _nb_ui_cmd(args, type_map[cell_type], need_cell=True)
