"""Chrome CDP 高层动作 SDK（macOS）。

提供比 cdp_client.py 更语义化的 API，供其他 skill 直接调用。
底层走 cdp_client._send_to_daemon，不直接操作 WebSocket。

功能分两层：
  Phase 1（原有）：
    - list_pages()      列出所有 page 类型的 target
    - get_active_page() 获取用户当前活动的 Chrome tab

  Phase 2（高级交互）：
    - snapshot()        获取页面可交互元素快照（带 @eN 引用）
    - click()           点击元素
    - fill()            填充 input/textarea
    - select()          选择下拉框
    - check()           勾选 checkbox
    - press()           发送按键
    - scroll()          滚动页面
    - wait_for()        等待元素/文本出现
    - get_text()        获取元素文本
    - get_url()         获取页面 URL
    - get_title()       获取页面标题

导入本模块不会启动 daemon，也不会连接 Chrome。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

# 复用 cdp_client 底层通信（同目录）
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from cdp_client import _send_to_daemon, ensure_daemon  # noqa: E402

# Phase 2 高级操作直接从 cdp_client 导入（底层走同一条 socket）
from cdp_client import (  # noqa: E402
    page_call,
    snapshot,
    click,
    fill,
    select,
    check,
    press,
    scroll,
    drag,
    wait_for,
    get_text,
    get_url,
    get_title,
    screenshot,
    network_capture_start,
    network_capture_stop,
    network_fetch,
    network_replay,
    editor_get,
    editor_set,
    editor_type,
    find_icon,
    click_icon,
    scan_tooltips,
    open_tab,
    close_tab,
    activate,
    tab_bind,
    tab_get,
    tab_list,
    tab_remove,
)


# ---------------------------------------------------------------------------
# Phase 1: 页面列表
# ---------------------------------------------------------------------------

def list_pages(auto_start: bool = False, instance: str = "") -> list[dict]:
    """返回当前所有打开的 page 类型 target。

    读取 daemon 内存中的 pages 缓存，不发新 CDP 请求，极快。

    :param auto_start: daemon 未运行时是否自动启动
    :return: [{"targetId": ..., "url": ..., "title": ..., "type": "page"}, ...]
    """
    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({"action": "list_pages"}, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"list_pages failed: {resp.get('error', '?')}")
    return resp.get("pages", [])


# ---------------------------------------------------------------------------
# Phase 1: 活动页面（macOS only）
# ---------------------------------------------------------------------------

def get_active_page(auto_start: bool = False, instance: str = "") -> dict | None:
    """获取用户当前活动的 Chrome tab（macOS only）。

    流程：
      1. AppleScript 拿 frontmost Chrome 窗口 active tab 的 url + title
      2. 在 daemon pages 缓存中精确匹配 targetId
      3. 返回完整 page dict；如果 daemon 未连接则只含 url/title

    :param auto_start: daemon 未运行时是否自动启动
    :return: {"targetId": ..., "url": ..., "title": ..., "type": "page"} 或 None
    :raises RuntimeError: macOS 以外平台调用
    """
    if sys.platform != "darwin":
        raise RuntimeError("get_active_page() is only supported on macOS")

    resolved_instance = ensure_daemon(auto_start=auto_start, instance=instance)
    resp = _send_to_daemon({"action": "active_page"}, timeout=25, instance=resolved_instance)
    if not resp.get("ok"):
        raise RuntimeError(f"get_active_page failed: {resp.get('error', '?')}")
    return resp.get("page")


# ---------------------------------------------------------------------------
# 独立 AppleScript 辅助（可在 daemon 未运行时使用）
# ---------------------------------------------------------------------------

def get_active_tab_info() -> dict | None:
    """仅通过 AppleScript 获取活动 tab 的 url/title，不依赖 daemon。

    适用于只需要 url/title 而不需要 targetId 的轻量场景。
    :return: {"url": ..., "title": ...} 或 None
    """
    if sys.platform != "darwin":
        raise RuntimeError("get_active_tab_info() is only supported on macOS")
    return _applescript_active_tab()


def _applescript_active_tab() -> dict | None:
    """Swift/CGWindowList 实现：获取 frontmost Chrome 窗口的 title（macOS only）。

    不走 Apple Events（AppleScript/JXA 在 CDP 开启时会超时），
    使用 CoreGraphics CGWindowList API，无授权弹窗、无超时风险。
    返回 {"title": ...} 或 None。
    """
    swift_code = """
import CoreGraphics
import Foundation

let windowList = CGWindowListCopyWindowInfo(
    [.optionOnScreenOnly, .excludeDesktopElements],
    kCGNullWindowID
) as! [[String: Any]]

let chromeOwners: Set<String> = [
    "Google Chrome", "Google Chrome Canary",
    "Google Chrome Beta", "Google Chrome Dev", "Chromium"
]

for w in windowList {
    guard let owner = w[kCGWindowOwnerName as String] as? String,
          chromeOwners.contains(owner),
          let layer = w[kCGWindowLayer as String] as? Int,
          layer == 0,
          let title = w[kCGWindowName as String] as? String,
          !title.isEmpty else { continue }
    print(title)
    exit(0)
}
exit(1)
"""
    try:
        r = subprocess.run(
            ["swift", "-"],
            input=swift_code,
            capture_output=True, text=True, timeout=8,
        )
        title = r.stdout.strip()
        if r.returncode != 0 or not title:
            return None
        return {"title": title}
    except Exception:
        return None
