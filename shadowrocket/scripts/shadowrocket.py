#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shadowrocket VPN 开关 CLI（macOS URL Scheme + scutil 状态）。"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from typing import Any, Dict, Optional

VPN_MARKER = "com.liguangming.Shadowrocket"
APP_PATH = "/Applications/Shadowrocket.app"
STATUS_RE = re.compile(
    r"\((Connected|Disconnected|Connecting|Disconnecting)\).*?"
    + re.escape(VPN_MARKER),
    re.IGNORECASE,
)
ROUTE_CHOICES = ("proxy", "config", "direct", "scene")


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=True,
        errors="replace",
    )


def ensure_app_installed() -> None:
    if not PathExists(APP_PATH):
        raise SystemExit(f"未找到 Shadowrocket：{APP_PATH}")


def PathExists(path: str) -> bool:
    from os.path import exists

    return exists(path)


def open_url(url: str) -> None:
    ensure_app_installed()
    _run(["open", url])


def scutil_list() -> str:
    return _run(["scutil", "--nc", "list"]).stdout


def parse_status(raw: Optional[str] = None) -> Dict[str, Any]:
    text = raw if raw is not None else scutil_list()
    match = STATUS_RE.search(text)
    if not match:
        return {
            "installed": PathExists(APP_PATH),
            "found": False,
            "state": "unknown",
            "connected": False,
            "raw": text.strip(),
        }
    state = match.group(1).lower()
    return {
        "installed": PathExists(APP_PATH),
        "found": True,
        "state": state,
        "connected": state == "connected",
        "raw_line": match.group(0).strip(),
    }


def wait_for_state(
    want_connected: bool,
    timeout: float = 8.0,
    interval: float = 0.4,
) -> Dict[str, Any]:
    deadline = time.time() + timeout
    last = parse_status()
    while time.time() < deadline:
        last = parse_status()
        if last["found"] and last["connected"] is want_connected:
            return last
        # Connecting/Disconnecting 也继续等
        time.sleep(interval)
        last = parse_status()
    return last


def emit(data: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    state = data.get("state", "unknown")
    action = data.get("action")
    changed = data.get("changed")
    if action:
        print(f"action={action} state={state} changed={changed}")
    else:
        print(f"state={state} connected={data.get('connected')}")


def cmd_status(as_json: bool) -> int:
    data = parse_status()
    emit(data, as_json)
    return 0 if data["found"] else 2


def cmd_on(as_json: bool, force: bool, timeout: float) -> int:
    before = parse_status()
    if before["connected"] and not force:
        result = {
            **before,
            "action": "on",
            "changed": False,
            "message": "already connected",
        }
        emit(result, as_json)
        return 0
    open_url("shadowrocket://connect?autoclose=true")
    after = wait_for_state(True, timeout=timeout)
    result = {
        **after,
        "action": "on",
        "changed": (not before.get("connected")) and after.get("connected", False),
        "before": before.get("state"),
    }
    emit(result, as_json)
    return 0 if after.get("connected") else 1


def cmd_off(as_json: bool, force: bool, timeout: float) -> int:
    before = parse_status()
    if before["found"] and not before["connected"] and not force:
        result = {
            **before,
            "action": "off",
            "changed": False,
            "message": "already disconnected",
        }
        emit(result, as_json)
        return 0
    open_url("shadowrocket://disconnect?autoclose=true")
    after = wait_for_state(False, timeout=timeout)
    result = {
        **after,
        "action": "off",
        "changed": before.get("connected", False) and (not after.get("connected", True)),
        "before": before.get("state"),
    }
    emit(result, as_json)
    return 0 if (after.get("found") and not after.get("connected")) else 1


def cmd_toggle(as_json: bool, timeout: float) -> int:
    before = parse_status()
    want = not before.get("connected", False)
    open_url("shadowrocket://toggle?autoclose=true")
    after = wait_for_state(want, timeout=timeout)
    result = {
        **after,
        "action": "toggle",
        "changed": before.get("connected") != after.get("connected"),
        "before": before.get("state"),
    }
    emit(result, as_json)
    return 0 if after.get("found") and after.get("connected") is want else 1


def cmd_route(mode: str, as_json: bool) -> int:
    if mode not in ROUTE_CHOICES:
        raise SystemExit(f"非法路由模式: {mode}")
    open_url(f"shadowrocket://route/{mode}?autoclose=true")
    result = {
        "action": "route",
        "route": mode,
        "ok": True,
        **parse_status(),
    }
    emit(result, as_json)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shadowrocket",
        description="控制本机 Shadowrocket VPN 开关与全局路由。",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="以 JSON 输出")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", parents=[common], help="查询当前连接状态")

    on_p = sub.add_parser("on", parents=[common], help="开启 VPN")
    on_p.add_argument("--force", action="store_true", help="即使已连接也再次发送 connect")
    on_p.add_argument("--timeout", type=float, default=8.0, help="等待状态变化秒数")

    off_p = sub.add_parser("off", parents=[common], help="关闭 VPN")
    off_p.add_argument("--force", action="store_true", help="即使已断开也再次发送 disconnect")
    off_p.add_argument("--timeout", type=float, default=8.0, help="等待状态变化秒数")

    toggle_p = sub.add_parser("toggle", parents=[common], help="切换 VPN 开关")
    toggle_p.add_argument("--timeout", type=float, default=8.0, help="等待状态变化秒数")

    route_p = sub.add_parser("route", parents=[common], help="切换全局路由")
    route_p.add_argument(
        "mode",
        choices=ROUTE_CHOICES,
        help="proxy / config / direct / scene",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    as_json = bool(args.json)

    if args.command == "status":
        return cmd_status(as_json)
    if args.command == "on":
        return cmd_on(as_json, force=args.force, timeout=args.timeout)
    if args.command == "off":
        return cmd_off(as_json, force=args.force, timeout=args.timeout)
    if args.command == "toggle":
        return cmd_toggle(as_json, timeout=args.timeout)
    if args.command == "route":
        return cmd_route(args.mode, as_json)
    parser.error(f"未知命令: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
