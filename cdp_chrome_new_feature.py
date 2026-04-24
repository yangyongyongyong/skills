#!/usr/bin/env python3
"""
用于验证 Chrome 远程调试连接的示例脚本（不依赖 MCP）。

脚本目标：
1. 演示并封装 CDP 连接 Chrome 的两种方案。
2. 返回并校验可用的浏览器级 WebSocket 地址。
3. 在连接成功后，可选通过扩展上下文读取当前选中页面信息。

连接方案：
- 方案1：通过用户目录中的 chrome versoin>144 打开 chrome://inspect/#remote-debugging DevToolsActivePort 勾选:Allow remote debugging for this browser instance  获取浏览器级 WS 地址（优先）。
    方案1注意要使用后台常驻服务复用用户已授权同意的连接,否则会导致用户需要频繁手动授权.
- 方案2：通过 --remote-debugging-port 对应的 /json/version 获取 WS 地址（回退）。
  eg:
        nohup "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
          --remote-debugging-port=9222 \
          --remote-allow-origins=http://127.0.0.1:9222 \
          --user-data-dir=/Users/lc/chrome-profile \
          >/tmp/chrome-cdp.log 2>&1 &

说明：
- 该流程不同于仅依赖传统的 http://127.0.0.1:9222/json 列表接口。
- 在新特性链路下，DevToolsActivePort 提供的浏览器级 WS 地址更可靠。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import websockets

CHROME_USER_DATA_DIRS = {
    "stable": Path.home() / "Library/Application Support/Google/Chrome",
    "beta": Path.home() / "Library/Application Support/Google/Chrome Beta",
    "dev": Path.home() / "Library/Application Support/Google/Chrome Dev",
    "canary": Path.home() / "Library/Application Support/Google/Chrome Canary",
    "chromium": Path.home() / "Library/Application Support/Chromium",
}

AUTO_CONNECT_HOSTS = ["127.0.0.1", "[::1]", "localhost"]


class CdpClient:
    """最小 CDP WebSocket 客户端，负责发送命令并等待对应响应。"""

    def __init__(self, ws_url: str):
        """初始化客户端并保存目标 WebSocket 地址。"""
        self.ws_url = ws_url
        self._next_id = 0
        self._ws: websockets.ClientConnection | None = None

    async def __aenter__(self) -> "CdpClient":
        """进入上下文时建立 WebSocket 连接。"""
        self._ws = await websockets.connect(self.ws_url)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """退出上下文时关闭 WebSocket 连接。"""
        if self._ws is not None:
            await self._ws.close()

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """发送一条 CDP 命令并返回对应 result，可选指定子会话 session_id。"""
        if self._ws is None:
            raise RuntimeError("WebSocket is not connected")
        self._next_id += 1
        msg_id = self._next_id
        payload: dict[str, Any] = {
            "id": msg_id,
            "method": method,
            "params": params or {},
        }
        if session_id:
            payload["sessionId"] = session_id
        await self._ws.send(
            json.dumps(payload)
        )
        while True:
            raw = await self._ws.recv()
            data = json.loads(raw)
            if data.get("id") != msg_id:
                continue
            if "error" in data:
                raise RuntimeError(f"{method} failed: {json.dumps(data['error'], ensure_ascii=False)}")
            return data.get("result", {})


def read_devtools_active_port(user_data_dir: Path) -> tuple[int, str]:
    """读取 DevToolsActivePort，返回调试端口和浏览器级 WS 路径。"""
    port_file = user_data_dir / "DevToolsActivePort"
    if not port_file.exists():
        raise FileNotFoundError(
            f"DevToolsActivePort not found: {port_file}\n"
            "Make sure Chrome is running and chrome://inspect/#remote-debugging is enabled."
        )

    lines = [line.strip() for line in port_file.read_text().splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"Invalid DevToolsActivePort contents: {lines!r}")

    port = int(lines[0])
    ws_path = lines[1]
    return port, ws_path


async def validate(ws_url: str) -> None:
    """校验单个浏览器级 WS endpoint 是否可用，并打印关键信息。"""
    # 关键校验：通过浏览器级 WebSocket 直连并调用核心 CDP 方法，确认链路真实可用。
    async with CdpClient(ws_url) as cdp:
        version = await cdp.call("Browser.getVersion")
        targets = await cdp.call("Target.getTargets")

    page_targets = [t for t in targets.get("targetInfos", []) if t.get("type") == "page"]

    print("CDP connection: OK")
    print(f"WebSocket endpoint: {ws_url}")
    print(f"Browser: {version.get('product')}")
    print(f"Protocol version: {version.get('protocolVersion')}")
    print(f"User agent: {version.get('userAgent')}")
    print(f"Targets: {len(targets.get('targetInfos', []))} total, {len(page_targets)} page(s)")

    if page_targets:
        print("\nOpen pages:")
        for idx, target in enumerate(page_targets[:10], start=1):
            title = target.get("title") or "(no title)"
            url = target.get("url") or "(no url)"
            print(f"{idx}. {title} -> {url}")


def _normalize_host_for_url(host: str) -> str:
    """将 IPv6 主机规范化为 URL 可用格式（必要时补方括号）。"""
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def build_ws_candidates(port: int, ws_path: str, preferred_host: str = "auto") -> list[str]:
    """根据 host 策略构造浏览器级 WS 候选地址列表。"""
    # Chrome 可能只监听 IPv4(127.0.0.1) 或 IPv6(::1)，这里统一生成候选地址并按顺序重试。
    if preferred_host != "auto":
        host = _normalize_host_for_url(preferred_host)
        return [f"ws://{host}:{port}{ws_path}"]

    return [f"ws://{host}:{port}{ws_path}" for host in AUTO_CONNECT_HOSTS]


async def validate_any(ws_urls: list[str]) -> str:
    """按顺序验证多个 WS 地址，返回第一个可用地址。"""
    # 逐个尝试候选 endpoint，兼容 Chrome 新特性下的 IPv4/IPv6 监听差异。
    last_exc: Exception | None = None
    recovered = set()
    for idx, ws_url in enumerate(ws_urls, start=1):
        print(f"\n[{idx}/{len(ws_urls)}] Trying: {ws_url}")
        try:
            await validate(ws_url)
            return ws_url
        except Exception as exc:  # noqa: PERF203
            last_exc = exc
            print(f"Failed: {exc}")
            # 关键兜底：若 DevToolsActivePort 的 browser id 已过期，404 时从同 host/port 的
            # /json/version 读取最新 webSocketDebuggerUrl 自动修复一次。
            if "HTTP 404" in str(exc):
                refreshed = resolve_ws_from_json_version(ws_url)
                if refreshed and refreshed != ws_url and refreshed not in recovered:
                    recovered.add(refreshed)
                    print(f"Recovered via /json/version: {refreshed}")
                    try:
                        await validate(refreshed)
                        return refreshed
                    except Exception as retry_exc:
                        last_exc = retry_exc
                        print(f"Retry failed: {retry_exc}")
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("No websocket endpoint candidates to try.")


def resolve_ws_from_json_version(ws_url: str) -> str | None:
    """在 WS 404 时回查 /json/version，尝试恢复最新的可用 WS 地址。"""
    # 根据失败的 ws endpoint 推导同 host/port 的 /json/version 地址，获取当前有效 ws。
    parsed = urllib.parse.urlparse(ws_url)
    if not parsed.hostname or parsed.port is None:
        return None

    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    version_url = f"http://{host}:{parsed.port}/json/version"

    try:
        with urllib.request.urlopen(version_url, timeout=2) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None

    refreshed = data.get("webSocketDebuggerUrl")
    if isinstance(refreshed, str) and refreshed.startswith("ws://"):
        return refreshed
    return None


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="Test Chrome auto-connect remote debugging without MCP."
    )
    parser.add_argument(
        "--channel",
        choices=sorted(CHROME_USER_DATA_DIRS),
        default="stable",
        help="Chrome channel to inspect. Default: stable",
    )
    parser.add_argument(
        "--user-data-dir",
        help="Override Chrome user data dir. If set, channel is ignored.",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Only print the resolved WebSocket endpoint without connecting.",
    )
    parser.add_argument(
        "--host",
        default="auto",
        help="Host for websocket endpoint. Use 'auto' to try 127.0.0.1, [::1], localhost.",
    )
    parser.add_argument(
        "--extension-id",
        help="可选：指定用于 chrome.tabs.query 的扩展 ID，不传则自动挑选第一个可用扩展 target。",
    )
    return parser.parse_args()


def connect_via_devtools_active_port(user_data_dir: Path, host: str = "auto") -> str:
    """
    方案1：通过 DevToolsActivePort 建立连接并返回可用 WS 地址。

    适用场景：
    - Chrome >= 144，且在 chrome://inspect/#remote-debugging 勾选
      “Allow remote debugging for this browser instance”。
    - 或者 Chrome 以带 profile 的远程调试方式启动并写入 DevToolsActivePort。
    """
    port, ws_path = read_devtools_active_port(user_data_dir)
    ws_candidates = build_ws_candidates(port, ws_path, host)

    print("\n=== 方案1：DevToolsActivePort 自动连接 ===")
    print(f"User data dir: {user_data_dir}")
    print(f"DevToolsActivePort: {user_data_dir / 'DevToolsActivePort'}")
    print(f"Resolved port: {port}")
    print(f"Resolved browser ws path: {ws_path}")
    print("Resolved browser ws endpoint candidates:")
    for ws_url in ws_candidates:
        print(f"- {ws_url}")

    return asyncio.run(validate_any(ws_candidates))


def connect_via_remote_debugging_port(host: str = "auto", port: int = 9222) -> str:
    """
    方案2：通过 --remote-debugging-port 对应的 /json/version 获取并验证 WS 地址。

    适用场景：
    - Chrome 通过脚本启动并显式指定 --remote-debugging-port=9222。
    - 可以不依赖 DevToolsActivePort 文件，直接从 HTTP discovery 接口获取浏览器 WS。
    """
    print("\n=== 方案2：--remote-debugging-port 回退连接 ===")
    deduped_ws_urls = resolve_ws_candidates_via_remote_debugging_port(host, port, verbose=True)

    print("Resolved websocket endpoint candidates from /json/version:")
    for ws_url in deduped_ws_urls:
        print(f"- {ws_url}")

    return asyncio.run(validate_any(deduped_ws_urls))


def resolve_ws_candidates_via_remote_debugging_port(
    host: str = "auto",
    port: int = 9222,
    verbose: bool = True,
) -> list[str]:
    """通过 /json/version 解析浏览器 WS 地址列表，不做 CDP 命令校验。"""
    host_candidates = AUTO_CONNECT_HOSTS if host == "auto" else [host]
    resolved_ws_urls: list[str] = []
    errors: list[str] = []

    for raw_host in host_candidates:
        normalized_host = _normalize_host_for_url(raw_host)
        version_url = f"http://{normalized_host}:{port}/json/version"
        if verbose:
            print(f"Trying: {version_url}")
        try:
            with urllib.request.urlopen(version_url, timeout=2) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{version_url} -> {exc}")
            if verbose:
                print(f"Failed: {exc}")
            continue

        ws_url = data.get("webSocketDebuggerUrl")
        if isinstance(ws_url, str) and ws_url.startswith("ws://"):
            if verbose:
                print(f"Resolved webSocketDebuggerUrl: {ws_url}")
            resolved_ws_urls.append(ws_url)
        else:
            message = f"{version_url} -> missing/invalid webSocketDebuggerUrl"
            errors.append(message)
            if verbose:
                print(f"Failed: {message}")

    deduped_ws_urls = list(dict.fromkeys(resolved_ws_urls))
    if not deduped_ws_urls:
        detail = "; ".join(errors) if errors else "no version endpoint candidates were tried"
        raise RuntimeError(f"Failed to resolve websocket from /json/version: {detail}")
    return deduped_ws_urls


def _pick_extension_target(
    target_infos: list[dict[str, Any]],
    extension_id: str | None = None,
) -> dict[str, Any] | None:
    """从 Target.getTargets 结果中选择可用于调用 chrome.tabs.query 的扩展 target。"""
    candidates: list[dict[str, Any]] = []
    for target in target_infos:
        target_url = target.get("url", "")
        target_type = target.get("type")
        if not isinstance(target_url, str) or not target_url.startswith("chrome-extension://"):
            continue
        if target_type not in {"service_worker", "background_page", "page"}:
            continue
        if extension_id and f"chrome-extension://{extension_id}/" not in target_url:
            continue
        candidates.append(target)
    return candidates[0] if candidates else None


async def _query_selected_tab_via_extension(
    ws_url: str,
    extension_id: str | None = None,
) -> tuple[str, str]:
    """通过扩展上下文执行 chrome.tabs.query，返回当前激活页标题与 URL。"""
    async with CdpClient(ws_url) as cdp:
        targets = await cdp.call("Target.getTargets")
        target_infos = targets.get("targetInfos", [])
        if not isinstance(target_infos, list):
            raise RuntimeError("Target.getTargets 返回格式异常：缺少 targetInfos 列表")

        extension_target = _pick_extension_target(target_infos, extension_id)
        if not extension_target:
            raise RuntimeError(
                "未找到可用扩展 target，请确认已加载具备 tabs 权限的扩展，"
                "或通过 --extension-id 指定目标扩展。"
            )

        target_id = extension_target.get("targetId")
        if not isinstance(target_id, str) or not target_id:
            raise RuntimeError("扩展 target 缺少有效 targetId")

        attach_result = await cdp.call(
            "Target.attachToTarget",
            {"targetId": target_id, "flatten": True},
        )
        session_id = attach_result.get("sessionId")
        if not isinstance(session_id, str) or not session_id:
            raise RuntimeError("attachToTarget 未返回有效 sessionId")

        try:
            expression = (
                "(async () => {"
                "  const tabs = await new Promise((resolve, reject) => {"
                "    chrome.tabs.query({ active: true, currentWindow: true }, (result) => {"
                "      const err = chrome.runtime?.lastError;"
                "      if (err) { reject(new Error(err.message)); return; }"
                "      resolve(result || []);"
                "    });"
                "  });"
                "  if (!tabs.length) { return { title: '', url: '' }; }"
                "  return { title: tabs[0].title || '', url: tabs[0].url || '' };"
                "})()"
            )
            eval_result = await cdp.call(
                "Runtime.evaluate",
                {
                    "expression": expression,
                    "awaitPromise": True,
                    "returnByValue": True,
                },
                session_id=session_id,
            )
        finally:
            await cdp.call("Target.detachFromTarget", {"sessionId": session_id})

    result_obj = eval_result.get("result", {})
    value = result_obj.get("value", {})
    if not isinstance(value, dict):
        raise RuntimeError(f"Runtime.evaluate 返回结果异常: {value!r}")

    title = value.get("title", "")
    url = value.get("url", "")
    if not isinstance(title, str) or not isinstance(url, str):
        raise RuntimeError(f"Runtime.evaluate 返回字段类型异常: {value!r}")
    return title, url


def print_selected_page_via_chrome_extension(ws_url: str, extension_id: str | None = None) -> None:
    """扩展辅助能力：通过 chrome.tabs.query 打印当前选中页面名称。"""
    print("\n=== 扩展辅助：Chrome Extension 查询当前选中页面 ===")
    title, url = asyncio.run(_query_selected_tab_via_extension(ws_url, extension_id))
    display_title = title or "(no title)"
    display_url = url or "(no url)"
    print(f"Selected page title: {display_title}")
    print(f"Selected page url: {display_url}")


def main() -> int:
    """编排连接流程：优先方案1，失败后回退方案2。"""
    args = parse_args()
    user_data_dir = (
        Path(args.user_data_dir).expanduser() if args.user_data_dir else CHROME_USER_DATA_DIRS[args.channel]
    )

    if args.print_only:
        print("\n=== 仅打印连接地址（不执行 CDP 校验） ===")
        has_output = False

        print("\n=== 方案1：chrome://inspect/#remote-debugging DevToolsActivePort 地址解析 ===")
        try:
            port, ws_path = read_devtools_active_port(user_data_dir)
            ws_candidates = build_ws_candidates(port, ws_path, args.host)
            print(f"User data dir: {user_data_dir}")
            print(f"DevToolsActivePort: {user_data_dir / 'DevToolsActivePort'}")
            print(f"Resolved port: {port}")
            print(f"Resolved browser ws path: {ws_path}")
            print("Resolved browser ws endpoint candidates:")
            for ws_url in ws_candidates:
                print(f"- {ws_url}")
            has_output = True
        except Exception as exc:
            print(f"方案1不可用: {exc}", file=sys.stderr)

        print("\n=== 方案2：--remote-debugging-port 地址解析 ===")
        try:
            ws_urls = resolve_ws_candidates_via_remote_debugging_port(args.host, 9222, verbose=True)
            print("Resolved websocket endpoint candidates from /json/version:")
            for ws_url in ws_urls:
                print(f"- {ws_url}")
            has_output = True
        except Exception as exc:
            print(f"方案2不可用: {exc}", file=sys.stderr)

        return 0 if has_output else 1

    print("\nTrying Browser.getVersion and Target.getTargets ...")
    try:
        success_ws = connect_via_devtools_active_port(user_data_dir, args.host)
    except Exception as scheme1_exc:
        print(f"方案1失败: {scheme1_exc}", file=sys.stderr)
        print("开始回退到方案2...", file=sys.stderr)
        try:
            success_ws = connect_via_remote_debugging_port(args.host, 9222)
        except Exception as scheme2_exc:
            print(f"方案2失败: {scheme2_exc}", file=sys.stderr)
            print("CDP connection failed: two strategies all failed.", file=sys.stderr)
            return 2

    if not success_ws:
        print("CDP connection failed: empty websocket endpoint.", file=sys.stderr)
        return 2

    print(f"\nConnected endpoint: {success_ws}")
    try:
        print_selected_page_via_chrome_extension(success_ws, args.extension_id)
    except Exception as ext_exc:
        print(f"扩展辅助查询失败: {ext_exc}", file=sys.stderr)

    print(
        "\nNote: with this new Chrome feature, HTTP discovery endpoints like "
        "`/json` may still be unavailable. The browser WebSocket endpoint from "
        "DevToolsActivePort is the authoritative connection target."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
