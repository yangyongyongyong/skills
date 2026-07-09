#!/usr/bin/env python3
"""
Grafana / Loki 日志获取脚本（复用 Chrome 登录态）

用法：
    python grafana_loki_logs.py [--app <flink应用名>] [--hours <小时数>] [--limit <条数>] [--filter <关键词>]

示例：
    python grafana_loki_logs.py
    python grafana_loki_logs.py --app smart-energy-saving-v250704 --hours 2 --limit 500
    python grafana_loki_logs.py --filter "ERROR"

依赖：CDP daemon 正在运行 + Chrome 已登录 trace.tuya-inc.top
"""
import sys
import json
import time
import datetime
import argparse
import subprocess

# ── 配置 ────────────────────────────────────────────────────────────────────
BASE_URL       = "https://trace.tuya-inc.top:7799"
DATASOURCE_UID = "d062423e-7e44-419f-a7b4-f5576a7dfe52"
DATASOURCE_ID  = 234
PLUGIN_ID      = "tuya-tuyalogging-datasource"
ORG_ID         = "1"
DEVICE_ID      = "196cc9d1001d6c246a27bb0a8e4c8616"  # 固定设备 ID，Grafana 用来追踪 device

DAEMON_SCRIPT  = "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py"
PYTHON         = sys.executable

# ── 获取 Cookie ──────────────────────────────────────────────────────────────

def get_live_cookies() -> dict:
    """从 CDP daemon 获取当前浏览器 cookie（自动保持登录态）。"""
    result = subprocess.run(
        [PYTHON, DAEMON_SCRIPT, "cookies", "get", BASE_URL, "--json"],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        raise RuntimeError(f"cookies get failed:\n{result.stderr}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"cookies get invalid JSON:\n{result.stdout[:200]}")


# ── API 调用 ─────────────────────────────────────────────────────────────────

def query_logs(
    expr: str,
    from_ts_ms: int,
    to_ts_ms: int,
    max_lines: int = 500,
    cookies: dict | None = None,
) -> list[dict]:
    """
    调用 Grafana /api/ds/query 获取 Loki 日志。

    :param expr:        LogQL 表达式，如 '{application="flink-main-container",env="prod"} |= "keyword"'
    :param from_ts_ms:  开始时间（毫秒时间戳）
    :param to_ts_ms:    结束时间（毫秒时间戳）
    :param max_lines:   最多返回条数
    :param cookies:     {name: value} cookie 字典，None 则自动从 daemon 获取
    :return:            日志列表，每条 {"time": datetime, "pod": str, "line": str, "labels": dict}
    """
    import urllib.request, urllib.error

    if cookies is None:
        cookies = get_live_cookies()

    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())

    # 计算 intervalMs：让 Grafana 自动分桶（时间范围 / 1496 数据点）
    interval_ms = max(1000, (to_ts_ms - from_ts_ms) // 1496)

    payload = {
        "queries": [{
            "refId": "A",
            "expr": expr,
            "datasource": {
                "type": PLUGIN_ID,
                "uid": DATASOURCE_UID,
            },
            "editorMode": "code",
            "queryType": "range",
            "maxLines": max_lines,
            "legendFormat": "",
            "datasourceId": DATASOURCE_ID,
            "intervalMs": interval_ms,
            "maxDataPoints": 1496,
        }],
        "from": str(from_ts_ms),
        "to": str(to_ts_ms),
    }

    url = f"{BASE_URL}/api/ds/query?ds_type={PLUGIN_ID}&requestId=cli_query"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Cookie": cookie_str,
        "x-grafana-org-id": ORG_ID,
        "x-grafana-device-id": DEVICE_ID,
        "x-datasource-uid": DATASOURCE_UID,
        "x-plugin-id": PLUGIN_ID,
        "x-panel-id": "undefined",
        "Referer": f"{BASE_URL}/explore",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )

    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        raise RuntimeError(f"HTTP {e.code}: {body}")

    data = json.loads(raw)
    frames = data.get("results", {}).get("A", {}).get("frames", [])
    if not frames:
        return []

    frame = frames[0]
    values = frame["data"]["values"]
    # values: [labels[], timestamps_ms[], lines[], tsNs[], ids[]]
    labels_arr = values[0] if len(values) > 0 else []
    times_arr  = values[1] if len(values) > 1 else []
    lines_arr  = values[2] if len(values) > 2 else []

    logs = []
    for i in range(len(lines_arr)):
        ts = times_arr[i] if i < len(times_arr) else 0
        lbl = labels_arr[i] if i < len(labels_arr) else {}
        logs.append({
            "time": datetime.datetime.fromtimestamp(ts / 1000),
            "pod": lbl.get("pod", "?"),
            "host": lbl.get("host", "?"),
            "env": lbl.get("env", "?"),
            "labels": lbl,
            "line": lines_arr[i],
        })

    # 按时间升序排列
    logs.sort(key=lambda x: x["time"])
    return logs


def parse_log_message(raw_line: str) -> str:
    """从 JSON 格式日志中提取 message 字段（filebeat 包装层）。"""
    try:
        obj = json.loads(raw_line)
        msg = obj.get("message", raw_line)
        # 通常格式：2026-05-16T20:09:18.223Z stdout F <actual log>
        # 提取 "F " 之后的内容
        if " F " in msg:
            return msg.split(" F ", 1)[1]
        return msg
    except (json.JSONDecodeError, TypeError):
        return raw_line


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Grafana Loki 日志获取（复用 Chrome 登录态）")
    parser.add_argument("--app", default="smart-energy-saving-v250704",
                        help="Flink 应用名（作为 LogQL |= 过滤词）")
    parser.add_argument("--expr", default="",
                        help="完整 LogQL 表达式（会覆盖 --app）")
    parser.add_argument("--hours", type=float, default=1,
                        help="查询最近 N 小时（默认 1）")
    parser.add_argument("--limit", type=int, default=200,
                        help="最多返回条数（默认 200）")
    parser.add_argument("--filter", default="",
                        help="本地关键词过滤（在结果中再 grep）")
    parser.add_argument("--raw", action="store_true",
                        help="输出原始 JSON 行（不解析 message）")
    parser.add_argument("--json-out", action="store_true",
                        help="输出 JSON 格式")
    args = parser.parse_args()

    # 构建 LogQL
    if args.expr:
        expr = args.expr
    else:
        expr = f'{{application="flink-main-container",env="prod"}} |= "{args.app}"'

    now_ms = int(time.time() * 1000)
    from_ms = now_ms - int(args.hours * 3600 * 1000)

    print(f"[grafana-loki] 查询: {expr}", file=sys.stderr)
    print(f"[grafana-loki] 时间范围: 最近 {args.hours}h  limit={args.limit}", file=sys.stderr)
    print(f"[grafana-loki] 获取 cookie...", file=sys.stderr)

    try:
        cookies = get_live_cookies()
    except RuntimeError as e:
        print(f"[error] {e}", file=sys.stderr)
        print("请确认 CDP daemon 正在运行：", file=sys.stderr)
        print(f"  {PYTHON} {DAEMON_SCRIPT} start", file=sys.stderr)
        sys.exit(1)

    print(f"[grafana-loki] cookie 已获取（{len(cookies)} 个），发起查询...", file=sys.stderr)

    try:
        logs = query_logs(expr, from_ms, now_ms, max_lines=args.limit, cookies=cookies)
    except RuntimeError as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)

    # 本地过滤
    if args.filter:
        logs = [l for l in logs if args.filter.lower() in l["line"].lower()]

    print(f"[grafana-loki] 共 {len(logs)} 条日志", file=sys.stderr)

    if args.json_out:
        out = [{
            "time": l["time"].isoformat(),
            "pod": l["pod"],
            "labels": l["labels"],
            "line": l["line"] if args.raw else parse_log_message(l["line"]),
        } for l in logs]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    for l in logs:
        ts = l["time"].strftime("%Y-%m-%d %H:%M:%S")
        pod = l["pod"]
        line = l["line"] if args.raw else parse_log_message(l["line"])
        print(f"[{ts}] {pod}  {line}")


if __name__ == "__main__":
    main()
