#!/usr/bin/env python3
"""
Tuya DMS MySQL/StarRocks 查询脚本（复用 Chrome 登录态）

用法：
    python dms_mysql_query.py [--sql "SELECT ..."] [--db <database>] [--cluster <name>] [--limit N]

示例：
    python dms_mysql_query.py
    python dms_mysql_query.py --sql "SELECT 1+1 AS result"
    python dms_mysql_query.py --sql "SELECT * FROM banner_agg LIMIT 5" --db tuya_algorithm
    python dms_mysql_query.py --json-out

依赖：CDP daemon 正在运行 + Chrome 已登录 starrocks-dms-cn.tuya-inc.com
"""
import sys, os, json, argparse, subprocess, urllib.request, urllib.error, ssl

BASE_URL      = "https://starrocks-dms-cn.tuya-inc.com:7799"
DEFAULT_CLUSTER = "starrocks-k8s-prod"
DEFAULT_DB    = "tuya_algorithm"
DEFAULT_SQL   = "SELECT `dt`,`site`,`tab_from`,`position`,`materials_id`,`activity_id`,`app_id`,`event_tag`,`num` FROM `banner_agg` WHERE 1=1;"
DAEMON_SCRIPT = "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py"
PYTHON        = sys.executable

# ── 获取认证 token ────────────────────────────────────────────────────────────

def get_token_and_cookies() -> tuple[str, dict]:
    """从 Chrome cookie 获取 JWT token 和全部 cookie（含 httpOnly SSO token）。"""
    result = subprocess.run(
        [PYTHON, DAEMON_SCRIPT, "cookies", "get", BASE_URL, "--json"],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        raise RuntimeError(f"cookies get failed:\n{result.stderr}")
    cookies = json.loads(result.stdout)
    token = cookies.get("token")
    if not token:
        raise RuntimeError("cookie 中未找到 'token' 字段，请确认已登录 DMS 页面")
    return token, cookies

# ── 执行 SQL ──────────────────────────────────────────────────────────────────

def run_sql(
    sql: str,
    database: str = DEFAULT_DB,
    cluster: str = DEFAULT_CLUSTER,
    limit: int = 30,
    offset: int = 0,
    timeout_ms: int = 30000,
    token: str | None = None,
    cookies: dict | None = None,
) -> dict:
    """
    调用 DMS /api/v1/mysql_query/ 执行 SQL 查询。

    :param sql:        SQL 语句
    :param database:   数据库名
    :param cluster:    集群名（默认 starrocks-k8s-prod）
    :param limit:      返回行数上限
    :param offset:     分页偏移
    :param timeout_ms: 执行超时（毫秒）
    :param token:      JWT token（None 则自动从 cookie 获取）
    :param cookies:    完整 cookie 字典（含 httpOnly SSO token，None 则自动获取）
    :return: {"title": [...], "results": [[...], ...], "total": N}
    """
    if token is None or cookies is None:
        token, cookies = get_token_and_cookies()

    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())

    payload = {
        "cluster_name": cluster,
        "database_name": database,
        "sql": sql.strip(),
        "DBA_EXECUTION_TIME": timeout_ms,
        "explain": "",
        "limit": limit,
        "offset": offset,
    }

    url = f"{BASE_URL}/api/v1/mysql_query/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"TUYA {token}",
        "Cookie": cookie_str,          # SSO_USER_TOKEN / COOKICE_USER_TOKEN_PC 必须携带
        "Referer": f"{BASE_URL}/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        raise RuntimeError(f"HTTP {e.code}: {body}")

    # SSO 重定向检测（返回 HTML 说明 session 失效）
    if raw.lstrip().startswith("<!DOCTYPE") or raw.lstrip().startswith("<html"):
        raise RuntimeError(
            "服务器返回了登录页（SSO 重定向）。\n"
            "请确认 Chrome 已打开 DMS 页面且处于登录状态。"
        )

    data = json.loads(raw)
    if data.get("status") != 200:
        raise RuntimeError(f"DMS error: {data.get('message')} | {data}")

    return data.get("data", {})

# ── 格式化输出 ────────────────────────────────────────────────────────────────

def print_table(title: list, results: list, max_col_w: int = 30):
    """对齐列宽表格输出。"""
    if not title:
        print("(no columns)")
        return
    if not results:
        print("(no rows)")
        return

    # 计算列宽
    widths = [min(len(str(c)), max_col_w) for c in title]
    for row in results:
        for i, cell in enumerate(row):
            widths[i] = min(max(widths[i], len(str(cell) if cell is not None else "NULL")), max_col_w)

    def fmt_row(row):
        return " | ".join(str(v if v is not None else "NULL")[:widths[i]].ljust(widths[i]) for i, v in enumerate(row))

    sep = "-+-".join("-" * w for w in widths)
    print(fmt_row(title))
    print(sep)
    for row in results:
        print(fmt_row(row))

# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Tuya DMS MySQL/StarRocks 查询（复用 Chrome 登录态）")
    parser.add_argument("--sql", default=DEFAULT_SQL, help="SQL 语句")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"数据库名（默认 {DEFAULT_DB}）")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER, help=f"集群名（默认 {DEFAULT_CLUSTER}）")
    parser.add_argument("--limit", type=int, default=30, help="返回行数上限（默认 30）")
    parser.add_argument("--offset", type=int, default=0, help="分页偏移（默认 0）")
    parser.add_argument("--json-out", action="store_true", help="以 JSON 格式输出")
    parser.add_argument("--token", default="", help="手动指定 JWT token（不填则自动从 Chrome cookie 获取）")
    args = parser.parse_args()

    print(f"[dms-query] SQL: {args.sql[:100]}{'...' if len(args.sql)>100 else ''}", file=sys.stderr)
    print(f"[dms-query] db={args.db}  cluster={args.cluster}  limit={args.limit}", file=sys.stderr)

    try:
        token, cookies = get_token_and_cookies()
    except RuntimeError as e:
        print(f"[error] 获取 token 失败: {e}", file=sys.stderr)
        sys.exit(1)

    print("[dms-query] token + cookies 已获取，发起查询...", file=sys.stderr)

    try:
        result = run_sql(
            sql=args.sql,
            database=args.db,
            cluster=args.cluster,
            limit=args.limit,
            offset=args.offset,
            token=token,
            cookies=cookies,
        )
    except RuntimeError as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)

    title   = result.get("title", [])
    results = result.get("results", [])
    total   = result.get("total", len(results))

    print(f"[dms-query] 返回 {len(results)} 行（total={total}）", file=sys.stderr)

    if args.json_out:
        out = [dict(zip(title, row)) for row in results]
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print_table(title, results)

if __name__ == "__main__":
    main()
