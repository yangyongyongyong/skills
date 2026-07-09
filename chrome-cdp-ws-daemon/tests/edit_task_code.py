#!/usr/bin/env python3
"""
纯 Python 脚本：编辑 BDP 任务代码（不依赖 Chrome 页面）
通过 chrome-cdp-ws-daemon skill 的 cookies get 获取登录态，
再直接调用后台接口读取/保存任务代码。

用法:
    python edit_task_code.py --task-id 73590            # 读取当前代码
    python edit_task_code.py --task-id 73590 --code "SELECT 1"      # dry-run
    python edit_task_code.py --task-id 73590 --code "SELECT 1" --write  # 真实写入
    python edit_task_code.py --task-id 73590 --code-file new.sql --write
    python edit_task_code.py --task-id 73590 --rollback backup/73590_xxx.json
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings()

# ── 配置区 ──────────────────────────────────────────────────────
BASE_URL   = "https://bdp-ueaz.tuya-inc.com:7799"
JOB_API    = f"{BASE_URL}/open-api/v1.0/datafactory/job"
WKID       = "166"
DAEMON_PY  = Path(__file__).parent.parent / "scripts" / "daemon.py"
PYTHON_BIN = os.environ.get("CDP_PYTHON", "/Users/luca/miniforge3/envs/py311/bin/python")
BACKUP_DIR = Path(__file__).parent / "backup"

# ── 静态 Header（从抓包提取，csrf-token 随 session 固定）───────
# csrf-token 与 cookie 中的 _csrf 对应，不随请求变动
STATIC_HEADERS = {
    "TUYA-WKID": WKID,
    "micro-app-id": "1642796379380121613",
    "micro-app-code": "@tuya-fe/bdp-develop",
    "Referer": f"{BASE_URL}/apps/develop/index?workspaceId={WKID}",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}

# ── 从浏览器页面动态读取 csrf-token ──────────────────────────────
# uid 从抓包分析固定为此值（与 SSO session 绑定，不在 cookie 中直接暴露）
# csrf-token 来自 <meta name="csrf-token">，每次页面加载会变，必须实时读取
_UID_FROM_CAPTURE = "bue1688523976089cUDU"  # 从抓包拿到，随 SSO session 变化时需更新


def get_csrf_from_page(target_id: str = "B8869DE5") -> str:
    """从浏览器页面 meta 标签实时读取 csrf-token。"""
    try:
        sys.path.insert(0, str(DAEMON_PY.parent))
        from cdp_client import page_call
        r = page_call(target_id, "Runtime.evaluate", {
            "expression": "(document.querySelector('meta[name=\"csrf-token\"]') || {}).content || ''",
            "returnByValue": True,
        })
        return r.get("result", {}).get("value", "")
    except Exception as e:
        print(f"[WARN] 无法从页面读取 csrf-token: {e}", file=sys.stderr)
        return ""


# ── Cookie 获取 ─────────────────────────────────────────────────
def get_cookies() -> dict:
    """通过 daemon CLI 获取登录态 cookie。"""
    result = subprocess.run(
        [PYTHON_BIN, str(DAEMON_PY), "cookies", "get", BASE_URL, "--json"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        print(f"[ERROR] 获取 cookie 失败: {result.stderr.strip()}", file=sys.stderr)
        print("请确认:\n  1. chrome-cdp-ws-daemon 已运行（daemon.py start）\n  2. 浏览器已登录目标站点", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout.strip())


def build_session(cookies: dict, target_id: str = "B8869DE5") -> requests.Session:
    """构建带 cookie + csrf-token + uid 的 Session。

    注意：cookies 必须是按目标域名过滤后的结果（来自 `cookies get <url>`），
    不能把所有域名的 cookie 全部塞入，否则会触发 nginx 的 400 Too Large。
    """
    session = requests.Session()
    session.cookies.update(cookies)   # 只有 ~15 个目标域 cookie，安全

    headers = dict(STATIC_HEADERS)

    # csrf-token：从页面 meta 标签实时读（每次页面加载会变，不等于 _csrf cookie）
    csrf = get_csrf_from_page(target_id)
    if csrf:
        headers["csrf-token"] = csrf
        print(f"      ✓ csrf-token 从页面 meta 读取: {csrf[:20]}...")
    else:
        print("[WARN] 无法读取 csrf-token，PUT 请求可能 403", file=sys.stderr)

    # uid：从抓包分析获取（与 SSO session 绑定，不在 cookie 中直接暴露）
    # 当 SSO 重新登录时需要更新此值（从网络抓包重新获取）
    headers["uid"] = _UID_FROM_CAPTURE
    print(f"      ✓ uid: {_UID_FROM_CAPTURE}")

    session.headers.update(headers)
    session.verify = False
    return session


# ── API 调用 ─────────────────────────────────────────────────────
def get_task(session: requests.Session, job_id: str) -> dict:
    """获取任务完整 JSON（含 script 字段）。"""
    resp = session.get(JOB_API, params={"jobId": job_id}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"GET 任务失败: {data}")
    return data["data"]


def update_task(session: requests.Session, task_data: dict, new_code: str) -> dict:
    """PUT 任务，仅修改 script 字段，其余字段原样保留。

    注意：PUT body 必须包含顶层 jobId 字段（GET 响应里没有，需手动补充）。
    """
    task_data["script"] = new_code
    task_data["updateTime"] = int(time.time() * 1000)
    # GET 响应里没有顶层 jobId，PUT 需要手动加上
    if "jobId" not in task_data:
        job_id = str(task_data.get("jobBasicInfo", {}).get("jobId", ""))
        if job_id:
            task_data["jobId"] = job_id
    resp = session.put(JOB_API, json=task_data, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"PUT 任务失败: {result}")
    return result


# ── 备份 ─────────────────────────────────────────────────────────
def backup_task(task_data: dict, job_id: str) -> Path:
    """备份任务完整 JSON 到 tests/backup/。"""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"{job_id}_{ts}.json"
    path.write_text(json.dumps(task_data, ensure_ascii=False, indent=2))
    return path


# ── 主流程 ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BDP 任务代码编辑器（纯 Python，无需 Chrome）")
    parser.add_argument("--task-id", required=True, help="任务 ID，如 73590")
    parser.add_argument("--code", default=None, help="新代码内容（字符串）")
    parser.add_argument("--code-file", default=None, help="新代码文件路径")
    parser.add_argument("--write", action="store_true", help="真实写入（默认 dry-run）")
    parser.add_argument("--rollback", default=None, help="用备份 JSON 回滚任务")
    args = parser.parse_args()

    # 1. 获取 cookie + session
    print("[1/4] 获取浏览器登录态 cookie + 认证 header...")
    cookies = get_cookies()
    print(f"      ✓ 获取到 {len(cookies)} 个 cookie（域名: {BASE_URL.split('//')[1]}）")
    session = build_session(cookies)

    # 2. 拉取当前任务
    print(f"[2/4] 读取任务 {args.task_id} 当前内容...")
    task_data = get_task(session, args.task_id)
    current_code = task_data.get("script", "")
    task_name = task_data.get("jobBasicInfo", {}).get("name", "?")
    print(f"      ✓ 任务名: {task_name}")
    print(f"      ✓ 当前代码({len(current_code)}字符): {repr(current_code[:120])}")

    # ── 回滚模式 ─────────────────────────────────────────────────
    if args.rollback:
        rollback_data = json.loads(Path(args.rollback).read_text())
        rollback_code = rollback_data.get("script", "")
        print(f"\n[ROLLBACK] 回滚代码({len(rollback_code)}字符): {repr(rollback_code[:80])}")
        if not args.write:
            print("[DRY-RUN] 加 --write 才会真实回滚")
            return
        bak = backup_task(task_data, args.task_id)
        print(f"[3/4] 备份当前版本 -> {bak}")
        result = update_task(session, rollback_data, rollback_code)
        print(f"[4/4] 回滚成功: {result}")
        return

    # ── 只读模式（无 --code 也无 --code-file）────────────────────
    if not args.code and not args.code_file:
        print("\n[INFO] 未指定 --code / --code-file，仅显示当前代码：")
        print("─" * 60)
        print(current_code)
        print("─" * 60)
        return

    # ── 确定新代码 ───────────────────────────────────────────────
    if args.code_file:
        new_code = Path(args.code_file).read_text(encoding="utf-8")
    else:
        new_code = args.code

    print(f"\n[3/4] 新代码({len(new_code)}字符): {repr(new_code[:120])}")

    if not args.write:
        print("\n[DRY-RUN] 以下是即将发送的 PUT 请求（加 --write 才真实提交）:")
        import pprint
        payload_preview = dict(task_data)
        payload_preview["script"] = new_code
        payload_preview["updateTime"] = int(time.time() * 1000)
        print(f"  URL    : PUT {JOB_API}")
        print(f"  jobId  : {args.task_id}")
        print(f"  script : {repr(new_code[:200])}")
        print(f"  Headers: csrf-token={session.headers.get('csrf-token', '(从cookie推断)')}")
        return

    # ── 真实写入 ─────────────────────────────────────────────────
    bak = backup_task(task_data, args.task_id)
    print(f"      ✓ 备份当前版本 -> {bak}")

    print("[4/4] 提交新代码...")
    result = update_task(session, task_data, new_code)
    print(f"      ✓ 保存成功: {result}")

    # 回读验证
    print("      ✓ 回读验证...")
    verify = get_task(session, args.task_id)
    saved_code = verify.get("script", "")
    if saved_code == new_code:
        print(f"      ✓ 回读一致，任务 {args.task_id} 代码已更新")
    else:
        print(f"      ✗ 警告：回读代码与期望不符！")
        print(f"        期望: {repr(new_code[:80])}")
        print(f"        实际: {repr(saved_code[:80])}")
        sys.exit(1)


if __name__ == "__main__":
    main()
