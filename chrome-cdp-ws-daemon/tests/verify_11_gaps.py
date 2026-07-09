#!/usr/bin/env python3
"""
11 个缺失点自验脚本

运行方式：
    /Users/luca/miniforge3/envs/py311/bin/python tests/verify_11_gaps.py

需要：CDP daemon 正在运行（有活动页面）
"""
import subprocess
import sys
import json
import importlib.util
import os

PYTHON = sys.executable
DAEMON = os.path.join(os.path.dirname(__file__), "..", "scripts", "daemon.py")
DAEMON = os.path.abspath(DAEMON)

PASS = "✓"
FAIL = "✗"
results = []

def run(args: list[str], timeout=15) -> tuple[int, str, str]:
    r = subprocess.run(
        [PYTHON, DAEMON] + args,
        capture_output=True, text=True, timeout=timeout
    )
    return r.returncode, r.stdout, r.stderr


def check(name: str, ok: bool, detail: str = ""):
    icon = PASS if ok else FAIL
    results.append((name, ok, detail))
    print(f"  {icon} {name}" + (f"  ({detail})" if detail else ""))


print("=" * 60)
print("Chrome CDP Skill — 11 缺失点自验")
print("=" * 60)

# ------------------------------------------------------------------
# #3 press 组合键（静态逻辑测试，不依赖 daemon）
# ------------------------------------------------------------------
print("\n[#3] press 组合键 — 解析逻辑")
sys.path.insert(0, os.path.join(os.path.dirname(DAEMON)))
from page_manager import PageManager

mods, key = PageManager._parse_key_combo("Meta+S")
check("#3a Meta+S → modifiers=4, key=S", mods == 4 and key == "S", f"got mods={mods} key={key}")

mods, key = PageManager._parse_key_combo("Ctrl+Shift+P")
check("#3b Ctrl+Shift+P → modifiers=10", mods == 10 and key == "P", f"got mods={mods} key={key}")

mods, key = PageManager._parse_key_combo("Alt+F4")
check("#3c Alt+F4 → modifiers=1", mods == 1 and key == "F4", f"got mods={mods} key={key}")

mods, key = PageManager._parse_key_combo("Enter")
check("#3d 单键 Enter → modifiers=0", mods == 0 and key == "Enter", f"got mods={mods} key={key}")

# ------------------------------------------------------------------
# #8 get_all_cookies 文档警告
# ------------------------------------------------------------------
print("\n[#8] get_all_cookies — docstring 警告")
import inspect
src_path = os.path.join(os.path.dirname(DAEMON), "cdp_client.py")
spec = importlib.util.spec_from_file_location("cdp_client", src_path)
cdp_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cdp_mod)
doc = cdp_mod.get_all_cookies.__doc__ or ""
check("#8 docstring 含 warning", "warning" in doc.lower() or "警告" in doc, doc[:80])
check("#8 docstring 提示 get_cookies(url)", "get_cookies(url)" in doc, "")

# ------------------------------------------------------------------
# #11 _export_python 保留非标 header
# ------------------------------------------------------------------
print("\n[#11] _export_python 保留 uid / csrf-token")
fake_reqs = [{
    "method": "PUT",
    "url": "https://example.com/api",
    "status": 200,
    "headers": {
        "uid": "user123",
        "csrf-token": "tok456",
        "host": "example.com",
        "sec-fetch-site": "same-origin",
        "content-type": "application/json",
    },
    "postData": '{"key": "val"}',
}]
code = PageManager._export_python(fake_reqs)
check("#11a uid 保留在导出代码", '"uid"' in code or "'uid'" in code, "")
check("#11b csrf-token 保留在导出代码", "csrf-token" in code, "")
check("#11c sec-fetch-site 已过滤", "sec-fetch-site" not in code, "")
check("#11d host 已过滤", '"host"' not in code, "")

# ------------------------------------------------------------------
# #5 _export_python_client 完整客户端
# ------------------------------------------------------------------
print("\n[#5] _export_python_client 完整客户端")
client_code = PageManager._export_python_client(fake_reqs, daemon_script="/path/to/daemon.py")
check("#5a 含 get_live_cookies", "get_live_cookies" in client_code, "")
check("#5b 含 cookies get 子命令", "cookies" in client_code and "get" in client_code, "")
check("#5c 含 session.cookies.update", "session.cookies.update" in client_code, "")
check("#5d 含断言 assert", "assert" in client_code, "")
check("#5e 含 uid 非标 header 示例", "uid" in client_code, "")

# ------------------------------------------------------------------
# #2 scan_shortcuts 方法存在且可调用
# ------------------------------------------------------------------
print("\n[#2] scan_shortcuts — 方法存在")
check("#2 PageManager.scan_shortcuts 存在", hasattr(PageManager, "scan_shortcuts"), "")
check("#2 capture_headers 存在", hasattr(PageManager, "capture_headers"), "")
check("#2 eval_js 存在", hasattr(PageManager, "eval_js"), "")

# ------------------------------------------------------------------
# Daemon 是否运行 — 后续 live 测试前提
# ------------------------------------------------------------------
print("\n[live 前置检查] daemon 是否运行")
rc, out, err = run(["status"], timeout=5)
daemon_ok = rc == 0
check("daemon running", daemon_ok, out.strip()[:60] if daemon_ok else err.strip()[:60])

if not daemon_ok:
    print("\n  daemon 未运行，跳过 live 测试（#4/#6/#7/#9/#10）")
else:
    # ------------------------------------------------------------------
    # #4 eval-js
    # ------------------------------------------------------------------
    print("\n[#4] eval-js — 执行 JS 表达式")
    rc, out, err = run(["eval-js", "1 + 1"])
    check("#4a eval-js 1+1 = 2", rc == 0 and "2" in out, out.strip()[:40])

    rc, out, err = run(["eval-js", 'document.title'])
    check("#4b eval-js 获取 document.title", rc == 0 and len(out.strip()) > 0, out.strip()[:40])

    # #4c 获取 meta csrf-token（可能不存在，只要不报错）
    rc, out, err = run(["eval-js", 'document.querySelector("meta[name=\'csrf-token\']")?.content || "not-found"'])
    check("#4c eval-js 获取 csrf-token meta（不崩溃）", rc == 0, out.strip()[:60])

    # ------------------------------------------------------------------
    # #9 network fetch --headers
    # ------------------------------------------------------------------
    print("\n[#9] network fetch --headers")
    # 用页面上下文 fetch httpbin（如果网络通的话）；退而求其次 fetch 当前页
    rc, out, err = run([
        "network", "fetch",
        "https://httpbin.org/headers",
        "--headers", '{"X-Test-Header": "verify9"}',
    ], timeout=15)
    if rc == 0 and "X-Test-Header" in out:
        check("#9 --headers 透传到请求", True, "httpbin echoed header")
    else:
        # 网络不通时，只检查 CLI 不报错 / 参数解析正确
        rc2, out2, err2 = run([
            "network", "fetch",
            "javascript:void(0)",
            "--headers", '{"X-Test": "ok"}',
        ], timeout=10)
        check("#9 --headers JSON 解析不崩溃", '"--headers must be valid JSON"' not in err2,
              f"rc={rc2} err={err2[:60]}")

    # ------------------------------------------------------------------
    # #7 network-capture stop 默认 get_body（通过抓一个简单操作验证）
    # ------------------------------------------------------------------
    print("\n[#7] network-capture stop 默认 get_body")
    run(["network-capture", "start"], timeout=10)
    import time; time.sleep(1)  # 短暂等待
    rc, out, err = run(["network-capture", "stop", "--no-body"], timeout=30)
    check("#7a stop --no-body 不崩溃", rc == 0 or "(no API requests captured)" in out,
          out.strip()[:60])

    run(["network-capture", "start"], timeout=10)
    time.sleep(1)
    rc, out, err = run(["network-capture", "stop"], timeout=60)
    check("#7b stop 默认（无 --no-body）不崩溃", rc == 0 or "(no API requests captured)" in out,
          out.strip()[:60])

    # ------------------------------------------------------------------
    # #10 network-capture filter
    # ------------------------------------------------------------------
    print("\n[#10] network-capture filter")
    import tempfile, pathlib
    # 写一个假抓包文件
    fake_capture = [
        {"method": "GET", "url": "https://api.example.com/data", "status": 200, "headers": {}},
        {"method": "POST", "url": "https://api.example.com/save", "status": 201, "headers": {}, "postData": "{}"},
        {"method": "GET", "url": "https://cdn.example.com/img.png", "status": 200, "headers": {}},
    ]
    tmp = pathlib.Path(tempfile.gettempdir()) / "cdp_network_capture.json"
    tmp.write_text(json.dumps(fake_capture))
    rc, out, err = run(["network-capture", "filter", "--method", "GET", "--exclude-domain", "cdn"])
    check("#10a filter method=GET exclude cdn → 1 result",
          rc == 0 and "[1]" in out and "[2]" not in out and "cdn" not in out,
          out.strip()[:80])

    rc, out, err = run(["network-capture", "filter", "--url", "save"])
    check("#10b filter --url save → 1 result",
          rc == 0 and "save" in out,
          out.strip()[:80])

    # ------------------------------------------------------------------
    # #6 capture-headers（daemon live，抓 0 秒等待，验证不崩溃）
    # ------------------------------------------------------------------
    print("\n[#6] capture-headers")
    rc, out, err = run(["capture-headers", "--wait", "2"], timeout=25)
    check("#6 capture-headers 运行不崩溃", rc == 0,
          (out + err).strip()[:80])

    # ------------------------------------------------------------------
    # #2 scan-shortcuts live
    # ------------------------------------------------------------------
    print("\n[#2] scan-shortcuts live")
    rc, out, err = run(["scan-shortcuts"], timeout=20)
    check("#2 scan-shortcuts 运行不崩溃", rc == 0,
          (out + err).strip()[:80])

# ------------------------------------------------------------------
# 汇总
# ------------------------------------------------------------------
print("\n" + "=" * 60)
passed = sum(1 for _, ok, _ in results if ok)
failed = sum(1 for _, ok, _ in results if not ok)
print(f"结果：{passed} 通过 / {failed} 失败 / {len(results)} 总计")
if failed:
    print("\n失败项：")
    for name, ok, detail in results:
        if not ok:
            print(f"  {FAIL} {name}  {detail}")
print("=" * 60)
sys.exit(0 if failed == 0 else 1)
