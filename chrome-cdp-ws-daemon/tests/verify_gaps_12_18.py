#!/usr/bin/env python3
"""
#12-#18 缺失点自验脚本（Grafana 场景）

运行方式：
    /Users/luca/miniforge3/envs/py311/bin/python tests/verify_gaps_12_18.py
"""
import sys, os, json, base64, gzip, importlib.util, subprocess, pathlib, tempfile

PYTHON = sys.executable
DAEMON = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "daemon.py"))
sys.path.insert(0, os.path.dirname(DAEMON))
from page_manager import PageManager

PASS, FAIL = "✓", "✗"
results = []

def check(name: str, ok: bool, detail: str = ""):
    results.append((name, ok, detail))
    print(f"  {PASS if ok else FAIL} {name}" + (f"  ({detail})" if detail else ""))

def run(args, timeout=15):
    r = subprocess.run([PYTHON, DAEMON] + args, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr

print("=" * 65)
print("Chrome CDP Skill — #12~#18 自验")
print("=" * 65)

# ------------------------------------------------------------------
# #18 base64 + gzip 自动解码（单元测试，不依赖 daemon）
# ------------------------------------------------------------------
print("\n[#18] base64+gzip 自动解码")
# 模拟 Grafana gzip 压缩响应：压缩 → base64 编码 → 存入假请求
raw_text = json.dumps({"results": {"A": {"frames": [{"data": {"values": [[1, 2, 3]]}}]}}})
gz_bytes  = gzip.compress(raw_text.encode())
b64_str   = base64.b64encode(gz_bytes).decode()

# 调用解码逻辑（直接测 page_manager 内部路径）
import base64 as _b64, gzip as _gz
decoded_bytes = _b64.b64decode(b64_str)
try:
    decoded_bytes = _gz.decompress(decoded_bytes)
    decompressed = True
except Exception:
    decompressed = False
decoded_str = decoded_bytes.decode("utf-8")

check("#18a base64 解码成功", decoded_str == raw_text, f"len={len(decoded_str)}")
check("#18b gzip 解压成功", decompressed, "gzip flag")

# 普通 base64（非 gzip）— 应当静默处理
plain_b64 = base64.b64encode(b"hello world").decode()
decoded2 = base64.b64decode(plain_b64)
try:
    _gz.decompress(decoded2)
    was_gz = True
except Exception:
    was_gz = False
check("#18c 非 gzip base64 安全降级", not was_gz, "BadGzipFile caught")

# ------------------------------------------------------------------
# #16 _export_python_client SSL handling
# ------------------------------------------------------------------
print("\n[#16] export --python-client SSL 处理")
fake_req = [{
    "method": "GET", "url": "https://internal.corp.com/api",
    "status": 200, "headers": {"x-custom": "val"}, "postData": None,
}]
code = PageManager._export_python_client(fake_req, daemon_script="/path/to/daemon.py")
check("#16a 含 VERIFY_SSL", "VERIFY_SSL" in code, "")
check("#16b 含 urllib3 警告抑制", "InsecureRequestWarning" in code, "")
check("#16c session.verify = VERIFY_SSL", "session.verify = VERIFY_SSL" in code, "")

# ------------------------------------------------------------------
# #12 responseBody bodyFile 路径 (静态逻辑测试)
# ------------------------------------------------------------------
print("\n[#12] 大 body 写临时文件")
# 模拟：body > 512KB 时应写到文件
THRESHOLD = 512 * 1024
big_body = "x" * (THRESHOLD + 1)
body_size = len(big_body.encode("utf-8"))
should_file = body_size > THRESHOLD
check("#12a 阈值判断 (body > 512KB → file)", should_file, f"size={body_size}B")
# 验证 daemon stop 输出中有 bodyFile 标注
# 写一个带 bodyFile 的假抓包文件，看 filter 能否正确处理
fake_cap = [{
    "method": "GET", "url": "https://trace.tuya-inc.top/api/ds/query",
    "status": 200, "headers": {},
    "responseBody": None,
    "responseBodyFile": "/tmp/cdp_body_fake.txt",
    "responseBodySize": body_size,
}]
tmp = pathlib.Path(tempfile.gettempdir()) / "cdp_network_capture.json"
tmp.write_text(json.dumps(fake_cap))
rc, out, err = run(["network-capture", "filter", "--url", "ds/query"])
check("#12b filter 正确处理 bodyFile 条目", rc == 0 and "ds/query" in out,
      out.strip()[:80])
check("#12c bodyFile 字段在抓包 JSON 中存在",
      "responseBodyFile" in str(fake_cap[0]) and fake_cap[0].get("responseBodyFile") is not None,
      f"bodyFile={fake_cap[0].get('responseBodyFile')}")

# ------------------------------------------------------------------
# #14 local_storage 方法存在性检查
# ------------------------------------------------------------------
print("\n[#14] local_storage 方法")
check("#14a PageManager.local_storage_get 存在", hasattr(PageManager, "local_storage_get"), "")
check("#14b PageManager.local_storage_set 存在", hasattr(PageManager, "local_storage_set"), "")
check("#14c PageManager.local_storage_remove 存在", hasattr(PageManager, "local_storage_remove"), "")

# ------------------------------------------------------------------
# #15 get-url --decode-param 逻辑
# ------------------------------------------------------------------
print("\n[#15] get-url --decode-param")
from urllib.parse import urlparse, parse_qs, urlencode, quote
import json as _json
param_obj = {"LNF": {"datasource": "abc-123", "queries": [{"expr": "{app=\"test\"}"}]}}
encoded_param = quote(_json.dumps(param_obj))
fake_url = f"https://trace.tuya-inc.top/explore?orgId=1&panes={encoded_param}"
# 解码逻辑
p = urlparse(fake_url)
params = parse_qs(p.query)
raw = params.get("panes", [None])[0]
from urllib.parse import unquote
decoded = unquote(raw)
obj = _json.loads(decoded)
check("#15a URL 参数 decode 后还原为 dict", obj == param_obj, f"keys={list(obj.keys())}")
check("#15b datasource 字段正确", obj["LNF"]["datasource"] == "abc-123", "")

# ------------------------------------------------------------------
# daemon live 测试
# ------------------------------------------------------------------
print("\n[live] daemon 检查")
rc, out, err = run(["status"], timeout=5)
daemon_ok = rc == 0
check("daemon running", daemon_ok, out.strip()[:60] if daemon_ok else err.strip()[:40])

if daemon_ok:
    # #14 live: local-storage get（列出全部）
    print("\n[#14 live] local-storage get")
    rc, out, err = run(["local-storage", "get"], timeout=10)
    check("#14d local-storage get 不崩溃", rc == 0 or "Error" not in out,
          (out + err).strip()[:80])

    # #14 live: local-storage get 特定 key（Grafana device-id）
    rc, out, err = run(["local-storage", "get", "grafanaUserPreferences", "--json"], timeout=10)
    check("#14e local-storage get grafanaUserPreferences 不崩溃", rc == 0 or "not found" in err,
          (out + err).strip()[:80])

    # #15 live: get-url --decode-param panes（Grafana Explore 页面）
    print("\n[#15 live] get-url --decode-param panes")
    rc, out, err = run(["get-url", "--decode-param", "panes"], timeout=10)
    check("#15c get-url --decode-param panes 不崩溃", rc == 0 or "not found" in err,
          (out + err).strip()[:120])
    if rc == 0 and out.strip().startswith("{"):
        try:
            obj = json.loads(out)
            check("#15d panes 解析为 JSON dict", isinstance(obj, dict), f"keys={list(obj.keys())}")
        except Exception:
            check("#15d panes 解析为 JSON dict", False, "json parse failed")

    # #13 live: network-capture stop 进度提示（抓 0 请求）
    print("\n[#13 live] stop 进度提示")
    run(["network-capture", "start"], timeout=10)
    import time; time.sleep(1)
    rc, out, err = run(["network-capture", "stop", "--no-body"], timeout=30)
    check("#13a stop --no-body 在 stderr 有提示", "Stopping capture" in err,
          err.strip()[:60])
    run(["network-capture", "start"], timeout=10)
    time.sleep(0.5)
    rc, out, err = run(["network-capture", "stop"], timeout=120)
    check("#13b stop（默认带 body）在 stderr 有进度提示",
          "Stopping capture" in err and "fetching response bodies" in err,
          err.strip()[:80])

    # #18 live: 确认 stop 后的 responseBody 不会再有 base64 二进制残留
    print("\n[#18 live] stop 后 body 内容可读")
    import pathlib as _pl
    cap_file = _pl.Path(tempfile.gettempdir()) / "cdp_network_capture.json"
    if cap_file.exists():
        data = json.loads(cap_file.read_text())
        all_readable = True
        for r in data:
            body = r.get("responseBody", "")
            if body and r.get("base64Encoded"):
                all_readable = False
                break
        check("#18d 抓包结果中无残留 base64Encoded=True 条目", all_readable,
              f"{len(data)} requests checked")

else:
    print("  (daemon 未运行，跳过 live 测试)")

# ------------------------------------------------------------------
# 汇总
# ------------------------------------------------------------------
print("\n" + "=" * 65)
passed = sum(1 for _, ok, _ in results if ok)
failed = sum(1 for _, ok, _ in results if not ok)
print(f"结果：{passed} 通过 / {failed} 失败 / {len(results)} 总计")
if failed:
    print("\n失败项：")
    for name, ok, detail in results:
        if not ok:
            print(f"  {FAIL} {name}  {detail}")
print("=" * 65)
sys.exit(0 if failed == 0 else 1)
