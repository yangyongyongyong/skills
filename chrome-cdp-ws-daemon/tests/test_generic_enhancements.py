"""chrome-cdp-ws-daemon 通用增强能力单测。

这些用例只验证纯函数、SDK 请求形状和 CLI 输出，不依赖真实 Chrome。
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import cdp_client  # noqa: E402
import daemon  # noqa: E402
from page_manager import PageManager  # noqa: E402


class FakeCdp:
    """提供 PageManager 测试所需的最小 CDP 接口。"""

    def __init__(self, pages: list[dict]):
        self._pages = pages
        self._network_event_handler = None
        self._target_event_handler = None

    def get_pages(self) -> list[dict]:
        """返回固定页面集合。"""
        return list(self._pages)


def run_daemon_cli(argv: list[str]) -> tuple[int, str, str]:
    """在当前进程执行 daemon CLI，并捕获 stdout/stderr。"""
    old_argv = sys.argv
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        sys.argv = ["daemon.py", *argv]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = daemon.main()
    finally:
        sys.argv = old_argv
    return code, stdout.getvalue(), stderr.getvalue()


class CookieTests(unittest.TestCase):
    """验证 URL 级 cookie 获取和脱敏诊断。"""

    def test_cdp_client_get_cookies_uses_url_action(self) -> None:
        """SDK 应调用 get_cookies_for_url，而不是拉全量 cookie。"""
        sent: list[dict] = []

        def fake_send(req: dict, timeout: float = 15, instance: str = "") -> dict:
            sent.append(req)
            return {
                "ok": True,
                "cookies": [
                    {"name": "SESSION", "value": "s1"},
                    {"name": "token", "value": "t1"},
                ],
            }

        with mock.patch.object(cdp_client, "ensure_daemon", return_value="chrome-9222"), \
                mock.patch.object(cdp_client, "_send_to_daemon", side_effect=fake_send):
            cookies = cdp_client.get_cookies("https://example.com/path")

        self.assertEqual({"SESSION": "s1", "token": "t1"}, cookies)
        self.assertEqual("get_cookies_for_url", sent[0]["action"])
        self.assertEqual("https://example.com/path", sent[0]["url"])

    def test_daemon_get_cookies_for_url_defaults_to_browser_level_url(self) -> None:
        """daemon 默认应按 URL 查询 cookie，不应解析 active 页面。"""

        class FakeCookieCdp:
            """记录浏览器级 cookie 查询调用。"""

            def __init__(self) -> None:
                self.ensure_called = 0
                self.urls: list[str] = []

            def ensure_connected(self) -> None:
                """记录连接检查次数。"""
                self.ensure_called += 1

            def get_cookies_for_url(self, url: str) -> list[dict]:
                """返回按 URL 匹配的伪 cookie。"""
                self.urls.append(url)
                return [{"name": "URL_COOKIE", "value": "1"}]

        class FailingPageManager:
            """如果默认路径误用 active 页面，此类会让测试失败。"""

            def get_cookies_for_url(self, url: str, target: str = "active") -> dict:
                """禁止默认请求触碰页面级 cookie 查询。"""
                raise AssertionError(f"unexpected page cookie lookup: {url} {target}")

        cdp = FakeCookieCdp()
        resp = daemon._handle_get_cookies_for_url_request(
            {"url": "https://example.com/path"}, cdp, FailingPageManager()
        )

        self.assertTrue(resp["ok"])
        self.assertEqual(["https://example.com/path"], cdp.urls)
        self.assertEqual(1, cdp.ensure_called)
        self.assertEqual([{"name": "URL_COOKIE", "value": "1"}], resp["cookies"])

    def test_daemon_get_cookies_for_url_uses_page_manager_only_with_target(self) -> None:
        """调用方显式传 target 时，daemon 才绑定指定页面 session。"""

        class FakeCookieCdp:
            """提供连接检查并禁止浏览器级 cookie 查询。"""

            def __init__(self) -> None:
                self.ensure_called = 0

            def ensure_connected(self) -> None:
                """记录连接检查次数。"""
                self.ensure_called += 1

            def get_cookies_for_url(self, url: str) -> list[dict]:
                """显式 target 路径不应走浏览器级查询。"""
                raise AssertionError(f"unexpected browser cookie lookup: {url}")

        class RecordingPageManager:
            """记录页面级 cookie 查询参数。"""

            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def get_cookies_for_url(self, url: str, target: str = "active") -> dict:
                """返回指定 target 页面 session 中的伪 cookie。"""
                self.calls.append((url, target))
                return {"ok": True, "cookies": [{"name": "PAGE_COOKIE", "value": "2"}]}

        cdp = FakeCookieCdp()
        page_mgr = RecordingPageManager()
        resp = daemon._handle_get_cookies_for_url_request(
            {"url": "https://example.com/path", "target": "tab:demo"}, cdp, page_mgr
        )

        self.assertTrue(resp["ok"])
        self.assertEqual(1, cdp.ensure_called)
        self.assertEqual([("https://example.com/path", "tab:demo")], page_mgr.calls)
        self.assertEqual([{"name": "PAGE_COOKIE", "value": "2"}], resp["cookies"])

    def test_cdp_connection_filters_storage_cookies_by_url(self) -> None:
        """browser 级连接应使用 Storage.getCookies 后本地按 URL 过滤。"""
        conn = daemon.CdpConnection()
        calls: list[tuple[str, dict | None]] = []

        def fake_call(method: str, params: dict | None = None) -> dict:
            """返回覆盖域名、路径、secure 和过期场景的伪 cookie。"""
            calls.append((method, params))
            return {
                "cookies": [
                    {"name": "ROOT", "value": "1", "domain": ".example.com", "path": "/", "secure": True},
                    {"name": "APP", "value": "2", "domain": "sub.example.com", "path": "/app"},
                    {"name": "OTHER_PATH", "value": "3", "domain": ".example.com", "path": "/admin"},
                    {"name": "OTHER_DOMAIN", "value": "4", "domain": "other.example.com", "path": "/"},
                    {"name": "HTTP_ONLY", "value": "5", "domain": ".example.com", "path": "/", "secure": True},
                    {"name": "EXPIRED", "value": "6", "domain": ".example.com", "path": "/", "expires": 1},
                ],
            }

        conn._call_locked = fake_call  # type: ignore[method-assign]
        cookies = conn.get_cookies_for_url("https://sub.example.com/app/page")

        self.assertEqual([("Storage.getCookies", None)], calls)
        self.assertEqual(["ROOT", "APP", "HTTP_ONLY"], [item["name"] for item in cookies])

    def test_cookies_inspect_redacts_values(self) -> None:
        """cookies inspect 不输出 cookie value。"""
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value={
                    "ok": True,
                    "cookies": [
                        {
                            "name": "SESSION",
                            "value": "secret-value",
                            "domain": "example.com",
                            "path": "/",
                            "httpOnly": True,
                            "secure": True,
                        }
                    ],
                }):
            code, out, err = run_daemon_cli(["cookies", "inspect", "https://example.com", "--json"])

        self.assertEqual(0, code, err)
        self.assertIn("SESSION", out)
        self.assertNotIn("secret-value", out)
        payload = json.loads(out)
        self.assertNotIn("value", payload["cookies"][0])

    def test_cookies_validate_all_present(self) -> None:
        """validate 全部命中时返回 0。"""
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value={
                    "ok": True,
                    "cookies": [{"name": "A", "value": "1"}, {"name": "B", "value": "2"}],
                }):
            code, out, _ = run_daemon_cli(["cookies", "validate", "https://example.com", "--expect", "A,B", "--json"])

        self.assertEqual(0, code)
        payload = json.loads(out)
        self.assertEqual(["A", "B"], payload["present"])
        self.assertEqual([], payload["missing"])

    def test_cookies_validate_missing_or_empty(self) -> None:
        """validate 缺失或无 cookie 时返回 1 并列出 missing。"""
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value={"ok": True, "cookies": [{"name": "A"}]}):
            code, out, _ = run_daemon_cli(["cookies", "validate", "https://example.com", "--expect", "A,B", "--json"])
        self.assertEqual(1, code)
        self.assertEqual(["B"], json.loads(out)["missing"])

        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value={"ok": True, "cookies": []}):
            code, out, _ = run_daemon_cli(["cookies", "validate", "https://example.com", "--expect", "A", "--json"])
        self.assertEqual(1, code)
        self.assertEqual(["A"], json.loads(out)["missing"])


class NetworkSummaryTests(unittest.TestCase):
    """验证抓包摘要结构和脱敏行为。"""

    def sample_requests(self) -> list[dict]:
        """构造一条包含请求体、响应体、认证头和查询参数的抓包记录。"""
        return [
            {
                "url": "https://example.com/api/items?page=1&q=x",
                "method": "POST",
                "status": 200,
                "headers": {
                    "Cookie": "SESSION=secret",
                    "x-csrf-token": "csrf-secret",
                    "x-plain": "ok",
                },
                "responseHeaders": {"Content-Type": "application/json"},
                "postData": json.dumps({"name": "demo"}),
                "responseBody": json.dumps({"data": [{"id": 1}]}),
            }
        ]

    def test_summary_contains_structured_fields_and_redacts_auth(self) -> None:
        """summary JSON 应包含结构化字段，且认证头必须脱敏。"""
        summary = daemon._summarize_requests(self.sample_requests())
        item = summary["requests"][0]

        self.assertEqual("example.com", item["host"])
        self.assertEqual("/api/items", item["path"])
        self.assertEqual(["page", "q"], item["query_keys"])
        self.assertEqual("application/json", item["content_type"])
        self.assertIn("request_shape", item)
        self.assertIn("response_shape", item)
        self.assertEqual(item["body_size"], item["response_bytes"])
        self.assertEqual("***", item["auth_headers_redacted"]["Cookie"])
        self.assertEqual("***", item["auth_headers_redacted"]["x-csrf-token"])

    def test_summary_cli_writes_out_file(self) -> None:
        """network-capture summary --out 应写出同一份结构化 JSON。"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            capture_path = tmp / "capture.json"
            out_path = tmp / "summary.json"
            capture_path.write_text(json.dumps(self.sample_requests(), ensure_ascii=False), encoding="utf-8")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_path", return_value=capture_path):
                code, out, err = run_daemon_cli(["network-capture", "summary", "--json", "--out", str(out_path)])

            self.assertEqual(0, code, err)
            self.assertTrue(out_path.exists())
            stdout_payload = json.loads(out)
            file_payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(stdout_payload["requests"][0]["host"], file_payload["requests"][0]["host"])
            self.assertNotIn("secret", out)


class AuthMaterialTests(unittest.TestCase):
    """验证通用认证材料汇总和 token 换取能力。"""

    def test_auth_material_redacts_by_default_and_reveals_when_requested(self) -> None:
        """material 默认脱敏，显式 reveal 才输出真实值。"""
        capture = [{
            "url": "https://example.com/api/items",
            "method": "GET",
            "headers": {
                "Authorization": "Bearer header-secret",
                "x-plain": "ok",
            },
        }]
        payload = daemon._auth_material_payload(
            url="https://example.com",
            cookies=[{"name": "SESSION", "value": "cookie-secret", "domain": "example.com"}],
            local_items={"accessToken": "local-secret", "theme": "dark"},
            session_items={"csrfToken": "csrf-secret"},
            capture_requests=capture,
            reveal=False,
        )
        text = json.dumps(payload, ensure_ascii=False)
        self.assertIn("SESSION", text)
        self.assertIn("accessToken", text)
        self.assertIn("Authorization", text)
        self.assertNotIn("cookie-secret", text)
        self.assertNotIn("local-secret", text)
        self.assertNotIn("header-secret", text)

        revealed = daemon._auth_material_payload(
            url="https://example.com",
            cookies=[{"name": "SESSION", "value": "cookie-secret", "domain": "example.com"}],
            local_items={"accessToken": "local-secret"},
            session_items={},
            capture_requests=capture,
            reveal=True,
        )
        self.assertIn("cookie-secret", json.dumps(revealed, ensure_ascii=False))
        self.assertIn("local-secret", json.dumps(revealed, ensure_ascii=False))

    def test_auth_material_key_filter_can_include_non_token_storage_key(self) -> None:
        """--key 过滤应能按名称命中 cookie/storage/header。"""
        payload = daemon._auth_material_payload(
            url="https://example.com",
            cookies=[{"name": "theme_cookie", "value": "c1"}],
            local_items={"theme": "dark", "accessToken": "secret"},
            session_items={},
            capture_requests=[],
            reveal=True,
            key_filter="theme",
        )
        text = json.dumps(payload, ensure_ascii=False)
        self.assertIn("theme_cookie", text)
        self.assertIn("theme", text)
        self.assertIn("dark", text)
        self.assertNotIn("accessToken", text)

    def test_auth_material_cli_outputs_daemon_response(self) -> None:
        """auth material CLI 应转发 daemon 的结构化 JSON。"""
        response = {
            "ok": True,
            "url": "https://example.com",
            "cookies": [{"name": "SESSION", "value": "<redacted>"}],
            "storage": {"localStorage": [], "sessionStorage": []},
            "auth_headers": [],
        }
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value=response) as mocked_send:
            code, out, err = run_daemon_cli([
                "auth", "material", "https://example.com", "--key", "SESSION", "--json"
            ])
        self.assertEqual(0, code, err)
        payload = json.loads(out)
        self.assertEqual("https://example.com", payload["url"])
        self.assertEqual("auth_material", mocked_send.call_args[0][0]["action"])
        self.assertEqual("SESSION", mocked_send.call_args[0][0]["key_filter"])

    def test_auth_token_extracts_dotted_and_jsonpath_and_renders_headers(self) -> None:
        """token helper 应支持 dotted path / $.path，并按模板渲染 header。"""

        class FakeResponse:
            ok = True
            status_code = 200
            text = '{"data":{"token":"token-secret"}}'

            def json(self) -> dict:
                """返回固定 token 响应。"""
                return {"data": {"token": "token-secret"}}

        class FakeSession:
            def __init__(self) -> None:
                """记录请求参数。"""
                self.cookies = {}
                self.calls: list[dict] = []

            def request(self, *args, **kwargs) -> FakeResponse:
                """模拟 requests.Session.request。"""
                kwargs["args"] = args
                self.calls.append(kwargs)
                return FakeResponse()

        fake = FakeSession()
        with mock.patch.object(daemon.requests, "Session", return_value=fake):
            redacted = daemon._request_auth_token_from_cookies(
                request_url="https://example.com/api-token",
                cookies={"SESSION": "cookie-secret"},
                method="POST",
                body='{"a":1}',
                headers={"Cache-Control": "no-cache"},
                extract="data.token",
                header_templates=["Authorization=TUYA {token}"],
                reveal=False,
            )
        self.assertTrue(redacted["ok"])
        self.assertEqual("<redacted>", redacted["token"])
        self.assertEqual("<redacted>", redacted["headers"]["Authorization"])
        self.assertEqual("cookie-secret", fake.cookies["SESSION"])
        self.assertEqual({"a": 1}, fake.calls[0]["json"])

        with mock.patch.object(daemon.requests, "Session", return_value=FakeSession()):
            revealed = daemon._request_auth_token_from_cookies(
                request_url="https://example.com/api-token",
                cookies={"SESSION": "cookie-secret"},
                extract="$.data.token",
                header_templates=["Authorization=TUYA {token}"],
                reveal=True,
            )
        self.assertEqual("token-secret", revealed["token"])
        self.assertEqual("TUYA token-secret", revealed["headers"]["Authorization"])

    def test_auth_token_missing_extract_returns_clear_error(self) -> None:
        """extract 路径不存在时应返回清晰错误。"""

        class FakeResponse:
            ok = True
            status_code = 200
            text = '{"data":{}}'

            def json(self) -> dict:
                """返回不含 token 的响应。"""
                return {"data": {}}

        class FakeSession:
            cookies = {}

            def request(self, *args, **kwargs) -> FakeResponse:
                """模拟 requests.Session.request。"""
                return FakeResponse()

        with mock.patch.object(daemon.requests, "Session", return_value=FakeSession()):
            result = daemon._request_auth_token_from_cookies(
                request_url="https://example.com/api-token",
                cookies={"SESSION": "cookie-secret"},
                extract="data.token",
            )
        self.assertFalse(result["ok"])
        self.assertIn("未从响应中提取到 token", result["error"])

    def test_auth_token_cli_keeps_secret_redacted_without_reveal(self) -> None:
        """auth token CLI 默认不输出真实 token。"""
        response = {
            "ok": True,
            "token": "<redacted>",
            "headers": {"Authorization": "<redacted>"},
        }
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value=response) as mocked_send:
            code, out, err = run_daemon_cli([
                "auth", "token", "https://example.com/api-token",
                "--method", "POST",
                "--body", "{}",
                "--headers", '{"Cache-Control":"no-cache"}',
                "--extract", "data.token",
                "--header-template", "Authorization=TUYA {token}",
                "--json",
            ])
        self.assertEqual(0, code, err)
        self.assertNotIn("token-secret", out)
        req = mocked_send.call_args[0][0]
        self.assertEqual("auth_token", req["action"])
        self.assertFalse(req["reveal"])
        self.assertEqual({"Cache-Control": "no-cache"}, req["headers"])


class AuthSdkTests(unittest.TestCase):
    """验证 cdp_client 新增 SDK 只在显式调用时访问 daemon。"""

    def test_auth_sdk_calls_daemon_actions(self) -> None:
        """SDK 应向 daemon 发送通用 auth/storage action。"""
        sent: list[dict] = []

        def fake_send(req: dict, timeout: float = 15, instance: str = "") -> dict:
            sent.append(req)
            if req["action"] == "local_storage_get":
                return {"ok": True, "items": {"token": "s1"}}
            if req["action"] == "auth_material":
                return {"ok": True, "cookies": [], "storage": {}, "auth_headers": []}
            if req["action"] == "auth_token":
                return {"ok": True, "token": "t1", "headers": {"Authorization": "TUYA t1"}}
            return {"ok": False, "error": "unexpected"}

        with mock.patch.object(cdp_client, "ensure_daemon", return_value="chrome-9222"), \
                mock.patch.object(cdp_client, "_send_to_daemon", side_effect=fake_send):
            storage = cdp_client.get_storage("token", storage="session", target="active")
            material = cdp_client.get_auth_material("https://example.com", key_filter="token")
            token = cdp_client.request_auth_token(
                "https://example.com/api-token",
                method="POST",
                extract="data.token",
                header_templates=["Authorization=TUYA {token}"],
            )

        self.assertEqual({"token": "s1"}, storage["items"])
        self.assertTrue(material["ok"])
        self.assertEqual("t1", token["token"])
        self.assertEqual("local_storage_get", sent[0]["action"])
        self.assertEqual("auth_material", sent[1]["action"])
        self.assertTrue(sent[1]["reveal"])
        self.assertEqual("auth_token", sent[2]["action"])
        self.assertTrue(sent[2]["reveal"])


class ExportClientTests(unittest.TestCase):
    """验证抓包导出的 Python 客户端优先使用 SDK 鉴权 helper。"""

    def test_export_python_client_imports_auth_sdk_helpers(self) -> None:
        """导出客户端不应继续生成 subprocess cookies get 样板。"""
        code = PageManager._export_python_client([{
            "url": "https://example.com/api/items",
            "method": "GET",
            "headers": {"Authorization": "Bearer demo"},
            "status": 200,
        }], daemon_script="/tmp/daemon.py")
        self.assertIn("from cdp_client import get_auth_material, get_cookies, get_storage, request_auth_token", code)
        self.assertIn("live_cookies = get_cookies(TARGET_URL)", code)
        self.assertIn("request_auth_token", code)
        self.assertNotIn("subprocess.run", code)


class TargetResolveTests(unittest.TestCase):
    """验证新增 target selector 和多命中保护。"""

    def manager(self) -> PageManager:
        """构造带固定页面列表的 PageManager。"""
        return PageManager(FakeCdp([
            {"targetId": "ABCDEF00000000000000000000000001", "url": "https://a.example.com/app", "title": "Alpha"},
            {"targetId": "ABCDEF00000000000000000000000002", "url": "https://b.example.com/app", "title": "Beta"},
            {"targetId": "12345600000000000000000000000003", "url": "https://a.example.com/admin", "title": "Admin Alpha"},
        ]))

    def test_host_and_title_resolve_unique(self) -> None:
        """host/title 唯一命中时返回对应 targetId。"""
        mgr = self.manager()
        self.assertEqual("ABCDEF00000000000000000000000002", mgr.resolve_target("host:b.example.com"))
        self.assertEqual("ABCDEF00000000000000000000000002", mgr.resolve_target("title:Beta"))

    def test_strict_selectors_fail_on_multiple_candidates(self) -> None:
        """严格 selector 多命中时失败并提示绑定 alias。"""
        mgr = self.manager()
        with self.assertRaisesRegex(RuntimeError, "multiple pages matching host:a.example.com"):
            mgr.resolve_target("host:a.example.com")
        with self.assertRaisesRegex(RuntimeError, "multiple pages matching url-strict:app"):
            mgr.resolve_target("url-strict:app")

    def test_url_selector_keeps_first_match_compatibility(self) -> None:
        """旧 url: selector 继续保持第一个命中的兼容行为。"""
        self.assertEqual(
            "ABCDEF00000000000000000000000001",
            self.manager().resolve_target("url:app"),
        )

    def test_target_prefix_multiple_matches_fail(self) -> None:
        """targetId 短前缀多命中时失败，避免误操作。"""
        with self.assertRaisesRegex(RuntimeError, "multiple pages matching targetId prefix: ABC"):
            self.manager().resolve_target("ABC")

    def test_resolve_target_info_returns_candidates(self) -> None:
        """结构化 resolve 失败时返回候选列表。"""
        result = self.manager().resolve_target_info("host:a.example.com")
        self.assertFalse(result["ok"])
        self.assertEqual(2, len(result["candidates"]))
        self.assertIn("tab bind", result["suggestion"])

    def test_target_cli_resolve_prints_candidates(self) -> None:
        """target resolve CLI 失败时应输出候选和 alias 绑定建议。"""
        response = {
            "ok": False,
            "error": "multiple pages matching host:a.example.com",
            "candidates": [
                {"targetId": "ABCDEF00000000000000000000000001", "url": "https://a.example.com/app", "title": "Alpha"}
            ],
            "suggestion": "tab bind <name> --target <targetId>",
        }
        with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                mock.patch.object(daemon, "_send", return_value=response):
            code, out, err = run_daemon_cli(["target", "resolve", "host:a.example.com"])

        self.assertEqual(1, code)
        self.assertEqual("", out)
        self.assertIn("candidate: ABCDEF00", err)
        self.assertIn("tab bind", err)


if __name__ == "__main__":
    unittest.main()
