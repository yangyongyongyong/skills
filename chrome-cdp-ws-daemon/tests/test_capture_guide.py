"""capture-guide 会话式手动抓包单测。"""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import types
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

    def __init__(self) -> None:
        """初始化空页面列表与事件回调槽位。"""
        self._network_event_handler = None
        self._target_event_handler = None

    def get_pages(self) -> list[dict]:
        """返回空页面列表。"""
        return []


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


class NetworkCapturePeekTests(unittest.TestCase):
    """验证 network_capture_peek 的只读语义。"""

    def test_page_manager_peek_returns_requests_without_clearing_buffer(self) -> None:
        """peek 应返回累计请求，并保持缓冲区不变。"""
        manager = PageManager(FakeCdp())
        manager.resolve_target = lambda target="active": "tab-1"  # type: ignore[method-assign]
        manager._get_or_attach = lambda target_id: "sid-1"  # type: ignore[method-assign]
        manager._drain_network_events = lambda target_id, drain_ms=200: None  # type: ignore[method-assign]
        request = {
            "requestId": "r1",
            "url": "https://example.com/api/items",
            "method": "GET",
            "status": 200,
            "sessionId": "sid-1",
        }
        manager._net_capture_active["sid-1"] = True
        manager._net_capture_buffer["sid-1"] = [request]
        manager._net_capture_started_at["sid-1"] = 1.0
        manager._net_last_event_at["sid-1"] = 2.0

        resp = manager.network_capture_peek("active")

        self.assertTrue(resp["ok"])
        self.assertEqual(1, resp["count"])
        self.assertEqual("https://example.com/api/items", resp["requests"][0]["url"])
        self.assertEqual(1, len(manager._net_capture_buffer["sid-1"]))

    def test_cdp_client_network_capture_peek_uses_page_action(self) -> None:
        """SDK 应通过 page action 调用 network_capture_peek。"""
        with mock.patch.object(cdp_client, "_page_action", return_value={"ok": True, "count": 3}) as mocked:
            resp = cdp_client.network_capture_peek(target="tab:demo")
        self.assertEqual({"ok": True, "count": 3}, resp)
        self.assertEqual("network_capture_peek", mocked.call_args[0][0])
        self.assertEqual("tab:demo", mocked.call_args[1]["target"])


class CaptureGuideCliTests(unittest.TestCase):
    """验证 capture-guide 的会话流转与导出能力。"""

    def _write_session(self, root: Path, payload: dict) -> Path:
        """把会话 JSON 写到临时目录。"""
        path = root / f"{payload['session_id']}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _base_session(self, session_id: str = "sess123") -> dict:
        """构造最小可运行的 capture-guide 会话。"""
        return {
            "session_id": session_id,
            "status": "awaiting_user",
            "target": "tab:demo",
            "created_at": 1.0,
            "updated_at": 1.0,
            "capture_started": True,
            "total_steps": 2,
            "idle_ms": 800,
            "baseline_ms": 1500,
            "stop_options": {
                "get_body": True,
                "body_mode": "filtered",
                "wait_ms": 0,
                "idle_ms": 800,
                "max_bodies": 6,
                "max_body_bytes": 0,
                "method_filter": "",
                "url_filter": "/api/",
                "exclude_domain": "",
                "status_filter": "",
                "until_match": "",
            },
            "baseline_summary": {"count": 1, "request_keys": {"GET /api/poll": 1}, "top_requests": []},
            "steps": [
                {
                    "index": 1,
                    "key": "step_1",
                    "text": "点击按钮 A",
                    "status": "active",
                    "retry_count": 0,
                    "capture_count_at_step_start": 1,
                    "capture_count_at_ack": 0,
                    "capture_count_after_idle": 0,
                    "captured_indexes": [],
                    "summary": None,
                    "attempts": [],
                    "last_completed_at": 0.0,
                },
                {
                    "index": 2,
                    "key": "step_2",
                    "text": "点击按钮 B",
                    "status": "pending",
                    "retry_count": 0,
                    "capture_count_at_step_start": 0,
                    "capture_count_at_ack": 0,
                    "capture_count_after_idle": 0,
                    "captured_indexes": [],
                    "summary": None,
                    "attempts": [],
                    "last_completed_at": 0.0,
                },
            ],
            "recent_step_summary": None,
            "capture_file": "",
            "filtered_capture_file": "",
            "summary": None,
            "crud": None,
            "body_diff": None,
            "body_fetch": {},
        }

    def test_start_creates_session_and_returns_first_prompt(self) -> None:
        """start --json 应创建会话并返回首个 next_prompt。"""
        with tempfile.TemporaryDirectory() as td:
            session_dir = Path(td)
            baseline_request = {
                "url": "https://example.com/api/poll",
                "method": "GET",
                "status": 200,
            }
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(daemon.time, "sleep", return_value=None), \
                    mock.patch.object(daemon.uuid, "uuid4", return_value=types.SimpleNamespace(hex="sess123456789")), \
                    mock.patch.object(daemon, "_send", side_effect=[
                        {"ok": True, "target_id": "tab-1"},
                        {"ok": True, "count": 1, "requests": [baseline_request], "last_event_at": 1.0},
                    ]):
                code, out, err = run_daemon_cli([
                    "capture-guide", "start",
                    "--step", "点击按钮 A",
                    "--step", "点击按钮 B",
                    "--json",
                ])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("sess12345678", payload["session_id"])
            self.assertEqual("awaiting_user", payload["status"])
            self.assertIn("点击按钮 A", payload["next_prompt"])
            self.assertTrue((session_dir / "sess12345678.json").exists())

    def test_ack_last_step_auto_finishes_and_saves_capture(self) -> None:
        """最后一步 ack 应自动 stop，并返回完成态摘要。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("sessack")
            session["total_steps"] = 1
            session["steps"] = [session["steps"][0]]
            session["steps"][0]["status"] = "active"
            self._write_session(session_dir, session)
            baseline = {
                "url": "https://example.com/api/poll",
                "method": "GET",
                "status": 200,
            }
            step_req = {
                "url": "https://example.com/api/save",
                "method": "POST",
                "status": 200,
                "postData": json.dumps({"name": "demo"}),
                "responseBody": json.dumps({"ok": True}),
            }
            stop_resp = {
                "ok": True,
                "requests": [baseline, step_req],
                "body_fetch": {"mode": "filtered", "selected": 1, "fetched": 1},
            }

            def fake_capture_path(filtered: bool = False) -> Path:
                """把默认抓包输出重定向到临时目录。"""
                name = "filtered.json" if filtered else "full.json"
                return capture_dir / name

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_capture_guide_wait_for_idle", return_value={
                        "ok": True,
                        "count": 2,
                        "requests": [baseline, step_req],
                        "last_event_at": 2.0,
                    }), \
                    mock.patch.object(daemon, "_send", side_effect=[
                        {"ok": True, "count": 2, "requests": [baseline, step_req], "last_event_at": 2.0},
                        stop_resp,
                    ]):
                code, out, err = run_daemon_cli(["capture-guide", "ack", "--session", "sessack", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("completed", payload["status"])
            self.assertTrue(payload["is_finished"])
            self.assertTrue(Path(payload["capture_file"]).exists())
            self.assertIn("/api/save", json.dumps(payload["summary"], ensure_ascii=False))

    def test_skip_moves_to_next_step(self) -> None:
        """skip 应跳过当前步骤并推进到下一步。"""
        with tempfile.TemporaryDirectory() as td:
            session_dir = Path(td)
            session = self._base_session("sessskip")
            self._write_session(session_dir, session)
            snapshot = {
                "ok": True,
                "count": 3,
                "requests": [
                    {"url": "https://example.com/api/poll", "method": "GET", "status": 200},
                    {"url": "https://example.com/api/detail", "method": "GET", "status": 200},
                    {"url": "https://example.com/api/detail", "method": "GET", "status": 200},
                ],
                "last_event_at": 3.0,
            }
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_send", return_value=snapshot):
                code, out, err = run_daemon_cli(["capture-guide", "skip", "--session", "sessskip", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("awaiting_user", payload["status"])
            self.assertEqual(2, payload["next_step"]["index"])
            updated = json.loads((session_dir / "sessskip.json").read_text(encoding="utf-8"))
            self.assertEqual("skipped", updated["steps"][0]["status"])
            self.assertEqual(3, updated["steps"][1]["capture_count_at_step_start"])

    def test_retry_resets_active_step_and_keeps_history(self) -> None:
        """retry 应重置当前 active 步骤边界，并保留旧结果到 attempts。"""
        with tempfile.TemporaryDirectory() as td:
            session_dir = Path(td)
            session = self._base_session("sessretry")
            session["steps"][0]["summary"] = {"count": 1}
            session["steps"][0]["captured_indexes"] = [2]
            self._write_session(session_dir, session)
            snapshot = {
                "ok": True,
                "count": 5,
                "requests": [],
                "last_event_at": 5.0,
            }
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_send", return_value=snapshot):
                code, out, err = run_daemon_cli(["capture-guide", "retry", "--session", "sessretry", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual(1, payload["current_step"]["retry_count"])
            updated = json.loads((session_dir / "sessretry.json").read_text(encoding="utf-8"))
            self.assertEqual(5, updated["steps"][0]["capture_count_at_step_start"])
            self.assertEqual(1, len(updated["steps"][0]["attempts"]))

    def test_status_reads_current_step_and_recent_summary(self) -> None:
        """status 应返回当前步骤和最近一步摘要。"""
        with tempfile.TemporaryDirectory() as td:
            session_dir = Path(td)
            session = self._base_session("sessstatus")
            session["recent_step_summary"] = {"step": {"index": 1}}
            self._write_session(session_dir, session)
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir):
                code, out, err = run_daemon_cli(["capture-guide", "status", "--session", "sessstatus", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual(1, payload["current_step"]["index"])
            self.assertEqual({"step": {"index": 1}}, payload["recent_step_summary"])

    def test_analyze_returns_step_and_flow_summary(self) -> None:
        """analyze 应输出步骤摘要和全流程分析字段。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_file = root / "capture.json"
            capture_file.write_text(json.dumps([
                {"url": "https://example.com/api/detail", "method": "GET", "status": 200},
                {
                    "url": "https://example.com/api/detail",
                    "method": "PUT",
                    "status": 200,
                    "postData": json.dumps({"id": 1, "name": "demo", "enabled": True, "code": "x"}),
                    "responseBody": json.dumps({"data": {"id": 1}}),
                },
            ], ensure_ascii=False), encoding="utf-8")
            session = self._base_session("sessanalyze")
            session["status"] = "completed"
            session["capture_file"] = str(capture_file)
            session["summary"] = {"ok": True, "count": 2}
            session["crud"] = {"ok": True, "count": 1}
            session["body_diff"] = {"ok": True, "count": 1}
            session["steps"][0]["summary"] = {"count": 1}
            session["steps"][0]["captured_indexes"] = [2]
            self._write_session(session_dir, session)
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir):
                code, out, err = run_daemon_cli(["capture-guide", "analyze", "--session", "sessanalyze", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("completed", payload["status"])
            self.assertEqual(2, payload["summary"]["count"])
            self.assertEqual(1, payload["steps"][0]["request_count"])

    def test_export_with_step_only_uses_target_step_requests(self) -> None:
        """export --step N 应仅导出该步骤的请求。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_file = root / "capture.json"
            capture_file.write_text(json.dumps([
                {"url": "https://example.com/api/a", "method": "GET", "status": 200},
                {"url": "https://example.com/api/b", "method": "POST", "status": 200},
            ], ensure_ascii=False), encoding="utf-8")
            session = self._base_session("sessexport")
            session["status"] = "completed"
            session["capture_file"] = str(capture_file)
            session["steps"][0]["captured_indexes"] = [1]
            session["steps"][1]["captured_indexes"] = [2]
            self._write_session(session_dir, session)
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(PageManager, "_export_python_client", side_effect=lambda requests, daemon_script="": f"COUNT={len(requests)}"):
                code, out, err = run_daemon_cli([
                    "capture-guide", "export", "--session", "sessexport",
                    "--step", "2", "--python-client", "--json",
                ])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual(1, payload["request_count"])
            self.assertIn("COUNT=1", payload["code"])

    def test_abort_stops_capture_and_keeps_saved_json(self) -> None:
        """abort 应结束会话，并保留已落盘的抓包文件。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("sessabort")
            self._write_session(session_dir, session)
            stop_resp = {
                "ok": True,
                "requests": [{"url": "https://example.com/api/detail", "method": "GET", "status": 200}],
                "body_fetch": {},
            }

            def fake_capture_path(filtered: bool = False) -> Path:
                """把默认抓包输出重定向到临时目录。"""
                return capture_dir / ("filtered.json" if filtered else "full.json")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_guide_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_send", return_value=stop_resp):
                code, out, err = run_daemon_cli(["capture-guide", "abort", "--session", "sessabort", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("aborted", payload["status"])
            self.assertTrue(Path(payload["capture_file"]).exists())


if __name__ == "__main__":
    unittest.main()
