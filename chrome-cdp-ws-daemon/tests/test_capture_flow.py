"""capture-flow 全流程优先抓包单测。"""

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

import daemon  # noqa: E402
from page_manager import PageManager  # noqa: E402


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


class CaptureFlowCliTests(unittest.TestCase):
    """验证 capture-flow 的全流程抓包与回退建议。"""

    def _write_session(self, root: Path, payload: dict) -> Path:
        """把会话 JSON 写入临时目录。"""
        path = root / f"{payload['session_id']}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _base_session(self, session_id: str = "flow123") -> dict:
        """构造最小 capture-flow 会话。"""
        return {
            "session_id": session_id,
            "status": "capturing",
            "goal": "获取 metric 链路",
            "target": "tab:demo",
            "created_at": 1.0,
            "updated_at": 1.0,
            "capture_started": True,
            "baseline_ms": 1500,
            "stop_options": {
                "method_filter": "",
                "url_filter": "",
                "exclude_domain": "",
                "status_filter": "",
                "body_mode": "filtered",
                "get_body": True,
                "wait_ms": 0,
                "idle_ms": 800,
                "max_bodies": 6,
                "max_body_bytes": 0,
                "until_match": "",
            },
            "baseline_summary": {
                "count": 2,
                "request_keys": {"GET /api/jobs": 2},
                "group_keys": {"GET /api/jobs": 2},
                "top_requests": [],
            },
            "capture_file": "",
            "filtered_capture_file": "",
            "analysis_file": "",
            "analysis_source": "",
            "summary": None,
            "crud": None,
            "body_diff": None,
            "body_fetch": {},
            "clarity_status": "",
            "candidate_requests": [],
            "candidate_groups": [],
            "noise_summary": {},
            "recommended_phases": [],
            "recommended_next_action": "",
        }

    def test_start_creates_session_and_returns_message(self) -> None:
        """start --json 应创建会话并返回基线摘要。"""
        with tempfile.TemporaryDirectory() as td:
            session_dir = Path(td)
            baseline_request = {
                "url": "https://example.com/api/jobs",
                "method": "GET",
                "status": 200,
            }
            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(daemon.time, "sleep", return_value=None), \
                    mock.patch.object(daemon.uuid, "uuid4", return_value=types.SimpleNamespace(hex="flow123456789")), \
                    mock.patch.object(daemon, "_send", side_effect=[
                        {"ok": True, "target_id": "tab-1"},
                        {"ok": True, "count": 1, "requests": [baseline_request], "last_event_at": 1.0},
                    ]):
                code, out, err = run_daemon_cli([
                    "capture-flow", "start",
                    "--goal", "获取 metric 链路",
                    "--json",
                ])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("flow12345678", payload["session_id"])
            self.assertEqual("capturing", payload["status"])
            self.assertIn("整段操作流", payload["message"])
            self.assertTrue((session_dir / "flow12345678.json").exists())

    def test_stop_returns_clear_no_network_when_only_baseline_noise(self) -> None:
        """整段流程只有 baseline 请求时，应判定为 clear_no_network。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("flow_noise")
            self._write_session(session_dir, session)
            requests = [
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
            ]

            def fake_capture_path(filtered: bool = False) -> Path:
                """把抓包输出写到临时目录。"""
                return capture_dir / ("filtered.json" if filtered else "full.json")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_send", return_value={"ok": True, "requests": requests, "body_fetch": {}}):
                code, out, err = run_daemon_cli(["capture-flow", "stop", "--session", "flow_noise", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("clear_no_network", payload["clarity_status"])
            self.assertEqual("likely_frontend_only", payload["recommended_next_action"])
            self.assertTrue(Path(payload["capture_file"]).exists())

    def test_stop_returns_clear_for_single_new_candidate_group(self) -> None:
        """整段流程出现明显新增接口时，应判定为 clear。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("flow_clear")
            self._write_session(session_dir, session)
            requests = [
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics?get=numRecordsIn", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics?get=numRecordsIn", "method": "GET", "status": 200},
            ]

            def fake_capture_path(filtered: bool = False) -> Path:
                """把抓包输出写到临时目录。"""
                return capture_dir / ("filtered.json" if filtered else "full.json")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_send", return_value={"ok": True, "requests": requests, "body_fetch": {}}):
                code, out, err = run_daemon_cli(["capture-flow", "stop", "--session", "flow_clear", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("clear", payload["clarity_status"])
            self.assertEqual(1, len(payload["candidate_requests"]))
            self.assertIn("/api/vertices/1/metrics", payload["candidate_requests"][0]["path"])

    def test_stop_returns_unclear_and_recommended_phases(self) -> None:
        """候选组较多且混杂时，应回退为 unclear 并给出推荐阶段。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("flow_unclear")
            self._write_session(session_dir, session)
            requests = [
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics?get=numRecordsIn", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/watermarks?scope=detail", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/backpressure?mode=full", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/logs?tab=stderr", "method": "GET", "status": 200},
            ]

            def fake_capture_path(filtered: bool = False) -> Path:
                """把抓包输出写到临时目录。"""
                return capture_dir / ("filtered.json" if filtered else "full.json")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_send", return_value={"ok": True, "requests": requests, "body_fetch": {}}):
                code, out, err = run_daemon_cli(["capture-flow", "stop", "--session", "flow_unclear", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("unclear", payload["clarity_status"])
            self.assertEqual("fallback_to_capture_guide", payload["recommended_next_action"])
            self.assertGreaterEqual(len(payload["recommended_phases"]), 2)
            self.assertEqual("open_metric_panel", payload["recommended_phases"][0]["key"])
            self.assertEqual("select_metric_value", payload["recommended_phases"][1]["key"])

    def test_status_and_analyze_work_without_daemon(self) -> None:
        """status/analyze 在 daemon 不在线时仍可读取已完成会话。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_file = root / "capture.json"
            capture_file.write_text(json.dumps([
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics?get=numRecordsIn", "method": "GET", "status": 200},
            ], ensure_ascii=False), encoding="utf-8")
            session = self._base_session("flow_status")
            session["status"] = "completed"
            session["capture_file"] = str(capture_file)
            session["analysis_file"] = str(capture_file)
            session["analysis_source"] = "full"
            session["summary"] = {"ok": True, "count": 2}
            self._write_session(session_dir, session)
            with mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir):
                code1, out1, err1 = run_daemon_cli(["capture-flow", "status", "--session", "flow_status", "--json"])
                code2, out2, err2 = run_daemon_cli(["capture-flow", "analyze", "--session", "flow_status", "--json"])

            self.assertEqual(0, code1, err1)
            self.assertEqual(0, code2, err2)
            payload1 = json.loads(out1)
            payload2 = json.loads(out2)
            self.assertEqual("completed", payload1["status"])
            self.assertIn("clarity_status", payload2)

    def test_export_candidate_group_only_uses_selected_requests(self) -> None:
        """export --candidate-group N 应只导出该候选组请求。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_file = root / "capture.json"
            capture_file.write_text(json.dumps([
                {"url": "https://example.com/api/a", "method": "GET", "status": 200},
                {"url": "https://example.com/api/b?get=x", "method": "GET", "status": 200},
                {"url": "https://example.com/api/b?get=x", "method": "GET", "status": 200},
            ], ensure_ascii=False), encoding="utf-8")
            session = self._base_session("flow_export")
            session["status"] = "completed"
            session["capture_file"] = str(capture_file)
            session["analysis_file"] = str(capture_file)
            session["analysis_source"] = "full"
            session["candidate_groups"] = [
                {"index": 1, "request_indexes": [2, 3]},
            ]
            self._write_session(session_dir, session)
            with mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(PageManager, "_export_python_client", side_effect=lambda requests, daemon_script="": f"COUNT={len(requests)}"):
                code, out, err = run_daemon_cli([
                    "capture-flow", "export", "--session", "flow_export",
                    "--candidate-group", "1", "--python-client", "--json",
                ])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual(2, payload["request_count"])
            self.assertIn("COUNT=2", payload["code"])

    def test_abort_keeps_saved_capture_json(self) -> None:
        """abort 应尽量 stop 并保留已落盘的抓包文件。"""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            session_dir = root / "sessions"
            session_dir.mkdir()
            capture_dir = root / "captures"
            capture_dir.mkdir()
            session = self._base_session("flow_abort")
            self._write_session(session_dir, session)
            requests = [
                {"url": "https://example.com/api/jobs", "method": "GET", "status": 200},
                {"url": "https://example.com/api/vertices/1/metrics", "method": "GET", "status": 200},
            ]

            def fake_capture_path(filtered: bool = False) -> Path:
                """把抓包输出写到临时目录。"""
                return capture_dir / ("filtered.json" if filtered else "full.json")

            with mock.patch.object(daemon, "_daemon_is_running", return_value=True), \
                    mock.patch.object(daemon, "_capture_flow_dir", return_value=session_dir), \
                    mock.patch.object(daemon, "_capture_path", side_effect=fake_capture_path), \
                    mock.patch.object(daemon, "_send", return_value={"ok": True, "requests": requests, "body_fetch": {}}):
                code, out, err = run_daemon_cli(["capture-flow", "abort", "--session", "flow_abort", "--json"])

            self.assertEqual(0, code, err)
            payload = json.loads(out)
            self.assertEqual("aborted", payload["status"])
            self.assertTrue(Path(payload["capture_file"]).exists())


if __name__ == "__main__":
    unittest.main()
