"""同一浏览器被多次登记时的实例去重 + 默认实例兜底单测。

只验证纯函数与解析逻辑，不依赖真实 Chrome。
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import cdp_client  # noqa: E402
import daemon  # noqa: E402


def _inst(instance_id, *, ws_url="", host="", port=9222, pid=None, source="remote_debugging_port", user_data_dir=""):
    return {
        "instance_id": instance_id,
        "channel": "stable",
        "source": source,
        "port": port,
        "user_data_dir": user_data_dir,
        "ws_url": ws_url,
        "ws_path": "",
        "host": host,
        "product": "Chrome/149",
        "pid": pid,
        "command": "",
    }


class DedupeSameBrowserTests(unittest.TestCase):
    def test_collapses_same_ws_url_and_keeps_live_pid_primary(self):
        same_ws = "ws://[::1]:9222/devtools/browser/06b20a95-3feb-4f8f-b285-b2b7d1b26467"
        raw = [
            _inst("chrome-9222", ws_url=same_ws, pid=None, source="devtools_active_port",
                  user_data_dir="/Users/luca/Library/Application Support/Google/Chrome"),
            _inst("chrome-profile-9222", ws_url=same_ws, pid=7359, source="remote_debugging_port",
                  user_data_dir="/Users/luca/chrome-profile"),
        ]
        merged = daemon._dedupe_same_browser(raw)
        self.assertEqual(len(merged), 1)
        primary = merged[0]
        self.assertEqual(primary["instance_id"], "chrome-profile-9222")
        self.assertEqual(primary["pid"], 7359)
        self.assertIn("chrome-9222", primary.get("aliases", []))

    def test_keeps_distinct_browsers_separate(self):
        raw = [
            _inst("a-9222", ws_url="ws://[::1]:9222/devtools/browser/AAAA", pid=1),
            _inst("b-9333", ws_url="ws://[::1]:9333/devtools/browser/BBBB", pid=2, port=9333),
        ]
        merged = daemon._dedupe_same_browser(raw)
        self.assertEqual({m["instance_id"] for m in merged}, {"a-9222", "b-9333"})

    def test_falls_back_to_host_port_when_ws_url_missing(self):
        raw = [
            _inst("chrome-9222", ws_url="", host="[::1]", port=9222, pid=None,
                  source="devtools_active_port"),
            _inst("chrome-profile-9222", ws_url="", host="[::1]", port=9222, pid=42,
                  source="remote_debugging_port"),
        ]
        merged = daemon._dedupe_same_browser(raw)
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["pid"], 42)


class MatchByAliasTests(unittest.TestCase):
    def test_match_instance_candidates_resolves_by_alias(self):
        candidates = [
            _inst("chrome-profile-9222", ws_url="ws://x", pid=7359),
        ]
        candidates[0]["aliases"] = ["chrome-9222"]
        matched = daemon._match_instance_candidates("chrome-9222", candidates)
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0]["instance_id"], "chrome-profile-9222")


class ResolveInstanceTests(unittest.TestCase):
    def test_returns_single_after_dedupe_without_selector(self):
        one = [_inst("chrome-profile-9222", ws_url="ws://x", pid=7359)]
        with mock.patch.object(daemon, "discover_cdp_instances", return_value=one), \
             mock.patch.dict(daemon.os.environ, {"CHROME_CDP_INSTANCE": ""}, clear=False), \
             mock.patch.object(daemon, "_load_cli_config", return_value={}):
            resolved = daemon.resolve_cdp_instance("")
        self.assertEqual(resolved["instance_id"], "chrome-profile-9222")

    def test_uses_config_default_instance_when_multiple(self):
        two = [
            _inst("a-9222", ws_url="ws://a", pid=1),
            _inst("b-9333", ws_url="ws://b", pid=2, port=9333),
        ]
        with mock.patch.object(daemon, "discover_cdp_instances", return_value=two), \
             mock.patch.dict(daemon.os.environ, {"CHROME_CDP_INSTANCE": ""}, clear=False), \
             mock.patch.object(daemon, "_load_cli_config", return_value={"default_instance": "b-9333"}):
            resolved = daemon.resolve_cdp_instance("")
        self.assertEqual(resolved["instance_id"], "b-9333")

    def test_still_raises_when_multiple_and_no_default(self):
        two = [
            _inst("a-9222", ws_url="ws://a", pid=1),
            _inst("b-9333", ws_url="ws://b", pid=2, port=9333),
        ]
        with mock.patch.object(daemon, "discover_cdp_instances", return_value=two), \
             mock.patch.dict(daemon.os.environ, {"CHROME_CDP_INSTANCE": ""}, clear=False), \
             mock.patch.object(daemon, "_load_cli_config", return_value={}):
            with self.assertRaises(daemon.CdpInstanceSelectionError):
                daemon.resolve_cdp_instance("")


class CdpClientAliasTests(unittest.TestCase):
    def test_resolve_instance_matches_alias(self):
        merged = [{"instance_id": "chrome-profile-9222", "port": 9222, "aliases": ["chrome-9222"]}]
        with mock.patch.object(cdp_client, "_discover_instances", return_value=merged), \
             mock.patch.dict(cdp_client.os.environ, {"CHROME_CDP_INSTANCE": ""}, clear=False):
            resolved = cdp_client._resolve_instance("chrome-9222")
        self.assertEqual(resolved, "chrome-profile-9222")


if __name__ == "__main__":
    unittest.main()
