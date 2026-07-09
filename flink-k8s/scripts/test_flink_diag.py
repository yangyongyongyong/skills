"""Offline tests for flink_diag.py."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

import httpx


def load_module() -> Any:
    """Load the CLI module from the local script path."""
    path = Path(__file__).with_name("flink_diag.py")
    spec = importlib.util.spec_from_file_location("flink_diag", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["flink_diag"] = module
    spec.loader.exec_module(module)
    return module


flink_diag = load_module()


class FlinkDiagTests(unittest.TestCase):
    """Unit tests for URL parsing, matching, summaries, and async HTTP helpers."""

    def test_parse_task_chain_url(self) -> None:
        """Parse job and task chain ids from a WebUI hash route."""
        url = (
            "https://flink.example.com:7799/demo-job/"
            "#/job/running/0000000036f0f0120000000000000000/overview/"
            "cbc357ccb763df2852fee8c4fc7d55f2/detail"
        )
        parsed = flink_diag.parse_web_url(url)
        self.assertEqual(parsed.base_url, "https://flink.example.com:7799/demo-job/")
        self.assertEqual(parsed.deployment, "demo-job")
        self.assertEqual(parsed.job_id, "0000000036f0f0120000000000000000")
        self.assertEqual(parsed.vertex_id, "cbc357ccb763df2852fee8c4fc7d55f2")
        self.assertEqual(parsed.tab, "detail")

    def test_parse_job_exceptions_routes_for_flink_115_and_118(self) -> None:
        """兼容 Flink 1.15 与 1.18 的 job exceptions hash route。"""
        flink_118 = flink_diag.parse_web_url(
            "https://flink.example.com:7799/demo/#/job/running/ffffffff985bcb050000000000000000/exceptions"
        )
        flink_115 = flink_diag.parse_web_url(
            "https://flink.example.com:7799/demo/#/job/00000000000000000000000000000000/exceptions"
        )
        self.assertEqual(flink_118.job_id, "ffffffff985bcb050000000000000000")
        self.assertEqual(flink_118.tab, "exceptions")
        self.assertEqual(flink_115.job_id, "00000000000000000000000000000000")
        self.assertEqual(flink_115.tab, "exceptions")

    def test_parse_taskmanager_url(self) -> None:
        """Parse a TaskManager id from the task-manager route."""
        url = "https://flink.example.com:7799/demo/#/task-manager/tm-1-2/metrics"
        parsed = flink_diag.parse_web_url(url)
        self.assertEqual(parsed.taskmanager_id, "tm-1-2")
        self.assertEqual(parsed.tab, "metrics")

    def test_parse_taskmanager_stdout_url(self) -> None:
        """解析 TaskManager stdout URL。"""
        url = "https://flink.example.com:7799/demo/#/task-manager/tm-1-2/stdout"
        parsed = flink_diag.parse_web_url(url)
        self.assertEqual(parsed.taskmanager_id, "tm-1-2")
        self.assertEqual(parsed.tab, "stdout")

    def test_match_metric_ids(self) -> None:
        """Resolve short metric names against available metric ids."""
        available = ["0.busyTimeMsPerSecond", "0.numRecordsOutPerSecond", "Status.JVM.CPU.Load"]
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["busyTimeMsPerSecond"], mode="auto"),
            ["0.busyTimeMsPerSecond"],
        )
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["Status.JVM.CPU.Load"], mode="exact"),
            ["Status.JVM.CPU.Load"],
        )

    def test_match_semantic_metric_alias(self) -> None:
        """Resolve semantic aliases to concrete prefixed metric ids."""
        available = [
            "0.Source__kafka.numRecordsIn",
            "0.Source__kafka.numRecordsInPerSecond",
            "7.KafkaProducer.record-send-total",
        ]
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["source.records_in"], mode="auto"),
            ["0.Source__kafka.numRecordsIn"],
        )
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["sink.records_send"], mode="auto"),
            ["7.KafkaProducer.record-send-total"],
        )

    def test_match_lookup_semantic_aliases(self) -> None:
        """解析 LookupJoin 语义 alias，拿到 operator 级真实 lookup 指标。"""
        available = [
            "0.LookupJoin[7].numRecordsInPerSecond",
            "0.LookupJoin[7].numRecordsOutPerSecond",
            "0.LookupJoin[7].lookupCacheHitRate",
            "0.Calc[6].numRecordsInPerSecond",
        ]
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["lookup.records_in_rate"], mode="auto"),
            ["0.LookupJoin[7].numRecordsInPerSecond"],
        )
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["lookup.cache_hit_rate"], mode="auto"),
            ["0.LookupJoin[7].lookupCacheHitRate"],
        )

    def test_log_error_pattern_is_case_sensitive_for_error(self) -> None:
        """ERROR 级别匹配不把 WARN 行里的普通 Error 单词误算进去。"""
        text = "WARN HBaseConfigurationUtil [] - Error while loading config\nERROR real failure\n"
        grep = flink_diag.grep_log_text(text, ["ERROR", "hbase-lookup"])
        self.assertEqual(grep["pattern_counts"]["ERROR"], 1)
        self.assertEqual(grep["pattern_counts"]["HBaseConfigurationUtil"], 1)

    def test_metric_explanation(self) -> None:
        """Explain a semantic metric alias."""
        explanation = flink_diag.metric_explanation("sink.records_send")
        self.assertEqual(explanation["kind"], "alias")
        self.assertIn("terminal sinks", explanation["explanation"])

    def test_human_formatters(self) -> None:
        """Format bytes and durations for summaries."""
        self.assertEqual(flink_diag.bytes_human(1048576), "1.00 MiB")
        self.assertEqual(flink_diag.millis_human(1500), "1.50 s")
        self.assertEqual(flink_diag.sum_metric_values([{"value": "1"}, {"value": "2.5"}]), "3.5")

    def test_select_endpoint_profile(self) -> None:
        """Select conservative profiles for supported Flink minor versions."""
        self.assertEqual(flink_diag.select_endpoint_profile("1.18.0"), "flink-1.18")
        self.assertEqual(flink_diag.select_endpoint_profile("1.20.1"), "flink-1.20")
        self.assertEqual(flink_diag.select_endpoint_profile("2.0.0"), "generic")

    def test_summarize_checkpoint_payload(self) -> None:
        """Summarize checkpoint history with readable units."""
        summary = flink_diag.summarize_checkpoint_payload(
            {
                "checkpoints": {
                    "counts": {"completed": 1},
                    "latest": {"completed": {"id": 42, "checkpointed_size": 1048576, "end_to_end_duration": 2000}},
                },
                "configuration": {"state_backend": "rocksdb", "interval": 60000},
            }
        )
        self.assertEqual(summary["latest_completed"]["id"], 42)
        self.assertEqual(summary["latest_completed"]["checkpointed_size"], "1.00 MiB")
        self.assertEqual(summary["interval"], "1.00 min")

    def test_summarize_vertex(self) -> None:
        """Summarize vertex metrics and backpressure ratios."""
        vertex = {
            "id": "v1",
            "name": "Map",
            "parallelism": 2,
            "metrics": {
                "read-records": 10,
                "write-records": 20,
                "accumulated-backpressured-time": 3,
            },
            "subtasks": [{"subtask": 0}, {"subtask": 1}],
        }
        bp = {"backpressureLevel": "high", "subtasks": [{"ratio": 0.5, "busyRatio": 0.2, "idleRatio": 0.3}]}
        summary = flink_diag.summarize_vertex(vertex, bp)
        self.assertEqual(summary["vertex_id"], "v1")
        self.assertEqual(summary["parallelism"], 2)
        self.assertEqual(summary["backpressured_max"], 0.5)
        self.assertEqual(summary["records_sent"], 20)

    def test_summarize_io_flow_spots_filtering_stage(self) -> None:
        """Summarize task-chain input and output volumes for filtering diagnosis."""
        vertices = [
            {
                "id": "source",
                "name": "Source",
                "parallelism": 2,
                "metrics": {"read-records": 100, "write-records": 100},
            },
            {
                "id": "filter",
                "name": "Filter invalid events",
                "parallelism": 2,
                "metrics": {"read-records": 100, "write-records": 25},
            },
            {
                "id": "sink",
                "name": "Sink",
                "parallelism": 1,
                "metrics": {"read-records": 25, "write-records": 0},
            },
        ]
        summary = flink_diag.summarize_io_flow(vertices)
        self.assertEqual(summary["taskchains"][1]["records_in"], 100)
        self.assertEqual(summary["taskchains"][1]["records_out"], 25)
        self.assertEqual(summary["taskchains"][1]["records_delta"], 75)
        self.assertEqual(summary["taskchains"][1]["pass_through_pct"], 25.0)
        self.assertEqual(summary["largest_drop"]["vertex_id"], "filter")
        self.assertEqual(summary["largest_filter_drop"]["vertex_id"], "filter")

    def test_summarize_io_flow_treats_paimon_writer_as_sink_semantics(self) -> None:
        """Paimon Writer 的输出是提交消息，不应误判为业务过滤丢数。"""
        vertices = [
            {
                "id": "source",
                "name": "Source: kafka",
                "parallelism": 2,
                "metrics": {"read-records": 0, "write-records": 1000},
            },
            {
                "id": "writer",
                "name": "Writer : ods_log_power",
                "parallelism": 2,
                "metrics": {"read-records": 1000, "write-records": 10},
            },
            {
                "id": "committer",
                "name": "Global Committer : ods_log_power -> end: Writer",
                "parallelism": 1,
                "metrics": {"read-records": 10, "write-records": 0},
            },
        ]
        summary = flink_diag.summarize_io_flow(vertices)
        self.assertEqual(summary["taskchains"][1]["diagnosis"], "sink_writer_internal_records")
        self.assertEqual(summary["taskchains"][2]["diagnosis"], "sink_committer_terminal")
        self.assertIsNone(summary["largest_filter_drop"])

    def test_summarize_thread_dump(self) -> None:
        """Summarize thread states from stringified dump entries."""
        data = {
            "threadInfos": [
                {
                    "threadName": "main",
                    "stringifiedThreadInfo": '"main" Id=1 WAITING\n\tat java.lang.Object.wait(Object.java:1)',
                },
                {
                    "threadName": "worker",
                    "stringifiedThreadInfo": '"worker" Id=2 RUNNABLE\n\tat com.example.Work.run(Work.java:2)',
                },
            ]
        }
        summary = flink_diag.summarize_thread_dump(data, ["RUNNABLE"])
        self.assertEqual(summary["states"]["WAITING"], 1)
        self.assertEqual(summary["states"]["RUNNABLE"], 1)
        self.assertEqual(len(summary["selected"]), 1)

    def test_summarize_flamegraph(self) -> None:
        """Summarize top flamegraph nodes by value."""
        data = {
            "endTimestamp": 1,
            "data": {
                "name": "root",
                "value": 100,
                "children": [{"name": "hot", "value": 80, "children": []}],
            },
        }
        summary = flink_diag.summarize_flamegraph(data, top_n=2)
        self.assertEqual(summary["root_value"], 100)
        self.assertEqual(summary["top"][0]["name"], "root")
        self.assertEqual(summary["top"][1]["name"], "hot")

    def test_stream_tail_keeps_tail_bytes(self) -> None:
        """Stream a large response and keep only the requested tail bytes."""
        async def run_case() -> dict[str, Any]:
            """Run the async stream-tail helper with a mock transport."""
            transport = httpx.MockTransport(lambda request: httpx.Response(200, text="abcdef"))
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                return await client.stream_tail("jobmanager/log", 3)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertTrue(result["available"])
        self.assertEqual(result["text"], "def")
        self.assertTrue(result["truncated"])

    def test_fetch_job_graph_with_mock_transport(self) -> None:
        """Fetch a job graph summary using mocked Flink endpoints."""
        async def run_case() -> dict[str, Any]:
            """Run the async job graph helper with mocked responses."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return a mock response for the requested path."""
                path = request.url.path
                if path.endswith("/jobs/job-1"):
                    return httpx.Response(
                        200,
                        json={
                            "jid": "job-1",
                            "name": "demo",
                            "state": "RUNNING",
                            "vertices": [
                                {
                                    "id": "v1",
                                    "name": "Source",
                                    "parallelism": 1,
                                    "metrics": {"write-records": 5},
                                    "subtasks": [{"subtask": 0}],
                                }
                            ],
                        },
                    )
                if path.endswith("/jobs/job-1/vertices/v1/backpressure"):
                    return httpx.Response(200, json={"backpressureLevel": "ok", "subtasks": [{"ratio": 0.0}]})
                return httpx.Response(404, json={"errors": ["missing"]})

            transport = httpx.MockTransport(handler)
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                args = type("Args", (), {"top_by": "backpressure"})()
                return await flink_diag.fetch_job_graph(client, "job-1", args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["job_id"], "job-1")
        self.assertEqual(result["vertices"][0]["vertex_id"], "v1")

    def test_source_stats_with_mock_transport(self) -> None:
        """Fetch source semantic metrics with mocked Flink endpoints."""
        async def run_case() -> dict[str, Any]:
            """Run source-stats against mocked responses."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return a mock response for source stats."""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/v1") and not query:
                    return httpx.Response(200, json={"id": "v1", "name": "Source", "parallelism": 1})
                if path.endswith("/jobs/job-1/vertices/v1/metrics") and not query:
                    return httpx.Response(200, json=[{"id": "0.Source__kafka.numRecordsIn"}])
                if path.endswith("/jobs/job-1/vertices/v1/metrics"):
                    return httpx.Response(200, json=[{"id": "0.Source__kafka.numRecordsIn", "value": "123"}])
                return httpx.Response(404)

            transport = httpx.MockTransport(handler)
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                return await flink_diag.command_source_stats(client, "job-1", "v1")
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["summary"]["records_in"], "123")

    def test_sink_stats_sums_parallel_writer_metrics(self) -> None:
        """Summarize sink writer metrics across parallel subtasks."""
        async def run_case() -> dict[str, Any]:
            """Run sink-stats against mocked parallel sink metrics."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return a mock response for sink stats."""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/sink") and not query:
                    return httpx.Response(200, json={"id": "sink", "name": "Sink", "parallelism": 2})
                if path.endswith("/jobs/job-1/vertices/sink/metrics") and not query:
                    return httpx.Response(
                        200,
                        json=[
                            {"id": "0.sink__Writer.numRecordsSend"},
                            {"id": "1.sink__Writer.numRecordsSend"},
                            {"id": "0.sink__Writer.numRecordsSendErrors"},
                            {"id": "1.sink__Writer.numRecordsSendErrors"},
                        ],
                    )
                if path.endswith("/jobs/job-1/vertices/sink/metrics"):
                    return httpx.Response(
                        200,
                        json=[
                            {"id": "0.sink__Writer.numRecordsSend", "value": "10"},
                            {"id": "1.sink__Writer.numRecordsSend", "value": "20"},
                            {"id": "0.sink__Writer.numRecordsSendErrors", "value": "0"},
                            {"id": "1.sink__Writer.numRecordsSendErrors", "value": "1"},
                        ],
                    )
                return httpx.Response(404)

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                return await flink_diag.command_sink_stats(client, "job-1", "sink")
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["summary"]["records_send"], "30")
        self.assertEqual(result["summary"]["records_send_errors"], "1")

    def test_all_subtask_metrics_with_mock_transport(self) -> None:
        """Fetch selected metrics for all subtasks and aggregate numeric totals."""
        async def run_case() -> dict[str, Any]:
            """Run all-subtask metrics against mocked responses."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock responses for all-subtask metric fetching."""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/v1") and not query:
                    return httpx.Response(200, json={"subtasks": [{"subtask": 0}, {"subtask": 1}]})
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/0/metrics") and not query:
                    return httpx.Response(200, json=[{"id": "numRecordsIn"}, {"id": "numRecordsOut"}])
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/1/metrics") and not query:
                    return httpx.Response(200, json=[{"id": "numRecordsIn"}, {"id": "numRecordsOut"}])
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/0/metrics"):
                    return httpx.Response(200, json=[{"id": "numRecordsIn", "value": "10"}, {"id": "numRecordsOut", "value": "4"}])
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/1/metrics"):
                    return httpx.Response(200, json=[{"id": "numRecordsIn", "value": "20"}, {"id": "numRecordsOut", "value": "8"}])
                return httpx.Response(404)

            transport = httpx.MockTransport(handler)
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                return await flink_diag.fetch_all_subtask_metric_values(
                    client,
                    "job-1",
                    "v1",
                    ["numRecordsIn", "numRecordsOut"],
                    "auto",
                )
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["subtasks"][0]["metrics"]["numRecordsIn"], "10")
        self.assertEqual(result["subtasks"][1]["metrics"]["numRecordsOut"], "8")
        self.assertEqual(result["totals"]["numRecordsIn"], "30")
        self.assertEqual(result["totals"]["numRecordsOut"], "12")

    def test_summarize_subtask_skew_detects_outlier(self) -> None:
        """Summarize subtask metric skew and identify the hottest subtask."""
        data = {
            "subtasks": [
                {"subtask": 0, "metrics": {"numRecordsInPerSecond": "10"}},
                {"subtask": 1, "metrics": {"numRecordsInPerSecond": "40"}},
            ]
        }
        summary = flink_diag.summarize_subtask_skew(data)
        metric = summary["metrics"]["numRecordsInPerSecond"]
        self.assertEqual(metric["max_subtask"], 1)
        self.assertEqual(metric["max"], 40)
        self.assertEqual(metric["skew_ratio"], 4.0)

    def test_summarize_taskmanager_aggregates_ranks_rows(self) -> None:
        """Summarize TaskManager row aggregated metrics and rank by a metric."""
        data = {
            "id": "v1",
            "name": "Writer",
            "taskmanagers": [
                {
                    "host": "tm-a:1234",
                    "taskmanager-id": "tm-a",
                    "status": "RUNNING",
                    "metrics": {"read-records": 10},
                    "aggregated": {
                        "metrics": {
                            "read-records": {"sum": 10, "max": 10, "avg": 10},
                            "accumulated-busy-time": {"sum": 100, "max": 100, "avg": 100},
                        }
                    },
                },
                {
                    "host": "tm-b:1234",
                    "taskmanager-id": "tm-b",
                    "status": "RUNNING",
                    "metrics": {"read-records": 40},
                    "aggregated": {
                        "metrics": {
                            "read-records": {"sum": 40, "max": 40, "avg": 40},
                            "accumulated-busy-time": {"sum": 300, "max": 300, "avg": 300},
                        }
                    },
                },
            ],
        }
        summary = flink_diag.summarize_taskmanager_aggregates(
            data,
            metrics=["read-records", "accumulated-busy-time"],
            sort_by="read-records",
            top=1,
        )
        self.assertEqual(summary["vertex_id"], "v1")
        self.assertEqual(summary["taskmanager_count"], 2)
        self.assertEqual(summary["metrics"], ["read-records", "accumulated-busy-time"])
        self.assertEqual(summary["rows"][0]["taskmanager_id"], "tm-b")
        self.assertEqual(summary["rows"][0]["metrics"]["read-records"]["sum"], 40)
        self.assertEqual(summary["rows"][0]["summary"]["read-records"], 40)
        self.assertEqual(len(summary["rows"]), 1)

    def test_summarize_capacity_subtask_metrics_and_vertex_findings(self) -> None:
        """汇总容量指标，并识别 busy 饱和和 subtask 倾斜。"""
        subtask_data = {
            "subtasks": [
                {
                    "subtask": 0,
                    "metrics": {
                        "numRecordsInPerSecond": "10",
                        "busyTimeMsPerSecond": "900",
                        "idleTimeMsPerSecond": "50",
                        "backPressuredTimeMsPerSecond": "0",
                    },
                },
                {
                    "subtask": 1,
                    "metrics": {
                        "numRecordsInPerSecond": "100",
                        "busyTimeMsPerSecond": "100",
                        "idleTimeMsPerSecond": "850",
                        "backPressuredTimeMsPerSecond": "0",
                    },
                },
            ]
        }
        metrics = flink_diag.summarize_capacity_subtask_metrics(subtask_data)
        self.assertEqual(metrics["numRecordsInPerSecond"]["sum"], 110)
        self.assertEqual(metrics["numRecordsInPerSecond"]["skew_ratio"], 10.0)
        report = flink_diag.summarize_vertex_capacity(
            {"id": "v1", "name": "Map", "parallelism": 2, "status": "RUNNING"},
            {"backpressured_max": 0.0, "backpressure_level": "ok"},
            subtask_data,
            None,
            None,
        )
        warning_areas = {finding["area"] for finding in report["findings"] if finding["level"] == "warning"}
        self.assertIn("parallelism", warning_areas)
        self.assertIn("skew", warning_areas)
        self.assertEqual(report["assessment"], "parallelism_may_be_low")

    def test_taskmanager_distribution_marks_empty_aggregates_unavailable(self) -> None:
        """Flink 1.15 可能返回空 TM 聚合，报告必须标记 unavailable 避免误读。"""
        summary = flink_diag.summarize_taskmanager_distribution(
            {
                "rows": [
                    {
                        "taskmanager_id": "tm-1",
                        "host": "tm-1",
                        "status_counts": {"RUNNING": 2},
                        "summary": {},
                        "metrics": {},
                    }
                ]
            }
        )
        self.assertFalse(summary["available"])
        self.assertEqual(summary["reason"], "empty_taskmanager_aggregates")
        self.assertEqual(summary["running_subtasks_max"], 2)

    def test_job_capacity_command_reports_reasonable_parallelism(self) -> None:
        """用 mock REST 验证 job capacity 组合并行度、反压、checkpoint 和 TM 分布。"""
        async def run_case() -> dict[str, Any]:
            """运行容量诊断命令。"""
            metric_values = {
                "numRecordsInPerSecond": "100",
                "numRecordsOutPerSecond": "100",
                "busyTimeMsPerSecond": "50",
                "idleTimeMsPerSecond": "950",
                "backPressuredTimeMsPerSecond": "0",
                "numBytesInPerSecond": "1024",
                "numBytesOutPerSecond": "1024",
                "checkpointStartDelayNanos": "0",
            }

            def handler(request: httpx.Request) -> httpx.Response:
                """返回容量诊断需要的 Flink REST mock 响应。"""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1"):
                    return httpx.Response(
                        200,
                        json={
                            "jid": "job-1",
                            "name": "demo",
                            "state": "RUNNING",
                            "vertices": [
                                {
                                    "id": "v1",
                                    "name": "Map",
                                    "parallelism": 1,
                                    "maxParallelism": 128,
                                    "status": "RUNNING",
                                    "metrics": {"read-records": 100, "write-records": 100},
                                }
                            ],
                        },
                    )
                if path.endswith("/jobs/job-1/checkpoints"):
                    return httpx.Response(200, json={"history": [{"id": 1, "status": "COMPLETED", "end_to_end_duration": 1000, "state_size": 10}]})
                if path.endswith("/jobs/job-1/vertices/v1/backpressure"):
                    return httpx.Response(200, json={"backpressureLevel": "ok", "subtasks": [{"ratio": 0.0, "busyRatio": 0.05, "idleRatio": 0.95}]})
                if path.endswith("/jobs/job-1/vertices/v1") and not query:
                    return httpx.Response(200, json={"id": "v1", "name": "Map", "parallelism": 1, "subtasks": [{"subtask": 0}]})
                if path.endswith("/jobs/job-1/vertices/v1/metrics"):
                    return httpx.Response(200, json=[])
                if path.endswith("/jobs/job-1/vertices/v1/taskmanagers"):
                    return httpx.Response(
                        200,
                        json={
                            "id": "v1",
                            "name": "Map",
                            "taskmanagers": [
                                {
                                    "taskmanager-id": "tm-1",
                                    "host": "tm-1:1234",
                                    "status": "RUNNING",
                                    "status-counts": {"RUNNING": 1},
                                    "aggregated": {
                                        "metrics": {
                                            "read-records": {"sum": 100},
                                            "write-records": {"sum": 100},
                                            "accumulated-busy-time": {"sum": 50},
                                            "accumulated-backpressured-time": {"sum": 0},
                                        }
                                    },
                                }
                            ],
                        },
                    )
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/0/metrics") and not query:
                    return httpx.Response(200, json=[{"id": name} for name in metric_values])
                if path.endswith("/jobs/job-1/vertices/v1/subtasks/0/metrics"):
                    return httpx.Response(200, json=[{"id": name, "value": value} for name, value in metric_values.items() if name in query])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/job/running/job-1/overview")
            args = type("Args", (), {"metric_match": "auto", "limit": 10, "top_by": None})()
            try:
                return await flink_diag.command_job_capacity(client, parsed, args, "job-1")
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["conclusion"], "parallelism_currently_reasonable")
        self.assertEqual(result["vertices"][0]["assessment"], "has_headroom")
        self.assertEqual(result["checkpoint_trend"]["completed"], 1)

    def test_diagnose_lookup_uses_operator_qps_not_chain_input(self) -> None:
        """Lookup 诊断使用 LookupJoin operator QPS，不把 task chain 总输入误认为 HBase 查询量。"""
        async def run_case() -> dict[str, Any]:
            """运行 LookupJoin mock 诊断。"""
            metric_values = {
                "0.LookupJoin[7].numRecordsInPerSecond": "27",
                "1.LookupJoin[7].numRecordsInPerSecond": "30",
                "2.LookupJoin[7].numRecordsInPerSecond": "28",
                "0.LookupJoin[7].numRecordsOutPerSecond": "27",
                "1.LookupJoin[7].numRecordsOutPerSecond": "30",
                "2.LookupJoin[7].numRecordsOutPerSecond": "28",
                "0.LookupJoin[7].lookupCacheHitRate": "0",
                "1.LookupJoin[7].lookupCacheHitRate": "0.00001",
                "2.LookupJoin[7].lookupCacheHitRate": "0",
            }

            def handler(request: httpx.Request) -> httpx.Response:
                """返回 LookupJoin metric 列表和值。"""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/v1/metrics") and not query:
                    return httpx.Response(200, json=[{"id": key} for key in metric_values])
                if path.endswith("/jobs/job-1/vertices/v1/metrics"):
                    return httpx.Response(200, json=[{"id": key, "value": value} for key, value in metric_values.items()])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            args = type("Args", (), {"metric_match": "auto", "limit": 10})()
            job = {
                "jid": "job-1",
                "name": "lookup-demo",
                "state": "RUNNING",
                "vertices": [{"id": "v1", "name": "Calc -> LookupJoin[7] -> Sink", "parallelism": 3, "status": "RUNNING"}],
            }
            graph = {"vertices": [{"vertex_id": "v1", "backpressured_max": 0.0, "backpressure_level": "ok"}]}
            capacity_by_id = {
                "v1": {
                    "metrics": {
                        "numRecordsInPerSecond": {"sum": 7600, "max": 2600, "avg": 2533.3333},
                        "busyTimeMsPerSecond": {"max": 100, "avg": 80},
                        "idleTimeMsPerSecond": {"avg": 900},
                        "backPressuredTimeMsPerSecond": {"max": 0, "avg": 0},
                    }
                }
            }
            try:
                return await flink_diag.diagnose_lookup_job(
                    client,
                    "job-1",
                    args,
                    job=job,
                    graph=graph,
                    checkpoint_trend={"completed": 1},
                    capacity_by_id=capacity_by_id,
                    include_logs=False,
                )
            finally:
                await client.close()

        result = asyncio.run(run_case())
        lookup = result["lookup_joins"][0]
        self.assertEqual(lookup["chain_records_in_rate_sum"], 7600)
        self.assertEqual(lookup["actual_lookup_records_in_rate_sum"], 85)
        self.assertEqual(result["conclusion"], "lookup_risk_observed")
        self.assertIn("lookup_cache_miss", {risk["type"] for risk in result["risks"]})
        self.assertNotIn("lookup_bottleneck", {risk["type"] for risk in result["risks"]})

    def test_diagnose_lookup_flags_bottleneck_with_pressure(self) -> None:
        """LookupJoin 有实际查询量且 busy/反压高时输出 lookup_bottleneck。"""
        async def run_case() -> dict[str, Any]:
            """运行有压力证据的 LookupJoin mock。"""
            metric_values = {
                "0.LookupJoin[7].numRecordsInPerSecond": "100",
                "0.LookupJoin[7].lookupCacheHitRate": "0",
            }

            def handler(request: httpx.Request) -> httpx.Response:
                """返回最小 LookupJoin metric 响应。"""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/v1/metrics") and not query:
                    return httpx.Response(200, json=[{"id": key} for key in metric_values])
                if path.endswith("/jobs/job-1/vertices/v1/metrics"):
                    return httpx.Response(200, json=[{"id": key, "value": value} for key, value in metric_values.items()])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            args = type("Args", (), {"metric_match": "auto", "limit": 10})()
            job = {
                "jid": "job-1",
                "name": "lookup-demo",
                "state": "RUNNING",
                "vertices": [{"id": "v1", "name": "LookupJoin[7]", "parallelism": 1, "status": "RUNNING"}],
            }
            graph = {"vertices": [{"vertex_id": "v1", "backpressured_max": 0.8, "backpressure_level": "high"}]}
            capacity_by_id = {
                "v1": {
                    "metrics": {
                        "numRecordsInPerSecond": {"sum": 100},
                        "busyTimeMsPerSecond": {"max": 900, "avg": 900},
                        "idleTimeMsPerSecond": {"avg": 10},
                        "backPressuredTimeMsPerSecond": {"max": 800, "avg": 800},
                    }
                }
            }
            try:
                return await flink_diag.diagnose_lookup_job(
                    client,
                    "job-1",
                    args,
                    job=job,
                    graph=graph,
                    checkpoint_trend={"completed": 1},
                    capacity_by_id=capacity_by_id,
                    include_logs=False,
                )
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["conclusion"], "lookup_bottleneck")
        self.assertIn("lookup_bottleneck", {risk["type"] for risk in result["risks"]})

    def test_summarize_checkpoint_trend(self) -> None:
        """Summarize recent checkpoint history and size growth."""
        data = {
            "history": [
                {"id": 1, "status": "COMPLETED", "end_to_end_duration": 1000, "state_size": 100},
                {"id": 2, "status": "FAILED", "end_to_end_duration": 2000, "state_size": 120},
                {"id": 3, "status": "COMPLETED", "end_to_end_duration": 3000, "state_size": 150},
            ]
        }
        summary = flink_diag.summarize_checkpoint_trend(data, limit=3)
        self.assertEqual(summary["count"], 3)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["state_size_growth"], "50.00 B")

    def test_summarize_job_exceptions_supports_root_and_history(self) -> None:
        """同时汇总 root exception 和 Flink 1.15/1.18 exceptionHistory。"""
        payload = {
            "root-exception": "java.lang.RuntimeException: root failed\n\tat demo.Root",
            "timestamp": 1000,
            "all-exceptions": [],
            "exceptionHistory": {
                "entries": [
                    {
                        "exceptionName": "java.lang.RuntimeException",
                        "stacktrace": "java.lang.RuntimeException: root failed\n\tat demo.Root",
                        "timestamp": 1000,
                        "taskName": "LookupJoin (1/2)",
                        "location": "tm-1:1234",
                        "concurrentExceptions": [],
                    },
                    {
                        "exceptionName": "java.io.IOException",
                        "stacktrace": "java.io.IOException: network failed\n\tat demo.IO",
                        "timestamp": 900,
                        "taskName": "Source (1/2)",
                        "location": "tm-2:1234",
                        "concurrentExceptions": [{}],
                    },
                ],
                "truncated": False,
            },
        }
        summary = flink_diag.summarize_job_exceptions(payload, limit=5)
        self.assertTrue(summary["has_root_exception"])
        self.assertEqual(summary["root_exception_type"], "java.lang.RuntimeException")
        self.assertEqual(summary["history_count"], 2)
        self.assertEqual(summary["top_exception_types"][0]["exceptionName"], "java.lang.RuntimeException")
        self.assertTrue(summary["fields_present"]["exceptionHistory"])

    def test_command_job_exceptions_summary_uses_parsed_flink_115_route(self) -> None:
        """Flink 1.15 的 #/job/<jobid>/exceptions URL 可以直接查询 summary。"""
        async def run_case() -> dict[str, Any]:
            """运行 exceptions-summary mock。"""
            def handler(request: httpx.Request) -> httpx.Response:
                """验证 REST path 仍是官方 jobs/<jobid>/exceptions。"""
                if request.url.path.endswith("/jobs/job-1/exceptions"):
                    return httpx.Response(
                        200,
                        json={
                            "root-exception": "java.lang.RuntimeException: root failed",
                            "timestamp": 1000,
                            "exceptionHistory": {"entries": [{"exceptionName": "java.lang.RuntimeException", "timestamp": 1000}]},
                        },
                    )
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/job/job-1/exceptions")
            args = type(
                "Args",
                (),
                {
                    "command": "job",
                    "subcommand": "exceptions-summary",
                    "flink_version": "1.15.1",
                    "endpoint_profile": "auto",
                    "job_id": "auto",
                    "job_name": None,
                    "job_index": None,
                    "job_state": None,
                    "limit": 5,
                },
            )()
            try:
                return await flink_diag.command_job(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["job_id"], "job-1")
        self.assertEqual(result["summary"]["history_count"], 1)

    def test_summarize_memory_top(self) -> None:
        """Rank TaskManagers by heap, direct memory, and GC."""
        data = {
            "taskmanagers": [
                {"id": "tm-1", "heap": {"used_pct": 50}, "direct": {"used_pct": 80}, "managed": {"used_pct": 100}, "gc": {"young_count": "3", "old_count": "0"}},
                {"id": "tm-2", "heap": {"used_pct": 90}, "direct": {"used_pct": 70}, "managed": {"used_pct": 50}, "gc": {"young_count": "7", "old_count": "1"}},
            ]
        }
        summary = flink_diag.summarize_memory_top(data, top=1)
        self.assertEqual(summary["top_heap"][0]["id"], "tm-2")
        self.assertEqual(summary["top_gc_young"][0]["id"], "tm-2")

    def test_scan_log_text_counts_risky_lines(self) -> None:
        """Scan log text for warning and error patterns."""
        summary = flink_diag.scan_log_text("INFO ok\nWARN slow checkpoint\nERROR failed\nOutOfMemoryError bad\n")
        self.assertEqual(summary["matches"]["ERROR"]["count"], 1)
        self.assertEqual(summary["matches"]["OutOfMemoryError"]["count"], 1)

    def test_stream_grep_keeps_context_without_full_output(self) -> None:
        """Stream grep a log and return only bounded matching context."""
        async def run_case() -> dict[str, Any]:
            """Run stream grep against a mocked large log endpoint."""
            text = "INFO boot\nERROR failed checkpoint\nCaused by: boom\nINFO done\n"
            transport = httpx.MockTransport(lambda request: httpx.Response(200, text=text))
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                return await client.stream_grep("jobmanager/log", ["ERROR"], before=1, after=1, max_matches=5)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["match_count"], 1)
        self.assertEqual(result["matches"][0]["before"], ["INFO boot"])
        self.assertEqual(result["matches"][0]["after"], ["Caused by: boom"])
        self.assertNotIn("text", result)

    def test_summarize_log_errors_groups_signatures(self) -> None:
        """Group repeated error lines by normalized signatures."""
        text = "ERROR failed checkpoint 100\nINFO ok\nERROR failed checkpoint 200\nWARN slow task 1\n"
        summary = flink_diag.summarize_log_errors_text(text, ["ERROR", "WARN"], before=1, after=1)
        self.assertEqual(summary["signature_count"], 2)
        top = summary["signatures"][0]
        self.assertEqual(top["signature"], "ERROR failed checkpoint <num>")
        self.assertEqual(top["count"], 2)
        self.assertEqual(top["samples"][0]["after"], ["INFO ok"])

    def test_download_text_respects_max_bytes(self) -> None:
        """Download only up to the requested byte ceiling."""
        async def run_case(output: Path) -> dict[str, Any]:
            """Run bounded download against a mocked log endpoint."""
            transport = httpx.MockTransport(lambda request: httpx.Response(200, text="abcdef"))
            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=transport)
            try:
                return await client.download_text("jobmanager/log", output, max_bytes=4, overwrite=False)
            finally:
                await client.close()

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "jobmanager.log"
            result = asyncio.run(run_case(output))
            self.assertEqual(output.read_text(), "abcd")
            self.assertEqual(result["bytes_written"], 4)
            self.assertTrue(result["truncated"])
            self.assertEqual(result["sha256"], hashlib.sha256(b"abcd").hexdigest())

    def test_command_logs_diagnose_taskmanager_url(self) -> None:
        """Diagnose a TaskManager logs URL with bounded tail processing."""
        async def run_case() -> dict[str, Any]:
            """Run logs diagnose against a mocked TaskManager log endpoint."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return a TaskManager log response."""
                if request.url.path.endswith("/taskmanagers/tm-1/log"):
                    return httpx.Response(200, text="INFO ok\nERROR task failed\nCaused by: timeout\n")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/task-manager/tm-1/logs")
            args = type(
                "Args",
                (),
                {
                    "scope": "taskmanager",
                    "logs_action": "diagnose",
                    "taskmanager_id": "tm-1",
                    "tail_bytes": 65536,
                    "patterns": None,
                    "before": 1,
                    "after": 1,
                    "max_signatures": 10,
                    "max_samples_per_signature": 1,
                    "full": False,
                },
            )()
            try:
                return await flink_diag.command_logs(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["scope"], "taskmanager")
        self.assertEqual(result["mode"], "tail")
        self.assertEqual(result["errors"]["signature_count"], 2)
        self.assertTrue(result["recommendations"])

    def test_command_logs_grep_all_taskmanagers_hbase_lookup(self) -> None:
        """logs grep 支持 all TaskManager、HBase 预设 pattern 和单 TM 错误隔离。"""
        async def run_case() -> dict[str, Any]:
            """运行多 TaskManager 日志 grep mock。"""
            def handler(request: httpx.Request) -> httpx.Response:
                """返回 TaskManager 列表、日志文本和一个网络错误。"""
                path = request.url.path
                if path.endswith("/taskmanagers"):
                    return httpx.Response(200, json={"taskmanagers": [{"id": "tm-1"}, {"id": "tm-2"}, {"id": "tm-3"}]})
                if path.endswith("/taskmanagers/tm-1/log"):
                    return httpx.Response(200, text="WARN HBaseConfigurationUtil [] - Error while loading config\n")
                if path.endswith("/taskmanagers/tm-2/log"):
                    raise httpx.ReadError("broken stream", request=request)
                if path.endswith("/taskmanagers/tm-3/log"):
                    return httpx.Response(200, text="ERROR real failure\nTimeoutException from hbase\n")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/overview")
            args = type(
                "Args",
                (),
                {
                    "scope": "taskmanager",
                    "logs_action": "grep",
                    "all_taskmanagers": True,
                    "taskmanager_id": None,
                    "taskmanager_host": None,
                    "taskmanager_index": None,
                    "tail_bytes": 65536,
                    "patterns": "ERROR,hbase-lookup",
                    "before": 0,
                    "after": 0,
                    "max_matches": 10,
                    "full": False,
                    "file": None,
                    "log_file": None,
                    "file_pattern": None,
                    "file_index": None,
                },
            )()
            try:
                return await flink_diag.command_logs(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["taskmanager_count"], 3)
        rows = {item["taskmanager_id"]: item for item in result["taskmanagers"]}
        self.assertEqual(rows["tm-1"]["grep"]["pattern_counts"]["ERROR"], 0)
        self.assertEqual(rows["tm-1"]["grep"]["pattern_counts"]["HBaseConfigurationUtil"], 1)
        self.assertFalse(rows["tm-2"]["available"])
        self.assertEqual(rows["tm-3"]["grep"]["pattern_counts"]["ERROR"], 1)

    def test_inspect_routes_taskmanager_logs_to_diagnose(self) -> None:
        """Route TaskManager logs WebUI URLs to logs diagnose."""
        async def run_case() -> dict[str, Any]:
            """Run inspect against a TaskManager logs URL with mocked REST."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock responses for inspect routing."""
                if request.url.path.endswith("/taskmanagers/tm-1/log"):
                    return httpx.Response(200, text="INFO ok\nERROR failed\n")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/task-manager/tm-1/logs")
            args = type(
                "Args",
                (),
                {
                    "command": "inspect",
                    "flink_version": "1.18.1",
                    "endpoint_profile": "auto",
                    "target": None,
                    "taskmanager_id": None,
                    "taskmanager_host": None,
                    "taskmanager_index": None,
                    "tail_bytes": 65536,
                    "patterns": None,
                    "before": 1,
                    "after": 1,
                    "max_signatures": 10,
                    "max_samples_per_signature": 1,
                    "full": False,
                },
            )()
            try:
                return await flink_diag.command_inspect(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["scope"], "taskmanager")
        self.assertEqual(result["mode"], "tail")
        self.assertIn("errors", result)

    def test_parser_accepts_new_log_actions(self) -> None:
        """Parse the new large-log workflow actions."""
        parser = flink_diag.build_parser()
        args = parser.parse_args(
            [
                "logs",
                "grep",
                "--url",
                "https://flink.example.com/demo/#/task-manager/tm-1/logs",
                "--scope",
                "taskmanager",
                "--patterns",
                "ERROR,Exception",
                "--before",
                "2",
                "--after",
                "3",
            ]
        )
        self.assertEqual(args.logs_action, "grep")
        self.assertEqual(args.scope, "taskmanager")
        self.assertEqual(args.before, 2)

    def test_stdout_tail_all_taskmanagers(self) -> None:
        """并行读取多个 TaskManager stdout 尾部。"""
        async def run_case() -> dict[str, Any]:
            """使用 mock transport 运行 stdout tail。"""
            def handler(request: httpx.Request) -> httpx.Response:
                """返回 TaskManager 列表和 stdout 文本。"""
                path = request.url.path
                if path.endswith("/taskmanagers"):
                    return httpx.Response(200, json={"taskmanagers": [{"id": "tm-1"}, {"id": "tm-2"}, {"id": "tm-3"}]})
                if path.endswith("/taskmanagers/tm-1/stdout"):
                    return httpx.Response(200, text="a\n")
                if path.endswith("/taskmanagers/tm-2/stdout"):
                    return httpx.Response(200, text="b\n")
                if path.endswith("/taskmanagers/tm-3/stdout"):
                    return httpx.Response(200, text="c\n")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/task-manager/tm-1/stdout")
            args = type(
                "Args",
                (),
                {
                    "stdout_action": "tail",
                    "all_taskmanagers": True,
                    "taskmanager_id": None,
                    "taskmanager_host": None,
                    "taskmanager_index": None,
                    "provider": "rest",
                    "tail_bytes": 64,
                    "max_bytes_per_poll": None,
                    "namespace": None,
                    "kube_context": None,
                    "container": None,
                },
            )()
            try:
                return await flink_diag.command_stdout(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["taskmanager_count"], 3)
        self.assertEqual([item["taskmanager_id"] for item in result["taskmanagers"]], ["tm-1", "tm-2", "tm-3"])
        self.assertEqual(result["taskmanagers"][1]["text"], "b\n")

    def test_stdout_missing_does_not_download_placeholder(self) -> None:
        """stdout 缺失时不把 Kubernetes 提示写成本地文件。"""
        async def run_case(output_dir: Path) -> dict[str, Any]:
            """运行 stdout download 缺失场景。"""
            missing_text = (
                "The file STDOUT does not exist on the TaskExecutor. \n"
                "If you are using kubernetes mode, please use \"kubectl logs <pod-name>\" to get stdout content."
            )

            def handler(request: httpx.Request) -> httpx.Response:
                """返回 TaskManager 列表和 stdout 缺失提示。"""
                path = request.url.path
                if path.endswith("/taskmanagers"):
                    return httpx.Response(200, json={"taskmanagers": [{"id": "tm-1"}]})
                if path.endswith("/taskmanagers/tm-1/stdout"):
                    return httpx.Response(200, text=missing_text)
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/task-manager/tm-1/stdout")
            args = type(
                "Args",
                (),
                {
                    "stdout_action": "download",
                    "all_taskmanagers": False,
                    "taskmanager_id": None,
                    "taskmanager_host": None,
                    "taskmanager_index": None,
                    "provider": "rest",
                    "max_bytes": 1024,
                    "full": False,
                    "output_dir": str(output_dir),
                    "overwrite": False,
                    "namespace": "prod",
                    "kube_context": "us",
                    "container": None,
                },
            )()
            try:
                return await flink_diag.command_stdout(client, parsed, args)
            finally:
                await client.close()

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            result = asyncio.run(run_case(output_dir))
            item = result["taskmanagers"][0]
            self.assertFalse(item["available"])
            self.assertEqual(item["reason"], "kubernetes_stdout_missing")
            self.assertIsNone(item["download"])
            self.assertIn("kubectl --context us -n prod logs tm-1", item["recommendation"]["command"])
            self.assertFalse(any(output_dir.iterdir()))

    def test_stdout_watch_since_end_emits_incremental_line(self) -> None:
        """watch 使用 since-end 时只输出第二轮新增行。"""
        async def run_case() -> str:
            """运行两轮 stdout watch 并捕获文本输出。"""
            calls = {"tm-1": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                """第一轮返回基线，第二轮返回新增行。"""
                path = request.url.path
                if path.endswith("/taskmanagers"):
                    return httpx.Response(200, json={"taskmanagers": [{"id": "tm-1"}]})
                if path.endswith("/taskmanagers/tm-1/stdout"):
                    calls["tm-1"] += 1
                    return httpx.Response(200, text="old\nnew\n" if calls["tm-1"] > 1 else "old\n")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/task-manager/tm-1/stdout")
            args = type(
                "Args",
                (),
                {
                    "stdout_action": "watch",
                    "all_taskmanagers": False,
                    "taskmanager_id": None,
                    "taskmanager_host": None,
                    "taskmanager_index": None,
                    "provider": "rest",
                    "tail_bytes": 64,
                    "max_bytes_per_poll": None,
                    "interval": 0.1,
                    "duration": None,
                    "max_events": None,
                    "polls": 2,
                    "since_end": True,
                    "json": False,
                    "namespace": None,
                    "kube_context": None,
                    "container": None,
                },
            )()
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    await flink_diag.command_stdout(client, parsed, args)
                return buffer.getvalue()
            finally:
                await client.close()

        output = asyncio.run(run_case())
        self.assertNotIn("old", output)
        self.assertIn("[tm-1] new", output)

    def test_parser_accepts_stdout_watch_options(self) -> None:
        """解析 stdout watch 的多 TaskManager 和性能保护参数。"""
        parser = flink_diag.build_parser()
        args = parser.parse_args(
            [
                "stdout",
                "watch",
                "--url",
                "https://flink.example.com/demo/#/task-manager/tm-1/stdout",
                "--all-taskmanagers",
                "--interval",
                "2",
                "--duration",
                "5",
                "--max-events",
                "10",
                "--max-bytes-per-poll",
                "4096",
                "--since-end",
            ]
        )
        self.assertEqual(args.command, "stdout")
        self.assertEqual(args.stdout_action, "watch")
        self.assertTrue(args.all_taskmanagers)
        self.assertEqual(args.max_bytes_per_poll, 4096)

    def test_parser_accepts_capacity_diagnostics(self) -> None:
        """解析 job capacity 和 diagnose parallelism 诊断入口。"""
        parser = flink_diag.build_parser()
        job_args = parser.parse_args(["job", "capacity", "--url", "https://flink.example.com/demo/#/job/running/job-1/overview"])
        exception_args = parser.parse_args(["job", "exceptions-summary", "--url", "https://flink.example.com/demo/#/job/job-1/exceptions", "--limit", "3"])
        diag_args = parser.parse_args(["diagnose", "parallelism", "--url", "https://flink.example.com/demo/#/job/running/job-1/overview"])
        lookup_args = parser.parse_args(["diagnose", "lookup", "--url", "https://flink.example.com/demo/#/job/running/job-1/overview"])
        analyze_args = parser.parse_args(
            [
                "metric",
                "analyze",
                "--url",
                "https://flink.example.com/demo/#/job/running/job-1/overview/vertex-1/metrics",
                "--scope",
                "subtask",
                "--samples",
                "3",
                "--interval",
                "5",
                "--peak-threshold",
                "900",
            ]
        )
        self.assertEqual(job_args.command, "job")
        self.assertEqual(job_args.subcommand, "capacity")
        self.assertEqual(exception_args.subcommand, "exceptions-summary")
        self.assertEqual(exception_args.limit, 3)
        self.assertEqual(diag_args.command, "diagnose")
        self.assertEqual(diag_args.playbook, "parallelism")
        self.assertEqual(lookup_args.playbook, "lookup")
        self.assertEqual(analyze_args.subcommand, "analyze")
        self.assertEqual(analyze_args.peak_threshold, 900)

    def test_build_health_report_flags_risks(self) -> None:
        """Build a compact health report from collected diagnostics."""
        report = flink_diag.build_health_report(
            {
                "job": {"name": "demo", "state": "RUNNING"},
                "flow": {"largest_filter_drop": {"name": "Filter", "pass_through_pct": 10.0}},
                "backpressure": {"vertices": [{"name": "Map", "backpressure_level": "high", "backpressured_max": 0.7}]},
                "checkpoint": {"counts": {"failed": 1}, "latest_completed": {"state_size": "1.00 GiB"}},
                "exceptions": {"all-exceptions": []},
                "memory_top": {"top_heap": [{"id": "tm-1", "heap_used_pct": 90}]},
            }
        )
        self.assertEqual(report["job_name"], "demo")
        self.assertGreaterEqual(len(report["risks"]), 3)

    def test_fetch_metrics_by_chunks(self) -> None:
        """Fetch metric values in chunks to avoid long get URLs."""
        async def run_case() -> list[dict[str, Any]]:
            """Run chunked metric fetch against mocked responses."""
            seen_queries: list[str] = []

            def handler(request: httpx.Request) -> httpx.Response:
                """Return metric values for each chunked request."""
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                seen_queries.append(query)
                get_part = query.split("get=")[-1].split("&")[0]
                return httpx.Response(200, json=[{"id": item, "value": "1"} for item in get_part.split(",")])

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                result = await flink_diag.fetch_metrics_by_chunks(client, "metrics", ["a", "b", "c"], chunk_size=2)
                self.assertEqual(len(seen_queries), 2)
                return result
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(len(result), 3)

    def test_metric_query_escapes_each_metric_id(self) -> None:
        """逐个转义 metric id 中的特殊字符，同时保留逗号分隔符。"""
        query = flink_diag.build_metric_query(
            "taskmanagers/metrics",
            metrics=["a_+_b", "x/y", "m#n", "q?r", "k=v", "a&b", "mail@x", "semi;colon", "dollar$"],
            agg=["min", "max"],
            taskmanagers=["tm-1", "tm-2"],
        )
        self.assertIn("get=a_%2B_b,x%2Fy,m%23n,q%3Fr,k%3Dv,a%26b,mail%40x,semi%3Bcolon,dollar%24", query)
        self.assertIn("agg=min,max", query)
        self.assertIn("taskmanagers=tm-1,tm-2", query)

    def test_metric_aggregate_taskmanager_subset_and_agg(self) -> None:
        """调用官方 TaskManager 聚合 endpoint，支持 subset 与 agg。"""
        async def run_case() -> dict[str, Any]:
            """运行 TaskManager aggregate mock。"""
            seen_queries: list[str] = []

            def handler(request: httpx.Request) -> httpx.Response:
                """返回聚合 metric 列表和值。"""
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                seen_queries.append(query)
                if "get=" not in query:
                    return httpx.Response(200, json=[{"id": "metric1"}, {"id": "metric2"}])
                return httpx.Response(
                    200,
                    json=[
                        {"id": "metric1", "min": 1, "max": 3},
                        {"id": "metric2", "min": 2, "max": 4},
                    ],
                )

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/overview")
            args = type(
                "Args",
                (),
                {
                    "scope": "taskmanager",
                    "taskmanagers": "tm-1,tm-2",
                    "jobs": None,
                    "subtasks": None,
                    "taskmanager_id": None,
                    "get": "metric1,metric2",
                    "agg": "min,max",
                    "metric_match": "auto",
                },
            )()
            try:
                result = await flink_diag.fetch_metric_aggregate(client, parsed, args)
                result["seen_queries"] = seen_queries
                return result
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertTrue(result["available"])
        self.assertEqual(result["values"][0]["min"], 1)
        self.assertIn("taskmanagers=tm-1,tm-2", result["seen_queries"][0])
        self.assertIn("get=metric1,metric2", result["seen_queries"][1])
        self.assertIn("agg=min,max", result["seen_queries"][1])

    def test_metric_aggregate_jm_operator_unavailable(self) -> None:
        """jm-operator-metrics 不存在时返回 unavailable，不抛异常。"""
        async def run_case() -> dict[str, Any]:
            """运行 jm-operator unavailable mock。"""
            def handler(request: httpx.Request) -> httpx.Response:
                """返回 404 表示 endpoint 不可用。"""
                if request.url.path.endswith("/jm-operator-metrics"):
                    return httpx.Response(404, text="not found")
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url(
                "https://flink.example.com/demo/#/job/running/job-1/overview/vertex-1/metrics"
            )
            args = type(
                "Args",
                (),
                {
                    "scope": "jm-operator",
                    "job_id": "auto",
                    "job_name": None,
                    "job_index": None,
                    "job_state": None,
                    "vertex_id": None,
                    "task_chain_id": None,
                    "vertex_name": None,
                    "task_chain_name": None,
                    "subtask": None,
                    "subtasks": None,
                    "get": "metric1",
                    "agg": None,
                    "metric_match": "auto",
                },
            )()
            try:
                return await flink_diag.fetch_metric_aggregate(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertFalse(result["available"])
        self.assertEqual(result["scope"], "jm-operator")

    def test_metric_search_structured_auto_probes_jm_operator(self) -> None:
        """结构化搜索会解析 subtask/operator/metric 并探测 jm-operator endpoint。"""
        async def run_case() -> dict[str, Any]:
            """运行结构化 search mock。"""
            def handler(request: httpx.Request) -> httpx.Response:
                """返回普通 vertex metrics 和 jm operator metrics。"""
                path = request.url.path
                if path.endswith("/vertices/vertex-1/metrics"):
                    return httpx.Response(200, json=[{"id": "0.Source__kafka.numRecordsIn"}])
                if path.endswith("/vertices/vertex-1/jm-operator-metrics"):
                    return httpx.Response(200, json=[{"id": "0.OperatorCoordinator.numRecordsIn"}])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url(
                "https://flink.example.com/demo/#/job/running/job-1/overview/vertex-1/metrics"
            )
            args = type(
                "Args",
                (),
                {
                    "subcommand": "search",
                    "scope": "auto",
                    "keyword": "numRecordsIn",
                    "metric": None,
                    "regex": False,
                    "limit": 10,
                    "structured": True,
                },
            )()
            try:
                return await flink_diag.command_metric(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(len(result["matches"]), 2)
        first = result["matches"][0]
        self.assertEqual(first["kind"], "operator")
        self.assertEqual(first["subtask"], 0)
        self.assertEqual(first["operator"], "Source__kafka")
        self.assertEqual(first["metric_name"], "numRecordsIn")

    def test_metric_watch_emits_delta_and_rate_jsonl(self) -> None:
        """metric watch 输出 JSONL，并计算 delta/rate。"""
        async def run_case() -> list[dict[str, Any]]:
            """运行两轮 watch 并解析 JSONL 输出。"""
            calls = {"values": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                """第一轮返回 10，第二轮返回 15。"""
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if "get=" not in query:
                    return httpx.Response(200, json=[{"id": "metric1"}])
                calls["values"] += 1
                value = 10 if calls["values"] == 1 else 15
                return httpx.Response(200, json=[{"id": "metric1", "sum": value}])

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/overview")
            args = type(
                "Args",
                (),
                {
                    "scope": "taskmanager",
                    "taskmanagers": None,
                    "jobs": None,
                    "subtasks": None,
                    "taskmanager_id": None,
                    "get": "metric1",
                    "agg": "sum",
                    "metric_match": "auto",
                    "samples": 2,
                    "duration": None,
                    "interval": 0.1,
                    "delta": True,
                    "rate": True,
                    "json": True,
                },
            )()
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    await flink_diag.command_metric_watch(client, parsed, args)
                return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]
            finally:
                await client.close()

        rows = asyncio.run(run_case())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["delta"]["metric1.sum"], 5)
        self.assertGreater(rows[1]["rate_per_second"]["metric1.sum"], 0)

    def test_analyze_metric_samples_classifies_checkpoint_state_spike(self) -> None:
        """带记忆的 metric 分析能把无反压 busy 峰值关联到 checkpoint/state。"""
        base_ms = 1_700_000_000_000
        samples = [
            {
                "sample": 1,
                "elapsed_seconds": 0.0,
                "timestamp_ms": base_ms,
                "metric_map": {
                    "busyTimeMsPerSecond.max": 200,
                    "backPressuredTimeMsPerSecond.max": 0,
                    "numRecordsInPerSecond.sum": 1000,
                    "checkpointStartDelayNanos.max": 200_000_000,
                },
            },
            {
                "sample": 2,
                "elapsed_seconds": 10.0,
                "timestamp_ms": base_ms + 10_000,
                "metric_map": {
                    "busyTimeMsPerSecond.max": 250,
                    "backPressuredTimeMsPerSecond.max": 0,
                    "numRecordsInPerSecond.sum": 1010,
                    "checkpointStartDelayNanos.max": 200_000_000,
                },
            },
            {
                "sample": 3,
                "elapsed_seconds": 20.0,
                "timestamp_ms": base_ms + 20_000,
                "metric_map": {
                    "busyTimeMsPerSecond.max": 950,
                    "backPressuredTimeMsPerSecond.max": 0,
                    "numRecordsInPerSecond.sum": 1005,
                    "checkpointStartDelayNanos.max": 220_000_000,
                },
            },
        ]
        checkpoints = {
            "history": [
                {
                    "id": 1,
                    "status": "COMPLETED",
                    "trigger_timestamp": base_ms + 15_000,
                    "latest_ack_timestamp": base_ms + 23_000,
                    "end_to_end_duration": 8000,
                }
            ]
        }
        args = type(
            "Args",
            (),
            {
                "peak_metric": None,
                "peak_threshold": 900,
                "period_tolerance": 0.35,
                "checkpoint_window": 15,
                "max_peak_samples": 10,
                "include_samples": False,
            },
        )()
        result = flink_diag.analyze_metric_samples(samples, args, checkpoint_data=checkpoints)
        self.assertEqual(result["conclusion"], "checkpoint_state_spike")
        self.assertEqual(result["peaks"]["event_count"], 1)
        self.assertEqual(result["checkpoint_correlation"]["correlated_event_count"], 1)
        self.assertTrue(result["traffic"]["stable"])

    def test_metric_analyze_reuses_metric_plan_and_reports_peak(self) -> None:
        """metric analyze 只解析一次 metric 列表，后续采样复用查询计划。"""
        async def run_case() -> tuple[dict[str, Any], dict[str, int]]:
            """运行三轮 analyze mock，并记录 list/value 请求次数。"""
            calls = {"list": 0, "values": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                """返回一次 metric 列表和三轮 metric 值。"""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/jobs/job-1/vertices/vertex-1/subtasks/metrics") and "get=" not in query:
                    calls["list"] += 1
                    return httpx.Response(
                        200,
                        json=[
                            {"id": "busyTimeMsPerSecond"},
                            {"id": "backPressuredTimeMsPerSecond"},
                            {"id": "numRecordsInPerSecond"},
                        ],
                    )
                if path.endswith("/jobs/job-1/vertices/vertex-1/subtasks/metrics"):
                    values = [100, 950, 120]
                    busy = values[min(calls["values"], len(values) - 1)]
                    calls["values"] += 1
                    return httpx.Response(
                        200,
                        json=[
                            {"id": "busyTimeMsPerSecond", "max": busy, "avg": busy / 2},
                            {"id": "backPressuredTimeMsPerSecond", "max": 0},
                            {"id": "numRecordsInPerSecond", "sum": 1000},
                        ],
                    )
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url(
                "https://flink.example.com/demo/#/job/running/job-1/overview/vertex-1/metrics"
            )
            args = type(
                "Args",
                (),
                {
                    "command": "metric",
                    "subcommand": "analyze",
                    "scope": "subtask",
                    "job_id": "auto",
                    "job_name": None,
                    "job_index": None,
                    "job_state": None,
                    "vertex_id": None,
                    "task_chain_id": None,
                    "vertex_name": None,
                    "task_chain_name": None,
                    "subtask": None,
                    "subtasks": None,
                    "taskmanagers": None,
                    "jobs": None,
                    "taskmanager_id": None,
                    "get": "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond",
                    "agg": "max,avg,sum",
                    "metric_match": "auto",
                    "samples": 3,
                    "interval": 0.1,
                    "duration": None,
                    "peak_metric": None,
                    "peak_threshold": 900,
                    "period_tolerance": 0.35,
                    "checkpoint_window": 15,
                    "no_checkpoint_correlation": True,
                    "include_samples": False,
                    "max_peak_samples": 10,
                },
            )()
            try:
                return await flink_diag.command_metric_analyze(client, parsed, args), calls
            finally:
                await client.close()

        result, calls = asyncio.run(run_case())
        self.assertEqual(calls["list"], 1)
        self.assertEqual(calls["values"], 3)
        self.assertEqual(result["sample_count"], 3)
        self.assertEqual(result["peaks"]["event_count"], 1)
        self.assertEqual(result["conclusion"], "busy_spike_without_backpressure")

    def test_match_paimon_semantic_aliases(self) -> None:
        """Resolve Paimon semantic aliases to concrete metric ids."""
        available = [
            "0.Writer.numRecordsIn",
            "0.Writer.numRecordsInPerSecond",
            "paimon.table.db.tbl.writerBuffer.numWriters",
            "paimon.table.db.tbl.writerBuffer.bufferPreemptCount",
            "paimon.table.db.tbl.compaction.compactionThreadBusy",
            "paimon.table.db.tbl.compaction.completedCompactionCount",
            "paimon.table.db.tbl.compaction.level0FileCount",
            "paimon.table.db.tbl.commit.commitDuration_p99",
            "paimon.table.db.tbl.commit.lastTableFilesAdded",
        ]
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["paimon.writer.records_in"], mode="auto"),
            ["0.Writer.numRecordsIn"],
        )
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["paimon.compaction.level0_file_count"], mode="auto"),
            ["paimon.table.db.tbl.compaction.level0FileCount"],
        )
        self.assertEqual(
            flink_diag.match_metric_ids(available, ["paimon.commit.duration_p99"], mode="auto"),
            ["paimon.table.db.tbl.commit.commitDuration_p99"],
        )

    def test_detect_paimon_metric_roles(self) -> None:
        """Classify Paimon writer and committer metrics without mislabeling Kafka sources."""
        writer_metrics = [
            "paimon.table.db.tbl.writerBuffer.numWriters",
            "paimon.table.db.tbl.compaction.compactionThreadBusy",
        ]
        committer_metrics = [
            "paimon.table.db.tbl.commit.commitDuration_p99",
            "paimon.table.db.tbl.commit.lastCommitAttempts",
        ]
        kafka_metrics = [
            "0.Source__kafka.numRecordsIn",
            "0.Source__kafka.records-lag-max",
        ]
        self.assertEqual(flink_diag.detect_paimon_metric_role(writer_metrics)["role"], "writer")
        self.assertEqual(flink_diag.detect_paimon_metric_role(committer_metrics)["role"], "committer")
        self.assertEqual(flink_diag.detect_paimon_metric_role(kafka_metrics)["role"], "unknown")

    def test_detect_prefixed_real_paimon_metric_roles(self) -> None:
        """Classify real prefixed Paimon metric ids without using generic Writer input alone."""
        writer_metrics = [
            "0.Writer___tbl.paimon.table.tbl.compaction.avgCompactionTime",
            "0.Writer___tbl.paimon.table.tbl.compaction.maxLevel0FileCount",
        ]
        committer_metrics = [
            "0.end__Writer.numRecordsIn",
            "0.Global_Committer___tbl.paimon.table.tbl.commit.commitDuration_p99",
            "0.Global_Committer___tbl.paimon.table.tbl.commit.lastCommitAttempts",
        ]
        generic_writer_metrics = [
            "0.end__Writer.numRecordsIn",
            "0.end__Writer.numRecordsInPerSecond",
        ]
        self.assertEqual(flink_diag.detect_paimon_metric_role(writer_metrics)["role"], "writer")
        self.assertEqual(flink_diag.detect_paimon_metric_role(committer_metrics)["role"], "committer")
        self.assertEqual(flink_diag.detect_paimon_metric_role(generic_writer_metrics)["role"], "unknown")

    def test_summarize_paimon_writer_metrics_flags_pressure_and_skew(self) -> None:
        """Summarize Paimon writer throughput, compaction pressure, and subtask skew."""
        metrics = {
            "paimon.writer.records_in": [{"id": "0.Writer.numRecordsIn", "value": "300"}],
            "paimon.writer.records_in_rate": [{"id": "0.Writer.numRecordsInPerSecond", "value": "30"}],
            "paimon.writer.buffer_writers": [{"id": "paimon.table.db.tbl.writerBuffer.numWriters", "value": "8"}],
            "paimon.writer.buffer_preempt_count": [{"id": "paimon.table.db.tbl.writerBuffer.bufferPreemptCount", "value": "2"}],
            "paimon.compaction.busy": [{"id": "paimon.table.db.tbl.compaction.compactionThreadBusy", "value": "95"}],
            "paimon.compaction.level0_file_count": [{"id": "paimon.table.db.tbl.compaction.level0FileCount", "value": "80"}],
        }
        skew = {"metrics": {"0.Writer.numRecordsInPerSecond": {"skew_ratio": 5.0, "max_subtask": 1}}}
        summary = flink_diag.summarize_paimon_writer_metrics(metrics, skew)
        self.assertEqual(summary["summary"]["records_in"], "300")
        self.assertEqual(summary["summary"]["buffer_preempt_count"], "2")
        self.assertEqual(summary["summary"]["compaction_busy"], "95")
        self.assertIn("compaction_busy", {risk["type"] for risk in summary["risks"]})
        self.assertIn("writer_skew", {risk["type"] for risk in summary["risks"]})

    def test_summarize_paimon_committer_metrics_flags_commit_and_file_risks(self) -> None:
        """Summarize Paimon committer latency, attempts, files, and small-file risk."""
        metrics = {
            "paimon.commit.duration_p99": [{"id": "paimon.table.db.tbl.commit.commitDuration_p99", "value": "45000"}],
            "paimon.commit.duration": [{"id": "paimon.table.db.tbl.commit.commitDuration_max", "value": "60000"}],
            "paimon.commit.files_added": [{"id": "paimon.table.db.tbl.commit.lastTableFilesAdded", "value": "500"}],
            "paimon.commit.partitions_written": [{"id": "paimon.table.db.tbl.commit.lastPartitionsWritten", "value": "120"}],
            "paimon.commit.buckets_written": [{"id": "paimon.table.db.tbl.commit.lastBucketsWritten", "value": "240"}],
            "paimon.commit.attempts": [{"id": "paimon.table.db.tbl.commit.lastCommitAttempts", "value": "3"}],
        }
        summary = flink_diag.summarize_paimon_committer_metrics(metrics)
        self.assertEqual(summary["summary"]["commit_duration_p99"], "45000")
        self.assertEqual(summary["summary"]["commit_duration_p99_human"], "45.00 s")
        self.assertEqual(summary["summary"]["commit_attempts"], "3")
        self.assertIn("commit_slow", {risk["type"] for risk in summary["risks"]})
        self.assertIn("small_files_risk", {risk["type"] for risk in summary["risks"]})

    def test_job_connectors_command_identifies_paimon_roles_and_absent_source(self) -> None:
        """Scan a job graph and report Paimon writer/committer while source is Kafka."""
        async def run_case() -> dict[str, Any]:
            """Run job connectors against mocked Flink endpoints."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock responses for connector discovery."""
                path = request.url.path
                if path.endswith("/config"):
                    return httpx.Response(200, json={"flink-version": "1.18.1"})
                if path.endswith("/jobs/job-1"):
                    return httpx.Response(
                        200,
                        json={
                            "jid": "job-1",
                            "name": "demo",
                            "state": "RUNNING",
                            "vertices": [
                                {"id": "source", "name": "Kafka Source", "parallelism": 1},
                                {"id": "writer", "name": "Writer : ods_log_power", "parallelism": 2},
                                {"id": "committer", "name": "Global Committer : ods_log_power -> end: Writer", "parallelism": 1},
                            ],
                        },
                    )
                if path.endswith("/jobs/job-1/vertices/source/metrics"):
                    return httpx.Response(200, json=[{"id": "0.Source__kafka.numRecordsIn"}])
                if path.endswith("/jobs/job-1/vertices/writer/metrics"):
                    return httpx.Response(200, json=[{"id": "paimon.table.db.tbl.writerBuffer.numWriters"}])
                if path.endswith("/jobs/job-1/vertices/committer/metrics"):
                    return httpx.Response(200, json=[{"id": "paimon.table.db.tbl.commit.commitDuration_p99"}])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/job/running/job-1/overview")
            args = type(
                "Args",
                (),
                {
                    "command": "job",
                    "subcommand": "connectors",
                    "job_id": "job-1",
                    "flink_version": "auto",
                    "endpoint_profile": "auto",
                    "target": None,
                    "vertex_id": None,
                    "task_chain_id": None,
                    "vertex_name": None,
                    "task_chain_name": None,
                    "job_name": None,
                    "regex": False,
                    "job_state": None,
                    "job_index": None,
                },
            )()
            try:
                return await flink_diag.command_job(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        roles = {row["paimon_role"] for row in result["connectors"]}
        self.assertIn("writer", roles)
        self.assertIn("committer", roles)
        self.assertTrue(result["source_absent"])
        self.assertEqual(result["actual_source_vertices"][0]["name"], "Kafka Source")

    def test_task_chain_paimon_stats_command_summarizes_writer(self) -> None:
        """Fetch Paimon stats for a writer task chain through the task-chain command."""
        async def run_case() -> dict[str, Any]:
            """Run task-chain paimon-stats against mocked Flink endpoints."""
            metric_values = {
                "0.Writer.numRecordsIn": "300",
                "0.Writer.numRecordsInPerSecond": "30",
                "paimon.table.db.tbl.writerBuffer.numWriters": "8",
                "paimon.table.db.tbl.writerBuffer.bufferPreemptCount": "2",
                "paimon.table.db.tbl.compaction.compactionThreadBusy": "95",
                "paimon.table.db.tbl.compaction.level0FileCount": "80",
            }

            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock responses for Paimon writer stats."""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/config"):
                    return httpx.Response(200, json={"flink-version": "1.18.1"})
                if path.endswith("/jobs/job-1/vertices/writer") and not query:
                    return httpx.Response(200, json={"id": "writer", "name": "Writer : ods_log_power", "parallelism": 1, "subtasks": [{"subtask": 0}]})
                if path.endswith("/jobs/job-1/vertices/writer/metrics") and not query:
                    return httpx.Response(200, json=[{"id": name} for name in metric_values])
                if path.endswith("/jobs/job-1/vertices/writer/metrics"):
                    return httpx.Response(200, json=[{"id": name, "value": value} for name, value in metric_values.items() if name in query])
                if path.endswith("/jobs/job-1/vertices/writer/subtasks/0/metrics") and not query:
                    return httpx.Response(200, json=[{"id": "0.Writer.numRecordsInPerSecond"}])
                if path.endswith("/jobs/job-1/vertices/writer/subtasks/0/metrics"):
                    return httpx.Response(200, json=[{"id": "0.Writer.numRecordsInPerSecond", "value": "30"}])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/job/running/job-1/overview/writer/metrics")
            args = type(
                "Args",
                (),
                {
                    "command": "task-chain",
                    "subcommand": "paimon-stats",
                    "job_id": "job-1",
                    "vertex_id": "writer",
                    "task_chain_id": None,
                    "role": "auto",
                    "flink_version": "auto",
                    "endpoint_profile": "auto",
                    "target": None,
                    "vertex_name": None,
                    "task_chain_name": None,
                    "job_name": None,
                    "regex": False,
                    "job_state": None,
                    "job_index": None,
                    "subtask": None,
                    "all_subtasks": False,
                    "metric_match": "auto",
                },
            )()
            try:
                return await flink_diag.command_task_chain(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["paimon_role"], "writer")
        self.assertEqual(result["summary"]["records_in"], "300")
        self.assertIn("compaction_busy", {risk["type"] for risk in result["risks"]})

    def test_diagnose_paimon_command_combines_writer_committer_and_source_note(self) -> None:
        """Diagnose a Paimon sink job across writer, committer, and source absence."""
        async def run_case() -> dict[str, Any]:
            """Run diagnose paimon against mocked Flink endpoints."""
            metric_values = {
                "writer": {
                    "0.Writer.numRecordsIn": "300",
                    "paimon.table.db.tbl.compaction.compactionThreadBusy": "95",
                    "paimon.table.db.tbl.compaction.level0FileCount": "80",
                },
                "committer": {
                    "paimon.table.db.tbl.commit.commitDuration_p99": "45000",
                    "paimon.table.db.tbl.commit.lastTableFilesAdded": "500",
                    "paimon.table.db.tbl.commit.lastCommitAttempts": "3",
                },
            }

            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock responses for Paimon diagnosis."""
                path = request.url.path
                query = request.url.query.decode("utf-8") if isinstance(request.url.query, bytes) else str(request.url.query)
                if path.endswith("/config"):
                    return httpx.Response(200, json={"flink-version": "1.18.1"})
                if path.endswith("/jobs/job-1"):
                    return httpx.Response(
                        200,
                        json={
                            "jid": "job-1",
                            "name": "demo",
                            "state": "RUNNING",
                            "vertices": [
                                {"id": "source", "name": "Kafka Source", "parallelism": 1},
                                {"id": "writer", "name": "Writer : ods_log_power", "parallelism": 1, "subtasks": [{"subtask": 0}]},
                                {"id": "committer", "name": "Global Committer : ods_log_power -> end: Writer", "parallelism": 1, "subtasks": [{"subtask": 0}]},
                            ],
                        },
                    )
                if path.endswith("/jobs/job-1/vertices/source/metrics"):
                    return httpx.Response(200, json=[{"id": "0.Source__kafka.numRecordsIn"}])
                for vertex_id, values in metric_values.items():
                    if path.endswith(f"/jobs/job-1/vertices/{vertex_id}") and not query:
                        return httpx.Response(200, json={"id": vertex_id, "name": vertex_id.title(), "parallelism": 1, "subtasks": [{"subtask": 0}]})
                    if path.endswith(f"/jobs/job-1/vertices/{vertex_id}/metrics") and not query:
                        return httpx.Response(200, json=[{"id": name} for name in values])
                    if path.endswith(f"/jobs/job-1/vertices/{vertex_id}/metrics"):
                        return httpx.Response(200, json=[{"id": name, "value": value} for name, value in values.items() if name in query])
                    if path.endswith(f"/jobs/job-1/vertices/{vertex_id}/subtasks/0/metrics") and not query:
                        return httpx.Response(200, json=[{"id": name} for name in values])
                    if path.endswith(f"/jobs/job-1/vertices/{vertex_id}/subtasks/0/metrics"):
                        return httpx.Response(200, json=[{"id": name, "value": value} for name, value in values.items() if name in query])
                return httpx.Response(404, json={"errors": ["missing"]})

            client = flink_diag.FlinkClient("https://flink.example.com/demo/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            parsed = flink_diag.parse_web_url("https://flink.example.com/demo/#/job/running/job-1/overview")
            args = type(
                "Args",
                (),
                {
                    "command": "diagnose",
                    "playbook": "paimon",
                    "job_id": "job-1",
                    "flink_version": "auto",
                    "endpoint_profile": "auto",
                    "target": None,
                    "vertex_id": None,
                    "task_chain_id": None,
                    "vertex_name": None,
                    "task_chain_name": None,
                    "job_name": None,
                    "regex": False,
                    "job_state": None,
                    "job_index": None,
                    "metric_match": "auto",
                },
            )()
            try:
                return await flink_diag.command_diagnose(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["job_id"], "job-1")
        self.assertEqual(result["writer"]["summary"]["records_in"], "300")
        self.assertEqual(result["committer"]["summary"]["commit_attempts"], "3")
        self.assertIn("source_absent", {risk["type"] for risk in result["risks"]})
        self.assertIn("commit_slow", {risk["type"] for risk in result["risks"]})


if __name__ == "__main__":
    unittest.main()
