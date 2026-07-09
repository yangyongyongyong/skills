"""Offline tests for spark_diag.py."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any

import httpx


def load_module() -> Any:
    """Load the CLI module from the local script path."""
    path = Path(__file__).with_name("spark_diag.py")
    spec = importlib.util.spec_from_file_location("spark_diag", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["spark_diag"] = module
    spec.loader.exec_module(module)
    return module


def load_stage_lib_module() -> Any:
    """Load the stage helper library module from the local script path."""
    path = Path(__file__).parent / "lib" / "stages.py"
    spec = importlib.util.spec_from_file_location("spark_stage_helpers", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["spark_stage_helpers"] = module
    spec.loader.exec_module(module)
    return module


spark_diag = load_module()


class SparkDiagTests(unittest.TestCase):
    """Unit tests for Spark URL parsing, summaries, and HTTP helpers."""

    def test_parse_running_ui_url(self) -> None:
        """Parse a running Spark Web UI URL."""
        parsed = spark_diag.parse_web_url(
            "https://spark-k8s-us.tuya-inc.com:7799/spark-2061228901727735842/jobs/"
        )
        self.assertEqual(parsed.base_url, "https://spark-k8s-us.tuya-inc.com:7799/spark-2061228901727735842/")
        self.assertEqual(parsed.ui_kind, "running")
        self.assertEqual(parsed.deployment, "spark-2061228901727735842")
        self.assertEqual(parsed.tab, "jobs")

    def test_parse_history_app_url(self) -> None:
        """Parse a Spark History Server application URL."""
        parsed = spark_diag.parse_web_url(
            "https://spark-k8s-historyserver-us.tuya-inc.com:7799/history/spark-app-1/stages/"
        )
        self.assertEqual(parsed.base_url, "https://spark-k8s-historyserver-us.tuya-inc.com:7799/")
        self.assertEqual(parsed.ui_kind, "history")
        self.assertEqual(parsed.app_id, "spark-app-1")
        self.assertEqual(parsed.tab, "stages")

    def test_detect_version_from_app(self) -> None:
        """Detect Spark version from application attempts."""
        app = {"attempts": [{"appSparkVersion": "3.5.2"}]}
        self.assertEqual(spark_diag.detect_spark_version_from_app(app), "3.5.2")
        self.assertEqual(spark_diag.select_endpoint_profile("3.5.2"), "spark-3.5")

    def test_parse_query_ids(self) -> None:
        """Parse common Spark UI query identifiers."""
        parsed = spark_diag.parse_web_url(
            "https://spark.example.com/spark-1/stages/stage/?stageId=7&attempt=2"
        )
        self.assertEqual(parsed.stage_id, 7)
        self.assertEqual(parsed.attempt_id, 2)
        self.assertEqual(spark_diag.first_present(None, 0, 1), 0)

    def test_summarize_jobs(self) -> None:
        """Summarize job counts and task failures."""
        jobs = [
            {"jobId": 1, "status": "RUNNING", "numTasks": 10, "numFailedTasks": 1},
            {"jobId": 2, "status": "SUCCEEDED", "numTasks": 5, "numFailedTasks": 0},
        ]
        summary = spark_diag.summarize_jobs(jobs)
        self.assertEqual(summary["total_jobs"], 2)
        self.assertEqual(summary["running_jobs"], 1)
        self.assertEqual(summary["failed_tasks"], 1)

    def test_summarize_jobs_brief_includes_duration_without_description(self) -> None:
        """Summarize job durations quickly without long SQL descriptions."""
        jobs = [
            {
                "jobId": 1,
                "status": "SUCCEEDED",
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:00:02.500GMT",
                "numTasks": 3,
                "numFailedTasks": 0,
                "description": "SELECT " + "x" * 1000,
            }
        ]
        summary = spark_diag.summarize_jobs(jobs, brief=True)
        self.assertEqual(summary["jobs"][0]["duration_ms"], 2500)
        self.assertEqual(summary["jobs"][0]["duration"], "2.50 s")
        self.assertNotIn("description", summary["jobs"][0])

    def test_summarize_stage_skew(self) -> None:
        """Detect stage skew from task metrics."""
        tasks = [
            {"taskId": 1, "duration": 1000, "taskMetrics": {"inputMetrics": {"bytesRead": 100}}},
            {"taskId": 2, "duration": 4000, "taskMetrics": {"inputMetrics": {"bytesRead": 500}}},
        ]
        summary = spark_diag.summarize_task_skew(tasks)
        self.assertEqual(summary["duration"]["max_task_id"], 2)
        self.assertEqual(summary["duration"]["skew_ratio"], 4.0)
        self.assertEqual(summary["input_bytes"]["max"], 500)

    def test_summarize_stages_uses_top_level_shuffle_fields(self) -> None:
        """Summarize shuffle bytes exposed directly by Spark stage list rows."""
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "shuffleReadBytes": 1024, "shuffleWriteBytes": 2048},
            {"stageId": 2, "attemptId": 0, "status": "SKIPPED", "shuffleReadBytes": 0, "shuffleWriteBytes": 0},
        ]
        summary = spark_diag.summarize_stages(stages)
        self.assertEqual(summary["total_shuffle_read_bytes"], 1024)
        self.assertEqual(summary["total_shuffle_write_bytes"], 2048)
        self.assertEqual(summary["total_shuffle_bytes"], 3072)
        self.assertEqual(summary["stages"][0]["shuffleRead"], "1.00 KiB")
        self.assertEqual(summary["stages"][0]["shuffleWrite"], "2.00 KiB")
        self.assertEqual(summary["stages"][0]["shuffleTotal"], "3.00 KiB")

    def test_summarize_stage_shuffle_compact(self) -> None:
        """Return a compact shuffle report for stage list pages."""
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "numTasks": 2, "shuffleReadBytes": 1024, "shuffleWriteBytes": 2048},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "numTasks": 1, "shuffleReadBytes": 4096, "shuffleWriteBytes": 0},
        ]
        summary = spark_diag.summarize_stage_shuffle(stages)
        self.assertEqual(summary["total_shuffle"], "7.00 KiB")
        self.assertEqual(summary["stages"][0]["stageId"], 1)
        self.assertEqual(summary["top_shuffle"][0]["stageId"], 2)
        self.assertNotIn("executorRunTime", summary["top_shuffle"][0])

    def test_summarize_stages_io_compact(self) -> None:
        """Summarize stage input/output/shuffle movement."""
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "inputBytes": 1024, "outputBytes": 512, "shuffleReadBytes": 0, "shuffleWriteBytes": 2048},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "inputBytes": 0, "outputBytes": 256, "shuffleReadBytes": 2048, "shuffleWriteBytes": 0},
        ]
        summary = spark_diag.summarize_stages_io(stages)
        self.assertEqual(summary["total_input"], "1.00 KiB")
        self.assertEqual(summary["total_output"], "768.00 B")
        self.assertEqual(summary["stages"][0]["stageId"], 1)

    def test_summarize_stage_executor_io_groups_task_metrics(self) -> None:
        """Summarize one stage's task IO by executor."""
        stage = {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "numTasks": 3, "inputBytes": 0, "shuffleReadBytes": 3072}
        tasks = [
            {"taskId": 1, "status": "SUCCESS", "executorId": "1", "duration": 1000, "taskMetrics": {"inputMetrics": {"bytesRead": 0}, "shuffleReadMetrics": {"remoteBytesRead": 1024}}},
            {"taskId": 2, "status": "FAILED", "executorId": "1", "duration": 2000, "taskMetrics": {"shuffleReadMetrics": {"remoteBytesRead": 0}}},
            {"taskId": 3, "status": "SUCCESS", "executorId": "2", "duration": 3000, "taskMetrics": {"shuffleReadMetrics": {"remoteBytesRead": 2048}}},
        ]
        summary = spark_diag.summarize_stage_executor_io(stage, tasks, top=10)
        self.assertEqual(summary["stage_id"], 2)
        self.assertEqual(summary["task_count"], 3)
        self.assertEqual(summary["failed_tasks"], 1)
        self.assertEqual(summary["executors"][0]["executor_id"], "2")
        self.assertEqual(summary["executors"][0]["shuffle_read_bytes"], 2048)
        self.assertEqual(summary["executors"][1]["failed_tasks"], 1)

    def test_build_input_distribution_diagnosis_explains_shuffle_only_executors(self) -> None:
        """Explain why executors can have zero input but nonzero shuffle read."""
        stages = [
            {"stageId": 0, "attemptId": 0, "status": "COMPLETE", "inputBytes": 2048, "shuffleWriteBytes": 4096, "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:01:00.000GMT", "executorRunTime": 120000, "numTasks": 2},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "inputBytes": 0, "shuffleReadBytes": 4096, "submissionTime": "2026-06-01T10:01:00.000GMT", "completionTime": "2026-06-01T10:05:00.000GMT", "executorRunTime": 480000, "numTasks": 4},
        ]
        executors = [
            {"id": "1", "totalInputBytes": 1024, "totalShuffleRead": 0, "totalShuffleWrite": 2048},
            {"id": "2", "totalInputBytes": 1024, "totalShuffleRead": 0, "totalShuffleWrite": 2048},
            {"id": "3", "totalInputBytes": 0, "totalShuffleRead": 4096, "totalShuffleWrite": 0},
        ]
        environment = {"sparkProperties": [["spark.executor.instances", "2"], ["spark.dynamicAllocation.enabled", "false"], ["spark.executor.cores", "1"]]}
        diagnosis = spark_diag.build_input_distribution_diagnosis(stages, executors, environment=environment, top=5)
        self.assertEqual(diagnosis["classification"], "shuffle_only_later_stage")
        self.assertEqual(diagnosis["executors_with_input"], 2)
        self.assertEqual(diagnosis["executors_with_shuffle_read_only"], 1)
        self.assertEqual(diagnosis["external_input_stages"][0]["stageId"], 0)
        self.assertIn("spark.executor.instances=2", diagnosis["evidence"])

    def test_summarize_stage_tasks_brief(self) -> None:
        """Summarize stage task skew without returning full task payloads."""
        tasks = [
            {"taskId": 1, "duration": 1000, "taskMetrics": {"inputMetrics": {"bytesRead": 100}}},
            {"taskId": 2, "duration": 4000, "taskMetrics": {"inputMetrics": {"bytesRead": 500}}},
        ]
        summary = spark_diag.summarize_stage_tasks(tasks, brief=True)
        self.assertEqual(summary["task_count"], 2)
        self.assertEqual(summary["top_slow_tasks"][0]["taskId"], 2)
        self.assertNotIn("tasks", summary)

    def test_stage_helpers_are_importable_from_lib_module(self) -> None:
        """Import stage helpers from the split library module."""
        stage_helpers = load_stage_lib_module()
        tasks = [
            {"taskId": 1, "duration": 1000, "taskMetrics": {"inputMetrics": {"bytesRead": 100}}},
            {"taskId": 2, "duration": 4000, "taskMetrics": {"inputMetrics": {"bytesRead": 500}}},
        ]
        summary = stage_helpers.summarize_stage_tasks(tasks, brief=True)
        self.assertEqual(summary["task_count"], 2)
        self.assertEqual(summary["top_slow_tasks"][0]["taskId"], 2)

    def test_spark_diag_preserves_split_stage_helper_access(self) -> None:
        """Keep stage helper access stable through spark_diag.py."""
        self.assertTrue(callable(spark_diag.summarize_stage_tasks))
        self.assertTrue(callable(spark_diag.summarize_task_duration_distribution))
        self.assertTrue(callable(spark_diag.compact_task_duration_row))

    def test_summarize_executors(self) -> None:
        """Summarize executor resource and task health."""
        executors = [
            {"id": "1", "isActive": True, "totalGCTime": 10, "failedTasks": 0, "memoryUsed": 50, "maxMemory": 100},
            {"id": "2", "isActive": False, "totalGCTime": 50, "failedTasks": 2, "memoryUsed": 90, "maxMemory": 100},
        ]
        summary = spark_diag.summarize_executors(executors)
        self.assertEqual(summary["executor_count"], 2)
        self.assertEqual(summary["inactive_executors"], 1)
        self.assertEqual(summary["top_gc"][0]["id"], "2")

    def test_summarize_executor_gc(self) -> None:
        """Summarize executor GC as a fast path."""
        executors = [
            {"id": "1", "isActive": True, "totalGCTime": 10, "totalDuration": 100, "totalTasks": 1, "failedTasks": 0},
            {"id": "2", "isActive": True, "totalGCTime": 50, "totalDuration": 100, "totalTasks": 2, "failedTasks": 0},
        ]
        summary = spark_diag.summarize_executor_gc(executors)
        self.assertEqual(summary["total_gc_ms"], 60)
        self.assertEqual(summary["gc_ratio"], "30.00%")
        self.assertEqual(summary["top_gc"][0]["id"], "2")

    def test_summarize_executor_health(self) -> None:
        """Summarize executor health with failed tasks and removed executors."""
        executors = [
            {"id": "1", "isActive": True, "failedTasks": 0, "totalGCTime": 10, "totalDuration": 100, "memoryUsed": 50, "maxMemory": 100},
            {"id": "2", "isActive": False, "failedTasks": 3, "totalGCTime": 60, "totalDuration": 100, "memoryUsed": 90, "maxMemory": 100, "removeTime": "2026-06-01T10:00:00.000GMT", "removeReason": "Executor lost"},
        ]
        summary = spark_diag.summarize_executor_health(executors, top=1)
        self.assertEqual(summary["executor_count"], 2)
        self.assertEqual(summary["failed_tasks"], 3)
        self.assertEqual(summary["removed_executors"], 1)
        self.assertEqual(summary["remove_reasons"]["Executor lost"], 1)
        self.assertEqual(summary["top_failed_executors"][0]["id"], "2")
        self.assertTrue(summary["risks"])

    def test_build_parallelism_diagnosis_flags_static_low_capacity(self) -> None:
        """Diagnose low effective parallelism from stage wall time and Spark config."""
        stages = [
            {
                "stageId": 2,
                "attemptId": 0,
                "status": "COMPLETE",
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:10:00.000GMT",
                "executorRunTime": 1200000,
                "numTasks": 100,
            }
        ]
        executors = [{"id": "1", "totalCores": 1}, {"id": "2", "totalCores": 1}]
        environment = {"sparkProperties": [["spark.executor.instances", "2"], ["spark.executor.cores", "1"], ["spark.dynamicAllocation.enabled", "false"]]}
        diagnosis = spark_diag.build_parallelism_diagnosis(stages, executors, environment=environment, top=1)
        self.assertEqual(diagnosis["classification"], "low_static_parallelism")
        self.assertEqual(diagnosis["configured_executor_instances"], 2)
        self.assertEqual(diagnosis["configured_total_cores"], 2)
        self.assertEqual(diagnosis["top_stages"][0]["estimated_parallelism"], 2.0)
        self.assertTrue(diagnosis["recommendations"])

    def test_summarize_executor_task_health(self) -> None:
        """Group task health by executor id."""
        executors = [{"id": "1", "hostPort": "h1:1"}, {"id": "2", "hostPort": "h2:1"}]
        tasks = [
            {"taskId": 1, "executorId": "1", "host": "h1", "status": "SUCCESS", "duration": 1000, "taskMetrics": {"jvmGcTime": 10}},
            {"taskId": 2, "executorId": "2", "host": "h2", "status": "FAILED", "duration": 9000, "errorMessage": "FetchFailed", "taskMetrics": {"jvmGcTime": 3000, "memoryBytesSpilled": 1024}},
            {"taskId": 3, "executorId": "2", "host": "h2", "status": "KILLED", "duration": 2000, "taskMetrics": {"jvmGcTime": 100}},
        ]
        summary = spark_diag.summarize_executor_task_health(tasks, executors, top=1)
        self.assertEqual(summary["task_count"], 3)
        self.assertEqual(summary["failed_tasks"], 1)
        self.assertEqual(summary["killed_tasks"], 1)
        self.assertEqual(summary["executors"][0]["executor_id"], "2")
        self.assertEqual(summary["executors"][0]["failed_tasks"], 1)
        self.assertEqual(summary["top_failed_tasks"][0]["taskId"], 2)

    def test_build_task_health_diagnosis(self) -> None:
        """Build task health diagnosis risks from executor and task evidence."""
        executor_health = {"failed_tasks": 2, "removed_executors": 1, "top_gc_executors": [{"id": "2"}]}
        task_health = {"failed_tasks": 1, "killed_tasks": 0, "executors": [{"executor_id": "2", "failed_tasks": 1, "gc_ratio": "30.00%"}]}
        diagnosis = spark_diag.build_task_health_diagnosis(executor_health, task_health)
        self.assertGreaterEqual(diagnosis["risk_count"], 3)
        self.assertTrue(diagnosis["recommendations"])

    def test_scan_log_text(self) -> None:
        """Scan log text for risky patterns."""
        summary = spark_diag.scan_log_text("INFO ok\nWARN slow\nERROR failed\nFetchFailed x\n")
        self.assertEqual(summary["matches"]["ERROR"]["count"], 1)
        self.assertEqual(summary["matches"]["FetchFailed"]["count"], 1)

    def test_redact_sensitive(self) -> None:
        """Redact sensitive keys in nested data."""
        redacted = spark_diag.redact_sensitive({"spark.token": "abc", "nested": {"password": "p"}})
        self.assertEqual(redacted["spark.token"], "<redacted>")
        self.assertEqual(redacted["nested"]["password"], "<redacted>")

    def test_environment_get_maps_page_label(self) -> None:
        """Resolve page labels such as Scala Version from environment payloads."""
        env = {"runtime": {"scalaVersion": "version 2.12.18"}, "sparkProperties": [["spark.app.name", "demo"]]}
        result = spark_diag.get_environment_value(env, "Scala Version")
        self.assertEqual(result["path"], "runtime.scalaVersion")
        self.assertEqual(result["value"], "version 2.12.18")

    def test_summarize_jobs_timeline(self) -> None:
        """Build a compact job timeline."""
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:00:02.000GMT"},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:03.000GMT", "completionTime": "2026-06-01T10:00:04.000GMT"},
        ]
        summary = spark_diag.summarize_jobs_timeline(jobs)
        self.assertEqual(summary["job_count"], 2)
        self.assertEqual(summary["makespan"], "4.00 s")
        self.assertEqual(summary["idle_gaps"][0]["gap_ms"], 1000)

    def test_summarize_job_idle_gaps_detects_periodic_idle(self) -> None:
        """Summarize inter-job idle gaps and detect regular submission periods."""
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:00:01.000GMT"},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:15:00.000GMT", "completionTime": "2026-06-01T10:15:01.000GMT"},
            {"jobId": 3, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:30:00.000GMT", "completionTime": "2026-06-01T10:30:01.000GMT"},
        ]
        summary = spark_diag.summarize_job_idle_gaps(jobs, top=1)
        self.assertEqual(summary["job_count"], 3)
        self.assertEqual(summary["total_job_wall_time_ms"], 3000)
        self.assertEqual(summary["total_idle_time_ms"], 1798000)
        self.assertEqual(summary["regular_gap_seconds"], 899)
        self.assertEqual(summary["top_idle_gaps"][0]["after_job_id"], 1)
        self.assertEqual(len(summary["top_idle_gaps"]), 1)

    def test_classify_long_app_runtime_flags_idle_engine_session(self) -> None:
        """Classify long Kyuubi engine apps as session idle dominant."""
        app = {
            "id": "app-1",
            "name": "kyuubi_USER_SPARK_SQL_hadoop_default_engine",
            "attempts": [{"duration": 8 * 60 * 60 * 1000}],
        }
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:00:02.000GMT"},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:15:00.000GMT", "completionTime": "2026-06-01T10:15:03.000GMT"},
        ]
        summary = spark_diag.classify_long_app_runtime(app, jobs, sql_rows=[{"status": "FAILED"}], top=1)
        self.assertEqual(summary["classification"], "session_idle_dominant")
        self.assertEqual(summary["app_kind"], "interactive_engine_session")
        self.assertEqual(summary["primary_bottleneck"], "metadata_ddl_failures")
        self.assertGreater(summary["idle_ratio_value"], 0.99)

    def test_parse_sql_failed_html_extracts_error_messages(self) -> None:
        """Extract SQL failure messages from Spark SQL HTML failed table."""
        html = """
        <table>
          <tr><th>ID</th><th>Description</th><th>Error Message</th></tr>
          <tr>
            <td>7</td>
            <td>ALTER TABLE bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt DROP IF EXISTS PARTITION (dt='20260601', hour='10')</td>
            <td>ALTER TABLE DROP PARTITION is not allowed on ads_dealer_device_used_pid_dp_stat_rt since its partition metadata is not stored in the Hive metastore. To import this information into the metastore, run msck repair table ads_dealer_device_used_pid_dp_stat_rt.</td>
          </tr>
        </table>
        """
        rows = spark_diag.parse_sql_failed_html(html)
        self.assertEqual(rows[0]["id"], 7)
        self.assertIn("partition metadata", rows[0]["error_message"])

    def test_summarize_sql_failures_groups_errors_and_tables(self) -> None:
        """Group failed SQL executions by table, type, and HTML error message."""
        sql_rows = [
            {"id": 7, "status": "FAILED", "description": "ALTER TABLE bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt DROP IF EXISTS PARTITION (dt='20260601', hour='10')"},
            {"id": 8, "status": "COMPLETED", "description": "SELECT 1"},
        ]
        html_errors = [
            {
                "id": 7,
                "description": sql_rows[0]["description"],
                "error_message": "ALTER TABLE DROP PARTITION is not allowed since its partition metadata is not stored in the Hive metastore. To import this information into the metastore, run msck repair table ads_dealer_device_used_pid_dp_stat_rt.",
            }
        ]
        summary = spark_diag.summarize_sql_failures(sql_rows, html_errors=html_errors, top=5)
        self.assertEqual(summary["failed_sql"], 1)
        self.assertEqual(summary["completed_sql"], 1)
        self.assertEqual(summary["failed_by_table"]["bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt"], 1)
        self.assertEqual(summary["failed_by_statement_type"]["ALTER TABLE DROP PARTITION"], 1)
        self.assertTrue(any("msck repair table" in item for item in summary["recommendations"]))

    def test_summarize_ddl_failures_groups_partitions_without_jobs(self) -> None:
        """Summarize batch DDL failures and partition distributions."""
        rows = [
            {"id": 1, "status": "FAILED", "description": "ALTER TABLE bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt DROP IF EXISTS PARTITION (dt='20260601', hour='10')"},
            {"id": 2, "status": "FAILED", "description": "ALTER TABLE bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt DROP IF EXISTS PARTITION (dt='20260601', hour='11')"},
            {"id": 3, "status": "COMPLETED", "description": "SELECT 1", "successJobIds": [9]},
        ]
        summary = spark_diag.summarize_ddl_failures(rows, html_errors=[], top=5)
        self.assertEqual(summary["ddl_total"], 2)
        self.assertEqual(summary["ddl_failed"], 2)
        self.assertEqual(summary["tables"]["bi_ads_real.ads_dealer_device_used_pid_dp_stat_rt"], 2)
        self.assertEqual(summary["partition_values"]["dt"]["20260601"], 2)
        self.assertTrue(summary["mostly_without_spark_jobs"])

    def test_summarize_duration_distribution(self) -> None:
        """Summarize duration percentiles and skew ratios."""
        rows = [{"duration": 1000}, {"duration": 2000}, {"duration": 3000}, {"duration": 10000}]
        summary = spark_diag.summarize_duration_distribution(rows, "duration")
        self.assertEqual(summary["count"], 4)
        self.assertEqual(summary["min_ms"], 1000)
        self.assertEqual(summary["max_ms"], 10000)
        self.assertEqual(summary["p50_ms"], 2500)
        self.assertEqual(summary["max_vs_median"], 4.0)
        self.assertEqual(spark_diag.classify_duration_skew(summary)["level"], "warning")

    def test_summarize_job_and_stage_durations(self) -> None:
        """Summarize job and stage duration distributions."""
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:00:02.000GMT"},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:03.000GMT", "completionTime": "2026-06-01T10:00:13.000GMT"},
        ]
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "COMPLETE", "executorRunTime": 1000, "inputBytes": 100},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "executorRunTime": 9000, "inputBytes": 200},
        ]
        job_summary = spark_diag.summarize_job_durations(jobs, top=1)
        stage_summary = spark_diag.summarize_stage_durations(stages, top=1)
        self.assertEqual(job_summary["distribution"]["max_ms"], 10000)
        self.assertEqual(job_summary["top_slow_jobs"][0]["jobId"], 2)
        self.assertEqual(stage_summary["distribution"]["max_ms"], 9000)
        self.assertEqual(stage_summary["top_slow_stages"][0]["stageId"], 2)

    def test_summarize_stage_wall_times_separates_executor_runtime(self) -> None:
        """Summarize stage wall time separately from cumulative executor runtime."""
        stages = [
            {
                "stageId": 2,
                "attemptId": 0,
                "status": "COMPLETE",
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:02:00.000GMT",
                "executorRunTime": 240000,
                "numTasks": 12,
            }
        ]
        self.assertEqual(spark_diag.stage_wall_time_ms(stages[0]), 120000)
        summary = spark_diag.summarize_stage_wall_times(stages, top=1)
        row = summary["top_wall_time_stages"][0]
        self.assertEqual(summary["distribution"]["max_ms"], 120000)
        self.assertEqual(row["wall_duration_ms"], 120000)
        self.assertEqual(row["executorRunTime_ms"], 240000)
        self.assertEqual(row["estimated_parallelism"], 2.0)
        self.assertEqual(row["avg_task_duration_ms"], 20000)

    def test_summarize_executor_churn_classifies_framework_deletes(self) -> None:
        """Classify executor churn when removed executors overlap one stage."""
        stages = [
            {
                "stageId": 2,
                "attemptId": 0,
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:05:00.000GMT",
            }
        ]
        executors = [
            {"id": "1", "isActive": True, "addTime": "2026-06-01T09:59:00.000GMT", "hostPort": "h1:1"},
            {
                "id": "2",
                "isActive": False,
                "addTime": "2026-06-01T09:59:00.000GMT",
                "removeTime": "2026-06-01T10:02:00.000GMT",
                "removeReason": "The executor with id 2 was deleted by a user or the framework.",
                "totalTasks": 4,
                "failedTasks": 1,
                "hostPort": "h2:1",
            },
        ]
        summary = spark_diag.summarize_executor_churn(executors, stages, stage_id=2, top=10)
        self.assertEqual(summary["removed_executors"], 1)
        self.assertEqual(summary["removed_during_stage"], 1)
        self.assertEqual(summary["classification"], "framework_deleted_executors")
        self.assertEqual(summary["remove_reasons"]["The executor with id 2 was deleted by a user or the framework."], 1)
        self.assertEqual(summary["executors"][0]["stage_overlap_ms"], 120000)
        self.assertEqual(summary["executors"][0]["stage_relation"], "removed_during_stage")
        self.assertEqual(summary["stage"]["stageId"], 2)

    def test_build_executor_loss_diagnosis_correlates_removed_executors(self) -> None:
        """Diagnose executor lost tasks that recovered after executor removal."""
        stages = [
            {
                "stageId": 2,
                "attemptId": 0,
                "status": "COMPLETE",
                "numFailedTasks": 1,
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:05:00.000GMT",
            }
        ]
        executors = [
            {
                "id": "2",
                "isActive": False,
                "addTime": "2026-06-01T09:59:00.000GMT",
                "removeTime": "2026-06-01T10:02:00.000GMT",
                "removeReason": "The executor with id 2 was deleted by a user or the framework.",
                "hostPort": "h2:1",
            }
        ]
        tasks = [
            {"taskId": 7, "status": "FAILED", "executorId": "2", "host": "h2", "errorMessage": "ExecutorLostFailure executor 2 exited"},
            {"taskId": 8, "status": "SUCCESS", "executorId": "3", "host": "h3", "duration": 1000},
        ]
        diagnosis = spark_diag.build_executor_loss_diagnosis(stages, executors, tasks, stage_id=2, jobs=[{"jobId": 1, "status": "SUCCEEDED", "stageIds": [2]}])
        self.assertEqual(diagnosis["failed_task_count"], 1)
        self.assertEqual(diagnosis["error_reasons"]["executor_lost"], 1)
        self.assertEqual(diagnosis["failed_on_removed_executor"], 1)
        self.assertTrue(diagnosis["recovered"])
        self.assertEqual(diagnosis["classification"], "executor_lost_recovered")

    def test_summarize_task_duration_distribution(self) -> None:
        """Summarize task duration skew with input and GC evidence."""
        tasks = [
            {
                "taskId": 1,
                "duration": 1000,
                "executorId": "1",
                "host": "h1",
                "taskMetrics": {
                    "inputMetrics": {"bytesRead": 1000},
                    "shuffleReadMetrics": {"remoteBytesRead": 0},
                    "shuffleWriteMetrics": {"bytesWritten": 100},
                    "jvmGcTime": 10,
                    "memoryBytesSpilled": 0,
                    "diskBytesSpilled": 0,
                },
            },
            {
                "taskId": 2,
                "duration": 9000,
                "executorId": "2",
                "host": "h2",
                "taskMetrics": {
                    "inputMetrics": {"bytesRead": 9000},
                    "shuffleReadMetrics": {"remoteBytesRead": 0},
                    "shuffleWriteMetrics": {"bytesWritten": 900},
                    "jvmGcTime": 3000,
                    "memoryBytesSpilled": 1024,
                    "diskBytesSpilled": 2048,
                },
            },
        ]
        summary = spark_diag.summarize_task_duration_distribution(tasks, top=1)
        self.assertEqual(summary["distribution"]["max_ms"], 9000)
        self.assertEqual(summary["skew"]["level"], "critical")
        self.assertEqual(summary["top_slow_tasks"][0]["taskId"], 2)
        self.assertEqual(summary["top_slow_tasks"][0]["gc_ratio"], "33.33%")
        self.assertTrue(summary["recommendations"])

    def test_summarize_task_duration_distribution_ignores_failed_attempts_by_default(self) -> None:
        """Exclude failed attempts from skew math unless explicitly included."""
        tasks = [
            {"taskId": 1, "status": "FAILED", "duration": 1000},
            {"taskId": 2, "status": "SUCCESS", "duration": 9000},
            {"taskId": 3, "status": "SUCCESS", "duration": 10000},
        ]
        summary = spark_diag.summarize_task_duration_distribution(tasks)
        self.assertEqual(summary["task_count"], 3)
        self.assertEqual(summary["analyzed_task_count"], 2)
        self.assertEqual(summary["excluded_failed_attempts"], 1)
        self.assertEqual(summary["distribution"]["min_ms"], 9000)
        self.assertEqual(summary["skew"]["level"], "none")
        with_failed = spark_diag.summarize_task_duration_distribution(tasks, include_failed_attempts=True)
        self.assertEqual(with_failed["analyzed_task_count"], 3)
        self.assertEqual(with_failed["distribution"]["min_ms"], 1000)

    def test_group_stage_attempts_sorts_attempts(self) -> None:
        """Group stage attempts by stage id in attempt order."""
        stages = [
            {"stageId": 1, "attemptId": 1, "status": "COMPLETE"},
            {"stageId": 1, "attemptId": 0, "status": "FAILED"},
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE"},
        ]
        grouped = spark_diag.group_stage_attempts(stages)
        self.assertEqual([stage["attemptId"] for stage in grouped[1]], [0, 1])
        self.assertEqual(len(grouped[2]), 1)

    def test_summarize_stage_retries_classifies_recovered_and_failed(self) -> None:
        """Classify retry attempts as recovered or finally failed."""
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "FAILED", "numFailedTasks": 2},
            {"stageId": 1, "attemptId": 1, "status": "COMPLETE", "numFailedTasks": 0},
            {"stageId": 2, "attemptId": 0, "status": "FAILED", "numFailedTasks": 1},
            {"stageId": 2, "attemptId": 1, "status": "FAILED", "numFailedTasks": 1},
        ]
        jobs = [{"jobId": 9, "status": "FAILED", "stageIds": [2]}]
        summary = spark_diag.summarize_stage_retries(stages, jobs, top=10)
        self.assertEqual(summary["retry_stage_count"], 2)
        self.assertEqual(summary["recovered_retries"], 1)
        self.assertEqual(summary["final_failed_retries"], 1)
        self.assertEqual(summary["stages"][0]["classification"], "recovered_stage_attempt")
        self.assertEqual(summary["stages"][1]["classification"], "fatal_stage_failure")
        self.assertEqual(summary["stages"][1]["job_ids"], [9])

    def test_summarize_stage_retries_handles_stage_zero(self) -> None:
        """Classify retry attempts correctly when the stage id is zero."""
        stages = [
            {"stageId": 0, "attemptId": 0, "status": "COMPLETE", "numFailedTasks": 0},
            {"stageId": 0, "attemptId": 1, "status": "COMPLETE", "numFailedTasks": 0},
        ]
        summary = spark_diag.summarize_stage_retries(stages, [], top=10)
        self.assertEqual(summary["retry_stage_count"], 1)
        self.assertEqual(summary["recovered_retries"], 1)
        self.assertEqual(summary["stages"][0]["classification"], "stage_retry_without_failure")

    def test_summarize_task_failures_classifies_reasons_and_limits_samples(self) -> None:
        """Summarize failed tasks by status, reason, executor, and host."""
        tasks = [
            {"taskId": 1, "status": "FAILED", "executorId": "2", "host": "h2", "duration": 1000, "errorMessage": "FetchFailed(BlockManagerId(2))"},
            {"taskId": 2, "status": "FAILED", "executorId": "2", "host": "h2", "duration": 2000, "errorMessage": "java.lang.OutOfMemoryError: Java heap space"},
            {"taskId": 3, "status": "KILLED", "executorId": "3", "host": "h3", "duration": 300},
            {"taskId": 4, "status": "SUCCESS", "executorId": "4", "host": "h4", "duration": 100},
        ]
        stage = {"stageId": 7, "attemptId": 0, "status": "COMPLETE", "numFailedTasks": 2}
        summary = spark_diag.summarize_task_failures(tasks, stage, top=1)
        self.assertEqual(summary["failed_tasks"], 2)
        self.assertEqual(summary["killed_tasks"], 1)
        self.assertEqual(summary["successful_tasks"], 1)
        self.assertEqual(summary["classification"], "recovered_task_failure")
        self.assertEqual(summary["error_reasons"]["fetch_failed"], 1)
        self.assertEqual(summary["error_reasons"]["out_of_memory"], 1)
        self.assertEqual(summary["executors"]["2"], 2)
        self.assertEqual(len(summary["failed_task_samples"]), 1)

    def test_classify_stage_failure_distinguishes_recovered_and_fatal(self) -> None:
        """Classify stage failure impact from attempts and related jobs."""
        stages = [
            {"stageId": 1, "attemptId": 0, "status": "FAILED", "numFailedTasks": 1},
            {"stageId": 1, "attemptId": 1, "status": "COMPLETE", "numFailedTasks": 0},
            {"stageId": 2, "attemptId": 0, "status": "FAILED", "numFailedTasks": 1},
            {"stageId": 3, "attemptId": 0, "status": "KILLED", "numFailedTasks": 0},
        ]
        grouped = spark_diag.group_stage_attempts(stages)
        jobs = [{"jobId": 10, "status": "FAILED", "stageIds": [2]}]
        self.assertEqual(spark_diag.classify_stage_failure(stages[0], grouped, jobs)["classification"], "recovered_stage_attempt")
        self.assertEqual(spark_diag.classify_stage_failure(stages[2], grouped, jobs)["classification"], "fatal_stage_failure")
        self.assertEqual(spark_diag.classify_stage_failure(stages[3], grouped, jobs)["classification"], "non_fatal_killed")

    def test_command_stages_retries_with_mock_transport(self) -> None:
        """Run stages retries through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked stages retries command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock Spark stage retry responses."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(
                        200,
                        json=[
                            {"stageId": 1, "attemptId": 0, "status": "FAILED", "numFailedTasks": 1},
                            {"stageId": 1, "attemptId": 1, "status": "COMPLETE", "numFailedTasks": 0},
                        ],
                    )
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["stages", "retries", "--url", "https://spark.example.com/history/app-1/stages/", "--top", "5"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_stages(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["retry_stage_count"], 1)
        self.assertEqual(result["recovered_retries"], 1)

    def test_command_stages_failures_fetches_beyond_default_task_page(self) -> None:
        """Fetch enough task rows to find failed tasks beyond Spark's default page."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked stages failures command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock Spark stage failure responses."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 10, "attemptId": 0, "status": "COMPLETE", "numTasks": 200, "numCompleteTasks": 200, "numFailedTasks": 1}])
                if request.url.path.endswith("/api/v1/applications/app-1/jobs"):
                    return httpx.Response(200, json=[{"jobId": 4, "status": "SUCCEEDED", "stageIds": [10]}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/10/0/taskList"):
                    length = request.url.params.get("length")
                    if length is None:
                        return httpx.Response(200, json=[{"taskId": task_id, "status": "SUCCESS", "duration": 10} for task_id in range(20)])
                    return httpx.Response(
                        200,
                        json=[
                            *[{"taskId": task_id, "status": "SUCCESS", "duration": 10} for task_id in range(20)],
                            {"taskId": 508, "index": 184, "status": "FAILED", "executorId": "5", "host": "h5", "duration": 8499, "errorMessage": "ExecutorLostFailure executor 5"},
                        ],
                    )
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["stages", "failures", "--url", "https://spark.example.com/history/app-1/stages/", "--top", "5"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_stages(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["task_failures"][0]["failed_tasks"], 1)
        self.assertEqual(result["task_failures"][0]["failed_task_samples"][0]["taskId"], 508)

    def test_build_event_timeline_filters_sql_related_jobs(self) -> None:
        """Build an event timeline for one SQL execution and executor churn."""
        sql_rows = [
            {
                "id": 9,
                "status": "COMPLETED",
                "description": "SELECT * FROM t",
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:00:05.000GMT",
                "duration": 5000,
                "succeededJobIds": [1],
            }
        ]
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:01.000GMT", "completionTime": "2026-06-01T10:00:04.000GMT", "stageIds": [2]},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:10:01.000GMT", "completionTime": "2026-06-01T10:10:04.000GMT", "stageIds": [3]},
        ]
        stages = [
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "submissionTime": "2026-06-01T10:00:02.000GMT", "completionTime": "2026-06-01T10:00:03.000GMT"},
            {"stageId": 3, "attemptId": 0, "status": "COMPLETE", "submissionTime": "2026-06-01T10:10:02.000GMT", "completionTime": "2026-06-01T10:10:03.000GMT"},
        ]
        executors = [
            {"id": "1", "hostPort": "host:1", "addTime": "2026-06-01T09:59:59.000GMT", "removeTime": "2026-06-01T10:00:06.000GMT", "removeReason": "idle"},
            {"id": "2", "hostPort": "host:2", "addTime": "2026-06-01T10:00:02.000GMT"},
        ]
        summary = spark_diag.build_event_timeline(sql_rows, jobs, stages, executors, sql_id=9, limit=20)
        self.assertEqual(summary["sql_executions"][0]["sql_id"], 9)
        self.assertEqual(summary["sql_executions"][0]["duration"], "5.00 s")
        self.assertEqual(summary["executor_events"]["removed"], 1)
        self.assertIn("executor_removed", [event["event"] for event in summary["events"]])
        self.assertIn(1, [event.get("job_id") for event in summary["events"]])
        self.assertNotIn(2, [event.get("job_id") for event in summary["events"]])

    def test_build_event_timeline_filters_by_sql_time_without_job_ids(self) -> None:
        """Filter jobs by SQL time range when SQL job ids are unavailable."""
        sql_rows = [
            {
                "id": 9,
                "status": "COMPLETED",
                "submissionTime": "2026-06-01T10:00:00.000GMT",
                "completionTime": "2026-06-01T10:00:05.000GMT",
                "duration": 5000,
            }
        ]
        jobs = [
            {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:01.000GMT", "completionTime": "2026-06-01T10:00:04.000GMT", "stageIds": [2]},
            {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:10:01.000GMT", "completionTime": "2026-06-01T10:10:04.000GMT", "stageIds": [3]},
        ]
        stages = [
            {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "submissionTime": "2026-06-01T10:00:02.000GMT", "completionTime": "2026-06-01T10:00:03.000GMT"},
            {"stageId": 3, "attemptId": 0, "status": "COMPLETE", "submissionTime": "2026-06-01T10:10:02.000GMT", "completionTime": "2026-06-01T10:10:03.000GMT"},
        ]
        summary = spark_diag.build_event_timeline(sql_rows, jobs, stages, [], sql_id=9, limit=20)
        self.assertIn(1, [event.get("job_id") for event in summary["events"]])
        self.assertNotIn(2, [event.get("job_id") for event in summary["events"]])
        self.assertIn(2, [event.get("stage_id") for event in summary["events"]])
        self.assertNotIn(3, [event.get("stage_id") for event in summary["events"]])

    def test_summarize_sql_operators(self) -> None:
        """Extract important operators from SQL plans."""
        rows = [{"id": 1, "status": "COMPLETED", "duration": 1000, "planDescription": "BatchScan t\nExchange hashpartitioning\nBroadcastHashJoin\nWindow\nSort"}]
        summary = spark_diag.summarize_sql_operators(rows)
        self.assertEqual(summary["sql"][0]["operators"]["Exchange"], 1)
        self.assertEqual(summary["sql"][0]["operators"]["BroadcastHashJoin"], 1)

    def test_analyze_sql_performance(self) -> None:
        """Produce high-level SQL speed diagnostics from collected summaries."""
        sql_rows = [{"id": 1, "status": "COMPLETED", "duration": 1000, "planDescription": "BatchScan t\nExchange\nBroadcastHashJoin\nWindow"}]
        stages = [{"stageId": 1, "status": "COMPLETE", "inputBytes": 10 * 1024**3, "shuffleReadBytes": 1024, "shuffleWriteBytes": 1024}]
        executors = [{"id": "1", "isActive": True, "totalGCTime": 10, "totalDuration": 10000}]
        result = spark_diag.analyze_sql_performance(sql_rows, [], stages, executors)
        self.assertEqual(result["primary_bottleneck"], "scan")
        self.assertTrue(result["recommendations"])

    def test_build_speed_diagnosis(self) -> None:
        """Build a one-shot speed diagnosis report."""
        report = spark_diag.build_speed_diagnosis(
            [
                {"jobId": 1, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:00:01.000GMT"},
                {"jobId": 2, "status": "SUCCEEDED", "submissionTime": "2026-06-01T10:00:02.000GMT", "completionTime": "2026-06-01T10:00:03.000GMT"},
            ],
            [{"stageId": 1, "status": "COMPLETE", "inputBytes": 1024, "shuffleReadBytes": 0, "shuffleWriteBytes": 0}],
            [{"id": "1", "totalGCTime": 1, "totalDuration": 100}],
            [{"id": 1, "planDescription": "BatchScan t"}],
            limit=1,
        )
        self.assertIn("jobs", report)
        self.assertIn("sql_analysis", report)
        self.assertNotIn("stages", report["stage_io"])
        self.assertNotIn("stage_io", report["sql_analysis"])
        self.assertEqual(len(report["jobs"]["jobs"]), 1)
        self.assertEqual(len(report["timeline"]["jobs"]), 1)
        self.assertIn("job_durations", report)
        self.assertIn("stage_durations", report)

    def test_command_duration_tasks_fetches_beyond_default_task_page(self) -> None:
        """Fetch a complete taskList for duration tasks instead of Spark's first page."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked duration tasks command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stage metadata and taskList pages."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 10, "attemptId": 0, "status": "COMPLETE", "numTasks": 200, "numCompleteTasks": 200, "numFailedTasks": 1}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/10/0/taskList"):
                    length = request.url.params.get("length")
                    if length is None:
                        return httpx.Response(200, json=[{"taskId": task_id, "status": "SUCCESS", "duration": 10} for task_id in range(20)])
                    return httpx.Response(200, json=[*[{"taskId": task_id, "status": "SUCCESS", "duration": 10} for task_id in range(20)], {"taskId": 508, "status": "FAILED", "duration": 9000}])
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["duration", "tasks", "--url", "https://spark.example.com/history/app-1/stages/", "--stage-id", "10"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_duration(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["task_count"], 21)
        self.assertEqual(result["analyzed_task_count"], 20)
        self.assertEqual(result["excluded_failed_attempts"], 1)

    def test_command_executors_failed_tasks_fetches_beyond_default_task_page(self) -> None:
        """Fetch complete taskList rows for executor failed task correlation."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked executors failed-tasks command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return executor, stage, and taskList payloads."""
                if request.url.path.endswith("/api/v1/applications/app-1/executors"):
                    return httpx.Response(200, json=[{"id": "5", "isActive": False, "failedTasks": 1, "hostPort": "h5:1"}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 10, "attemptId": 0, "status": "COMPLETE", "numTasks": 200, "numCompleteTasks": 200, "numFailedTasks": 1, "executorRunTime": 1000}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/10/0/taskList"):
                    length = request.url.params.get("length")
                    if length is None:
                        return httpx.Response(200, json=[{"taskId": task_id, "status": "SUCCESS", "executorId": "1", "duration": 10} for task_id in range(20)])
                    return httpx.Response(200, json=[*[{"taskId": task_id, "status": "SUCCESS", "executorId": "1", "duration": 10} for task_id in range(20)], {"taskId": 508, "status": "FAILED", "executorId": "5", "host": "h5", "duration": 9000, "errorMessage": "ExecutorLostFailure"}])
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["executors", "failed-tasks", "--url", "https://spark.example.com/history/app-1/executors/", "--limit-stages", "1"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_executors(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["task_health"]["failed_tasks"], 1)
        self.assertEqual(result["task_health"]["top_failed_tasks"][0]["taskId"], 508)

    def test_command_stages_io_by_executor_with_mock_transport(self) -> None:
        """Run stages io --by-executor through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked stages io by-executor command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stage and task IO payloads."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 2, "attemptId": 0, "status": "COMPLETE", "numTasks": 2, "inputBytes": 0, "shuffleReadBytes": 3072}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/2/0/taskList"):
                    return httpx.Response(
                        200,
                        json=[
                            {"taskId": 1, "status": "SUCCESS", "executorId": "1", "duration": 1000, "taskMetrics": {"shuffleReadMetrics": {"remoteBytesRead": 1024}}},
                            {"taskId": 2, "status": "SUCCESS", "executorId": "2", "duration": 2000, "taskMetrics": {"shuffleReadMetrics": {"remoteBytesRead": 2048}}},
                        ],
                    )
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["stages", "io", "--url", "https://spark.example.com/history/app-1/stages/", "--stage-id", "2", "--by-executor"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_stages(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["stage_id"], 2)
        self.assertEqual(result["executors"][0]["executor_id"], "2")
        self.assertEqual(result["executors"][0]["shuffle_read_bytes"], 2048)

    def test_command_executors_gc_all_uses_all_executors(self) -> None:
        """Use allexecutors for executors gc --all."""
        seen_paths: list[str] = []

        async def run_case() -> dict[str, Any]:
            """Run a mocked executors gc --all command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return executor GC payloads and record endpoint usage."""
                seen_paths.append(request.url.path)
                if request.url.path.endswith("/api/v1/applications/app-1/allexecutors"):
                    return httpx.Response(200, json=[{"id": "1", "isActive": False, "totalGCTime": 50, "totalDuration": 100}])
                if request.url.path.endswith("/api/v1/applications/app-1/executors"):
                    return httpx.Response(200, json=[])
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["executors", "gc", "--url", "https://spark.example.com/history/app-1/executors/", "--all"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_executors(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertIn("/api/v1/applications/app-1/allexecutors", seen_paths)
        self.assertEqual(result["executor_count"], 1)
        self.assertEqual(result["total_gc_ms"], 50)

    def test_command_diagnose_input_distribution_with_mock_transport(self) -> None:
        """Run input distribution diagnosis through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked input-distribution diagnosis."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stages, executors, and environment payloads."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[
                        {"stageId": 0, "attemptId": 0, "status": "COMPLETE", "inputBytes": 2048, "shuffleWriteBytes": 4096},
                        {"stageId": 2, "attemptId": 0, "status": "COMPLETE", "inputBytes": 0, "shuffleReadBytes": 4096},
                    ])
                if request.url.path.endswith("/api/v1/applications/app-1/allexecutors"):
                    return httpx.Response(200, json=[
                        {"id": "1", "totalInputBytes": 1024, "totalShuffleRead": 0, "totalShuffleWrite": 2048},
                        {"id": "2", "totalInputBytes": 1024, "totalShuffleRead": 0, "totalShuffleWrite": 2048},
                        {"id": "3", "totalInputBytes": 0, "totalShuffleRead": 4096, "totalShuffleWrite": 0},
                    ])
                if request.url.path.endswith("/api/v1/applications/app-1/environment"):
                    return httpx.Response(200, json={"sparkProperties": [["spark.executor.instances", "2"], ["spark.dynamicAllocation.enabled", "false"]]})
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["diagnose", "input-distribution", "--url", "https://spark.example.com/history/app-1/executors/"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_diagnose(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["diagnosis"]["classification"], "shuffle_only_later_stage")
        self.assertEqual(result["diagnosis"]["executors_with_input"], 2)

    def test_command_diagnose_parallelism_with_mock_transport(self) -> None:
        """Run parallelism diagnosis through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked parallelism diagnosis."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stages, executors, and environment payloads."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 2, "attemptId": 0, "status": "COMPLETE", "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:10:00.000GMT", "executorRunTime": 1200000, "numTasks": 100}])
                if request.url.path.endswith("/api/v1/applications/app-1/allexecutors"):
                    return httpx.Response(200, json=[{"id": "1", "totalCores": 1}, {"id": "2", "totalCores": 1}])
                if request.url.path.endswith("/api/v1/applications/app-1/environment"):
                    return httpx.Response(200, json={"sparkProperties": [["spark.executor.instances", "2"], ["spark.executor.cores", "1"], ["spark.dynamicAllocation.enabled", "false"]]})
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["diagnose", "parallelism", "--url", "https://spark.example.com/history/app-1/stages/", "--top", "1"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_diagnose(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["diagnosis"]["classification"], "low_static_parallelism")
        self.assertEqual(result["diagnosis"]["configured_total_cores"], 2)

    def test_command_stages_wall_time_with_mock_transport(self) -> None:
        """Run stages wall-time through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked stages wall-time command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stage rows with wall timestamps."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(
                        200,
                        json=[
                            {
                                "stageId": 2,
                                "attemptId": 0,
                                "status": "COMPLETE",
                                "submissionTime": "2026-06-01T10:00:00.000GMT",
                                "completionTime": "2026-06-01T10:02:00.000GMT",
                                "executorRunTime": 240000,
                                "numTasks": 12,
                            }
                        ],
                    )
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["stages", "wall-time", "--url", "https://spark.example.com/history/app-1/stages/", "--top", "5"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_stages(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["app_id"], "app-1")
        self.assertEqual(result["top_wall_time_stages"][0]["wall_duration_ms"], 120000)

    def test_command_diagnose_executor_loss_with_mock_transport(self) -> None:
        """Run executor-loss diagnosis through the REST command path."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked executor-loss diagnosis command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return stage, executor, job, and taskList payloads."""
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 2, "attemptId": 0, "status": "COMPLETE", "numTasks": 10, "numCompleteTasks": 10, "numFailedTasks": 1, "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:05:00.000GMT"}])
                if request.url.path.endswith("/api/v1/applications/app-1/jobs"):
                    return httpx.Response(200, json=[{"jobId": 1, "status": "SUCCEEDED", "stageIds": [2]}])
                if request.url.path.endswith("/api/v1/applications/app-1/allexecutors"):
                    return httpx.Response(200, json=[{"id": "2", "isActive": False, "removeTime": "2026-06-01T10:02:00.000GMT", "removeReason": "The executor with id 2 was deleted by a user or the framework.", "hostPort": "h2:1"}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/2/0/taskList"):
                    return httpx.Response(200, json=[{"taskId": 7, "status": "FAILED", "executorId": "2", "host": "h2", "errorMessage": "ExecutorLostFailure"}])
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["diagnose", "executor-loss", "--url", "https://spark.example.com/history/app-1/executors/", "--stage-id", "2"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_diagnose(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["diagnosis"]["classification"], "executor_lost_recovered")
        self.assertEqual(result["diagnosis"]["failed_on_removed_executor"], 1)

    def test_command_diagnose_speed_uses_all_executors_for_churn(self) -> None:
        """Use all executors when speed diagnosis summarizes executor churn."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked speed diagnosis command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return speed diagnosis payloads with removed executor history."""
                if request.url.path.endswith("/api/v1/applications/app-1"):
                    return httpx.Response(200, json={"id": "app-1", "attempts": [{"duration": 60000}]})
                if request.url.path.endswith("/api/v1/applications/app-1/jobs"):
                    return httpx.Response(200, json=[{"jobId": 1, "status": "SUCCEEDED", "stageIds": [2], "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:01:00.000GMT"}])
                if request.url.path.endswith("/api/v1/applications/app-1/stages"):
                    return httpx.Response(200, json=[{"stageId": 2, "attemptId": 0, "status": "COMPLETE", "numTasks": 10, "numCompleteTasks": 10, "numFailedTasks": 1, "submissionTime": "2026-06-01T10:00:00.000GMT", "completionTime": "2026-06-01T10:01:00.000GMT", "executorRunTime": 120000}])
                if request.url.path.endswith("/api/v1/applications/app-1/executors"):
                    return httpx.Response(200, json=[{"id": "9", "isActive": True, "totalDuration": 60000, "totalGCTime": 0}])
                if request.url.path.endswith("/api/v1/applications/app-1/allexecutors"):
                    return httpx.Response(200, json=[{"id": "2", "isActive": False, "removeTime": "2026-06-01T10:00:30.000GMT", "removeReason": "The executor with id 2 was deleted by a user or the framework.", "hostPort": "h2:1"}])
                if request.url.path.endswith("/api/v1/applications/app-1/sql"):
                    return httpx.Response(200, json=[])
                if request.url.path.endswith("/api/v1/applications/app-1/stages/2/0/taskList"):
                    return httpx.Response(200, json=[{"taskId": 7, "status": "FAILED", "executorId": "2", "host": "h2", "errorMessage": "ExecutorLostFailure"}])
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["diagnose", "speed", "--url", "https://spark.example.com/history/app-1/executors/", "--limit", "3"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_diagnose(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["primary_bottleneck"], "executor_churn")
        self.assertEqual(result["executor_capacity"]["executor_churn_count_during_top_stage"], 1)

    def test_filter_applications(self) -> None:
        """Filter History Server application lists."""
        apps = [
            {"id": "a1", "name": "etl-demo", "attempts": [{"completed": True}]},
            {"id": "a2", "name": "stream-demo", "attempts": [{"completed": False}]},
        ]
        args = spark_diag.build_parser().parse_args(
            ["applications", "--url", "https://spark.example.com/", "--app-name", "etl", "--completed", "true"]
        )
        self.assertEqual([item["id"] for item in spark_diag.filter_applications(apps, args)], ["a1"])

    def test_build_health_report_flags_risks(self) -> None:
        """Build health report risks from failed components."""
        report = spark_diag.build_health_report(
            {
                "app": {"id": "app-1"},
                "jobs": {"failed_jobs": 1, "failed_tasks": 2},
                "stages": {"failed_stages": 0, "failed_tasks": 0},
                "executors": {"inactive_executors": 1, "failed_tasks": 0},
                "sql": {"failed_sql": 1},
            }
        )
        self.assertEqual(report["risk_count"], 3)
        self.assertEqual(report["risks"][0]["area"], "jobs")

    def test_parser_supports_plan_aliases(self) -> None:
        """Parse command aliases described in the skill plan."""
        parser = spark_diag.build_parser()
        self.assertEqual(parser.parse_args(["history", "applications", "--url", "https://spark.example.com/"]).command, "history")
        self.assertTrue(parser.parse_args(["jobs", "--url", "https://spark.example.com/", "--brief"]).brief)
        self.assertEqual(parser.parse_args(["jobs", "timeline", "--url", "https://spark.example.com/"]).subcommand, "timeline")
        self.assertEqual(parser.parse_args(["jobs", "idle-gaps", "--url", "https://spark.example.com/", "--top", "5"]).subcommand, "idle-gaps")
        self.assertEqual(parser.parse_args(["job", "show", "--url", "https://spark.example.com/", "--job-id", "1"]).subcommand, "show")
        self.assertEqual(parser.parse_args(["stages", "io", "--url", "https://spark.example.com/"]).subcommand, "io")
        self.assertEqual(parser.parse_args(["stages", "shuffle", "--url", "https://spark.example.com/"]).subcommand, "shuffle")
        self.assertEqual(parser.parse_args(["stages", "wall-time", "--url", "https://spark.example.com/"]).subcommand, "wall-time")
        self.assertTrue(parser.parse_args(["stage", "tasks", "--url", "https://spark.example.com/", "--stage-id", "1", "--brief"]).brief)
        self.assertEqual(parser.parse_args(["executors", "gc", "--url", "https://spark.example.com/"]).subcommand, "gc")
        self.assertEqual(parser.parse_args(["executors", "churn", "--url", "https://spark.example.com/", "--stage-id", "2"]).subcommand, "churn")
        self.assertEqual(parser.parse_args(["executor", "top", "--url", "https://spark.example.com/"]).subcommand, "top")
        self.assertEqual(parser.parse_args(["environment", "get", "--url", "https://spark.example.com/", "--key", "Scala Version"]).subcommand, "get")
        self.assertEqual(parser.parse_args(["sql", "analyze", "--url", "https://spark.example.com/"]).subcommand, "analyze")
        self.assertTrue(parser.parse_args(["sql", "plan", "--url", "https://spark.example.com/", "--operators"]).operators)
        self.assertEqual(parser.parse_args(["sql", "failures", "--url", "https://spark.example.com/", "--limit", "1000"]).subcommand, "failures")
        self.assertEqual(parser.parse_args(["sql", "ddl-summary", "--url", "https://spark.example.com/", "--limit", "1000"]).subcommand, "ddl-summary")
        self.assertEqual(parser.parse_args(["diagnose", "speed", "--url", "https://spark.example.com/"]).subcommand, "speed")
        self.assertEqual(parser.parse_args(["diagnose", "long-app", "--url", "https://spark.example.com/", "--top", "5"]).subcommand, "long-app")
        self.assertEqual(parser.parse_args(["diagnose", "executor-loss", "--url", "https://spark.example.com/", "--stage-id", "2"]).subcommand, "executor-loss")
        self.assertEqual(parser.parse_args(["timeline", "events", "--url", "https://spark.example.com/", "--sql-id", "9"]).subcommand, "events")
        self.assertEqual(parser.parse_args(["duration", "jobs", "--url", "https://spark.example.com/"]).subcommand, "jobs")
        self.assertEqual(parser.parse_args(["duration", "stages", "--url", "https://spark.example.com/"]).subcommand, "stages")
        self.assertTrue(parser.parse_args(["duration", "tasks", "--url", "https://spark.example.com/", "--all-stages"]).all_stages)
        self.assertEqual(parser.parse_args(["skew", "duration", "--url", "https://spark.example.com/", "--stage-id", "1"]).subcommand, "duration")
        self.assertEqual(parser.parse_args(["executors", "health", "--url", "https://spark.example.com/"]).subcommand, "health")
        self.assertEqual(parser.parse_args(["executors", "failed-tasks", "--url", "https://spark.example.com/"]).subcommand, "failed-tasks")
        self.assertEqual(parser.parse_args(["executor", "health", "--url", "https://spark.example.com/", "--executor-id", "2"]).subcommand, "health")
        self.assertEqual(parser.parse_args(["executor", "tasks", "--url", "https://spark.example.com/", "--executor-id", "2"]).subcommand, "tasks")
        self.assertEqual(parser.parse_args(["diagnose", "task-health", "--url", "https://spark.example.com/"]).subcommand, "task-health")
        self.assertEqual(parser.parse_args(["stages", "retries", "--url", "https://spark.example.com/", "--limit-stages", "5"]).subcommand, "retries")
        self.assertTrue(parser.parse_args(["stages", "failures", "--url", "https://spark.example.com/", "--all-stages"]).all_stages)
        self.assertEqual(parser.parse_args(["stage", "retries", "--url", "https://spark.example.com/", "--stage-id", "1"]).subcommand, "retries")
        self.assertEqual(parser.parse_args(["stage", "failures", "--url", "https://spark.example.com/", "--stage-id", "1", "--attempt-id", "0"]).subcommand, "failures")
        self.assertEqual(parser.parse_args(["diagnose", "retries", "--url", "https://spark.example.com/", "--top", "5"]).subcommand, "retries")
        self.assertEqual(parser.parse_args(["diagnose", "failures", "--url", "https://spark.example.com/", "--include-successful-retries"]).subcommand, "failures")

    def test_fetch_app_with_mock_transport(self) -> None:
        """Fetch app details through the Spark REST client."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked Spark app fetch."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock Spark REST responses."""
                if request.url.path.endswith("/api/v1/applications"):
                    return httpx.Response(200, json=[{"id": "spark-app-1", "attempts": [{"appSparkVersion": "3.5.2"}]}])
                if request.url.path.endswith("/api/v1/applications/spark-app-1"):
                    return httpx.Response(200, json={"id": "spark-app-1", "name": "demo"})
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/app/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            try:
                app_id = await spark_diag.resolve_app_id(client, spark_diag.parse_web_url("https://spark.example.com/app/jobs/"), object())
                return await client.get_json(f"api/v1/applications/{app_id}")
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["name"], "demo")

    def test_command_sql_uses_limit_and_failed_html(self) -> None:
        """Fetch paginated SQL REST rows and merge failed HTML errors."""
        async def run_case() -> dict[str, Any]:
            """Run a mocked SQL failures command."""
            def handler(request: httpx.Request) -> httpx.Response:
                """Return mock Spark SQL and HTML responses."""
                if request.url.path.endswith("/api/v1/applications/app-1/sql"):
                    self.assertEqual(request.url.params.get("length"), "1000")
                    return httpx.Response(
                        200,
                        json=[
                            {"id": 7, "status": "FAILED", "description": "ALTER TABLE db.tbl DROP IF EXISTS PARTITION (dt='20260601')"},
                            {"id": 8, "status": "COMPLETED", "description": "SELECT 1"},
                        ],
                    )
                if request.url.path.endswith("/history/app-1/SQL/"):
                    return httpx.Response(
                        200,
                        text="<table><tr><th>ID</th><th>Description</th><th>Error Message</th></tr><tr><td>7</td><td>ALTER TABLE db.tbl DROP IF EXISTS PARTITION (dt='20260601')</td><td>partition metadata is not stored in the Hive metastore; run msck repair table tbl.</td></tr></table>",
                    )
                return httpx.Response(404, json={"error": "missing"})

            client = spark_diag.SparkClient("https://spark.example.com/")
            await client._client.aclose()
            client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://spark.example.com/")
            try:
                args = spark_diag.build_parser().parse_args(
                    ["sql", "failures", "--url", "https://spark.example.com/history/app-1/SQL/", "--limit", "1000"]
                )
                parsed = spark_diag.parse_web_url(args.url)
                return await spark_diag.command_sql(client, parsed, args)
            finally:
                await client.close()

        result = asyncio.run(run_case())
        self.assertEqual(result["failed_sql"], 1)
        self.assertEqual(result["completed_sql"], 1)
        self.assertEqual(result["html_errors"]["available"], True)
        self.assertTrue(result["recommendations"])


if __name__ == "__main__":
    unittest.main()
