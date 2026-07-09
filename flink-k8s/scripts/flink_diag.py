#!/Users/luca/miniforge3/envs/py311/bin/python3.11
"""Flink WebUI diagnostics CLI.

This CLI talks to the Flink REST API behind the WebUI and reuses browser
cookies from chrome-cdp-ws-daemon for authenticated internal deployments.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlsplit, urlunsplit

try:
    import httpx
except ImportError as exc:  # pragma: no cover - exercised only when dependency is absent
    raise SystemExit(
        "Missing dependency: httpx. Install the latest version with "
        "`/Users/luca/miniforge3/envs/py311/bin/python3.11 -m pip install -U httpx`."
    ) from exc

_CDP_SCRIPT_DIR = Path("/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
if _CDP_SCRIPT_DIR.exists() and str(_CDP_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_CDP_SCRIPT_DIR))

_SENSITIVE_KEY_RE = re.compile(
    r"(token|password|passwd|secret|authorization|cookie|session|credential|access[_-]?key|api[_-]?key)",
    re.IGNORECASE,
)
_TERMINAL_STATES = {"FINISHED", "CANCELED", "CANCELLED", "FAILED"}
_RUNNING_STATES = {"RUNNING", "CREATED", "SCHEDULED", "DEPLOYING", "INITIALIZING", "RECONCILING"}
_DEFAULT_LOG_ERROR_PATTERNS = [
    "ERROR",
    "WARN",
    "Exception",
    "Caused by",
    "OutOfMemoryError",
    "CheckpointException",
    "TimeoutException",
    "BackPressure",
    "backpressure",
]
_HBASE_LOOKUP_LOG_PATTERNS = [
    "HBaseConfigurationUtil",
    "TimeoutException",
    "RetriesExhausted",
    "ScannerTimeout",
    "CallTimeout",
]
_DEFAULT_LOG_DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / ".downloads"
_DEFAULT_STDOUT_DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / ".downloads" / "stdout"
_K8S_STDOUT_MISSING_RE = re.compile(
    r"(STDOUT does not exist|kubernetes mode|kubectl logs)",
    re.IGNORECASE,
)
_DEFAULT_METRICS = [
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
    "backPressuredTimeMsPerSecond",
    "softBackPressuredTimeMsPerSecond",
    "hardBackPressuredTimeMsPerSecond",
    "isBackPressured",
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    "numBytesInPerSecond",
    "numBytesOutPerSecond",
    "checkpointStartDelayNanos",
    "currentInputWatermark",
    "currentOutputWatermark",
]
_CAPACITY_METRICS = [
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    "numBytesInPerSecond",
    "numBytesOutPerSecond",
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
    "backPressuredTimeMsPerSecond",
    "checkpointStartDelayNanos",
]
_METRIC_ANALYZE_DEFAULT_GET = [
    "busyTimeMsPerSecond",
    "idleTimeMsPerSecond",
    "backPressuredTimeMsPerSecond",
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    "checkpointStartDelayNanos",
]
_CAPACITY_BUSY_HIGH_MS = 700.0
_CAPACITY_BACKPRESSURE_HIGH_MS = 500.0
_CAPACITY_SKEW_RATIO = 3.0
_TASKMANAGER_AGGREGATE_DEFAULT_METRICS = [
    "read-records",
    "write-records",
    "read-bytes",
    "write-bytes",
    "accumulated-busy-time",
    "accumulated-idle-time",
    "accumulated-backpressured-time",
]
_METRIC_ALIASES: dict[str, list[str]] = {
    "source.records_in": [r"^.*Source__.*\.numRecordsIn$"],
    "source.records_in_rate": [r"^.*Source__.*\.numRecordsInPerSecond$"],
    "source.records_out": [r"^.*Source__.*\.numRecordsOut$"],
    "source.records_out_rate": [r"^.*Source__.*\.numRecordsOutPerSecond$"],
    "source.current_offset": [r"^.*Source__.*currentOffset$"],
    "source.committed_offset": [r"^.*Source__.*committedOffset$"],
    "source.records_lag": [r"^.*Source__.*records-lag-max$", r"^.*Source__.*records-lag$"],
    "source.assigned_partitions": [r"^.*Source__.*assigned-partitions$"],
    "source.watermark": [r"^.*Source__.*currentInputWatermark$", r"^.*Source__.*currentOutputWatermark$"],
    "sink.records_send": [r"^.*Writer\.numRecordsSend$", r"^.*KafkaProducer\.record-send-total$"],
    "sink.records_send_rate": [r"^.*Writer\.numRecordsSendPerSecond$", r"^.*KafkaProducer\.record-send-rate$"],
    "sink.records_send_errors": [r"^.*Writer\.numRecordsSendErrors$", r"^.*KafkaProducer\.record-error-total$"],
    "sink.bytes_send": [r"^.*Writer\.numBytesSend$", r"^.*KafkaProducer\.outgoing-byte-total$"],
    "sink.bytes_send_rate": [r"^.*Writer\.numBytesSendPerSecond$", r"^.*KafkaProducer\.outgoing-byte-rate$"],
    "sink.request_latency": [r"^.*KafkaProducer\.request-latency-avg$", r"^.*KafkaProducer\.request-latency-max$"],
    "sink.throttle_time": [r"^.*KafkaProducer\.produce-throttle-time-avg$", r"^.*KafkaProducer\.produce-throttle-time-max$"],
    "taskchain.records_in": [r"^.*\.numRecordsIn$", r"^numRecordsIn$"],
    "taskchain.records_out": [r"^.*\.numRecordsOut$", r"^numRecordsOut$"],
    "taskchain.backpressure": [r"^.*backPressuredTimeMsPerSecond$", r"^.*BackPressureTimeMs$"],
    "taskchain.busy": [r"^.*busyTimeMsPerSecond$", r"^.*BusyTimeMs$"],
    "taskchain.idle": [r"^.*idleTimeMsPerSecond$"],
    "lookup.records_in": [r"^.*LookupJoin.*\.numRecordsIn$"],
    "lookup.records_in_rate": [r"^.*LookupJoin.*\.numRecordsInPerSecond$"],
    "lookup.records_out": [r"^.*LookupJoin.*\.numRecordsOut$"],
    "lookup.records_out_rate": [r"^.*LookupJoin.*\.numRecordsOutPerSecond$"],
    "lookup.cache_hit_rate": [r"^.*LookupJoin.*\.lookupCacheHitRate$"],
    "paimon.writer.records_in": [r"^.*Writer.*\.numRecordsIn$"],
    "paimon.writer.records_in_rate": [r"^.*Writer.*\.numRecordsInPerSecond$"],
    "paimon.writer.buffer_writers": [r"^.*paimon\.table\..*\.writerBuffer\.numWriters$"],
    "paimon.writer.buffer_preempt_count": [r"^.*paimon\.table\..*\.writerBuffer\.bufferPreemptCount$"],
    "paimon.compaction.busy": [r"^.*paimon\.table\..*\.compaction\.compactionThreadBusy$"],
    "paimon.compaction.completed_count": [r"^.*paimon\.table\..*\.compaction\.(compactionCompletedCount|completedCompactionCount|numCompletedCompactions)$"],
    "paimon.compaction.level0_file_count": [r"^.*paimon\.table\..*\.compaction\.(level0FileCount|avgLevel0FileCount|maxLevel0FileCount)$"],
    "paimon.compaction.total_file_size": [r"^.*paimon\.table\..*\.compaction\.(totalFileSize|avgTotalFileSize|maxTotalFileSize)$"],
    "paimon.compaction.input_size": [r"^.*paimon\.table\..*\.compaction\.(inputFileSize|inputSize|avgCompactionInputSize|maxCompactionInputSize)$"],
    "paimon.compaction.output_size": [r"^.*paimon\.table\..*\.compaction\.(outputFileSize|outputSize|avgCompactionOutputSize|maxCompactionOutputSize)$"],
    "paimon.compaction.time": [r"^.*paimon\.table\..*\.compaction\.(compactionTime|time|avgCompactionTime|maxCompactionTime)$"],
    "paimon.commit.duration": [r"^.*paimon\.table\..*\.commit\.(commitDuration_(max|mean|avg)|lastCommitDuration)$"],
    "paimon.commit.duration_p99": [r"^.*paimon\.table\..*\.commit\.commitDuration_p99$"],
    "paimon.commit.files_added": [r"^.*paimon\.table\..*\.commit\.lastTableFilesAdded$"],
    "paimon.commit.files_appended": [r"^.*paimon\.table\..*\.commit\.lastTableFilesAppended$"],
    "paimon.commit.files_deleted": [r"^.*paimon\.table\..*\.commit\.lastTableFilesDeleted$"],
    "paimon.commit.partitions_written": [r"^.*paimon\.table\..*\.commit\.lastPartitionsWritten$"],
    "paimon.commit.buckets_written": [r"^.*paimon\.table\..*\.commit\.lastBucketsWritten$"],
    "paimon.commit.snapshots": [r"^.*paimon\.table\..*\.commit\.lastGeneratedSnapshots$"],
    "paimon.commit.attempts": [r"^.*paimon\.table\..*\.commit\.lastCommitAttempts$"],
}
_METRIC_EXPLANATIONS: dict[str, str] = {
    "source.records_in": "Source operator consumed records. This may be a prefixed operator metric such as 0.Source__name.numRecordsIn.",
    "sink.records_send": "Records actually sent by a sink writer or KafkaProducer. Prefer this over task-chain Records Sent for terminal sinks.",
    "taskchain.records_out": "Records emitted to downstream Flink operators. Sink task chains often show 0 because they have no downstream operator.",
    "taskchain.backpressure": "Time or ratio spent backpressured. Non-zero current backpressure indicates downstream blockage.",
    "checkpointStartDelayNanos": "Delay before a checkpoint barrier reaches the task; high values can indicate backpressure or skew.",
    "lookup.records_in_rate": "LookupJoin operator input rate. Use this as actual lookup QPS instead of the surrounding task-chain input rate.",
    "lookup.records_out_rate": "LookupJoin operator output rate after HBase/dimension lookup.",
    "lookup.cache_hit_rate": "LookupJoin cache hit ratio when the connector exposes it. Near-zero means most lookups go to the external store, but it is not a bottleneck by itself.",
    "paimon.writer.records_in": "Records entering the Paimon writer task chain.",
    "paimon.compaction.busy": "Paimon compaction thread busy percentage; high values indicate compaction pressure.",
    "paimon.commit.duration_p99": "Paimon commit duration p99 in milliseconds; high values indicate slow global commits.",
}
_PAIMON_WRITER_ALIASES = [
    "paimon.writer.records_in",
    "paimon.writer.records_in_rate",
    "paimon.writer.buffer_writers",
    "paimon.writer.buffer_preempt_count",
    "paimon.compaction.busy",
    "paimon.compaction.completed_count",
    "paimon.compaction.level0_file_count",
    "paimon.compaction.total_file_size",
    "paimon.compaction.input_size",
    "paimon.compaction.output_size",
    "paimon.compaction.time",
]
_PAIMON_ROLE_WRITER_ALIASES = [
    alias
    for alias in _PAIMON_WRITER_ALIASES
    if alias not in {"paimon.writer.records_in", "paimon.writer.records_in_rate"}
]
_PAIMON_COMMITTER_ALIASES = [
    "paimon.commit.duration",
    "paimon.commit.duration_p99",
    "paimon.commit.files_added",
    "paimon.commit.files_appended",
    "paimon.commit.files_deleted",
    "paimon.commit.partitions_written",
    "paimon.commit.buckets_written",
    "paimon.commit.snapshots",
    "paimon.commit.attempts",
]
_PAIMON_SOURCE_RE = re.compile(r"^.*paimon\.table\..*\.(source|scan)\.", re.IGNORECASE)
_LOOKUP_JOIN_ALIASES = [
    "lookup.records_in",
    "lookup.records_in_rate",
    "lookup.records_out",
    "lookup.records_out_rate",
    "lookup.cache_hit_rate",
]


class FlinkDiagError(RuntimeError):
    """Raised for actionable CLI errors."""


@dataclass
class ParsedUrl:
    """Parsed representation of a Flink WebUI URL."""

    input_url: str | None
    base_url: str
    origin: str
    route_parts: list[str] = field(default_factory=list)
    route_kind: str | None = None
    deployment: str | None = None
    job_id: str | None = None
    vertex_id: str | None = None
    taskmanager_id: str | None = None
    subtask: int | None = None
    tab: str | None = None


@dataclass
class StdoutWatchState:
    """记录单个 TaskManager stdout watch 的本地增量状态。"""

    text: str = ""
    available: bool = True


@dataclass
class ResolveResult:
    """Resolved dynamic identifiers for a command invocation."""

    parsed: ParsedUrl
    flink_version: str | None = None
    endpoint_profile: str = "generic"
    job_id: str | None = None
    vertex_id: str | None = None
    taskmanager_id: str | None = None
    subtask: int | None = None
    job: dict[str, Any] | None = None
    vertex: dict[str, Any] | None = None
    taskmanager: dict[str, Any] | None = None


class FlinkClient:
    """Asynchronous Flink REST API client with bounded concurrency."""

    def __init__(
        self,
        base_url: str,
        *,
        cookies: dict[str, str] | None = None,
        timeout: float = 10.0,
        verify: bool = True,
        concurrency: int = 8,
        retries: int = 1,
    ) -> None:
        """Initialize the client.

        Args:
            base_url: REST base URL ending in a slash.
            cookies: Browser cookies to attach.
            timeout: Request timeout in seconds.
            verify: Whether to verify TLS certificates.
            concurrency: Maximum concurrent HTTP requests.
            retries: Retry count for transient GET failures.
        """
        self.base_url = ensure_trailing_slash(base_url)
        self.timeout = timeout
        self.verify = verify
        self.retries = retries
        self._semaphore = asyncio.Semaphore(max(1, concurrency))
        self._client = httpx.AsyncClient(
            cookies=cookies or {},
            timeout=httpx.Timeout(timeout),
            verify=verify,
            follow_redirects=True,
            headers={"Accept": "application/json, text/plain, */*", "Referer": self.base_url},
        )

    async def __aenter__(self) -> "FlinkClient":
        """Return this client for async context manager use."""
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the underlying HTTP client on context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        await self._client.aclose()

    def url_for(self, path: str) -> str:
        """Build an absolute URL for a REST path."""
        return urljoin(self.base_url, path.lstrip("/"))

    async def get_response(self, path: str, *, allow_error: bool = False) -> httpx.Response:
        """GET a REST path and return the response.

        Args:
            path: REST path relative to base URL.
            allow_error: Return 4xx/5xx responses instead of raising.

        Returns:
            The HTTP response.
        """
        url = self.url_for(path)
        last_error: Exception | None = None
        attempts = max(1, self.retries + 1)
        async with self._semaphore:
            for attempt in range(attempts):
                try:
                    response = await self._client.get(url)
                    if allow_error or response.status_code < 400:
                        return response
                    if response.status_code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                        raise FlinkDiagError(format_http_error(path, response))
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_error = exc
                    if attempt == attempts - 1:
                        raise FlinkDiagError(f"GET {path} failed: {exc}") from exc
                await asyncio.sleep(0.2 * (attempt + 1))
        raise FlinkDiagError(f"GET {path} failed: {last_error}")

    async def get_json(self, path: str, *, allow_error: bool = False) -> Any:
        """GET a REST path and parse a JSON body."""
        response = await self.get_response(path, allow_error=allow_error)
        if response.status_code >= 400 and allow_error:
            return {
                "available": False,
                "status_code": response.status_code,
                "body": response.text,
                "path": path,
            }
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise FlinkDiagError(f"GET {path} did not return JSON: {response.text[:200]}") from exc

    async def get_text_status(self, path: str) -> dict[str, Any]:
        """GET a REST path and return text plus availability metadata."""
        response = await self.get_response(path, allow_error=True)
        return {
            "available": response.status_code < 400,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type", ""),
            "text": response.text,
            "path": path,
        }

    async def stream_tail(self, path: str, tail_bytes: int, *, max_bytes: int | None = None) -> dict[str, Any]:
        """流式读取文本端点，只保留尾部字节，并可限制本轮最多读取的字节数。"""
        url = self.url_for(path)
        chunks: deque[bytes] = deque()
        total = 0
        retained = 0
        bytes_limited = False
        async with self._semaphore:
            async with self._client.stream("GET", url) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return {
                        "available": False,
                        "status_code": response.status_code,
                        "path": path,
                        "text": body.decode("utf-8", "replace"),
                    }
                async for chunk in response.aiter_bytes():
                    if max_bytes is not None:
                        remaining = max_bytes - total
                        if remaining <= 0:
                            bytes_limited = True
                            break
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                            bytes_limited = True
                    chunks.append(chunk)
                    total += len(chunk)
                    retained += len(chunk)
                    while retained > tail_bytes and chunks:
                        left = chunks[0]
                        overflow = retained - tail_bytes
                        if overflow >= len(left):
                            retained -= len(left)
                            chunks.popleft()
                        else:
                            chunks[0] = left[overflow:]
                            retained -= overflow
                            break
                    if bytes_limited:
                        break
        text = b"".join(chunks).decode("utf-8", "replace")
        return {
            "available": True,
            "status_code": 200,
            "path": path,
            "bytes_read": total,
            "tail_bytes": tail_bytes,
            "max_bytes": max_bytes,
            "bytes_limited": bytes_limited,
            "truncated": total > tail_bytes,
            "text": text,
        }

    async def stream_grep(
        self,
        path: str,
        patterns: list[str] | None,
        *,
        before: int = 0,
        after: int = 0,
        max_matches: int = 50,
        max_bytes: int | None = None,
    ) -> dict[str, Any]:
        """流式搜索日志端点，只保留命中行和有限上下文。"""
        compiled = compile_log_patterns(patterns)
        url = self.url_for(path)
        before_lines: deque[tuple[int, str]] = deque(maxlen=max(0, before))
        matches: list[dict[str, Any]] = []
        pending_after: list[dict[str, Any]] = []
        pattern_counts = {name: 0 for name, _ in compiled}
        bytes_read = 0
        bytes_limited = False
        line_number = 0
        stopped_early = False

        async def process_line(raw_line: str) -> None:
            """Process one decoded log line for grep matching."""
            nonlocal line_number, stopped_early
            line_number += 1
            line = raw_line.rstrip("\r")
            for pending in list(pending_after):
                if pending["remaining"] > 0:
                    pending["match"]["after"].append(line[:500])
                    pending["remaining"] -= 1
                if pending["remaining"] <= 0:
                    pending_after.remove(pending)
            matched = [name for name, expression in compiled if expression.search(line)]
            if matched:
                for name in matched:
                    pattern_counts[name] += 1
                if len(matches) < max(0, max_matches):
                    match = {
                        "line_number": line_number,
                        "patterns": matched,
                        "text": line[:500],
                        "before": [item[1][:500] for item in before_lines],
                        "after": [],
                    }
                    matches.append(match)
                    if after > 0:
                        pending_after.append({"match": match, "remaining": after})
                elif after <= 0:
                    stopped_early = True
            before_lines.append((line_number, line))

        async with self._semaphore:
            async with self._client.stream("GET", url) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return {
                        "available": False,
                        "status_code": response.status_code,
                        "path": path,
                        "text": body.decode("utf-8", "replace"),
                    }
                buffer = ""
                async for chunk in response.aiter_bytes():
                    if max_bytes is not None:
                        remaining = max_bytes - bytes_read
                        if remaining <= 0:
                            bytes_limited = True
                            break
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                            bytes_limited = True
                    bytes_read += len(chunk)
                    buffer += chunk.decode("utf-8", "replace")
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        await process_line(line)
                        if stopped_early and not pending_after:
                            break
                    if stopped_early and not pending_after:
                        break
                    if bytes_limited:
                        break
                if buffer and not (stopped_early and not pending_after):
                    await process_line(buffer)
        match_count = sum(pattern_counts.values())
        return {
            "available": True,
            "status_code": 200,
            "path": path,
            "bytes_read": bytes_read,
            "max_bytes": max_bytes,
            "bytes_limited": bytes_limited,
            "line_count": line_number,
            "patterns": [name for name, _ in compiled],
            "pattern_counts": pattern_counts,
            "match_count": match_count,
            "max_matches": max_matches,
            "truncated_matches": stopped_early or match_count > len(matches),
            "matches": matches,
        }

    async def stream_error_summary(
        self,
        path: str,
        patterns: list[str] | None,
        *,
        before: int = 2,
        after: int = 3,
        max_signatures: int = 20,
        max_samples_per_signature: int = 2,
    ) -> dict[str, Any]:
        """Stream a log endpoint and aggregate matching lines by error signature."""
        compiled = compile_log_patterns(patterns or _DEFAULT_LOG_ERROR_PATTERNS)
        url = self.url_for(path)
        before_lines: deque[str] = deque(maxlen=max(0, before))
        groups: dict[str, dict[str, Any]] = {}
        pattern_counts = {name: 0 for name, _ in compiled}
        pending_after: list[dict[str, Any]] = []
        bytes_read = 0
        line_number = 0

        async def process_line(raw_line: str) -> None:
            """Process one decoded log line for error aggregation."""
            nonlocal line_number
            line_number += 1
            line = raw_line.rstrip("\r")
            for pending in list(pending_after):
                if pending["remaining"] > 0:
                    pending["sample"]["after"].append(line[:500])
                    pending["remaining"] -= 1
                if pending["remaining"] <= 0:
                    pending_after.remove(pending)
            matched = [name for name, expression in compiled if expression.search(line)]
            if matched:
                for name in matched:
                    pattern_counts[name] += 1
                signature = log_error_signature(line)
                group = groups.setdefault(
                    signature,
                    {
                        "signature": signature,
                        "count": 0,
                        "first_line": line_number,
                        "last_line": line_number,
                        "patterns": set(),
                        "samples": [],
                    },
                )
                group["count"] += 1
                group["last_line"] = line_number
                group["patterns"].update(matched)
                if len(group["samples"]) < max(0, max_samples_per_signature):
                    sample = {
                        "line_number": line_number,
                        "text": line[:500],
                        "before": [item[:500] for item in before_lines],
                        "after": [],
                    }
                    group["samples"].append(sample)
                    if after > 0:
                        pending_after.append({"sample": sample, "remaining": after})
            before_lines.append(line)

        async with self._semaphore:
            async with self._client.stream("GET", url) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return {
                        "available": False,
                        "status_code": response.status_code,
                        "path": path,
                        "text": body.decode("utf-8", "replace"),
                    }
                buffer = ""
                async for chunk in response.aiter_bytes():
                    bytes_read += len(chunk)
                    buffer += chunk.decode("utf-8", "replace")
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        await process_line(line)
                if buffer:
                    await process_line(buffer)
        return finalize_log_error_summary(
            groups,
            pattern_counts,
            [name for name, _ in compiled],
            line_count=line_number,
            bytes_read=bytes_read,
            path=path,
            max_signatures=max_signatures,
        )

    async def download_text(
        self,
        path: str,
        destination: Path,
        *,
        max_bytes: int | None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """Download a text endpoint to a local file with an optional byte ceiling."""
        if destination.exists() and not overwrite:
            raise FlinkDiagError(f"Refusing to overwrite existing file: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        sha256 = hashlib.sha256()
        bytes_written = 0
        truncated = False
        url = self.url_for(path)
        async with self._semaphore:
            async with self._client.stream("GET", url) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return {
                        "available": False,
                        "status_code": response.status_code,
                        "path": path,
                        "text": body.decode("utf-8", "replace"),
                    }
                with destination.open("wb") as handle:
                    async for chunk in response.aiter_bytes():
                        if max_bytes is not None:
                            remaining = max_bytes - bytes_written
                            if remaining <= 0:
                                truncated = True
                                break
                            if len(chunk) > remaining:
                                chunk = chunk[:remaining]
                                truncated = True
                        handle.write(chunk)
                        sha256.update(chunk)
                        bytes_written += len(chunk)
                        if truncated:
                            break
        return {
            "available": True,
            "status_code": 200,
            "path": path,
            "output_path": str(destination),
            "bytes_written": bytes_written,
            "max_bytes": max_bytes,
            "truncated": truncated,
            "sha256": sha256.hexdigest(),
        }


def ensure_trailing_slash(value: str) -> str:
    """Return a URL string with a trailing slash."""
    return value if value.endswith("/") else value + "/"


def compact_json(value: Any) -> str:
    """Serialize a value as stable UTF-8 JSON."""
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)


def emit(value: Any, *, as_json: bool = False) -> None:
    """Emit either JSON or a concise text representation."""
    safe_value = redact_sensitive(value)
    if as_json:
        print(compact_json(safe_value))
        return
    print(render_text(safe_value))


def render_text(value: Any) -> str:
    """Render common response objects as readable text."""
    if isinstance(value, list):
        return "\n".join(render_text(item) for item in value)
    if not isinstance(value, dict):
        return str(value)
    if "text" in value and set(value).issuperset({"available", "text"}):
        prefix = "available: " + str(value.get("available")).lower()
        return prefix + "\n" + str(value.get("text", ""))
    lines: list[str] = []
    for key, item in value.items():
        if isinstance(item, (dict, list)):
            lines.append(f"{key}: {compact_json(item)}")
        else:
            lines.append(f"{key}: {item}")
    return "\n".join(lines)


def redact_sensitive(value: Any) -> Any:
    """Return a copy with sensitive-looking keys redacted."""
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if _SENSITIVE_KEY_RE.search(str(key)):
                result[key] = "<redacted>"
            else:
                result[key] = redact_sensitive(item)
        return result
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    return value


def format_http_error(path: str, response: httpx.Response) -> str:
    """Format an actionable HTTP error."""
    body = response.text[:500].replace("\n", "\\n")
    return f"GET {path} returned HTTP {response.status_code}: {body}"


def parse_web_url(url: str | None, *, base_url: str | None = None, origin: str | None = None) -> ParsedUrl:
    """Parse a Flink WebUI URL and extract dynamic route variables."""
    raw_url = url or os.environ.get("FLINK_WEB_URL")
    if not raw_url and not base_url:
        raise FlinkDiagError("Missing --url or --base-url. Example: flink_diag.py overview --url <flink-webui-url>")
    source = raw_url or base_url or ""
    split = urlsplit(source)
    if not split.scheme or not split.netloc:
        raise FlinkDiagError(f"Invalid URL: {source}")
    detected_origin = origin or f"{split.scheme}://{split.netloc}"
    if base_url:
        rest_base = ensure_trailing_slash(base_url)
        deployment = urlsplit(rest_base).path.strip("/").split("/")[0] or None
    else:
        first_segment = split.path.strip("/").split("/")[0] if split.path.strip("/") else ""
        if not first_segment:
            raise FlinkDiagError(f"Cannot infer deployment path from URL: {source}")
        deployment = first_segment
        rest_base = urlunsplit((split.scheme, split.netloc, f"/{first_segment}/", "", ""))
    route = split.fragment.strip("/")
    route_parts = [part for part in route.split("/") if part]
    parsed = ParsedUrl(
        input_url=raw_url,
        base_url=ensure_trailing_slash(rest_base),
        origin=detected_origin,
        route_parts=route_parts,
        deployment=deployment,
    )
    parse_route_parts(parsed)
    return parsed


def parse_route_parts(parsed: ParsedUrl) -> None:
    """Populate route-derived fields on a parsed URL."""
    parts = parsed.route_parts
    if not parts:
        return
    if parts[0] == "overview":
        parsed.route_kind = "overview"
        return
    if parts[:2] == ["job", "completed"] and len(parts) == 2:
        parsed.route_kind = "jobs_completed"
        return
    if len(parts) >= 2 and parts[0] == "job":
        parsed.route_kind = "job"
        if parts[1] in {"running", "completed"} and len(parts) >= 3:
            # Flink 1.18 style: #/job/running/<jobid>/exceptions
            parsed.job_id = parts[2]
            if len(parts) >= 4:
                parsed.tab = parts[3]
            if len(parts) >= 6 and parts[3] == "overview":
                parsed.vertex_id = parts[4]
                parsed.tab = parts[5]
        else:
            # Flink 1.15 style: #/job/<jobid>/exceptions
            parsed.job_id = parts[1]
            if len(parts) >= 3:
                parsed.tab = parts[2]
            if len(parts) >= 5 and parts[2] == "overview":
                parsed.vertex_id = parts[3]
                parsed.tab = parts[4]
        return
    if parts[0] == "task-manager":
        parsed.route_kind = "taskmanager"
        if len(parts) >= 2:
            parsed.taskmanager_id = parts[1]
        if len(parts) >= 3:
            parsed.tab = parts[2]
        return
    if parts[0] == "job-manager":
        parsed.route_kind = "jobmanager"
        if len(parts) >= 2:
            parsed.tab = parts[1]


def load_browser_cookies(origin: str, *, no_cookies: bool = False) -> dict[str, str]:
    """Load browser cookies for an origin through chrome-cdp-ws-daemon."""
    if no_cookies:
        return {}
    try:
        from cdp_client import get_cookies  # type: ignore
    except Exception as exc:
        raise FlinkDiagError(f"Unable to import chrome-cdp-ws-daemon cookie client: {exc}") from exc
    cookies = get_cookies(origin)
    if not isinstance(cookies, dict):
        raise FlinkDiagError("chrome-cdp-ws-daemon returned an unexpected cookie payload")
    return {str(key): str(value) for key, value in cookies.items()}


def get_cdp_text(target: str) -> str:
    """Read visible page text from a Chrome target using the daemon CLI."""
    python = "/Users/luca/miniforge3/envs/py311/bin/python"
    script = "/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts/daemon.py"
    proc = subprocess.run(
        [python, script, "get-text", "--target", target],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=15,
    )
    if proc.returncode != 0:
        raise FlinkDiagError(f"CDP get-text failed: {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout


def detect_version_from_text(text: str) -> str | None:
    """Extract a Flink version from WebUI text."""
    match = re.search(r"Version:\s*([0-9]+(?:\.[0-9]+){1,2})", text)
    return match.group(1) if match else None


async def detect_flink_version(client: FlinkClient, args: argparse.Namespace) -> str | None:
    """Detect the Flink version from REST or CDP fallback."""
    explicit = getattr(args, "flink_version", "auto")
    if explicit and explicit != "auto":
        return explicit
    for path in ("config", "overview"):
        try:
            data = await client.get_json(path)
            if isinstance(data, dict):
                version = data.get("flink-version") or data.get("flinkVersion")
                if version:
                    return str(version)
        except FlinkDiagError:
            continue
    target = getattr(args, "target", None)
    if target:
        return detect_version_from_text(get_cdp_text(target))
    return None


def select_endpoint_profile(version: str | None, override: str = "auto") -> str:
    """Select an endpoint profile for a Flink version."""
    if override and override != "auto":
        return override
    if version:
        match = re.match(r"^(\d+)\.(\d+)", version)
        if match and match.group(1) == "1":
            minor = int(match.group(2))
            if 15 <= minor <= 20:
                return f"flink-1.{minor}"
    return "generic"


def split_csv(value: str | None) -> list[str]:
    """Split a comma-separated option into non-empty values."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def clone_args_namespace(args: argparse.Namespace) -> argparse.Namespace:
    """复制 argparse 参数；兼容单测里用类属性模拟 Namespace 的写法。"""
    data = dict(vars(args))
    for name in dir(args):
        if name.startswith("_") or name in data:
            continue
        try:
            value = getattr(args, name)
        except AttributeError:
            continue
        if callable(value):
            continue
        data[name] = value
    return argparse.Namespace(**data)


def filter_jobs(jobs: list[dict[str, Any]], state: str | None) -> list[dict[str, Any]]:
    """Filter jobs by a user-facing state selector."""
    if not state or state == "all":
        return jobs
    if state == "running":
        return [job for job in jobs if str(job.get("state") or job.get("status")).upper() in _RUNNING_STATES]
    if state in {"completed", "terminal"}:
        return [job for job in jobs if str(job.get("state") or job.get("status")).upper() in _TERMINAL_STATES]
    return [job for job in jobs if str(job.get("state") or job.get("status")).lower() == state.lower()]


def match_name(items: list[dict[str, Any]], key: str, value: str | None, *, regex: bool = False) -> list[dict[str, Any]]:
    """Filter dictionaries by a name-like field."""
    if not value:
        return items
    if regex:
        pattern = re.compile(value)
        return [item for item in items if pattern.search(str(item.get(key, "")))]
    exact = [item for item in items if str(item.get(key, "")) == value]
    if exact:
        return exact
    return [item for item in items if value in str(item.get(key, ""))]


def pick_one(items: list[dict[str, Any]], *, index: int | None = None, label: str = "item") -> dict[str, Any]:
    """Pick a single item or raise with candidates."""
    if index is not None:
        try:
            return items[index]
        except IndexError as exc:
            raise FlinkDiagError(f"{label} index {index} out of range; {len(items)} candidate(s) found") from exc
    if len(items) == 1:
        return items[0]
    if not items:
        raise FlinkDiagError(f"No {label} matched")
    candidates = [
        {
            "index": idx,
            "id": item.get("jid") or item.get("id"),
            "name": item.get("name"),
            "state": item.get("state") or item.get("status"),
        }
        for idx, item in enumerate(items)
    ]
    raise FlinkDiagError(f"Multiple {label} candidates matched: {compact_json(candidates)}")


async def resolve_job_id(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> tuple[str, dict[str, Any] | None]:
    """Resolve a job id from URL, flags, or jobs overview."""
    explicit = getattr(args, "job_id", None)
    if explicit and explicit != "auto":
        return explicit, None
    if parsed.job_id:
        return parsed.job_id, None
    jobs_data = await client.get_json("jobs/overview")
    jobs = list(jobs_data.get("jobs", [])) if isinstance(jobs_data, dict) else []
    state = getattr(args, "job_state", None) or "running"
    candidates = filter_jobs(jobs, state)
    candidates = match_name(candidates, "name", getattr(args, "job_name", None), regex=getattr(args, "regex", False))
    index = getattr(args, "job_index", None)
    job = pick_one(candidates, index=index, label="job")
    return str(job.get("jid") or job.get("id")), job


async def resolve_vertex_id(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
    job_id: str,
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve a task chain / vertex id from URL, flags, or job vertices."""
    explicit = getattr(args, "vertex_id", None) or getattr(args, "task_chain_id", None)
    if explicit:
        return explicit, None
    if parsed.vertex_id:
        return parsed.vertex_id, None
    name = getattr(args, "vertex_name", None) or getattr(args, "task_chain_name", None)
    if not name:
        return None, None
    job = await client.get_json(f"jobs/{job_id}")
    vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []
    candidates = match_name(vertices, "name", name, regex=getattr(args, "regex", False))
    vertex = pick_one(candidates, label="vertex")
    return str(vertex.get("id")), vertex


async def resolve_taskmanager_id(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve a TaskManager id from URL, flags, or taskmanager list."""
    explicit = getattr(args, "taskmanager_id", None)
    if explicit:
        return explicit, None
    if parsed.taskmanager_id:
        return parsed.taskmanager_id, None
    host = getattr(args, "taskmanager_host", None)
    index = getattr(args, "taskmanager_index", None)
    if host is None and index is None:
        return None, None
    data = await client.get_json("taskmanagers")
    taskmanagers = list(data.get("taskmanagers", [])) if isinstance(data, dict) else []
    if host:
        taskmanagers = [
            tm for tm in taskmanagers if host in str(tm.get("id", "")) or host in str(tm.get("path", ""))
        ]
    taskmanager = pick_one(taskmanagers, index=index, label="taskmanager")
    return str(taskmanager.get("id")), taskmanager


async def resolve_context(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> ResolveResult:
    """Resolve all dynamic context requested by flags and URL route."""
    version = await detect_flink_version(client, args)
    profile = select_endpoint_profile(version, getattr(args, "endpoint_profile", "auto"))
    result = ResolveResult(parsed=parsed, flink_version=version, endpoint_profile=profile)
    needs_job = command_needs_job(args)
    if needs_job:
        result.job_id, result.job = await resolve_job_id(client, parsed, args)
    if result.job_id and command_needs_vertex(args):
        result.vertex_id, result.vertex = await resolve_vertex_id(client, parsed, args, result.job_id)
    if command_needs_taskmanager(args):
        result.taskmanager_id, result.taskmanager = await resolve_taskmanager_id(client, parsed, args)
    result.subtask = parse_subtask(args)
    return result


def parse_subtask(args: argparse.Namespace) -> int | None:
    """Return the requested subtask index if present."""
    value = getattr(args, "subtask", None)
    if value is None:
        return None
    return int(value)


def command_needs_job(args: argparse.Namespace) -> bool:
    """Return whether a command needs a job id."""
    cmd = getattr(args, "command", "")
    sub = getattr(args, "subcommand", None)
    if cmd == "resolve":
        return bool(
            getattr(args, "job_name", None)
            or getattr(args, "vertex_name", None)
            or getattr(args, "task_chain_name", None)
            or (getattr(args, "job_id", "auto") != "auto")
        )
    if cmd in {"backpressure", "metrics", "diagnose"}:
        return True
    if cmd == "metric" and getattr(args, "scope", None) in {"job", "task-chain", "auto", "subtask", "jm-operator"}:
        return bool(parsed_route_has_job_hint(args))
    if cmd == "job" and sub not in {None}:
        return True
    if cmd == "task-chain":
        return True
    return False


def command_needs_vertex(args: argparse.Namespace) -> bool:
    """Return whether a command needs a vertex id."""
    cmd = getattr(args, "command", "")
    if cmd == "resolve":
        return bool(
            getattr(args, "vertex_name", None)
            or getattr(args, "task_chain_name", None)
            or getattr(args, "vertex_id", None)
            or getattr(args, "task_chain_id", None)
        )
    if cmd == "diagnose" and getattr(args, "playbook", None) in {"source", "sink", "source-lag"}:
        return True
    if cmd in {"metrics"}:
        return bool(getattr(args, "vertex_id", None) or getattr(args, "task_chain_id", None) or getattr(args, "vertex_name", None) or getattr(args, "task_chain_name", None))
    if cmd == "metric" and getattr(args, "scope", None) in {"task-chain", "auto", "subtask", "jm-operator"}:
        return bool(
            getattr(args, "vertex_id", None)
            or getattr(args, "task_chain_id", None)
            or getattr(args, "vertex_name", None)
            or getattr(args, "task_chain_name", None)
            or getattr(args, "scope", None) in {"subtask", "jm-operator"}
        )
    if cmd == "task-chain":
        return True
    return False


def parsed_route_has_job_hint(args: argparse.Namespace) -> bool:
    """Return whether flags imply job-level metric resolution."""
    return bool(
        getattr(args, "job_id", "auto") != "auto"
        or getattr(args, "job_name", None)
        or getattr(args, "vertex_id", None)
        or getattr(args, "task_chain_id", None)
        or getattr(args, "vertex_name", None)
        or getattr(args, "task_chain_name", None)
    )


def command_needs_taskmanager(args: argparse.Namespace) -> bool:
    """Return whether a command needs a TaskManager id."""
    if getattr(args, "command", "") == "resolve":
        return bool(
            getattr(args, "taskmanager_id", None)
            or getattr(args, "taskmanager_host", None)
            or getattr(args, "taskmanager_index", None) is not None
        )
    if getattr(args, "command", "") == "logs" and getattr(args, "scope", None) == "taskmanager":
        return True
    return getattr(args, "command", "") == "taskmanager" and getattr(args, "subcommand", None) not in {"list", "memory-top", None}


def metric_ids(metrics: Any) -> list[str]:
    """Extract metric ids from a Flink metrics-list response."""
    if not isinstance(metrics, list):
        return []
    return [str(item.get("id")) for item in metrics if isinstance(item, dict) and item.get("id")]


def match_metric_ids(available: list[str], requested: list[str], mode: str = "auto") -> list[str]:
    """Resolve requested metric names against available metric ids."""
    resolved: list[str] = []
    for item in requested:
        if item in _METRIC_ALIASES:
            alias_matches = match_metric_alias(available, item)
            if alias_matches:
                resolved.extend(alias_matches)
                continue
        if item in available:
            resolved.append(item)
            continue
        matches: list[str] = []
        if mode in {"auto", "suffix"}:
            matches = [metric for metric in available if metric.endswith(item)]
        if not matches and mode in {"auto", "contains"}:
            matches = [metric for metric in available if item in metric]
        if not matches:
            resolved.append(item)
        elif len(matches) == 1:
            resolved.append(matches[0])
        else:
            raise FlinkDiagError(f"Metric {item!r} matched multiple candidates: {compact_json(matches[:20])}")
    return resolved


def match_metric_alias(available: list[str], alias: str) -> list[str]:
    """Resolve a semantic metric alias to available metric ids."""
    matches: list[str] = []
    for pattern in _METRIC_ALIASES.get(alias, []):
        compiled = re.compile(pattern)
        matches.extend(metric for metric in available if compiled.search(metric))
    return dedupe(matches)


def dedupe(values: list[str]) -> list[str]:
    """Return values with duplicates removed while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def search_metric_ids(available: list[str], keyword: str, *, regex: bool = False) -> list[str]:
    """Search available metric ids by keyword or regex."""
    if regex:
        compiled = re.compile(keyword)
        return [metric for metric in available if compiled.search(metric)]
    return [metric for metric in available if keyword.lower() in metric.lower()]


def parse_metric_identifier(metric_id: str) -> dict[str, Any]:
    """按官网 Dashboard 规则解析 task/operator metric 名称。"""
    parts = str(metric_id).split(".")
    if len(parts) >= 2 and parts[0].isdigit():
        if len(parts) == 2:
            return {
                "id": metric_id,
                "kind": "task",
                "subtask": int(parts[0]),
                "operator": None,
                "metric_name": parts[1],
            }
        return {
            "id": metric_id,
            "kind": "operator",
            "subtask": int(parts[0]),
            "operator": ".".join(parts[1:-1]),
            "metric_name": parts[-1],
        }
    return {
        "id": metric_id,
        "kind": "metric",
        "subtask": None,
        "operator": None,
        "metric_name": metric_id,
    }


def structure_metric_matches(metrics: list[str], *, source: str | None = None, path: str | None = None) -> list[dict[str, Any]]:
    """把 metric id 列表转换成结构化搜索结果。"""
    rows: list[dict[str, Any]] = []
    for metric in metrics:
        row = parse_metric_identifier(metric)
        if source:
            row["source"] = source
        if path:
            row["path"] = path
        rows.append(row)
    return rows


def metric_explanation(metric: str) -> dict[str, Any]:
    """Explain a metric id or semantic alias."""
    if metric in _METRIC_EXPLANATIONS:
        return {"metric": metric, "kind": "alias", "explanation": _METRIC_EXPLANATIONS[metric], "patterns": _METRIC_ALIASES.get(metric, [])}
    for alias, patterns in _METRIC_ALIASES.items():
        if any(re.compile(pattern).search(metric) for pattern in patterns):
            return {"metric": metric, "kind": "metric", "alias": alias, "explanation": _METRIC_EXPLANATIONS.get(alias, "")}
    return {"metric": metric, "kind": "metric", "explanation": _METRIC_EXPLANATIONS.get(metric, "No built-in explanation yet.")}


def quote_metric_query_value(value: str) -> str:
    """按 Flink 官方 REST 规则转义单个 metric 查询值。"""
    return quote(str(value), safe="")


def comma_join_query_values(values: list[str] | None) -> str | None:
    """逐个转义列表值，再用逗号作为 Flink REST 参数分隔符。"""
    if not values:
        return None
    return ",".join(quote_metric_query_value(value) for value in values)


def build_metric_query(
    path: str,
    *,
    metrics: list[str] | None = None,
    agg: list[str] | None = None,
    taskmanagers: list[str] | None = None,
    jobs: list[str] | None = None,
    subtasks: list[str] | None = None,
) -> str:
    """构造 Flink 官方 metrics REST 查询，保留逗号分隔但转义每个值。"""
    params: list[tuple[str, str]] = []
    for key, values in (
        ("get", metrics),
        ("agg", agg),
        ("taskmanagers", taskmanagers),
        ("jobs", jobs),
        ("subtask", subtasks),
    ):
        joined = comma_join_query_values(values)
        if joined:
            params.append((key, joined))
    if not params:
        return path
    query = "&".join(f"{key}={value}" for key, value in params)
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}{query}"


def query_with_get(path: str, metrics: list[str]) -> str:
    """兼容旧调用：给 metrics REST path 追加 get 查询参数。"""
    return build_metric_query(path, metrics=metrics)


async def fetch_metrics_by_chunks(
    client: FlinkClient,
    base_path: str,
    metrics: list[str],
    *,
    chunk_size: int = 20,
    agg: list[str] | None = None,
    taskmanagers: list[str] | None = None,
    jobs: list[str] | None = None,
    subtasks: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch metric values in bounded chunks to avoid long URLs."""
    if not metrics:
        return []
    chunks = [metrics[index : index + chunk_size] for index in range(0, len(metrics), chunk_size)]
    results = await asyncio.gather(
        *(
            client.get_json(
                build_metric_query(
                    base_path,
                    metrics=chunk,
                    agg=agg,
                    taskmanagers=taskmanagers,
                    jobs=jobs,
                    subtasks=subtasks,
                )
            )
            for chunk in chunks
        )
    )
    merged: list[dict[str, Any]] = []
    for result in results:
        if isinstance(result, list):
            merged.extend(item for item in result if isinstance(item, dict))
    return merged


def summarize_vertex(vertex: dict[str, Any], backpressure: dict[str, Any] | None = None) -> dict[str, Any]:
    """Summarize a Flink job vertex / task chain."""
    metrics = vertex.get("metrics") or {}
    subtasks = vertex.get("subtasks") or []
    aggregated = vertex.get("aggregated") or {}
    bp_subtasks = (backpressure or {}).get("subtasks") or []
    busy_values = [float(item.get("busyRatio", 0.0)) for item in bp_subtasks if isinstance(item, dict)]
    backpressure_values = [float(item.get("ratio", 0.0)) for item in bp_subtasks if isinstance(item, dict)]
    idle_values = [float(item.get("idleRatio", 0.0)) for item in bp_subtasks if isinstance(item, dict)]
    return {
        "vertex_id": vertex.get("id"),
        "name": vertex.get("name"),
        "status": vertex.get("status"),
        "parallelism": vertex.get("parallelism"),
        "maxParallelism": vertex.get("maxParallelism"),
        "duration": vertex.get("duration"),
        "tasks": vertex.get("tasks"),
        "bytes_received": metrics.get("read-bytes"),
        "records_received": metrics.get("read-records"),
        "bytes_sent": metrics.get("write-bytes"),
        "records_sent": metrics.get("write-records"),
        "accumulated_backpressured_time": metrics.get("accumulated-backpressured-time"),
        "accumulated_busy_time": metrics.get("accumulated-busy-time"),
        "accumulated_idle_time": metrics.get("accumulated-idle-time"),
        "backpressure_level": (backpressure or {}).get("backpressureLevel") or (backpressure or {}).get("backpressure-level"),
        "backpressured_max": max(backpressure_values) if backpressure_values else None,
        "busy_max": max(busy_values) if busy_values else None,
        "idle_max": max(idle_values) if idle_values else None,
        "subtask_count": len(subtasks),
        "aggregated": aggregated,
    }


def classify_io_vertex_semantics(name: Any) -> str:
    """按 task chain 名称识别 Source、Paimon/Sink Writer、Committer 等 IO 语义。"""
    lowered = str(name or "").lower()
    if "source" in lowered:
        return "source"
    if "global committer" in lowered or "committer" in lowered:
        return "sink_committer"
    if re.search(r"\bwriter\s*:", str(name or ""), flags=re.IGNORECASE):
        return "sink_writer"
    if "sink" in lowered:
        return "sink"
    return "operator"


def io_diagnosis_note(diagnosis: str) -> str | None:
    """为容易误读的 IO 诊断标签补充说明。"""
    notes = {
        "sink_writer_internal_records": "Sink Writer 的 Records Out 常是 committable/下游提交消息，不等于业务记录被过滤或丢弃。",
        "sink_committer_terminal": "Global Committer 是终端提交节点，Records Out 为 0 通常是预期现象。",
        "stops_here_or_terminal_sink": "终端 sink 没有下游 Flink operator 时 Records Sent 可以为 0。",
    }
    return notes.get(diagnosis)


def summarize_io_flow(vertices: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize input/output volume for each task chain."""
    rows: list[dict[str, Any]] = []
    for index, vertex in enumerate(vertices):
        metrics = vertex.get("metrics") or {}
        records_in = numeric_value(metrics.get("read-records"))
        records_out = numeric_value(metrics.get("write-records"))
        bytes_in = numeric_value(metrics.get("read-bytes"))
        bytes_out = numeric_value(metrics.get("write-bytes"))
        records_delta = numeric_value(records_in - records_out) if records_in is not None and records_out is not None else None
        bytes_delta = numeric_value(bytes_in - bytes_out) if bytes_in is not None and bytes_out is not None else None
        semantics = classify_io_vertex_semantics(vertex.get("name"))
        diagnosis = diagnose_io_row(records_in, records_out, vertex.get("name"))
        row = {
            "order": index,
            "vertex_id": vertex.get("id"),
            "name": vertex.get("name"),
            "status": vertex.get("status"),
            "parallelism": vertex.get("parallelism"),
            "io_semantics": semantics,
            "records_in": records_in,
            "records_out": records_out,
            "records_delta": records_delta,
            "pass_through_pct": ratio_pct(records_out, records_in),
            "bytes_in": bytes_in,
            "bytes_out": bytes_out,
            "bytes_delta": bytes_delta,
            "bytes_in_human": bytes_human(bytes_in),
            "bytes_out_human": bytes_human(bytes_out),
            "diagnosis": diagnosis,
            "diagnosis_note": io_diagnosis_note(diagnosis),
        }
        rows.append(row)
    ranked = sorted(
        [row for row in rows if to_float(row.get("records_delta")) is not None],
        key=lambda item: float(item.get("records_delta") or 0),
        reverse=True,
    )
    filter_ranked = [row for row in ranked if row.get("diagnosis") == "filters_or_drops_records"]
    return {
        "taskchains": rows,
        "largest_drop": ranked[0] if ranked else None,
        "largest_filter_drop": filter_ranked[0] if filter_ranked else None,
    }


def numeric_value(value: Any) -> int | float | None:
    """Return a numeric value, preserving integers when possible."""
    number = to_float(value)
    if number is None:
        return None
    return int(number) if number.is_integer() else number


def ratio_pct(numerator: Any, denominator: Any) -> float | None:
    """Return numerator/denominator as a rounded percentage."""
    numerator_float = to_float(numerator)
    denominator_float = to_float(denominator)
    if numerator_float is None or not denominator_float:
        return None
    return round(numerator_float / denominator_float * 100, 2)


def diagnose_io_row(records_in: Any, records_out: Any, name: Any = None) -> str:
    """Return a short diagnostic label for a task-chain IO row."""
    in_float = to_float(records_in)
    out_float = to_float(records_out)
    if in_float is None or out_float is None:
        return "missing_metrics"
    semantics = classify_io_vertex_semantics(name)
    if in_float == 0 and out_float == 0:
        return "no_records"
    if in_float == 0 and out_float > 0:
        return "source_or_generated_records"
    if semantics == "sink_committer" and in_float > 0:
        return "sink_committer_terminal"
    if semantics == "sink_writer" and in_float > 0 and out_float < in_float:
        return "sink_writer_internal_records"
    if in_float > 0 and out_float == 0:
        return "stops_here_or_terminal_sink"
    if in_float > 0 and out_float < in_float:
        return "filters_or_drops_records"
    if in_float > 0 and out_float > in_float:
        return "expands_records"
    return "passes_through"


def bytes_human(value: Any) -> str | None:
    """Format a byte value using binary units."""
    number = to_float(value)
    if number is None:
        return None
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(number) < 1024 or unit == "TiB":
            return f"{number:.2f} {unit}"
        number /= 1024
    return None


def millis_human(value: Any) -> str | None:
    """Format a millisecond duration."""
    number = to_float(value)
    if number is None:
        return None
    if number < 1000:
        return f"{number:.0f} ms"
    seconds = number / 1000
    if seconds < 60:
        return f"{seconds:.2f} s"
    return f"{seconds / 60:.2f} min"


def to_float(value: Any) -> float | None:
    """Convert a value to float if possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pct(used: Any, total: Any) -> float | None:
    """Return used/total as a percentage."""
    used_float = to_float(used)
    total_float = to_float(total)
    if used_float is None or not total_float:
        return None
    return round(used_float / total_float * 100, 2)


def summarize_checkpoint_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Summarize checkpoint history and configuration."""
    checkpoints = data.get("checkpoints", data)
    config = data.get("configuration") or data.get("checkpoint_configuration") or {}
    latest = (checkpoints.get("latest") or {}).get("completed") if isinstance(checkpoints, dict) else None
    return {
        "state_backend": config.get("state_backend"),
        "checkpoint_storage": config.get("checkpoint_storage"),
        "mode": config.get("mode"),
        "interval": millis_human(config.get("interval")),
        "timeout": millis_human(config.get("timeout")),
        "max_concurrent": config.get("max_concurrent"),
        "tolerable_failed_checkpoints": config.get("tolerable_failed_checkpoints"),
        "externalization": config.get("externalization"),
        "unaligned_checkpoints": config.get("unaligned_checkpoints"),
        "state_changelog_enabled": config.get("state_changelog_enabled"),
        "counts": checkpoints.get("counts") if isinstance(checkpoints, dict) else None,
        "latest_completed": {
            "id": latest.get("id") if latest else None,
            "checkpointed_size": bytes_human(latest.get("checkpointed_size")) if latest else None,
            "state_size": bytes_human(latest.get("state_size")) if latest else None,
            "end_to_end_duration": millis_human(latest.get("end_to_end_duration")) if latest else None,
            "external_path": latest.get("external_path") if latest else None,
        },
    }


def summarize_checkpoint_trend(data: dict[str, Any], limit: int = 10) -> dict[str, Any]:
    """Summarize recent checkpoint history."""
    history = data.get("history", []) if isinstance(data, dict) else []
    if isinstance(history, dict):
        history = history.get("completed", []) + history.get("failed", [])
    rows = [item for item in history if isinstance(item, dict)]
    rows = rows[-limit:] if limit else rows
    durations = [to_float(item.get("end_to_end_duration")) for item in rows]
    durations = [value for value in durations if value is not None]
    sizes = [to_float(item.get("state_size") or item.get("checkpointed_size")) for item in rows]
    sizes = [value for value in sizes if value is not None]
    failed = sum(1 for item in rows if str(item.get("status", "")).upper() == "FAILED")
    return {
        "count": len(rows),
        "completed": sum(1 for item in rows if str(item.get("status", "")).upper() == "COMPLETED"),
        "failed": failed,
        "duration_avg": millis_human(sum(durations) / len(durations)) if durations else None,
        "duration_max": millis_human(max(durations)) if durations else None,
        "state_size_first": bytes_human(sizes[0]) if sizes else None,
        "state_size_last": bytes_human(sizes[-1]) if sizes else None,
        "state_size_growth": bytes_human(sizes[-1] - sizes[0]) if len(sizes) >= 2 else None,
        "recent": rows,
    }


def first_exception_line(stacktrace: Any) -> str | None:
    """提取异常堆栈第一行，便于摘要展示。"""
    text = str(stacktrace or "").strip()
    if not text:
        return None
    return text.splitlines()[0][:500]


def exception_type_from_text(text: Any) -> str | None:
    """从 root exception 文本中提取异常类型。"""
    first = first_exception_line(text)
    if not first:
        return None
    return first.split(":", 1)[0].strip() or first


def exception_history_entries(data: dict[str, Any]) -> list[dict[str, Any]]:
    """兼容 Flink 1.15/1.18 的 exception history 字段。"""
    entries: list[dict[str, Any]] = []
    history = data.get("exceptionHistory") if isinstance(data, dict) else None
    if isinstance(history, dict):
        entries.extend(item for item in history.get("entries", []) if isinstance(item, dict))
    all_exceptions = data.get("all-exceptions") if isinstance(data, dict) else None
    if isinstance(all_exceptions, list):
        for item in all_exceptions:
            if not isinstance(item, dict):
                continue
            if item not in entries:
                entries.append(item)
    return entries


def summarize_job_exceptions(data: dict[str, Any], *, limit: int = 10) -> dict[str, Any]:
    """汇总 root exception 和 exception history，保留原始 REST 字段兼容不同 Flink 版本。"""
    root = data.get("root-exception") if isinstance(data, dict) else None
    entries = exception_history_entries(data if isinstance(data, dict) else {})
    by_type: Counter[str] = Counter()
    by_task: Counter[str] = Counter()
    by_location: Counter[str] = Counter()
    compact_entries: list[dict[str, Any]] = []
    for item in entries:
        name = str(item.get("exceptionName") or exception_type_from_text(item.get("stacktrace")) or "unknown")
        task = str(item.get("taskName") or "")
        location = str(item.get("location") or "")
        by_type[name] += 1
        if task:
            by_task[task] += 1
        if location:
            by_location[location] += 1
        compact_entries.append(
            {
                "exceptionName": name,
                "timestamp": item.get("timestamp"),
                "taskName": item.get("taskName"),
                "location": item.get("location"),
                "first_line": first_exception_line(item.get("stacktrace")),
                "concurrent_exception_count": len(item.get("concurrentExceptions") or []),
            }
        )
    compact_entries.sort(key=lambda item: int(item.get("timestamp") or 0), reverse=True)
    return {
        "has_root_exception": bool(root),
        "root_exception_type": exception_type_from_text(root),
        "root_exception_first_line": first_exception_line(root),
        "root_exception_timestamp": data.get("timestamp") if isinstance(data, dict) else None,
        "history_count": len(entries),
        "all_exceptions_count": len(data.get("all-exceptions") or []) if isinstance(data, dict) and isinstance(data.get("all-exceptions"), list) else 0,
        "history_truncated": bool(((data.get("exceptionHistory") or {}) if isinstance(data, dict) else {}).get("truncated")),
        "root_truncated": bool(data.get("truncated")) if isinstance(data, dict) else False,
        "top_exception_types": [{"exceptionName": name, "count": count} for name, count in by_type.most_common(limit)],
        "top_tasks": [{"taskName": name, "count": count} for name, count in by_task.most_common(limit)],
        "top_locations": [{"location": name, "count": count} for name, count in by_location.most_common(limit)],
        "recent": compact_entries[:limit],
        "fields_present": {
            "root-exception": "root-exception" in data if isinstance(data, dict) else False,
            "all-exceptions": "all-exceptions" in data if isinstance(data, dict) else False,
            "exceptionHistory": "exceptionHistory" in data if isinstance(data, dict) else False,
        },
    }


def summarize_subtask_skew(data: dict[str, Any]) -> dict[str, Any]:
    """Summarize subtask metric skew from an all-subtasks metrics payload."""
    rows = data.get("subtasks", []) if isinstance(data, dict) else []
    metric_values: dict[str, list[tuple[int, float]]] = {}
    for row in rows:
        subtask = int(row.get("subtask", 0))
        metrics = row.get("metrics", {})
        if not isinstance(metrics, dict):
            continue
        for name, value in metrics.items():
            number = to_float(value)
            if number is None:
                continue
            metric_values.setdefault(str(name), []).append((subtask, number))
    summaries: dict[str, Any] = {}
    for name, values in metric_values.items():
        only_values = [value for _, value in values]
        positive = [value for value in only_values if value > 0]
        max_subtask, max_value = max(values, key=lambda item: item[1])
        min_value = min(only_values)
        min_positive = min(positive) if positive else None
        summaries[name] = {
            "min": numeric_value(min_value),
            "max": numeric_value(max_value),
            "avg": round(sum(only_values) / len(only_values), 4) if only_values else None,
            "max_subtask": max_subtask,
            "skew_ratio": round(max_value / min_positive, 4) if min_positive else None,
            "top_subtasks": [
                {"subtask": subtask, "value": numeric_value(value)}
                for subtask, value in sorted(values, key=lambda item: item[1], reverse=True)[:5]
            ],
        }
    return {"job_id": data.get("job_id"), "vertex_id": data.get("vertex_id"), "metrics": summaries}


def summarize_taskmanager_aggregates(
    data: dict[str, Any],
    metrics: list[str] | None = None,
    sort_by: str | None = None,
    top: int | None = None,
) -> dict[str, Any]:
    """Summarize per-TaskManager aggregated metrics from a task-chain taskmanagers payload."""
    taskmanagers = data.get("taskmanagers", []) if isinstance(data, dict) else []
    requested = metrics or list(_TASKMANAGER_AGGREGATE_DEFAULT_METRICS)
    rows: list[dict[str, Any]] = []
    for item in taskmanagers:
        if not isinstance(item, dict):
            continue
        aggregate_metrics = ((item.get("aggregated") or {}).get("metrics") or {})
        selected_metrics = {
            metric: aggregate_metrics.get(metric)
            for metric in requested
            if isinstance(aggregate_metrics.get(metric), dict)
        }
        summary = {
            metric: (values.get("sum") if isinstance(values, dict) else None)
            for metric, values in selected_metrics.items()
        }
        rows.append(
            {
                "taskmanager_id": item.get("taskmanager-id") or item.get("id"),
                "host": item.get("host"),
                "status": item.get("status"),
                "duration": item.get("duration"),
                "status_counts": item.get("status-counts") or {},
                "summary": summary,
                "metrics": selected_metrics,
            }
        )
    active_sort = sort_by or (requested[0] if requested else None)
    if active_sort:
        rows.sort(key=lambda row: to_float((row.get("summary") or {}).get(active_sort)) or 0.0, reverse=True)
    if top is not None and top > 0:
        rows = rows[:top]
    return {
        "vertex_id": data.get("id"),
        "name": data.get("name"),
        "taskmanager_count": len(taskmanagers),
        "metrics": requested,
        "sort_by": active_sort,
        "rows": rows,
    }


def capacity_metric_matches(metric_id: str, wanted: str) -> bool:
    """判断一个实际 metric id 是否对应容量诊断需要的短 metric 名。"""
    return metric_id == wanted or metric_id.endswith("." + wanted)


def subtask_metric_value(metrics: dict[str, Any], wanted: str) -> float | None:
    """从单个 subtask 的 metrics 中提取并合并目标指标值。"""
    values = [
        number
        for key, value in metrics.items()
        if capacity_metric_matches(str(key), wanted) and (number := to_float(value)) is not None
    ]
    if not values:
        return None
    return sum(values)


def summarize_capacity_subtask_metrics(data: dict[str, Any], requested: list[str] | None = None) -> dict[str, Any]:
    """把 all-subtasks metric payload 汇总成容量诊断需要的 min/max/avg/sum/倾斜信息。"""
    rows = data.get("subtasks", []) if isinstance(data, dict) else []
    summaries: dict[str, Any] = {}
    for metric in requested or _CAPACITY_METRICS:
        values: list[tuple[int, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
            value = subtask_metric_value(metrics, metric)
            if value is None:
                continue
            values.append((int(row.get("subtask", 0)), value))
        if not values:
            continue
        only_values = [value for _, value in values]
        positive = [value for value in only_values if value > 0]
        min_positive = min(positive) if positive else None
        max_subtask, max_value = max(values, key=lambda item: item[1])
        active = sum(1 for value in only_values if value > 0)
        summaries[metric] = {
            "min": numeric_value(min(only_values)),
            "max": numeric_value(max_value),
            "avg": round(sum(only_values) / len(only_values), 4),
            "sum": numeric_value(sum(only_values)),
            "active_subtasks": active,
            "zero_subtasks": len(only_values) - active,
            "max_subtask": max_subtask,
            "skew_ratio": round(max_value / min_positive, 4) if min_positive else None,
            "top_subtasks": [
                {"subtask": subtask, "value": numeric_value(value)}
                for subtask, value in sorted(values, key=lambda item: item[1], reverse=True)[:5]
            ],
        }
    return summaries


def summarize_taskmanager_distribution(taskmanager_summary: dict[str, Any] | None) -> dict[str, Any]:
    """从 taskmanager-aggregates 结果中提取 slot/subtask 分布和吞吐倾斜。"""
    if not isinstance(taskmanager_summary, dict):
        return {
            "available": False,
            "reason": "missing_taskmanager_aggregates",
            "taskmanager_count": 0,
            "running_subtasks_min": None,
            "running_subtasks_max": None,
            "running_subtasks_skew_ratio": None,
            "read_records_skew_ratio": None,
            "rows": [],
        }
    rows = (taskmanager_summary or {}).get("rows", []) if isinstance(taskmanager_summary, dict) else []
    running_counts: list[float] = []
    read_records: list[float] = []
    compact_rows: list[dict[str, Any]] = []
    has_metric_payload = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        summary_payload = row.get("summary") if isinstance(row.get("summary"), dict) else {}
        metrics_payload = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        if summary_payload or metrics_payload:
            has_metric_payload = True
        running = to_float((row.get("status_counts") or {}).get("RUNNING")) or 0.0
        read = to_float(summary_payload.get("read-records"))
        if running > 0:
            running_counts.append(running)
        if read is not None:
            read_records.append(read)
        compact_rows.append(
            {
                "taskmanager_id": row.get("taskmanager_id"),
                "host": row.get("host"),
                "running_subtasks": numeric_value(running),
                "read_records": numeric_value(read),
                "accumulated_backpressured_time": summary_payload.get("accumulated-backpressured-time"),
            }
        )
    min_running = min(running_counts) if running_counts else None
    max_running = max(running_counts) if running_counts else None
    positive_reads = [value for value in read_records if value > 0]
    available = bool(has_metric_payload)
    return {
        "available": available,
        "reason": None if available else ("empty_taskmanager_aggregates" if rows else "no_taskmanager_rows"),
        "taskmanager_count": len(rows),
        "running_subtasks_min": numeric_value(min_running),
        "running_subtasks_max": numeric_value(max_running),
        "running_subtasks_skew_ratio": round(max_running / min_running, 4) if min_running and max_running else None,
        "read_records_skew_ratio": round(max(positive_reads) / min(positive_reads), 4) if len(positive_reads) >= 2 and min(positive_reads) > 0 else None,
        "rows": compact_rows,
    }


def capacity_finding(level: str, area: str, message: str, evidence: Any) -> dict[str, Any]:
    """构造容量诊断 finding，统一字段名便于 JSONL/脚本消费。"""
    return {"level": level, "area": area, "message": message, "evidence": evidence}


def capacity_skew_is_meaningful(metric_name: str, summary: dict[str, Any]) -> bool:
    """避免低绝对值指标因比例放大产生无意义的倾斜告警。"""
    max_value = to_float(summary.get("max")) or 0.0
    if max_value <= 0:
        return False
    if metric_name == "busyTimeMsPerSecond":
        return max_value >= 300
    if metric_name == "backPressuredTimeMsPerSecond":
        return max_value >= 100
    if metric_name == "checkpointStartDelayNanos":
        return max_value >= 1_000_000_000
    return True


def summarize_vertex_capacity(
    vertex: dict[str, Any],
    graph_row: dict[str, Any] | None,
    subtask_data: dict[str, Any] | None,
    taskmanager_summary: dict[str, Any] | None,
    connector: dict[str, Any] | None,
    *,
    subtask_error: str | None = None,
    taskmanager_error: str | None = None,
) -> dict[str, Any]:
    """汇总单个 task chain 的并行度、吞吐、busy/idle、反压和分布情况。"""
    vertex_id = str(vertex.get("id"))
    parallelism = int(vertex.get("parallelism", 0) or 0)
    metrics = summarize_capacity_subtask_metrics(subtask_data or {})
    busy = metrics.get("busyTimeMsPerSecond", {})
    idle = metrics.get("idleTimeMsPerSecond", {})
    backpressured = metrics.get("backPressuredTimeMsPerSecond", {})
    records_in = metrics.get("numRecordsInPerSecond", {})
    records_out = metrics.get("numRecordsOutPerSecond", {})
    tm_distribution = summarize_taskmanager_distribution(taskmanager_summary)
    paimon_role = (connector or {}).get("paimon_role") if isinstance(connector, dict) else None
    role = paimon_role if paimon_role in {"writer", "committer", "source"} else classify_io_vertex_semantics(vertex.get("name"))
    findings: list[dict[str, Any]] = []

    backpressure_ratio = to_float((graph_row or {}).get("backpressured_max"))
    backpressure_ms_max = to_float(backpressured.get("max"))
    busy_max = to_float(busy.get("max"))
    busy_avg = to_float(busy.get("avg"))
    idle_avg = to_float(idle.get("avg"))
    if (backpressure_ratio is not None and backpressure_ratio >= 0.5) or (
        backpressure_ms_max is not None and backpressure_ms_max >= _CAPACITY_BACKPRESSURE_HIGH_MS
    ):
        findings.append(
            capacity_finding(
                "warning",
                "backpressure",
                "该 task chain 出现明显反压，优先排查下游处理或 sink 写入能力。",
                {"backpressured_max": backpressure_ratio, "backPressuredTimeMsPerSecond": backpressured},
            )
        )
    if (busy_max is not None and busy_max >= _CAPACITY_BUSY_HIGH_MS) or (busy_avg is not None and busy_avg >= _CAPACITY_BUSY_HIGH_MS):
        findings.append(
            capacity_finding(
                "warning",
                "parallelism",
                "该 task chain busy 较高，并行度或单 subtask 处理能力可能限制吞吐。",
                {"busyTimeMsPerSecond": busy},
            )
        )
    elif (
        (backpressure_ratio is None or backpressure_ratio == 0)
        and (backpressure_ms_max is None or backpressure_ms_max == 0)
        and busy_max is not None
        and busy_max < 300
        and idle_avg is not None
        and idle_avg >= 700
    ):
        findings.append(
            capacity_finding(
                "info",
                "headroom",
                "busy 较低且 idle 较高，当前采样没有显示并行度不足。",
                {"busyTimeMsPerSecond": busy, "idleTimeMsPerSecond": idle},
            )
        )

    for metric_name, summary in metrics.items():
        ratio = to_float(summary.get("skew_ratio"))
        if ratio is not None and ratio >= _CAPACITY_SKEW_RATIO and capacity_skew_is_meaningful(metric_name, summary):
            findings.append(
                capacity_finding(
                    "warning",
                    "skew",
                    f"{metric_name} 在 subtasks 之间倾斜明显，可能导致局部吞吐受限。",
                    summary,
                )
            )
            break

    active_metric = records_in if to_float(records_in.get("sum")) else records_out
    active_subtasks = int(active_metric.get("active_subtasks") or 0)
    if parallelism > 1 and 0 < active_subtasks < parallelism:
        findings.append(
            capacity_finding(
                "info",
                "active_subtasks",
                "只有部分 subtasks 在当前采样窗口有吞吐；需要结合 source 分区数或 key 分布判断是否浪费并行度。",
                {"parallelism": parallelism, "active_subtasks": active_subtasks, "metric": active_metric},
            )
        )

    tm_skew = to_float(tm_distribution.get("running_subtasks_skew_ratio"))
    if tm_skew is not None and tm_skew >= _CAPACITY_SKEW_RATIO:
        findings.append(
            capacity_finding(
                "warning",
                "taskmanager_distribution",
                "该 task chain 在 TaskManager 上的 subtask 分布不均，可能造成局部资源热点。",
                tm_distribution,
            )
        )

    if role == "writer":
        findings.append(
            capacity_finding(
                "info",
                "sink_semantics",
                "Paimon Writer 的 Records Out 通常是提交消息，不应直接按业务记录过滤率解读。",
                {"io_semantics": "paimon_writer"},
            )
        )
    if role == "committer" and parallelism == 1 and busy_max is not None and busy_max < _CAPACITY_BUSY_HIGH_MS:
        findings.append(
            capacity_finding(
                "info",
                "committer",
                "Global Committer 并行度为 1 是常见提交语义；当前 busy 未显示单点饱和。",
                {"busyTimeMsPerSecond": busy, "parallelism": parallelism},
            )
        )

    if subtask_error:
        findings.append(capacity_finding("info", "metrics", "subtask metrics 读取失败，报告缺少细粒度倾斜信息。", subtask_error))
    if taskmanager_error:
        findings.append(capacity_finding("info", "taskmanager_distribution", "TaskManager 聚合读取失败，报告缺少 TM 分布信息。", taskmanager_error))

    warning_count = sum(1 for item in findings if item.get("level") in {"warning", "critical"})
    if any(item.get("area") == "backpressure" for item in findings):
        assessment = "backpressured"
    elif any(item.get("area") == "parallelism" for item in findings):
        assessment = "parallelism_may_be_low"
    elif warning_count:
        assessment = "needs_attention"
    elif any(item.get("area") == "headroom" for item in findings):
        assessment = "has_headroom"
    else:
        assessment = "balanced"

    return {
        "vertex_id": vertex_id,
        "name": vertex.get("name"),
        "status": vertex.get("status"),
        "parallelism": parallelism,
        "maxParallelism": vertex.get("maxParallelism"),
        "role": role,
        "assessment": assessment,
        "backpressure_level": (graph_row or {}).get("backpressure_level"),
        "backpressured_max": (graph_row or {}).get("backpressured_max"),
        "metrics": metrics,
        "taskmanager_distribution": tm_distribution,
        "findings": findings,
    }


def capacity_recommendations(vertex_reports: list[dict[str, Any]], paimon: dict[str, Any] | None, lookup_join: dict[str, Any] | None = None) -> list[str]:
    """根据容量报告和 Paimon 风险生成下一步建议。"""
    recommendations: list[str] = []
    warning_areas = {
        str(finding.get("area"))
        for vertex in vertex_reports
        for finding in vertex.get("findings", [])
        if finding.get("level") in {"warning", "critical"}
    }
    paimon_risks = [
        item
        for item in ((paimon or {}).get("risks", []) if isinstance(paimon, dict) else [])
        if isinstance(item, dict) and item.get("level") in {"warning", "critical"}
    ]
    paimon_types = {str(item.get("type")) for item in paimon_risks if isinstance(item, dict)}
    lookup_risks = [
        item
        for item in ((lookup_join or {}).get("risks", []) if isinstance(lookup_join, dict) else [])
        if isinstance(item, dict) and item.get("level") in {"warning", "critical"}
    ]
    lookup_types = {str(item.get("type")) for item in lookup_risks if isinstance(item, dict)}
    if "backpressure" in warning_areas:
        recommendations.append("先沿反压链路向下游定位瓶颈，再决定是否调整并行度或 sink 参数。")
    if "parallelism" in warning_areas:
        recommendations.append("busy 持续接近 1000ms/s 时，优先增加该 task chain 并行度或优化单条记录处理逻辑。")
    if "skew" in warning_areas:
        recommendations.append("存在 subtask 倾斜时，优先检查 source 分区、keyBy 分布、bucket/partition 设计，而不是整体盲目加并行度。")
    if "compaction_busy" in paimon_types:
        recommendations.append("Paimon compaction 持续 busy 时，优先趋势观察 level0/file-size/compaction time，再评估 bucket、target file size、write buffer、compaction 触发/并发参数。")
    if "commit_slow" in paimon_types:
        recommendations.append("Paimon commit 延迟或 attempts 偏高时，检查小文件数量、checkpoint 间隔、对象存储/元数据提交延迟和 commit 相关参数。")
    if "lookup_bottleneck" in lookup_types:
        recommendations.append("LookupJoin 同时有实际查询量和压力证据时，优先排查 HBase 维表访问、缓存配置、超时重试和 key 分布。")
    elif "lookup_cache_miss" in lookup_types:
        recommendations.append("LookupJoin cache hit 接近 0 只是风险信号；需要结合 busy、反压、HBase timeout/retry 日志和连续 QPS 再决定是否调缓存或 HBase 参数。")
    if not {"backpressure", "parallelism"} & warning_areas:
        recommendations.append("当前采样没有显示 Flink task 并行度直接限制吞吐，不建议只靠提高并行度解决问题。")
    recommendations.append("用 metric watch 对吞吐、busy、idle、backpressure、Paimon compaction/commit 做连续采样后再下调参结论。")
    return dedupe(recommendations)


def build_capacity_report(
    *,
    job_id: str,
    job: dict[str, Any],
    flow: dict[str, Any],
    checkpoint_trend: dict[str, Any],
    connectors: dict[str, Any],
    paimon: dict[str, Any] | None,
    lookup_join: dict[str, Any] | None,
    vertex_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    """构造面向并行度/吞吐/反压/Paimon sink 的总览报告。"""
    warning_findings = [
        finding
        for vertex in vertex_reports
        for finding in vertex.get("findings", [])
        if finding.get("level") in {"warning", "critical"}
    ]
    paimon_risks = [
        item
        for item in ((paimon or {}).get("risks", []) if isinstance(paimon, dict) else [])
        if isinstance(item, dict) and item.get("level") in {"warning", "critical"}
    ]
    lookup_risks = [
        item
        for item in ((lookup_join or {}).get("risks", []) if isinstance(lookup_join, dict) else [])
        if isinstance(item, dict) and item.get("level") in {"warning", "critical"}
    ]
    has_backpressure = any(item.get("area") == "backpressure" for item in warning_findings)
    has_parallelism_pressure = any(item.get("area") == "parallelism" for item in warning_findings)
    has_lookup_bottleneck = any(item.get("type") == "lookup_bottleneck" for item in lookup_risks)
    if has_lookup_bottleneck:
        conclusion = "lookup_join_bottleneck"
        conclusion_text = "LookupJoin 有实际查询量，并伴随 busy/反压或 HBase 日志异常，优先排查维表查询链路。"
    elif has_backpressure:
        conclusion = "observed_backpressure"
        conclusion_text = "当前存在反压证据，应优先定位下游瓶颈。"
    elif has_parallelism_pressure:
        conclusion = "parallelism_may_need_increase"
        conclusion_text = "存在 busy 饱和证据，相关 task chain 可能需要增加并行度或优化处理逻辑。"
    elif paimon_risks:
        conclusion = "sink_tuning_more_likely_than_parallelism"
        conclusion_text = "当前没有明显 Flink 并行度瓶颈，主要风险集中在 Paimon sink 的 compaction/commit。"
    else:
        conclusion = "parallelism_currently_reasonable"
        conclusion_text = "当前采样看并行度基本合理，没有明显反压或 task busy 饱和。"
    return {
        "job_id": job_id,
        "name": job.get("name"),
        "state": job.get("state"),
        "conclusion": conclusion,
        "conclusion_text": conclusion_text,
        "vertex_count": len(vertex_reports),
        "warning_count": len(warning_findings) + len(paimon_risks) + len(lookup_risks),
        "vertices": vertex_reports,
        "flow": flow,
        "checkpoint_trend": checkpoint_trend,
        "connectors": connectors,
        "paimon": paimon,
        "lookup_join": lookup_join,
        "recommendations": capacity_recommendations(vertex_reports, paimon, lookup_join),
        "next_commands": [
            "job capacity --url <job-url> --json",
            "backpressure --url <job-url> --samples 3 --interval 10 --json",
            "job skew --url <job-url> --json",
            "diagnose paimon --url <job-url> --json",
            "diagnose lookup --url <job-url> --json",
            "metric watch --url <writer-task-url> --scope subtask --get numRecordsInPerSecond,busyTimeMsPerSecond,backPressuredTimeMsPerSecond --agg min,max,avg,sum --samples 6 --interval 10 --json",
        ],
    }


def summarize_memory_top(data: dict[str, Any], top: int = 5) -> dict[str, Any]:
    """Rank TaskManagers by memory and GC pressure."""
    taskmanagers = data.get("taskmanagers", []) if isinstance(data, dict) else []

    def row(tm: dict[str, Any]) -> dict[str, Any]:
        """Build a sortable TaskManager memory row."""
        gc = tm.get("gc", {}) if isinstance(tm.get("gc"), dict) else {}
        return {
            "id": tm.get("id"),
            "heap_used_pct": to_float((tm.get("heap") or {}).get("used_pct")),
            "direct_used_pct": to_float((tm.get("direct") or {}).get("used_pct")),
            "managed_used_pct": to_float((tm.get("managed") or {}).get("used_pct")),
            "metaspace_used_pct": to_float((tm.get("metaspace") or {}).get("used_pct")),
            "gc_young_count": to_float(gc.get("young_count")),
            "gc_old_count": to_float(gc.get("old_count")),
            "cpu_load": to_float(tm.get("cpu_load")),
        }

    rows = [row(tm) for tm in taskmanagers if isinstance(tm, dict)]

    def top_by(key: str) -> list[dict[str, Any]]:
        """Return top rows by one numeric key."""
        return sorted(rows, key=lambda item: float(item.get(key) or 0), reverse=True)[:top]

    return {
        "taskmanager_count": len(rows),
        "top_heap": top_by("heap_used_pct"),
        "top_direct": top_by("direct_used_pct"),
        "top_managed": top_by("managed_used_pct"),
        "top_metaspace": top_by("metaspace_used_pct"),
        "top_gc_young": top_by("gc_young_count"),
        "top_gc_old": top_by("gc_old_count"),
        "top_cpu": top_by("cpu_load"),
    }


def expand_log_patterns(patterns: list[str] | None = None) -> list[str] | None:
    """展开日志预设 pattern；返回 None 表示使用默认错误模式。"""
    if not patterns:
        return None
    expanded: list[str] = []
    for pattern in patterns:
        normalized = str(pattern).strip()
        if not normalized:
            continue
        if normalized in {"hbase-lookup", "hbase", "lookup"}:
            expanded.extend(_HBASE_LOOKUP_LOG_PATTERNS)
        else:
            expanded.append(normalized)
    return dedupe(expanded)


def compile_log_patterns(patterns: list[str] | None = None) -> list[tuple[str, re.Pattern[str]]]:
    """编译日志匹配模式；ERROR 保持大小写敏感，避免把 WARN 行里的 Error 误算成 ERROR。"""
    wanted = expand_log_patterns(patterns) or _DEFAULT_LOG_ERROR_PATTERNS
    compiled = []
    for pattern in wanted:
        expression = rf"\b{re.escape(pattern)}\b" if re.fullmatch(r"[A-Za-z]+", pattern) else pattern
        flags = 0 if pattern == "ERROR" else re.IGNORECASE
        compiled.append((pattern, re.compile(expression, flags)))
    return compiled


def scan_log_text(text: str, patterns: list[str] | None = None, *, limit: int = 20) -> dict[str, Any]:
    """Scan log text for risky patterns and return sample lines."""
    compiled = compile_log_patterns(patterns)
    wanted = [name for name, _ in compiled]
    lines = text.splitlines()
    matches: dict[str, Any] = {}
    for pattern, expression in compiled:
        selected = [line[:500] for line in lines if expression.search(line)]
        matches[pattern] = {"count": len(selected), "samples": selected[:limit]}
    return {"line_count": len(lines), "patterns": wanted, "matches": matches}


def grep_log_text(
    text: str,
    patterns: list[str] | None = None,
    *,
    before: int = 0,
    after: int = 0,
    max_matches: int = 50,
) -> dict[str, Any]:
    """Find matching lines in bounded text with surrounding context."""
    compiled = compile_log_patterns(patterns)
    lines = text.splitlines()
    pattern_counts = {name: 0 for name, _ in compiled}
    matches: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        matched = [name for name, expression in compiled if expression.search(line)]
        if not matched:
            continue
        for name in matched:
            pattern_counts[name] += 1
        if len(matches) < max(0, max_matches):
            start = max(0, index - max(0, before))
            end = min(len(lines), index + max(0, after) + 1)
            matches.append(
                {
                    "line_number": index + 1,
                    "patterns": matched,
                    "text": line[:500],
                    "before": [item[:500] for item in lines[start:index]],
                    "after": [item[:500] for item in lines[index + 1 : end]],
                }
            )
    match_count = sum(pattern_counts.values())
    return {
        "line_count": len(lines),
        "patterns": [name for name, _ in compiled],
        "pattern_counts": pattern_counts,
        "match_count": match_count,
        "max_matches": max_matches,
        "truncated_matches": match_count > len(matches),
        "matches": matches,
    }


def log_error_signature(line: str) -> str:
    """Normalize a log line into a stable error signature."""
    cleaned = re.sub(r"^\d{4}-\d{2}-\d{2}[T\s]\S+\s*", "", line.strip())
    cleaned = re.sub(r"\b[0-9a-f]{16,}\b", "<hex>", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+\b", "<num>", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return (cleaned or line.strip())[:240]


def finalize_log_error_summary(
    groups: dict[str, dict[str, Any]],
    pattern_counts: dict[str, int],
    patterns: list[str],
    *,
    line_count: int,
    bytes_read: int | None,
    path: str | None,
    max_signatures: int,
) -> dict[str, Any]:
    """Render grouped error signatures as JSON-serializable summary rows."""
    signatures = sorted(groups.values(), key=lambda item: (-int(item.get("count", 0)), int(item.get("first_line", 0))))
    rows: list[dict[str, Any]] = []
    for item in signatures[: max(0, max_signatures)]:
        rows.append(
            {
                "signature": item["signature"],
                "count": item["count"],
                "first_line": item["first_line"],
                "last_line": item["last_line"],
                "patterns": sorted(item["patterns"]),
                "samples": item["samples"],
            }
        )
    result: dict[str, Any] = {
        "line_count": line_count,
        "patterns": patterns,
        "pattern_counts": pattern_counts,
        "signature_count": len(signatures),
        "max_signatures": max_signatures,
        "truncated_signatures": len(signatures) > max_signatures,
        "signatures": rows,
    }
    if bytes_read is not None:
        result["bytes_read"] = bytes_read
    if path is not None:
        result["path"] = path
    return result


def summarize_log_errors_text(
    text: str,
    patterns: list[str] | None = None,
    *,
    before: int = 2,
    after: int = 3,
    max_signatures: int = 20,
    max_samples_per_signature: int = 2,
) -> dict[str, Any]:
    """Aggregate matching lines from bounded text by normalized error signature."""
    compiled = compile_log_patterns(patterns or _DEFAULT_LOG_ERROR_PATTERNS)
    lines = text.splitlines()
    groups: dict[str, dict[str, Any]] = {}
    pattern_counts = {name: 0 for name, _ in compiled}
    for index, line in enumerate(lines):
        matched = [name for name, expression in compiled if expression.search(line)]
        if not matched:
            continue
        for name in matched:
            pattern_counts[name] += 1
        signature = log_error_signature(line)
        group = groups.setdefault(
            signature,
            {
                "signature": signature,
                "count": 0,
                "first_line": index + 1,
                "last_line": index + 1,
                "patterns": set(),
                "samples": [],
            },
        )
        group["count"] += 1
        group["last_line"] = index + 1
        group["patterns"].update(matched)
        if len(group["samples"]) < max(0, max_samples_per_signature):
            start = max(0, index - max(0, before))
            end = min(len(lines), index + max(0, after) + 1)
            group["samples"].append(
                {
                    "line_number": index + 1,
                    "text": line[:500],
                    "before": [item[:500] for item in lines[start:index]],
                    "after": [item[:500] for item in lines[index + 1 : end]],
                }
            )
    return finalize_log_error_summary(
        groups,
        pattern_counts,
        [name for name, _ in compiled],
        line_count=len(lines),
        bytes_read=None,
        path=None,
        max_signatures=max_signatures,
    )


def build_health_report(parts: dict[str, Any]) -> dict[str, Any]:
    """Build a compact health report from collected diagnostic parts."""
    job = parts.get("job", {})
    risks: list[dict[str, Any]] = []
    flow = parts.get("flow", {})
    largest_filter = flow.get("largest_filter_drop") if isinstance(flow, dict) else None
    if largest_filter and to_float(largest_filter.get("pass_through_pct")) is not None and float(largest_filter.get("pass_through_pct")) < 50:
        risks.append({"level": "warning", "area": "flow", "message": "Large filtering/drop stage", "evidence": largest_filter})
    backpressure = parts.get("backpressure", {})
    bp_rows = backpressure.get("vertices", []) if isinstance(backpressure, dict) else []
    high_bp = [row for row in bp_rows if str(row.get("backpressure_level", "")).lower() == "high" or float(row.get("backpressured_max") or 0) >= 0.5]
    if high_bp:
        risks.append({"level": "warning", "area": "backpressure", "message": "High backpressure observed", "evidence": high_bp[:5]})
    checkpoint = parts.get("checkpoint", {})
    counts = checkpoint.get("counts", {}) if isinstance(checkpoint, dict) else {}
    if int(counts.get("failed", 0) or 0) > 0:
        risks.append({"level": "info", "area": "checkpoint", "message": "Checkpoint failures exist in history", "evidence": counts})
    exceptions = parts.get("exceptions", {})
    if exceptions.get("root-exception") or exceptions.get("all-exceptions") or exception_history_entries(exceptions):
        risks.append({"level": "critical", "area": "exceptions", "message": "Job exceptions found", "evidence": summarize_job_exceptions(exceptions, limit=5)})
    memory = parts.get("memory_top", {})
    heap = memory.get("top_heap", []) if isinstance(memory, dict) else []
    if heap and float(heap[0].get("heap_used_pct") or 0) >= 85:
        risks.append({"level": "warning", "area": "memory", "message": "High heap usage", "evidence": heap[:3]})
    return {
        "job_name": job.get("name"),
        "job_id": job.get("jid") or job.get("id"),
        "state": job.get("state"),
        "risk_count": len(risks),
        "risks": risks,
        "checkpoint": checkpoint,
        "memory_top": memory,
    }


def summarize_taskmanager_memory(tm: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Summarize TaskManager memory and GC metrics."""
    return {
        "id": tm.get("id"),
        "slots": {"total": tm.get("slotsNumber"), "free": tm.get("freeSlots")},
        "configured_memory": {key: bytes_human(value) for key, value in (tm.get("memoryConfiguration") or {}).items()},
        "heap": {
            "used": bytes_human(metrics.get("Status.JVM.Memory.Heap.Used")),
            "max": bytes_human(metrics.get("Status.JVM.Memory.Heap.Max")),
            "used_pct": pct(metrics.get("Status.JVM.Memory.Heap.Used"), metrics.get("Status.JVM.Memory.Heap.Max")),
        },
        "non_heap": {
            "used": bytes_human(metrics.get("Status.JVM.Memory.NonHeap.Used")),
            "max": bytes_human(metrics.get("Status.JVM.Memory.NonHeap.Max")),
            "used_pct": pct(metrics.get("Status.JVM.Memory.NonHeap.Used"), metrics.get("Status.JVM.Memory.NonHeap.Max")),
        },
        "metaspace": {
            "used": bytes_human(metrics.get("Status.JVM.Memory.Metaspace.Used")),
            "max": bytes_human(metrics.get("Status.JVM.Memory.Metaspace.Max")),
            "used_pct": pct(metrics.get("Status.JVM.Memory.Metaspace.Used"), metrics.get("Status.JVM.Memory.Metaspace.Max")),
        },
        "direct": {
            "used": bytes_human(metrics.get("Status.JVM.Memory.Direct.MemoryUsed")),
            "capacity": bytes_human(metrics.get("Status.JVM.Memory.Direct.TotalCapacity")),
            "used_pct": pct(metrics.get("Status.JVM.Memory.Direct.MemoryUsed"), metrics.get("Status.JVM.Memory.Direct.TotalCapacity")),
        },
        "managed": {
            "used": bytes_human(metrics.get("Status.Flink.Memory.Managed.Used")),
            "total": bytes_human(metrics.get("Status.Flink.Memory.Managed.Total")),
            "used_pct": pct(metrics.get("Status.Flink.Memory.Managed.Used"), metrics.get("Status.Flink.Memory.Managed.Total")),
        },
        "shuffle_netty": {
            "used": bytes_human(metrics.get("Status.Shuffle.Netty.UsedMemory")),
            "total": bytes_human(metrics.get("Status.Shuffle.Netty.TotalMemory")),
            "used_pct": pct(metrics.get("Status.Shuffle.Netty.UsedMemory"), metrics.get("Status.Shuffle.Netty.TotalMemory")),
        },
        "network_segments": {
            "available": metrics.get("Status.Network.AvailableMemorySegments"),
            "total": metrics.get("Status.Network.TotalMemorySegments"),
        },
        "gc": {
            "young_count": metrics.get("Status.JVM.GarbageCollector.G1_Young_Generation.Count") or metrics.get("Status.JVM.GarbageCollector.Copy.Count"),
            "young_time": millis_human(metrics.get("Status.JVM.GarbageCollector.G1_Young_Generation.Time") or metrics.get("Status.JVM.GarbageCollector.Copy.Time")),
            "old_count": metrics.get("Status.JVM.GarbageCollector.G1_Old_Generation.Count") or metrics.get("Status.JVM.GarbageCollector.MarkSweepCompact.Count"),
            "old_time": millis_human(metrics.get("Status.JVM.GarbageCollector.G1_Old_Generation.Time") or metrics.get("Status.JVM.GarbageCollector.MarkSweepCompact.Time")),
        },
        "cpu_load": metrics.get("Status.JVM.CPU.Load"),
        "threads": metrics.get("Status.JVM.Threads.Count"),
    }


def summarize_thread_dump(data: Any, top: list[str] | None = None) -> dict[str, Any]:
    """Summarize a Flink thread dump response."""
    infos = data.get("threadInfos", []) if isinstance(data, dict) else []
    state_counter: Counter[str] = Counter()
    top_frames: Counter[str] = Counter()
    selected: list[dict[str, str]] = []
    wanted = {item.upper() for item in (top or [])}
    for item in infos:
        text = str(item.get("stringifiedThreadInfo", ""))
        name = str(item.get("threadName", ""))
        state_match = re.search(r"\b(RUNNABLE|WAITING|TIMED_WAITING|BLOCKED|NEW|TERMINATED)\b", text)
        state = state_match.group(1) if state_match else "UNKNOWN"
        state_counter[state] += 1
        frame_match = re.search(r"\n\tat ([^\n]+)", text)
        if frame_match:
            top_frames[frame_match.group(1)] += 1
        if not wanted or state in wanted:
            selected.append({"threadName": name, "state": state, "topFrame": frame_match.group(1) if frame_match else ""})
    return {
        "thread_count": len(infos),
        "states": dict(state_counter),
        "top_frames": top_frames.most_common(20),
        "selected": selected[:50],
    }


def summarize_flamegraph(data: Any, top_n: int = 20) -> dict[str, Any]:
    """Summarize a Flink FlameGraph tree."""
    root = data.get("data", {}) if isinstance(data, dict) else {}
    rows: list[dict[str, Any]] = []

    def visit(node: dict[str, Any], path: list[str]) -> None:
        """Visit a flamegraph tree node."""
        name = str(node.get("name", ""))
        current_path = path + [name]
        rows.append({"name": name, "value": node.get("value", 0), "path": " <- ".join(current_path)})
        for child in node.get("children", []) or []:
            if isinstance(child, dict):
                visit(child, current_path)

    if isinstance(root, dict):
        visit(root, [])
    rows.sort(key=lambda item: float(item.get("value") or 0), reverse=True)
    return {
        "endTimestamp": data.get("endTimestamp") if isinstance(data, dict) else None,
        "root_value": root.get("value") if isinstance(root, dict) else None,
        "top": rows[:top_n],
    }


async def fetch_metric_values(client: FlinkClient, base_path: str, requested: list[str], match_mode: str) -> Any:
    """Fetch metric values, resolving short names through the available metric list."""
    if not requested:
        return await client.get_json(base_path)
    available = metric_ids(await client.get_json(base_path))
    resolved = match_metric_ids(available, requested, mode=match_mode)
    return await fetch_metrics_by_chunks(client, base_path, resolved)


async def fetch_all_subtask_metric_values(
    client: FlinkClient,
    job_id: str,
    vertex_id: str,
    requested: list[str],
    match_mode: str,
) -> dict[str, Any]:
    """Fetch selected metric values for every subtask of a task chain."""
    if not requested:
        raise FlinkDiagError("--all-subtasks requires --get to avoid fetching every metric for every subtask")
    detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    subtasks = detail.get("subtasks", []) if isinstance(detail, dict) else []
    subtask_indexes = [int(item.get("subtask")) for item in subtasks if isinstance(item, dict) and item.get("subtask") is not None]
    if not subtask_indexes:
        parallelism = int(detail.get("parallelism", 0) or 0) if isinstance(detail, dict) else 0
        subtask_indexes = list(range(parallelism))

    async def fetch_one(subtask: int) -> dict[str, Any]:
        """Fetch selected metrics for one subtask."""
        base_path = f"jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask}/metrics"
        available = metric_ids(await client.get_json(base_path))
        resolved = match_metric_ids(available, requested, mode=match_mode)
        values = await fetch_metrics_by_chunks(client, base_path, resolved)
        metrics = {str(item.get("id")): item.get("value") for item in values if isinstance(item, dict)}
        return {"subtask": subtask, "metrics": metrics}

    rows = await asyncio.gather(*(fetch_one(subtask) for subtask in subtask_indexes))
    totals = summarize_metric_totals(rows)
    return {"job_id": job_id, "vertex_id": vertex_id, "subtasks": list(rows), "totals": totals}


def summarize_metric_totals(rows: list[dict[str, Any]]) -> dict[str, str]:
    """Sum numeric metrics across subtask rows."""
    totals: dict[str, float] = {}
    for row in rows:
        metrics = row.get("metrics", {})
        if not isinstance(metrics, dict):
            continue
        for name, value in metrics.items():
            number = to_float(value)
            if number is None:
                continue
            totals[name] = totals.get(name, 0.0) + number
    return {
        name: str(int(value)) if value.is_integer() else str(value)
        for name, value in totals.items()
    }


async def metric_path_for_context(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
    *,
    default_scope: str = "auto",
) -> str:
    """Choose a metric-list REST path from URL and flags."""
    scope = getattr(args, "scope", None) or default_scope
    if scope == "jobmanager" or parsed.route_kind == "jobmanager":
        return "jobmanager/metrics"
    if scope == "taskmanager" or parsed.route_kind == "taskmanager":
        if parsed.taskmanager_id:
            return f"taskmanagers/{quote(parsed.taskmanager_id, safe='')}/metrics"
        context = await resolve_context(client, parsed, args)
        tm_id = require_value(context.taskmanager_id, "taskmanager_id")
        return f"taskmanagers/{quote(tm_id, safe='')}/metrics"
    if scope == "jm-operator":
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id or parsed.job_id, "job_id")
        vertex_id = require_value(context.vertex_id or parsed.vertex_id, "vertex_id")
        return f"jobs/{job_id}/vertices/{vertex_id}/jm-operator-metrics"
    if parsed.job_id and parsed.vertex_id and scope in {"auto", "task-chain"}:
        return f"jobs/{parsed.job_id}/vertices/{parsed.vertex_id}/metrics"
    if parsed.job_id and scope in {"auto", "job"}:
        return f"jobs/{parsed.job_id}/metrics"
    context = await resolve_context(client, parsed, args)
    if scope == "job":
        job_id = require_value(context.job_id, "job_id")
        return f"jobs/{job_id}/metrics"
    if scope == "task-chain":
        job_id = require_value(context.job_id, "job_id")
        vertex_id = require_value(context.vertex_id, "vertex_id")
        return f"jobs/{job_id}/vertices/{vertex_id}/metrics"
    if context.job_id and context.vertex_id:
        return f"jobs/{context.job_id}/vertices/{context.vertex_id}/metrics"
    if context.job_id:
        return f"jobs/{context.job_id}/metrics"
    return "jobmanager/metrics"


def split_subset_csv(value: str | None) -> list[str]:
    """解析官方聚合 endpoint 的 subset 逗号列表。"""
    return split_csv(value)


async def metric_aggregate_context(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """根据 scope、URL 与参数生成官方 metrics 聚合 endpoint 上下文。"""
    scope = getattr(args, "scope", None) or "taskmanager"
    if scope == "taskmanager":
        taskmanagers = split_subset_csv(getattr(args, "taskmanagers", None))
        if not taskmanagers:
            explicit = getattr(args, "taskmanager_id", None)
            if explicit:
                taskmanagers = [explicit]
            elif parsed.taskmanager_id:
                taskmanagers = [parsed.taskmanager_id]
        return {"scope": scope, "path": "taskmanagers/metrics", "taskmanagers": taskmanagers}
    if scope == "job":
        jobs = split_subset_csv(getattr(args, "jobs", None))
        if not jobs:
            explicit = getattr(args, "job_id", "auto")
            if explicit and explicit != "auto":
                jobs = [explicit]
            elif parsed.job_id:
                jobs = [parsed.job_id]
        return {"scope": scope, "path": "jobs/metrics", "jobs": jobs}
    if scope in {"subtask", "jm-operator"}:
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id or parsed.job_id, "job_id")
        vertex_id = require_value(context.vertex_id or parsed.vertex_id, "vertex_id")
        path = (
            f"jobs/{job_id}/vertices/{vertex_id}/jm-operator-metrics"
            if scope == "jm-operator"
            else f"jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
        )
        subtasks = split_subset_csv(getattr(args, "subtasks", None))
        if not subtasks and getattr(args, "subtask", None) is not None:
            subtasks = [str(getattr(args, "subtask"))]
        return {"scope": scope, "path": path, "job_id": job_id, "vertex_id": vertex_id, "subtasks": subtasks}
    raise FlinkDiagError(f"Unsupported metric aggregate scope: {scope}")


def metric_subset_kwargs(context: dict[str, Any]) -> dict[str, list[str] | None]:
    """从聚合上下文中提取 subset 查询参数。"""
    return {
        "taskmanagers": context.get("taskmanagers") or None,
        "jobs": context.get("jobs") or None,
        "subtasks": context.get("subtasks") or None,
    }


async def metric_endpoint_available(client: FlinkClient, path: str, subset: dict[str, list[str] | None] | None = None) -> tuple[bool, Any]:
    """探测 metrics endpoint 是否可用，404 等错误以 unavailable 返回。"""
    probe_path = build_metric_query(path, **(subset or {}))
    data = await client.get_json(probe_path, allow_error=True)
    if isinstance(data, dict) and data.get("available") is False:
        return False, data
    return True, data


async def prepare_metric_aggregate_request(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """预解析 metric aggregate 请求，长时间采样时复用 path、subset 和 resolved metrics。"""
    context = await metric_aggregate_context(client, parsed, args)
    subset = metric_subset_kwargs(context)
    path = str(context["path"])
    available, listed = await metric_endpoint_available(client, path, subset)
    if not available:
        return {
            "available": False,
            "scope": context["scope"],
            "path": path,
            "subset": subset,
            "error": listed,
        }
    requested = split_csv(getattr(args, "get", None))
    agg = split_csv(getattr(args, "agg", None))
    if not requested:
        return {
            "available": True,
            "scope": context["scope"],
            "path": path,
            "subset": subset,
            "agg": agg or ["min", "max", "avg", "sum"],
            "metrics": [],
            "metric_ids": metric_ids(listed),
            "list_only": True,
        }
    resolved = match_metric_ids(metric_ids(listed), requested, mode=getattr(args, "metric_match", "auto"))
    return {
        "available": True,
        "scope": context["scope"],
        "path": path,
        "subset": subset,
        "agg": agg or ["min", "max", "avg", "sum"],
        "metrics": resolved,
        "list_only": False,
    }


async def fetch_prepared_metric_aggregate(client: FlinkClient, plan: dict[str, Any]) -> dict[str, Any]:
    """按预解析 plan 拉取 metric aggregate 当前值。"""
    if not plan.get("available"):
        return dict(plan)
    if plan.get("list_only"):
        return dict(plan)
    values = await fetch_metrics_by_chunks(
        client,
        str(plan["path"]),
        list(plan.get("metrics") or []),
        agg=list(plan.get("agg") or []) or None,
        **(plan.get("subset") or {}),
    )
    return {
        "available": True,
        "scope": plan.get("scope"),
        "path": plan.get("path"),
        "subset": plan.get("subset"),
        "agg": plan.get("agg"),
        "metrics": plan.get("metrics"),
        "values": values,
    }


async def fetch_metric_aggregate(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """读取官方 metrics 聚合 endpoint，支持 subset 和 agg 参数。"""
    plan = await prepare_metric_aggregate_request(client, parsed, args)
    return await fetch_prepared_metric_aggregate(client, plan)


def numeric_metric_map(values: list[dict[str, Any]]) -> dict[str, float]:
    """把 metric response 展平为可计算 delta/rate 的数值 map。"""
    flattened: dict[str, float] = {}
    for item in values:
        metric_id = item.get("id")
        if not metric_id:
            continue
        for key in ("value", "min", "max", "avg", "sum"):
            if key not in item:
                continue
            number = to_float(item.get(key))
            if number is None:
                continue
            flattened[str(metric_id) if key == "value" else f"{metric_id}.{key}"] = number
    return flattened


def diff_metric_maps(previous: dict[str, float] | None, current: dict[str, float], elapsed: float | None, *, rate: bool) -> dict[str, float]:
    """计算相邻两次采样的 delta 或每秒 rate。"""
    if not previous:
        return {}
    result: dict[str, float] = {}
    divisor = elapsed if rate and elapsed and elapsed > 0 else 1.0
    for key, value in current.items():
        if key not in previous:
            continue
        result[key] = (value - previous[key]) / divisor
    return result


async def fetch_semantic_metrics(
    client: FlinkClient,
    base_path: str,
    aliases: list[str],
) -> dict[str, Any]:
    """Fetch metric values for semantic aliases."""
    available = metric_ids(await client.get_json(base_path))
    resolved: dict[str, list[str]] = {alias: match_metric_alias(available, alias) for alias in aliases}
    all_metrics = dedupe([metric for metrics in resolved.values() for metric in metrics])
    values = await fetch_metrics_by_chunks(client, base_path, all_metrics) if all_metrics else []
    values_by_id = {item.get("id"): item.get("value") for item in values if isinstance(item, dict)}
    return {
        alias: [{"id": metric, "value": values_by_id.get(metric)} for metric in metrics]
        for alias, metrics in resolved.items()
    }


def best_metric_value(entries: list[dict[str, Any]]) -> Any:
    """Return the first non-empty semantic metric value."""
    for item in entries:
        value = item.get("value")
        if value not in {None, ""}:
            return value
    return None


def sum_metric_values(entries: list[dict[str, Any]]) -> str | None:
    """Sum numeric metric values across entries."""
    values = [to_float(item.get("value")) for item in entries]
    numbers = [value for value in values if value is not None]
    if not numbers:
        return None
    total = sum(numbers)
    if total.is_integer():
        return str(int(total))
    return str(total)


def is_lookup_metric_id(metric_id: str) -> bool:
    """判断 metric id 是否属于 LookupJoin operator。"""
    parsed = parse_metric_identifier(metric_id)
    operator = str(parsed.get("operator") or "")
    metric_name = str(parsed.get("metric_name") or "")
    return "LookupJoin" in operator and metric_name in {
        "numRecordsIn",
        "numRecordsOut",
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "lookupCacheHitRate",
    }


def is_lookup_vertex(vertex: dict[str, Any], available: list[str]) -> bool:
    """根据 task chain 名称或 operator metric 判断是否包含 LookupJoin。"""
    if "LookupJoin" in str(vertex.get("name") or ""):
        return True
    return any(is_lookup_metric_id(metric) for metric in available)


def summarize_lookup_metric_entries(entries: list[dict[str, Any]], *, parallelism: int | None = None) -> dict[str, Any]:
    """汇总 LookupJoin operator 指标，保留 subtask/operator 维度用于识别倾斜。"""
    rows: list[dict[str, Any]] = []
    for item in entries:
        metric_id = str(item.get("id") or "")
        value = to_float(item.get("value"))
        if not metric_id or value is None:
            continue
        parsed = parse_metric_identifier(metric_id)
        rows.append(
            {
                "id": metric_id,
                "subtask": parsed.get("subtask"),
                "operator": parsed.get("operator"),
                "metric_name": parsed.get("metric_name"),
                "value": numeric_value(value),
            }
        )
    values = [float(row["value"]) for row in rows if row.get("value") is not None]
    positive = [value for value in values if value > 0]
    if not values:
        return {"available": False, "count": 0, "rows": []}
    max_row = max(rows, key=lambda row: float(row.get("value") or 0))
    active_subtasks = len({row.get("subtask") for row in rows if to_float(row.get("value")) and to_float(row.get("value")) > 0})
    total_subtasks = parallelism or len({row.get("subtask") for row in rows if row.get("subtask") is not None}) or None
    max_value = max(values)
    min_positive = min(positive) if positive else None
    return {
        "available": True,
        "count": len(rows),
        "min": numeric_value(min(values)),
        "max": numeric_value(max_value),
        "avg": round(sum(values) / len(values), 6),
        "sum": numeric_value(sum(values)),
        "active_subtasks": active_subtasks,
        "zero_subtasks": (total_subtasks - active_subtasks) if total_subtasks is not None else None,
        "max_subtask": max_row.get("subtask"),
        "skew_ratio": round(max_value / min_positive, 4) if min_positive else None,
        "top_subtasks": [
            {"subtask": row.get("subtask"), "operator": row.get("operator"), "value": row.get("value")}
            for row in sorted(rows, key=lambda item: float(item.get("value") or 0), reverse=True)[:5]
        ],
        "rows": sorted(rows, key=lambda row: (-float(row.get("value") or 0), str(row.get("id"))))[:50],
    }


def lookup_metric_summaries(metrics: dict[str, list[dict[str, Any]]], *, parallelism: int | None) -> dict[str, Any]:
    """按语义 alias 汇总 LookupJoin 指标。"""
    return {
        alias: summarize_lookup_metric_entries(entries, parallelism=parallelism)
        for alias, entries in metrics.items()
    }


def lookup_pressure_flags(
    graph_row: dict[str, Any] | None,
    capacity_metrics: dict[str, Any],
    log_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """把 busy、idle、反压和日志异常压缩成瓶颈判断所需的布尔证据。"""
    busy = capacity_metrics.get("busyTimeMsPerSecond", {}) if isinstance(capacity_metrics, dict) else {}
    idle = capacity_metrics.get("idleTimeMsPerSecond", {}) if isinstance(capacity_metrics, dict) else {}
    backpressured = capacity_metrics.get("backPressuredTimeMsPerSecond", {}) if isinstance(capacity_metrics, dict) else {}
    busy_max = to_float(busy.get("max"))
    busy_avg = to_float(busy.get("avg"))
    idle_avg = to_float(idle.get("avg"))
    backpressure_ratio = to_float((graph_row or {}).get("backpressured_max"))
    backpressure_ms_max = to_float(backpressured.get("max"))
    log_match_count = int((log_summary or {}).get("match_count") or 0) if isinstance(log_summary, dict) else 0
    return {
        "busy_high": (busy_max is not None and busy_max >= _CAPACITY_BUSY_HIGH_MS) or (busy_avg is not None and busy_avg >= _CAPACITY_BUSY_HIGH_MS),
        "idle_high": idle_avg is not None and idle_avg >= 700,
        "backpressure_high": (backpressure_ratio is not None and backpressure_ratio >= 0.5)
        or (backpressure_ms_max is not None and backpressure_ms_max >= _CAPACITY_BACKPRESSURE_HIGH_MS),
        "log_anomaly": log_match_count > 0,
        "log_match_count": log_match_count,
        "busyTimeMsPerSecond": busy,
        "idleTimeMsPerSecond": idle,
        "backPressuredTimeMsPerSecond": backpressured,
        "backpressured_max": backpressure_ratio,
    }


def lookup_risks_from_summary(summary: dict[str, Any]) -> list[dict[str, Any]]:
    """根据 LookupJoin 汇总信息生成风险项；cache miss 本身不直接判定瓶颈。"""
    risks: list[dict[str, Any]] = []
    metric_summaries = summary.get("metric_summaries", {}) if isinstance(summary, dict) else {}
    records_in_rate = metric_summaries.get("lookup.records_in_rate", {})
    cache_hit = metric_summaries.get("lookup.cache_hit_rate", {})
    pressure = summary.get("pressure", {}) if isinstance(summary.get("pressure"), dict) else {}
    if not any((metric_summaries.get(alias) or {}).get("available") for alias in _LOOKUP_JOIN_ALIASES):
        risks.append(
            {
                "type": "lookup_metrics_missing",
                "level": "info",
                "message": "检测到 LookupJoin task chain，但没有读取到 operator 级 lookup 指标。",
                "evidence": {"vertex_id": summary.get("vertex_id"), "name": summary.get("name")},
            }
        )
        return risks
    cache_avg = to_float(cache_hit.get("avg"))
    cache_max = to_float(cache_hit.get("max"))
    cache_low = cache_hit.get("available") and (
        (cache_avg is not None and cache_avg <= 0.01) or (cache_max is not None and cache_max <= 0.01)
    )
    if cache_low:
        risks.append(
            {
                "type": "lookup_cache_miss",
                "level": "warning",
                "message": "LookupJoin cache hit rate 接近 0，HBase 查询多数会落到外部存储；需要结合 busy/反压/日志判断是否已成为瓶颈。",
                "evidence": cache_hit,
            }
        )
    skew_ratio = to_float(records_in_rate.get("skew_ratio"))
    if skew_ratio is not None and skew_ratio >= _CAPACITY_SKEW_RATIO:
        risks.append(
            {
                "type": "lookup_skew",
                "level": "warning",
                "message": "LookupJoin 实际查询 QPS 在 subtasks 之间倾斜明显。",
                "evidence": records_in_rate,
            }
        )
    pressure_count = sum(1 for key in ("busy_high", "backpressure_high", "log_anomaly") if pressure.get(key))
    if pressure_count > 0 and (cache_low or to_float(records_in_rate.get("sum"))):
        risks.append(
            {
                "type": "lookup_bottleneck",
                "level": "warning",
                "message": "LookupJoin 有实际查询量，并伴随 busy/反压或 HBase 日志异常证据，可能正在限制吞吐。",
                "evidence": {"records_in_rate": records_in_rate, "pressure": pressure},
            }
        )
    return risks


async def fetch_lookup_join_summary(
    client: FlinkClient,
    job_id: str,
    vertex: dict[str, Any],
    graph_row: dict[str, Any] | None,
    checkpoint_trend: dict[str, Any] | None,
    args: argparse.Namespace,
    *,
    capacity_report: dict[str, Any] | None = None,
    log_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """读取单个 LookupJoin task chain 的 operator 指标，并区分 chain 输入与实际 lookup QPS。"""
    vertex_id = str(vertex.get("id"))
    parallelism = int(vertex.get("parallelism", 0) or 0)
    base_path = f"jobs/{job_id}/vertices/{vertex_id}/metrics"
    available = metric_ids(await client.get_json(base_path))
    metrics = await fetch_semantic_metrics(client, base_path, _LOOKUP_JOIN_ALIASES)
    metric_summaries = lookup_metric_summaries(metrics, parallelism=parallelism or None)
    capacity_metrics = (capacity_report or {}).get("metrics") if isinstance(capacity_report, dict) else None
    capacity_error = None
    if not isinstance(capacity_metrics, dict):
        try:
            subtask_data = await fetch_all_subtask_metric_values(client, job_id, vertex_id, _CAPACITY_METRICS, getattr(args, "metric_match", "auto"))
            capacity_metrics = summarize_capacity_subtask_metrics(subtask_data)
        except FlinkDiagError as exc:
            capacity_metrics = {}
            capacity_error = str(exc)
    chain_in_rate = (capacity_metrics.get("numRecordsInPerSecond") or {}).get("sum") if isinstance(capacity_metrics, dict) else None
    lookup_in_rate = (metric_summaries.get("lookup.records_in_rate") or {}).get("sum")
    summary: dict[str, Any] = {
        "available": True,
        "vertex_id": vertex_id,
        "name": vertex.get("name"),
        "status": vertex.get("status"),
        "parallelism": parallelism,
        "lookup_metric_count": sum(len(entries) for entries in metrics.values()),
        "metric_evidence": [metric for metric in available if is_lookup_metric_id(metric)][:50],
        "metrics": metrics,
        "metric_summaries": metric_summaries,
        "chain_records_in_rate_sum": chain_in_rate,
        "actual_lookup_records_in_rate_sum": lookup_in_rate,
        "chain_vs_lookup_note": "chain_records_in_rate_sum 是整个 task chain 的输入速率；actual_lookup_records_in_rate_sum 才是 LookupJoin operator 的实际查询速率。",
        "capacity_metrics": capacity_metrics,
        "capacity_error": capacity_error,
        "pressure": lookup_pressure_flags(graph_row, capacity_metrics or {}, log_summary),
        "checkpoint": checkpoint_trend or {},
        "logs": log_summary,
    }
    summary["risks"] = lookup_risks_from_summary(summary)
    return summary


def summarize_lookup_logs_result(result: dict[str, Any] | None) -> dict[str, Any]:
    """汇总多 TaskManager HBase/Lookup 日志 grep 结果。"""
    if not isinstance(result, dict) or result.get("available") is False:
        return {"available": False, "match_count": 0, "error": (result or {}).get("error") if isinstance(result, dict) else None}
    taskmanagers = result.get("taskmanagers") or result.get("results") or []
    pattern_counts: Counter[str] = Counter()
    match_count = 0
    error_count = 0
    rows: list[dict[str, Any]] = []
    for item in taskmanagers:
        if not isinstance(item, dict):
            continue
        grep = item.get("grep") if isinstance(item.get("grep"), dict) else {}
        counts = grep.get("pattern_counts") if isinstance(grep.get("pattern_counts"), dict) else {}
        for key, value in counts.items():
            pattern_counts[str(key)] += int(value or 0)
        item_match_count = int(grep.get("match_count") or 0)
        match_count += item_match_count
        if item.get("error") or item.get("available") is False:
            error_count += 1
        rows.append(
            {
                "taskmanager_id": item.get("taskmanager_id"),
                "available": item.get("available", True),
                "match_count": item_match_count,
                "pattern_counts": counts,
                "error": item.get("error"),
            }
        )
    return {
        "available": True,
        "taskmanager_count": len(rows),
        "error_count": error_count,
        "match_count": match_count,
        "pattern_counts": dict(pattern_counts),
        "rows": rows,
    }


async def scan_hbase_lookup_logs(client: FlinkClient, parsed: ParsedUrl | None, args: argparse.Namespace) -> dict[str, Any]:
    """并行扫描所有 TaskManager 的 HBase/Lookup 预设日志模式，失败不影响主诊断。"""
    if parsed is None:
        return {"available": False, "match_count": 0, "reason": "parsed_url_missing"}
    try:
        log_args = clone_args_namespace(args)
        log_args.scope = "taskmanager"
        log_args.logs_action = "grep"
        log_args.all_taskmanagers = True
        log_args.patterns = "hbase-lookup"
        log_args.full = False
        log_args.before = getattr(args, "before", 0)
        log_args.after = getattr(args, "after", 0)
        log_args.max_matches = getattr(args, "max_matches", 50)
        result = await run_logs_all_taskmanagers(client, parsed, log_args, action="grep")
        return summarize_lookup_logs_result(result)
    except FlinkDiagError as exc:
        return {"available": False, "match_count": 0, "error": str(exc)}


async def diagnose_lookup_job(
    client: FlinkClient,
    job_id: str,
    args: argparse.Namespace,
    *,
    parsed: ParsedUrl | None = None,
    job: dict[str, Any] | None = None,
    graph: dict[str, Any] | None = None,
    checkpoint_trend: dict[str, Any] | None = None,
    capacity_by_id: dict[str, dict[str, Any]] | None = None,
    include_logs: bool = True,
) -> dict[str, Any]:
    """诊断 Job 内所有 HBase/LookupJoin task chain 的 lookup QPS、cache、倾斜和压力证据。"""
    job_data = job if isinstance(job, dict) else await client.get_json(f"jobs/{job_id}")
    graph_data = graph if isinstance(graph, dict) else await fetch_job_graph(client, job_id, args)
    if checkpoint_trend is None:
        try:
            checkpoint_trend = summarize_checkpoint_trend(await client.get_json(f"jobs/{job_id}/checkpoints"), limit=getattr(args, "limit", 10))
        except FlinkDiagError as exc:
            checkpoint_trend = {"available": False, "error": str(exc)}
    vertices = list(job_data.get("vertices", [])) if isinstance(job_data, dict) else []
    graph_by_id = {str(row.get("vertex_id")): row for row in graph_data.get("vertices", []) if isinstance(row, dict)}

    async def detect(vertex: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        """读取 vertex metric 列表，判断是否含 LookupJoin。"""
        vertex_id = str(vertex.get("id"))
        try:
            available = metric_ids(await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/metrics"))
        except FlinkDiagError:
            available = []
        return vertex, available

    inspected = await asyncio.gather(*(detect(vertex) for vertex in vertices))
    lookup_vertices = [(vertex, available) for vertex, available in inspected if is_lookup_vertex(vertex, available)]
    log_summary = await scan_hbase_lookup_logs(client, parsed, args) if include_logs and lookup_vertices else None

    async def summarize(vertex: dict[str, Any]) -> dict[str, Any]:
        """诊断单个 LookupJoin vertex。"""
        vertex_id = str(vertex.get("id"))
        return await fetch_lookup_join_summary(
            client,
            job_id,
            vertex,
            graph_by_id.get(vertex_id),
            checkpoint_trend,
            args,
            capacity_report=(capacity_by_id or {}).get(vertex_id),
            log_summary=log_summary,
        )

    lookup_joins = list(await asyncio.gather(*(summarize(vertex) for vertex, _ in lookup_vertices)))
    risks = [risk for row in lookup_joins for risk in row.get("risks", []) if isinstance(risk, dict)]
    if not lookup_joins:
        conclusion = "no_lookup_join_found"
        conclusion_text = "没有在 job graph 或 operator metrics 中发现 LookupJoin。"
    elif any(risk.get("type") == "lookup_bottleneck" for risk in risks):
        conclusion = "lookup_bottleneck"
        conclusion_text = "LookupJoin 有实际查询量，并伴随 busy/反压或 HBase 日志异常，建议优先排查 HBase lookup 链路。"
    elif any(risk.get("level") in {"warning", "critical"} for risk in risks):
        conclusion = "lookup_risk_observed"
        conclusion_text = "发现 LookupJoin 风险信号，但当前证据不足以单独判定 HBase lookup 已成为瓶颈。"
    else:
        conclusion = "lookup_currently_stable"
        conclusion_text = "LookupJoin 当前没有明显 busy、反压、倾斜或 HBase 日志异常证据。"
    return {
        "available": True,
        "job_id": job_id,
        "name": job_data.get("name") if isinstance(job_data, dict) else None,
        "state": job_data.get("state") if isinstance(job_data, dict) else None,
        "conclusion": conclusion,
        "conclusion_text": conclusion_text,
        "lookup_join_count": len(lookup_joins),
        "lookup_joins": lookup_joins,
        "logs": log_summary,
        "risks": risks,
        "next_commands": [
            "diagnose lookup --url <job-url> --json",
            "metric search LookupJoin --url <lookup-task-url> --scope auto --structured",
            "metric watch --url <lookup-task-url> --scope subtask --get lookup.records_in_rate,lookup.cache_hit_rate,busyTimeMsPerSecond,backPressuredTimeMsPerSecond --agg min,max,avg,sum --samples 6 --interval 10 --json",
            "logs grep --url <job-url> --scope taskmanager --all-taskmanagers --patterns hbase-lookup --tail-bytes 262144 --json",
        ],
    }


async def fetch_job_graph(client: FlinkClient, job_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """Fetch and summarize the job graph / task chain overview."""
    job = await client.get_json(f"jobs/{job_id}")
    vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []

    async def fetch_bp(vertex: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        """Fetch backpressure for one vertex."""
        vertex_id = str(vertex.get("id"))
        try:
            data = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/backpressure")
            return vertex_id, data
        except FlinkDiagError:
            return vertex_id, None

    backpressure_pairs = await asyncio.gather(*(fetch_bp(vertex) for vertex in vertices))
    backpressure_by_id = dict(backpressure_pairs)
    summaries = [summarize_vertex(vertex, backpressure_by_id.get(str(vertex.get("id")))) for vertex in vertices]
    top_by = getattr(args, "top_by", None)
    if top_by == "backpressure":
        summaries.sort(key=lambda item: float(item.get("backpressured_max") or 0), reverse=True)
    elif top_by == "busy":
        summaries.sort(key=lambda item: float(item.get("busy_max") or 0), reverse=True)
    elif top_by == "records":
        summaries.sort(key=lambda item: float(item.get("records_sent") or 0), reverse=True)
    return {
        "job_id": job_id,
        "name": job.get("name") if isinstance(job, dict) else None,
        "state": job.get("state") if isinstance(job, dict) else None,
        "vertices": summaries,
    }


async def build_inventory(client: FlinkClient, parsed: ParsedUrl) -> dict[str, Any]:
    """Build a deployment inventory for upstream dynamic selection."""
    overview_task = asyncio.create_task(client.get_json("overview"))
    jobs_task = asyncio.create_task(client.get_json("jobs/overview"))
    taskmanagers_task = asyncio.create_task(client.get_json("taskmanagers"))
    jm_metrics_task = asyncio.create_task(client.get_json("jobmanager/metrics"))
    overview, jobs_data, taskmanagers_data, jm_metrics = await asyncio.gather(
        overview_task, jobs_task, taskmanagers_task, jm_metrics_task
    )
    jobs = list(jobs_data.get("jobs", [])) if isinstance(jobs_data, dict) else []
    taskchains: dict[str, list[dict[str, Any]]] = {}
    for job in jobs:
        job_id = job.get("jid") or job.get("id")
        if not job_id:
            continue
        try:
            detail = await client.get_json(f"jobs/{job_id}")
            taskchains[str(job_id)] = [
                {
                    "vertex_id": vertex.get("id"),
                    "name": vertex.get("name"),
                    "status": vertex.get("status"),
                    "parallelism": vertex.get("parallelism"),
                }
                for vertex in detail.get("vertices", [])
            ]
        except FlinkDiagError:
            taskchains[str(job_id)] = []
    return {
        "base_url": parsed.base_url,
        "origin": parsed.origin,
        "deployment": parsed.deployment,
        "overview": overview,
        "jobs": jobs,
        "taskchains": taskchains,
        "taskmanagers": taskmanagers_data.get("taskmanagers", []) if isinstance(taskmanagers_data, dict) else [],
        "jobmanager_metric_ids": metric_ids(jm_metrics),
    }


async def command_overview(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle the overview command."""
    overview, jobs = await asyncio.gather(client.get_json("overview"), client.get_json("jobs/overview"))
    return {"base_url": parsed.base_url, "overview": overview, "jobs": jobs}


async def command_jobs(client: FlinkClient, args: argparse.Namespace) -> Any:
    """Handle jobs list commands."""
    jobs_data = await client.get_json("jobs/overview")
    jobs = list(jobs_data.get("jobs", [])) if isinstance(jobs_data, dict) else []
    state = getattr(args, "job_state", None)
    if getattr(args, "subcommand", None) == "completed":
        state = "completed"
    return {"jobs": filter_jobs(jobs, state)}


async def command_resolve(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle the resolve command."""
    context = await resolve_context(client, parsed, args)
    return {
        "base_url": parsed.base_url,
        "origin": parsed.origin,
        "deployment": parsed.deployment,
        "route_kind": parsed.route_kind,
        "route_parts": parsed.route_parts,
        "detected_flink_version": context.flink_version,
        "endpoint_profile": context.endpoint_profile,
        "job_id": context.job_id or parsed.job_id,
        "vertex_id": context.vertex_id or parsed.vertex_id,
        "taskmanager_id": context.taskmanager_id or parsed.taskmanager_id,
        "subtask": context.subtask,
    }


async def command_job(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle job subcommands."""
    context = await resolve_context(client, parsed, args)
    job_id = require_value(context.job_id, "job_id")
    sub = getattr(args, "subcommand", "show")
    if sub in {None, "show"}:
        return await client.get_json(f"jobs/{job_id}")
    if sub == "exceptions":
        return await client.get_json(f"jobs/{job_id}/exceptions")
    if sub == "exceptions-summary":
        data = await client.get_json(f"jobs/{job_id}/exceptions")
        return {
            "job_id": job_id,
            "summary": summarize_job_exceptions(data, limit=getattr(args, "limit", 10)),
        }
    if sub == "timeline":
        job = await client.get_json(f"jobs/{job_id}")
        return {
            "job_id": job_id,
            "timestamps": job.get("timestamps"),
            "status_counts": job.get("status-counts"),
            "vertices": [
                {
                    "id": vertex.get("id"),
                    "name": vertex.get("name"),
                    "status": vertex.get("status"),
                    "start-time": vertex.get("start-time"),
                    "end-time": vertex.get("end-time"),
                    "duration": vertex.get("duration"),
                    "tasks": vertex.get("tasks"),
                }
                for vertex in job.get("vertices", [])
            ],
        }
    if sub == "checkpoints":
        checkpoints = await client.get_json(f"jobs/{job_id}/checkpoints")
        if getattr(args, "include_config", False):
            config = await client.get_json(f"jobs/{job_id}/checkpoints/config")
            payload = {"checkpoints": checkpoints, "configuration": config}
            return summarize_checkpoint_payload(payload) if getattr(args, "summary", False) else payload
        return checkpoints
    if sub == "checkpoint-summary":
        checkpoints, config = await asyncio.gather(
            client.get_json(f"jobs/{job_id}/checkpoints"),
            client.get_json(f"jobs/{job_id}/checkpoints/config"),
        )
        return summarize_checkpoint_payload({"checkpoints": checkpoints, "configuration": config})
    if sub == "checkpoint-trend":
        checkpoints = await client.get_json(f"jobs/{job_id}/checkpoints")
        return summarize_checkpoint_trend(checkpoints, limit=getattr(args, "limit", 10))
    if sub == "config":
        return await client.get_json(f"jobs/{job_id}/config")
    if sub == "graph":
        return await fetch_job_graph(client, job_id, args)
    if sub == "connectors":
        return await summarize_paimon_job_connectors(client, job_id)
    if sub == "capacity":
        return await command_job_capacity(client, parsed, args, job_id)
    if sub == "io-flow":
        job = await client.get_json(f"jobs/{job_id}")
        vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []
        summary = summarize_io_flow(vertices)
        return {
            "job_id": job_id,
            "name": job.get("name") if isinstance(job, dict) else None,
            "state": job.get("state") if isinstance(job, dict) else None,
            **summary,
        }
    if sub == "skew":
        return await command_job_skew(client, job_id, args)
    if sub == "health":
        return await command_job_health(client, parsed, args, job_id)
    raise FlinkDiagError(f"Unsupported job subcommand: {sub}")


async def command_job_skew(client: FlinkClient, job_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """Analyze subtask skew for every task chain in a job."""
    job = await client.get_json(f"jobs/{job_id}")
    vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []
    requested = split_csv(getattr(args, "get", None)) or [
        "numRecordsInPerSecond",
        "numRecordsOutPerSecond",
        "busyTimeMsPerSecond",
        "idleTimeMsPerSecond",
        "backPressuredTimeMsPerSecond",
    ]

    async def fetch(vertex: dict[str, Any]) -> dict[str, Any]:
        """Fetch skew for one vertex."""
        vertex_id = str(vertex.get("id"))
        try:
            metrics = await fetch_all_subtask_metric_values(client, job_id, vertex_id, requested, getattr(args, "metric_match", "auto"))
            skew = summarize_subtask_skew(metrics)
            return {"vertex_id": vertex_id, "name": vertex.get("name"), "parallelism": vertex.get("parallelism"), **skew}
        except FlinkDiagError as exc:
            return {"vertex_id": vertex_id, "name": vertex.get("name"), "error": str(exc)}

    rows = await asyncio.gather(*(fetch(vertex) for vertex in vertices))
    return {"job_id": job_id, "name": job.get("name") if isinstance(job, dict) else None, "vertices": rows}


async def command_job_health(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace, job_id: str) -> dict[str, Any]:
    """Build a one-shot health report for a job."""
    job_task = asyncio.create_task(client.get_json(f"jobs/{job_id}"))
    graph_task = asyncio.create_task(fetch_job_graph(client, job_id, args))
    checkpoint_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/checkpoints"))
    checkpoint_config_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/checkpoints/config"))
    exceptions_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/exceptions"))
    memory_task = asyncio.create_task(diagnose_memory(client, parsed, args))
    job, graph, checkpoints, checkpoint_config, exceptions, memory = await asyncio.gather(
        job_task, graph_task, checkpoint_task, checkpoint_config_task, exceptions_task, memory_task
    )
    flow = summarize_io_flow(list(job.get("vertices", [])) if isinstance(job, dict) else [])
    checkpoint_summary = summarize_checkpoint_payload({"checkpoints": checkpoints, "configuration": checkpoint_config})
    memory_top = summarize_memory_top(memory, top=getattr(args, "top", 5))
    report = build_health_report(
        {
            "job": job,
            "flow": flow,
            "backpressure": graph,
            "checkpoint": checkpoint_summary,
            "exceptions": exceptions,
            "memory_top": memory_top,
        }
    )
    return {
        **report,
        "flow": flow,
        "backpressure_top": graph.get("vertices", [])[: getattr(args, "top", 5)],
        "exceptions": exceptions,
    }


async def command_job_capacity(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace, job_id: str) -> dict[str, Any]:
    """生成并行度、吞吐、反压、倾斜和 Paimon sink 的一站式容量诊断报告。"""
    job_task = asyncio.create_task(client.get_json(f"jobs/{job_id}"))
    graph_task = asyncio.create_task(fetch_job_graph(client, job_id, args))
    checkpoint_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/checkpoints"))
    try:
        connectors_task = asyncio.create_task(summarize_paimon_job_connectors(client, job_id))
        job, graph, checkpoints, connectors = await asyncio.gather(job_task, graph_task, checkpoint_task, connectors_task)
    except FlinkDiagError as exc:
        job, graph, checkpoints = await asyncio.gather(job_task, graph_task, checkpoint_task)
        connectors = {"available": False, "error": str(exc), "connectors": []}

    vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []
    graph_by_id = {str(row.get("vertex_id")): row for row in graph.get("vertices", []) if isinstance(row, dict)}
    connector_by_id = {
        str(row.get("vertex_id")): row
        for row in connectors.get("connectors", [])
        if isinstance(row, dict) and row.get("vertex_id")
    }
    match_mode = getattr(args, "metric_match", "auto")

    async def fetch_vertex(vertex: dict[str, Any]) -> dict[str, Any]:
        """读取单个 vertex 的 subtask metrics 和 TaskManager 聚合，错误隔离到当前 vertex。"""
        vertex_id = str(vertex.get("id"))
        subtask_data: dict[str, Any] | None = None
        taskmanager_summary: dict[str, Any] | None = None
        subtask_error = None
        taskmanager_error = None
        try:
            subtask_data = await fetch_all_subtask_metric_values(client, job_id, vertex_id, _CAPACITY_METRICS, match_mode)
        except FlinkDiagError as exc:
            subtask_error = str(exc)
        try:
            taskmanager_data = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/taskmanagers")
            taskmanager_summary = summarize_taskmanager_aggregates(
                taskmanager_data,
                metrics=["read-records", "write-records", "accumulated-busy-time", "accumulated-backpressured-time"],
                sort_by="read-records",
            )
        except FlinkDiagError as exc:
            taskmanager_error = str(exc)
        return summarize_vertex_capacity(
            vertex,
            graph_by_id.get(vertex_id),
            subtask_data,
            taskmanager_summary,
            connector_by_id.get(vertex_id),
            subtask_error=subtask_error,
            taskmanager_error=taskmanager_error,
        )

    vertex_reports = list(await asyncio.gather(*(fetch_vertex(vertex) for vertex in vertices)))
    has_paimon = any(row.get("paimon_role") in {"writer", "committer", "source"} for row in connectors.get("connectors", []) if isinstance(row, dict))
    paimon = None
    if has_paimon:
        try:
            paimon = await diagnose_paimon_job(client, job_id, args)
        except FlinkDiagError as exc:
            paimon = {"available": False, "error": str(exc), "risks": []}
    checkpoint_trend = summarize_checkpoint_trend(checkpoints, limit=getattr(args, "limit", 10))
    lookup_join = None
    try:
        lookup_join = await diagnose_lookup_job(
            client,
            job_id,
            args,
            parsed=parsed,
            job=job,
            graph=graph,
            checkpoint_trend=checkpoint_trend,
            capacity_by_id={str(row.get("vertex_id")): row for row in vertex_reports if isinstance(row, dict)},
            include_logs=True,
        )
    except FlinkDiagError as exc:
        lookup_join = {"available": False, "error": str(exc), "risks": []}
    return build_capacity_report(
        job_id=job_id,
        job=job,
        flow=summarize_io_flow(vertices),
        checkpoint_trend=checkpoint_trend,
        connectors=connectors,
        paimon=paimon,
        lookup_join=lookup_join,
        vertex_reports=vertex_reports,
    )


async def command_task_chain(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle task-chain subcommands."""
    context = await resolve_context(client, parsed, args)
    job_id = require_value(context.job_id, "job_id")
    vertex_id = require_value(context.vertex_id, "vertex_id")
    sub = getattr(args, "subcommand", None) or "detail"
    if sub in {"detail", None}:
        return await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    if sub == "subtasks":
        detail, times = await asyncio.gather(
            client.get_json(f"jobs/{job_id}/vertices/{vertex_id}"),
            client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/subtasktimes"),
        )
        return {"detail": detail, "subtasktimes": times}
    if sub == "taskmanagers":
        return await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/taskmanagers")
    if sub == "taskmanager-aggregates":
        data = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/taskmanagers")
        return summarize_taskmanager_aggregates(
            data,
            metrics=split_csv(getattr(args, "get", None)) or None,
            sort_by=getattr(args, "sort_by", None),
            top=getattr(args, "top", None),
        )
    if sub == "watermarks":
        return await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/watermarks")
    if sub == "accumulators":
        return await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/accumulators")
    if sub == "backpressure":
        return await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/backpressure")
    if sub == "metrics":
        scope = getattr(args, "scope", "aggregate")
        requested = split_csv(getattr(args, "get", None))
        match_mode = getattr(args, "metric_match", "auto")
        if scope == "vertex":
            return await fetch_metric_values(client, f"jobs/{job_id}/vertices/{vertex_id}/metrics", requested, match_mode)
        if scope == "subtask":
            if getattr(args, "all_subtasks", False):
                return await fetch_all_subtask_metric_values(client, job_id, vertex_id, requested, match_mode)
            subtask = context.subtask
            if subtask is None:
                detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
                subtask = infer_single_subtask(detail, args)
            return await fetch_metric_values(
                client,
                f"jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask}/metrics",
                requested,
                match_mode,
            )
        return await fetch_metric_values(
            client,
            f"jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics",
            requested,
            match_mode,
        )
    if sub == "skew":
        requested = split_csv(getattr(args, "get", None)) or [
            "numRecordsInPerSecond",
            "numRecordsOutPerSecond",
            "busyTimeMsPerSecond",
            "idleTimeMsPerSecond",
            "backPressuredTimeMsPerSecond",
        ]
        metrics = await fetch_all_subtask_metric_values(client, job_id, vertex_id, requested, getattr(args, "metric_match", "auto"))
        return summarize_subtask_skew(metrics)
    if sub == "source-stats":
        return await command_source_stats(client, job_id, vertex_id)
    if sub == "source-lag":
        return await command_source_lag(client, job_id, vertex_id)
    if sub == "sink-stats":
        return await command_sink_stats(client, job_id, vertex_id)
    if sub == "paimon-stats":
        return await fetch_paimon_task_chain_summary(
            client,
            job_id,
            vertex_id,
            role=getattr(args, "role", "auto"),
            match_mode=getattr(args, "metric_match", "auto"),
        )
    if sub == "flamegraph":
        return await fetch_flamegraph(client, job_id, vertex_id, args, context)
    raise FlinkDiagError(f"Unsupported task-chain subcommand: {sub}")


async def command_source_stats(client: FlinkClient, job_id: str, vertex_id: str) -> dict[str, Any]:
    """Analyze source-oriented metrics for a task chain."""
    detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    metrics = await fetch_semantic_metrics(
        client,
        f"jobs/{job_id}/vertices/{vertex_id}/metrics",
        ["source.records_in", "source.records_in_rate", "source.records_out", "source.records_out_rate", "source.current_offset", "source.committed_offset", "source.watermark"],
    )
    return {
        "job_id": job_id,
        "vertex_id": vertex_id,
        "name": detail.get("name") if isinstance(detail, dict) else None,
        "parallelism": detail.get("parallelism") if isinstance(detail, dict) else None,
        "summary": {
            "records_in": sum_metric_values(metrics.get("source.records_in", [])),
            "records_in_rate": sum_metric_values(metrics.get("source.records_in_rate", [])),
            "records_out": sum_metric_values(metrics.get("source.records_out", [])),
            "records_out_rate": sum_metric_values(metrics.get("source.records_out_rate", [])),
        },
        "metrics": metrics,
    }


async def command_source_lag(client: FlinkClient, job_id: str, vertex_id: str) -> dict[str, Any]:
    """Analyze source offset and lag metrics for a task chain."""
    detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    metrics = await fetch_semantic_metrics(
        client,
        f"jobs/{job_id}/vertices/{vertex_id}/metrics",
        ["source.current_offset", "source.committed_offset", "source.records_lag", "source.assigned_partitions"],
    )
    current_total = sum_metric_values(metrics.get("source.current_offset", []))
    committed_total = sum_metric_values(metrics.get("source.committed_offset", []))
    current = to_float(current_total)
    committed = to_float(committed_total)
    offset_commit_delta = numeric_value(current - committed) if current is not None and committed is not None else None
    return {
        "job_id": job_id,
        "vertex_id": vertex_id,
        "name": detail.get("name") if isinstance(detail, dict) else None,
        "parallelism": detail.get("parallelism") if isinstance(detail, dict) else None,
        "summary": {
            "current_offset_total": current_total,
            "committed_offset_total": committed_total,
            "offset_commit_delta": offset_commit_delta,
            "records_lag_max": max_metric_value(metrics.get("source.records_lag", [])),
            "assigned_partitions": sum_metric_values(metrics.get("source.assigned_partitions", [])),
        },
        "metrics": metrics,
    }


def max_metric_value(entries: list[dict[str, Any]]) -> Any:
    """Return the max numeric metric value from entries."""
    values = [to_float(item.get("value")) for item in entries]
    numbers = [value for value in values if value is not None]
    if not numbers:
        return None
    value = max(numbers)
    return str(int(value)) if value.is_integer() else str(value)


def detect_paimon_metric_role(available: list[str]) -> dict[str, Any]:
    """Detect whether a metric list belongs to a Paimon source, writer, or committer."""
    committer = dedupe([metric for alias in _PAIMON_COMMITTER_ALIASES for metric in match_metric_alias(available, alias)])
    writer = dedupe([metric for alias in _PAIMON_ROLE_WRITER_ALIASES for metric in match_metric_alias(available, alias)])
    source = [metric for metric in available if _PAIMON_SOURCE_RE.search(metric)]
    if committer:
        role = "committer"
        evidence = committer
    elif writer:
        role = "writer"
        evidence = writer
    elif source:
        role = "source"
        evidence = source
    else:
        role = "unknown"
        evidence = []
    return {
        "role": role,
        "evidence": evidence[:20],
        "writer_metric_count": len(writer),
        "committer_metric_count": len(committer),
        "source_metric_count": len(source),
    }


def is_source_vertex(vertex: dict[str, Any], available: list[str]) -> bool:
    """Return whether a vertex appears to be an upstream source task chain."""
    name = str(vertex.get("name", "")).lower()
    if "source" in name:
        return True
    return bool(
        match_metric_alias(available, "source.records_in")
        or match_metric_alias(available, "source.records_out")
        or match_metric_alias(available, "source.records_lag")
    )


def skew_risks(skew: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return Paimon writer skew risks from a subtask skew summary."""
    if not isinstance(skew, dict):
        return []
    risks: list[dict[str, Any]] = []
    for metric, summary in (skew.get("metrics") or {}).items():
        if not isinstance(summary, dict):
            continue
        ratio = to_float(summary.get("skew_ratio"))
        if ratio is not None and ratio >= 3:
            risks.append(
                {
                    "type": "writer_skew",
                    "level": "warning",
                    "message": "Paimon writer metrics are skewed across subtasks.",
                    "metric": metric,
                    "evidence": summary,
                }
            )
            break
    return risks


def summarize_paimon_writer_metrics(metrics: dict[str, list[dict[str, Any]]], skew: dict[str, Any] | None = None) -> dict[str, Any]:
    """Summarize Paimon writer throughput, buffer pressure, compaction, and skew."""
    compaction_busy = max_metric_value(metrics.get("paimon.compaction.busy", []))
    level0_file_count = max_metric_value(metrics.get("paimon.compaction.level0_file_count", []))
    summary = {
        "records_in": sum_metric_values(metrics.get("paimon.writer.records_in", [])),
        "records_in_rate": sum_metric_values(metrics.get("paimon.writer.records_in_rate", [])),
        "buffer_writers": sum_metric_values(metrics.get("paimon.writer.buffer_writers", [])),
        "buffer_preempt_count": sum_metric_values(metrics.get("paimon.writer.buffer_preempt_count", [])),
        "compaction_busy": compaction_busy,
        "compaction_completed_count": sum_metric_values(metrics.get("paimon.compaction.completed_count", [])),
        "compaction_level0_file_count": level0_file_count,
        "compaction_total_file_size": sum_metric_values(metrics.get("paimon.compaction.total_file_size", [])),
        "compaction_total_file_size_human": bytes_human(sum_metric_values(metrics.get("paimon.compaction.total_file_size", []))),
        "compaction_input_size": sum_metric_values(metrics.get("paimon.compaction.input_size", [])),
        "compaction_output_size": sum_metric_values(metrics.get("paimon.compaction.output_size", [])),
        "compaction_time": max_metric_value(metrics.get("paimon.compaction.time", [])),
        "compaction_time_human": millis_human(max_metric_value(metrics.get("paimon.compaction.time", []))),
    }
    risks: list[dict[str, Any]] = []
    if float(to_float(compaction_busy) or 0) >= 80 or float(to_float(level0_file_count) or 0) >= 50:
        risks.append(
            {
                "type": "compaction_busy",
                "level": "warning",
                "message": "Paimon compaction looks busy or has a large level-0 backlog.",
                "evidence": {
                    "compaction_busy": compaction_busy,
                    "level0_file_count": level0_file_count,
                },
            }
        )
    risks.extend(skew_risks(skew))
    return {"summary": summary, "risks": risks, "metrics": metrics, "skew": skew}


def summarize_paimon_committer_metrics(metrics: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Summarize Paimon committer latency, attempts, file counts, and commit risks."""
    duration = max_metric_value(metrics.get("paimon.commit.duration", []))
    duration_p99 = max_metric_value(metrics.get("paimon.commit.duration_p99", []))
    files_added = sum_metric_values(metrics.get("paimon.commit.files_added", []))
    files_appended = sum_metric_values(metrics.get("paimon.commit.files_appended", []))
    partitions = sum_metric_values(metrics.get("paimon.commit.partitions_written", []))
    buckets = sum_metric_values(metrics.get("paimon.commit.buckets_written", []))
    attempts = max_metric_value(metrics.get("paimon.commit.attempts", []))
    summary = {
        "commit_duration": duration,
        "commit_duration_human": millis_human(duration),
        "commit_duration_p99": duration_p99,
        "commit_duration_p99_human": millis_human(duration_p99),
        "files_added": files_added,
        "files_appended": files_appended,
        "files_deleted": sum_metric_values(metrics.get("paimon.commit.files_deleted", [])),
        "partitions_written": partitions,
        "buckets_written": buckets,
        "snapshots": sum_metric_values(metrics.get("paimon.commit.snapshots", [])),
        "commit_attempts": attempts,
    }
    risks: list[dict[str, Any]] = []
    if float(to_float(duration_p99) or to_float(duration) or 0) >= 30000 or float(to_float(attempts) or 0) > 1:
        risks.append(
            {
                "type": "commit_slow",
                "level": "warning",
                "message": "Paimon commit latency or retry attempts are high.",
                "evidence": {
                    "commit_duration": duration,
                    "commit_duration_p99": duration_p99,
                    "commit_attempts": attempts,
                },
            }
        )
    file_total = float(to_float(files_added) or 0) + float(to_float(files_appended) or 0)
    if file_total >= 100 or float(to_float(partitions) or 0) >= 100 or float(to_float(buckets) or 0) >= 100:
        risks.append(
            {
                "type": "small_files_risk",
                "level": "info",
                "message": "A commit touched many files, partitions, or buckets; check small-file pressure.",
                "evidence": {
                    "files_added": files_added,
                    "files_appended": files_appended,
                    "partitions_written": partitions,
                    "buckets_written": buckets,
                },
            }
        )
    return {"summary": summary, "risks": risks, "metrics": metrics}


async def summarize_paimon_job_connectors(client: FlinkClient, job_id: str) -> dict[str, Any]:
    """Scan a job graph and classify Paimon connector roles by vertex metrics."""
    job = await client.get_json(f"jobs/{job_id}")
    vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []

    async def inspect_vertex(vertex: dict[str, Any]) -> dict[str, Any]:
        """Fetch metric ids for one vertex and classify its connector role."""
        vertex_id = str(vertex.get("id"))
        try:
            available = metric_ids(await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/metrics"))
            role = detect_paimon_metric_role(available)
            error = None
        except FlinkDiagError as exc:
            available = []
            role = detect_paimon_metric_role(available)
            error = str(exc)
        return {
            "vertex_id": vertex_id,
            "name": vertex.get("name"),
            "parallelism": vertex.get("parallelism"),
            "status": vertex.get("status"),
            "paimon_role": role["role"],
            "evidence": role["evidence"],
            "metric_counts": {
                "writer": role["writer_metric_count"],
                "committer": role["committer_metric_count"],
                "source": role["source_metric_count"],
            },
            "is_source": is_source_vertex(vertex, available),
            "error": error,
        }

    connectors = list(await asyncio.gather(*(inspect_vertex(vertex) for vertex in vertices)))
    actual_sources = [
        {
            "vertex_id": row.get("vertex_id"),
            "name": row.get("name"),
            "paimon_role": row.get("paimon_role"),
        }
        for row in connectors
        if row.get("is_source")
    ]
    return {
        "job_id": job_id,
        "name": job.get("name") if isinstance(job, dict) else None,
        "state": job.get("state") if isinstance(job, dict) else None,
        "connectors": connectors,
        "source_absent": not any(row.get("paimon_role") == "source" for row in connectors),
        "actual_source_vertices": actual_sources,
    }


async def fetch_paimon_task_chain_summary(
    client: FlinkClient,
    job_id: str,
    vertex_id: str,
    *,
    role: str = "auto",
    match_mode: str = "auto",
) -> dict[str, Any]:
    """Fetch and summarize Paimon metrics for one task chain."""
    detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    base_path = f"jobs/{job_id}/vertices/{vertex_id}/metrics"
    available = metric_ids(await client.get_json(base_path))
    detected = detect_paimon_metric_role(available)
    active_role = detected["role"] if role == "auto" else role
    if active_role == "committer":
        metrics = await fetch_semantic_metrics(client, base_path, _PAIMON_COMMITTER_ALIASES)
        payload = summarize_paimon_committer_metrics(metrics)
    elif active_role == "writer":
        metrics = await fetch_semantic_metrics(client, base_path, _PAIMON_WRITER_ALIASES)
        skew = None
        try:
            skew_data = await fetch_all_subtask_metric_values(
                client,
                job_id,
                vertex_id,
                ["paimon.writer.records_in_rate", "paimon.compaction.busy"],
                match_mode,
            )
            skew = summarize_subtask_skew(skew_data)
        except FlinkDiagError:
            skew = None
        payload = summarize_paimon_writer_metrics(metrics, skew)
    else:
        metrics = await fetch_semantic_metrics(client, base_path, _PAIMON_WRITER_ALIASES + _PAIMON_COMMITTER_ALIASES)
        payload = {"summary": {}, "risks": [], "metrics": metrics}
    return {
        "job_id": job_id,
        "vertex_id": vertex_id,
        "name": detail.get("name") if isinstance(detail, dict) else None,
        "parallelism": detail.get("parallelism") if isinstance(detail, dict) else None,
        "paimon_role": active_role,
        "detected_role": detected,
        **payload,
    }


async def diagnose_paimon_job(client: FlinkClient, job_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """Diagnose Paimon writer, committer, source absence, and connector risks for a job."""
    connectors = await summarize_paimon_job_connectors(client, job_id)
    writer_rows = [row for row in connectors["connectors"] if row.get("paimon_role") == "writer"]
    committer_rows = [row for row in connectors["connectors"] if row.get("paimon_role") == "committer"]
    match_mode = getattr(args, "metric_match", "auto")
    writer = (
        await fetch_paimon_task_chain_summary(client, job_id, str(writer_rows[0]["vertex_id"]), role="writer", match_mode=match_mode)
        if writer_rows
        else None
    )
    committer = (
        await fetch_paimon_task_chain_summary(client, job_id, str(committer_rows[0]["vertex_id"]), role="committer", match_mode=match_mode)
        if committer_rows
        else None
    )
    risks: list[dict[str, Any]] = []
    if writer:
        risks.extend(writer.get("risks", []))
    if committer:
        risks.extend(committer.get("risks", []))
    if connectors.get("source_absent"):
        risks.append(
            {
                "type": "source_absent",
                "level": "info",
                "message": "No Paimon source metrics were found; the upstream source appears to be another connector.",
                "evidence": connectors.get("actual_source_vertices", []),
                "suggestion": "Use `diagnose source` or `task-chain source-lag` on the actual source vertex.",
            }
        )
    return {
        "job_id": job_id,
        "name": connectors.get("name"),
        "state": connectors.get("state"),
        "connectors": connectors,
        "writer": writer,
        "committer": committer,
        "risks": risks,
        "next_commands": [
            "job connectors --url <job-url> --json",
            "task-chain paimon-stats --url <paimon-writer-or-committer-url> --json",
            "diagnose source-lag --url <source-task-chain-url> --json",
            "job checkpoint-trend --url <job-url> --json",
        ],
    }


async def command_sink_stats(client: FlinkClient, job_id: str, vertex_id: str) -> dict[str, Any]:
    """Analyze sink-oriented metrics for a task chain."""
    detail = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}")
    metrics = await fetch_semantic_metrics(
        client,
        f"jobs/{job_id}/vertices/{vertex_id}/metrics",
        ["sink.records_send", "sink.records_send_rate", "sink.records_send_errors", "sink.bytes_send", "sink.bytes_send_rate", "sink.request_latency", "sink.throttle_time"],
    )
    errors = best_metric_value(metrics.get("sink.records_send_errors", []))
    return {
        "job_id": job_id,
        "vertex_id": vertex_id,
        "name": detail.get("name") if isinstance(detail, dict) else None,
        "parallelism": detail.get("parallelism") if isinstance(detail, dict) else None,
        "summary": {
            "records_send": sum_metric_values(metrics.get("sink.records_send", [])),
            "records_send_rate": sum_metric_values(metrics.get("sink.records_send_rate", [])),
            "records_send_errors": sum_metric_values(metrics.get("sink.records_send_errors", [])) or errors,
            "bytes_send": sum_metric_values(metrics.get("sink.bytes_send", [])),
            "note": "Task-chain Records Sent can be 0 for terminal sinks; use sink writer / KafkaProducer send metrics for actual writes.",
        },
        "metrics": metrics,
    }


async def command_backpressure(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle the backpressure command."""
    context = await resolve_context(client, parsed, args)
    job_id = require_value(context.job_id, "job_id")
    if getattr(args, "samples", 1) > 1:
        return await command_backpressure_trend(client, job_id, args)
    job = await client.get_json(f"jobs/{job_id}")
    vertices = list(job.get("vertices", []))

    async def fetch(vertex: dict[str, Any]) -> dict[str, Any]:
        """Fetch and summarize backpressure for one vertex."""
        vertex_id = str(vertex.get("id"))
        bp = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/backpressure")
        summary = summarize_vertex(vertex, bp)
        return summary

    results = await asyncio.gather(*(fetch(vertex) for vertex in vertices))
    results.sort(key=lambda item: float(item.get("backpressured_max") or 0), reverse=True)
    top = getattr(args, "top", None)
    return {"job_id": job_id, "vertices": results[:top] if top else results}


async def command_backpressure_trend(client: FlinkClient, job_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """Sample backpressure repeatedly to distinguish transient from persistent pressure."""
    samples = max(1, int(getattr(args, "samples", 1)))
    interval = max(0.0, float(getattr(args, "interval", 0.0)))
    snapshots: list[dict[str, Any]] = []
    for index in range(samples):
        job = await client.get_json(f"jobs/{job_id}")
        vertices = list(job.get("vertices", [])) if isinstance(job, dict) else []

        async def fetch(vertex: dict[str, Any]) -> dict[str, Any]:
            """Fetch one vertex backpressure sample."""
            vertex_id = str(vertex.get("id"))
            bp = await client.get_json(f"jobs/{job_id}/vertices/{vertex_id}/backpressure")
            return summarize_vertex(vertex, bp)

        rows = await asyncio.gather(*(fetch(vertex) for vertex in vertices))
        rows.sort(key=lambda item: float(item.get("backpressured_max") or 0), reverse=True)
        snapshots.append({"sample": index, "vertices": rows[: getattr(args, "top", 10)]})
        if index < samples - 1 and interval:
            await asyncio.sleep(interval)
    persistent: dict[str, dict[str, Any]] = {}
    for snapshot in snapshots:
        for row in snapshot["vertices"]:
            key = str(row.get("vertex_id"))
            entry = persistent.setdefault(key, {"vertex_id": key, "name": row.get("name"), "high_samples": 0, "max_backpressured": 0.0})
            ratio = float(row.get("backpressured_max") or 0)
            entry["max_backpressured"] = max(float(entry["max_backpressured"]), ratio)
            if ratio >= 0.5 or str(row.get("backpressure_level", "")).lower() == "high":
                entry["high_samples"] += 1
    ranked = sorted(persistent.values(), key=lambda item: (int(item["high_samples"]), float(item["max_backpressured"])), reverse=True)
    return {"job_id": job_id, "samples": samples, "interval": interval, "persistent": ranked, "snapshots": snapshots}


async def command_metrics(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle the generic metrics command."""
    context = await resolve_context(client, parsed, args)
    job_id = require_value(context.job_id, "job_id")
    requested = split_csv(getattr(args, "get", None))
    match_mode = getattr(args, "metric_match", "auto")
    if context.vertex_id:
        subtask = context.subtask
        if subtask is not None:
            path = f"jobs/{job_id}/vertices/{context.vertex_id}/subtasks/{subtask}/metrics"
        else:
            path = f"jobs/{job_id}/vertices/{context.vertex_id}/subtasks/metrics"
        return await fetch_metric_values(client, path, requested, match_mode)
    return await fetch_metric_values(client, f"jobs/{job_id}/metrics", requested, match_mode)


async def command_metric(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle metric search and explanation commands."""
    sub = getattr(args, "subcommand", "search")
    if sub == "explain":
        return metric_explanation(require_value(getattr(args, "metric", None), "metric"))
    if sub == "aggregate":
        return await fetch_metric_aggregate(client, parsed, args)
    if sub == "watch":
        return await command_metric_watch(client, parsed, args)
    if sub == "analyze":
        return await command_metric_analyze(client, parsed, args)
    path = await metric_path_for_context(client, parsed, args)
    available = metric_ids(await client.get_json(path))
    sources = [{"source": "primary", "path": path, "metric_ids": available}]
    if getattr(args, "scope", None) == "auto" and "/vertices/" in path and path.endswith("/metrics"):
        jm_path = path[: -len("/metrics")] + "/jm-operator-metrics"
        jm_available, jm_data = await metric_endpoint_available(client, jm_path)
        if jm_available:
            sources.append({"source": "jm-operator", "path": jm_path, "metric_ids": metric_ids(jm_data)})
    keyword = getattr(args, "keyword", None) or getattr(args, "metric", None)
    if not keyword:
        if getattr(args, "structured", False):
            return {
                "path": path,
                "sources": [
                    {
                        "source": source["source"],
                        "path": source["path"],
                        "metrics": structure_metric_matches(source["metric_ids"], source=source["source"], path=source["path"]),
                    }
                    for source in sources
                ],
            }
        result: dict[str, Any] = {"path": path, "metric_ids": available}
        if len(sources) > 1:
            result["sources"] = sources
        return result
    limit = getattr(args, "limit", 100)
    matched_sources: list[dict[str, Any]] = []
    merged_matches: list[Any] = []
    for source in sources:
        matches = search_metric_ids(source["metric_ids"], keyword, regex=getattr(args, "regex", False))
        limited = matches[:limit]
        rendered: list[Any] = (
            structure_metric_matches(limited, source=source["source"], path=source["path"])
            if getattr(args, "structured", False)
            else limited
        )
        matched_sources.append({"source": source["source"], "path": source["path"], "matches": rendered})
        merged_matches.extend(rendered)
    result = {
        "path": path,
        "keyword": keyword,
        "matches": merged_matches[:limit],
    }
    if len(matched_sources) > 1:
        result["sources"] = matched_sources
    return result


def emit_metric_watch_sample(sample: dict[str, Any], args: argparse.Namespace) -> None:
    """输出 metric watch 的单轮采样结果。"""
    if getattr(args, "json", False):
        print(json.dumps(redact_sensitive(sample), ensure_ascii=False, sort_keys=False), flush=True)
        return
    line = f"sample={sample.get('sample')} scope={sample.get('scope')} elapsed={sample.get('elapsed_seconds'):.3f}s values={compact_json(sample.get('values'))}"
    if sample.get("delta"):
        line += f" delta={compact_json(sample.get('delta'))}"
    if sample.get("rate_per_second"):
        line += f" rate_per_second={compact_json(sample.get('rate_per_second'))}"
    print(line, flush=True)


async def command_metric_watch(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> dict[str, Any]:
    """按固定间隔轮询官方 metrics endpoint，并输出当前值、delta 或 rate。"""
    start = time.monotonic()
    previous_values: dict[str, float] | None = None
    previous_at: float | None = None
    sample_count = 0
    requested_samples = int(getattr(args, "samples", 1) or 1)
    duration = getattr(args, "duration", None)
    max_samples = None if duration is not None and requested_samples == 1 else requested_samples
    interval = max(0.1, float(getattr(args, "interval", 10.0) or 10.0))
    while True:
        now = time.monotonic()
        result = await fetch_metric_aggregate(client, parsed, args)
        values = list(result.get("values", [])) if isinstance(result.get("values"), list) else []
        current_map = numeric_metric_map(values)
        elapsed_delta = None if previous_at is None else now - previous_at
        sample: dict[str, Any] = {
            "type": "metric_sample",
            "sample": sample_count + 1,
            "elapsed_seconds": now - start,
            "scope": result.get("scope"),
            "path": result.get("path"),
            "available": result.get("available"),
            "values": values,
        }
        if getattr(args, "delta", False):
            sample["delta"] = diff_metric_maps(previous_values, current_map, elapsed_delta, rate=False)
        if getattr(args, "rate", False):
            sample["rate_per_second"] = diff_metric_maps(previous_values, current_map, elapsed_delta, rate=True)
        emit_metric_watch_sample(sample, args)
        sample_count += 1
        previous_values = current_map
        previous_at = now
        if max_samples is not None and sample_count >= max_samples:
            break
        if duration is not None and time.monotonic() - start >= duration:
            break
        await asyncio.sleep(interval)
    return {"_skip_emit": True, "samples": sample_count}


def choose_metric_analyze_peak_key(samples: list[dict[str, Any]], requested: str | None = None) -> str | None:
    """选择用于峰值检测的 metric key，默认优先使用 busyTimeMsPerSecond.max。"""
    keys = list(samples[0].get("metric_map", {}).keys()) if samples else []
    if requested:
        if requested in keys:
            return requested
        suffix_matches = [key for key in keys if key.endswith(requested)]
        return suffix_matches[0] if len(suffix_matches) == 1 else requested
    for candidate in ("busyTimeMsPerSecond.max", "busyTimeMsPerSecond.avg", "Status.JVM.CPU.Load.max", "Status.JVM.CPU.Load.value"):
        if candidate in keys:
            return candidate
    for key in keys:
        if "busyTimeMsPerSecond" in key and key.endswith(".max"):
            return key
    return keys[0] if keys else None


def default_metric_peak_threshold(peak_key: str | None, explicit: float | None) -> float | None:
    """根据峰值 metric 类型选择默认阈值。"""
    if explicit is not None:
        return explicit
    if peak_key and "busyTimeMsPerSecond" in peak_key:
        return 900.0
    if peak_key and ("CPU.Load" in peak_key or peak_key.endswith(".cpu_load")):
        return 0.9
    return None


def metric_series_stats(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """汇总每个 metric key 在采样窗口内的 min/max/avg/last。"""
    series: dict[str, list[tuple[float, float]]] = {}
    for sample in samples:
        elapsed = float(sample.get("elapsed_seconds") or 0.0)
        metric_map = sample.get("metric_map") if isinstance(sample.get("metric_map"), dict) else {}
        for key, value in metric_map.items():
            number = to_float(value)
            if number is None:
                continue
            series.setdefault(str(key), []).append((elapsed, number))
    summary: dict[str, Any] = {}
    for key, rows in series.items():
        values = [value for _, value in rows]
        first = values[0]
        last = values[-1]
        summary[key] = {
            "sample_count": len(values),
            "min": numeric_value(min(values)),
            "max": numeric_value(max(values)),
            "avg": round(sum(values) / len(values), 6),
            "first": numeric_value(first),
            "last": numeric_value(last),
            "change": numeric_value(last - first),
            "change_pct": ratio_pct(last - first, first) if first else None,
            "nonzero_samples": sum(1 for value in values if value != 0),
        }
    return summary


def group_metric_peak_events(
    samples: list[dict[str, Any]],
    peak_key: str,
    threshold: float | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把连续超过阈值的样本合并成峰值事件。"""
    if threshold is None:
        return [], []
    peak_samples: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for sample in samples:
        metric_map = sample.get("metric_map") if isinstance(sample.get("metric_map"), dict) else {}
        value = to_float(metric_map.get(peak_key))
        if value is None or value < threshold:
            if current is not None:
                events.append(current)
                current = None
            continue
        peak_row = {
            "sample": sample.get("sample"),
            "elapsed_seconds": sample.get("elapsed_seconds"),
            "timestamp_ms": sample.get("timestamp_ms"),
            "value": numeric_value(value),
        }
        peak_samples.append(peak_row)
        if current is None:
            current = {
                "start_sample": sample.get("sample"),
                "end_sample": sample.get("sample"),
                "start_elapsed_seconds": sample.get("elapsed_seconds"),
                "end_elapsed_seconds": sample.get("elapsed_seconds"),
                "start_timestamp_ms": sample.get("timestamp_ms"),
                "end_timestamp_ms": sample.get("timestamp_ms"),
                "max_value": numeric_value(value),
                "max_sample": sample.get("sample"),
            }
        else:
            current["end_sample"] = sample.get("sample")
            current["end_elapsed_seconds"] = sample.get("elapsed_seconds")
            current["end_timestamp_ms"] = sample.get("timestamp_ms")
            if value > float(current.get("max_value") or 0):
                current["max_value"] = numeric_value(value)
                current["max_sample"] = sample.get("sample")
        current["duration_seconds"] = round(float(current.get("end_elapsed_seconds") or 0) - float(current.get("start_elapsed_seconds") or 0), 3)
    if current is not None:
        events.append(current)
    return peak_samples, events


def summarize_peak_periodicity(events: list[dict[str, Any]], *, tolerance: float = 0.35) -> dict[str, Any]:
    """根据峰值事件间隔判断是否呈现周期性。"""
    starts = [to_float(event.get("start_elapsed_seconds")) for event in events]
    starts = [value for value in starts if value is not None]
    intervals = [round(starts[index] - starts[index - 1], 3) for index in range(1, len(starts))]
    if not intervals:
        return {"event_count": len(events), "intervals_seconds": [], "periodic": False, "possible_periodic": False}
    avg_interval = sum(intervals) / len(intervals)
    variance = sum((value - avg_interval) ** 2 for value in intervals) / len(intervals)
    stddev = variance ** 0.5
    cv = stddev / avg_interval if avg_interval else None
    return {
        "event_count": len(events),
        "intervals_seconds": intervals,
        "avg_interval_seconds": round(avg_interval, 3),
        "min_interval_seconds": min(intervals),
        "max_interval_seconds": max(intervals),
        "coefficient_of_variation": round(cv, 6) if cv is not None else None,
        "possible_periodic": len(events) >= 2,
        "periodic": len(events) >= 3 and cv is not None and cv <= tolerance,
        "tolerance": tolerance,
    }


def checkpoint_history_rows(data: Any) -> list[dict[str, Any]]:
    """从 Flink checkpoints payload 中提取最近 checkpoint 行。"""
    checkpoints = data.get("checkpoints", data) if isinstance(data, dict) else {}
    history = checkpoints.get("history", []) if isinstance(checkpoints, dict) else []
    if isinstance(history, dict):
        history = history.get("completed", []) + history.get("failed", [])
    latest = checkpoints.get("latest") if isinstance(checkpoints, dict) else {}
    rows = [item for item in history if isinstance(item, dict)]
    if isinstance(latest, dict):
        for item in latest.values():
            if isinstance(item, dict) and item.get("id") is not None:
                rows.append(item)
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("id") or row.get("trigger_timestamp") or id(row))
        deduped[key] = row
    return sorted(deduped.values(), key=lambda item: int(item.get("trigger_timestamp") or 0))


def summarize_checkpoint_correlation(
    samples: list[dict[str, Any]],
    peak_events: list[dict[str, Any]],
    checkpoints: Any,
    *,
    window_seconds: float,
) -> dict[str, Any]:
    """判断 busy 峰值是否落在 checkpoint trigger/ack 附近。"""
    rows = checkpoint_history_rows(checkpoints)
    if not rows:
        return {"available": False, "reason": "no_checkpoint_history", "correlated_event_count": 0}
    triggers = [to_float(row.get("trigger_timestamp")) for row in rows]
    triggers = [value for value in triggers if value is not None]
    intervals = [(triggers[index] - triggers[index - 1]) / 1000.0 for index in range(1, len(triggers))]
    window_ms = max(0.0, window_seconds) * 1000.0
    correlated: list[dict[str, Any]] = []
    for event in peak_events:
        peak_ts = to_float(event.get("start_timestamp_ms")) or to_float(event.get("end_timestamp_ms"))
        if peak_ts is None:
            continue
        matches: list[dict[str, Any]] = []
        for row in rows:
            trigger = to_float(row.get("trigger_timestamp"))
            ack = to_float(row.get("latest_ack_timestamp")) or (
                trigger + to_float(row.get("end_to_end_duration")) if trigger is not None and to_float(row.get("end_to_end_duration")) is not None else None
            )
            if trigger is None:
                continue
            in_window = abs(peak_ts - trigger) <= window_ms or (ack is not None and trigger - window_ms <= peak_ts <= ack + window_ms)
            if not in_window:
                continue
            matches.append(
                {
                    "id": row.get("id"),
                    "status": row.get("status") or row.get("className"),
                    "trigger_timestamp": row.get("trigger_timestamp"),
                    "latest_ack_timestamp": row.get("latest_ack_timestamp"),
                    "end_to_end_duration_ms": row.get("end_to_end_duration"),
                    "distance_to_trigger_ms": numeric_value(peak_ts - trigger),
                }
            )
        if matches:
            correlated.append({"event": event, "checkpoints": matches[:3]})
    durations = [to_float(row.get("end_to_end_duration")) for row in rows]
    durations = [value for value in durations if value is not None]
    return {
        "available": True,
        "window_seconds": window_seconds,
        "checkpoint_count": len(rows),
        "interval_avg_seconds": round(sum(intervals) / len(intervals), 3) if intervals else None,
        "duration_avg_ms": round(sum(durations) / len(durations), 3) if durations else None,
        "duration_max_ms": numeric_value(max(durations)) if durations else None,
        "correlated_event_count": len(correlated),
        "correlated_events": correlated[:10],
    }


def choose_traffic_metric_key(series: dict[str, Any]) -> str | None:
    """选择代表吞吐的 metric key，优先使用 LookupJoin 实际输入 QPS。"""
    keys = list(series.keys())
    for key in keys:
        if "LookupJoin" in key and key.endswith("numRecordsInPerSecond.sum"):
            return key
    for key in keys:
        if key.endswith("numRecordsInPerSecond.sum"):
            return key
    for key in keys:
        if key.endswith("numRecordsOutPerSecond.sum"):
            return key
    return None


def traffic_stability(series: dict[str, Any]) -> dict[str, Any]:
    """判断峰值窗口内吞吐是否基本稳定。"""
    key = choose_traffic_metric_key(series)
    if not key:
        return {"available": False, "stable": None}
    summary = series.get(key, {})
    min_value = to_float(summary.get("min"))
    max_value = to_float(summary.get("max"))
    ratio = max_value / min_value if min_value and max_value is not None else None
    return {
        "available": True,
        "metric": key,
        "min": summary.get("min"),
        "max": summary.get("max"),
        "avg": summary.get("avg"),
        "max_min_ratio": round(ratio, 6) if ratio is not None else None,
        "stable": ratio is not None and ratio <= 1.3,
    }


def summarize_metric_analyze_findings(
    *,
    peak_key: str | None,
    peak_threshold: float | None,
    peak_events: list[dict[str, Any]],
    series: dict[str, Any],
    periodicity: dict[str, Any],
    checkpoint_correlation: dict[str, Any] | None,
) -> tuple[str, str, list[dict[str, Any]], list[str]]:
    """根据采样统计生成诊断结论、finding 和建议。"""
    findings: list[dict[str, Any]] = []
    recommendations: list[str] = []
    backpressure_max = to_float((series.get("backPressuredTimeMsPerSecond.max") or {}).get("max"))
    checkpoint_delay_max = to_float((series.get("checkpointStartDelayNanos.max") or {}).get("max"))
    traffic = traffic_stability(series)
    if not peak_events:
        conclusion = "no_peak_observed"
        conclusion_text = "采样窗口内没有超过阈值的峰值。"
        recommendations.append("如果 WebUI 峰值是周期性的，延长 --duration 或增加 --samples 覆盖至少 2-3 个周期。")
        return conclusion, conclusion_text, findings, recommendations
    findings.append(
        {
            "type": "peak_observed",
            "level": "warning",
            "message": "采样窗口内观察到目标 metric 峰值。",
            "evidence": {"metric": peak_key, "threshold": peak_threshold, "events": peak_events[:5]},
        }
    )
    if backpressure_max is not None and backpressure_max >= 100:
        findings.append(
            {
                "type": "backpressure_during_peak",
                "level": "warning",
                "message": "峰值期间存在 backPressuredTimeMsPerSecond，优先按下游反压排查。",
                "evidence": series.get("backPressuredTimeMsPerSecond.max"),
            }
        )
        conclusion = "backpressure_spike"
        conclusion_text = "busy 峰值伴随反压，优先排查下游链路或 sink。"
        recommendations.append("先用 backpressure --samples 和 job graph 沿下游定位瓶颈，再决定是否调整并行度。")
    elif (checkpoint_correlation or {}).get("correlated_event_count") or checkpoint_delay_max:
        findings.append(
            {
                "type": "checkpoint_state_spike",
                "level": "info",
                "message": "busy 峰值没有反压，更像 checkpoint/state/算子内部周期性工作造成的短时忙碌。",
                "evidence": {
                    "checkpoint_correlation": checkpoint_correlation,
                    "checkpointStartDelayNanos.max": series.get("checkpointStartDelayNanos.max"),
                },
            }
        )
        conclusion = "checkpoint_state_spike"
        conclusion_text = "busy(max) 周期性升高更像 checkpoint/state 或算子内部周期性工作；当前没有反压证据。"
        recommendations.append("观察峰值是否与 checkpoint 周期一致；如果 checkpoint duration/state size 持续升高，再评估状态大小、TTL、RocksDB/增量 checkpoint 和 checkpoint 间隔。")
    elif periodicity.get("possible_periodic"):
        conclusion = "periodic_busy_spike_without_backpressure"
        conclusion_text = "busy(max) 呈周期性峰值，但没有反压证据；更像周期性内部任务而非持续吞吐瓶颈。"
        recommendations.append("延长采样窗口，并同时采集 checkpointStartDelayNanos、GC/CPU、日志 warning 来确认周期来源。")
    else:
        conclusion = "busy_spike_without_backpressure"
        conclusion_text = "观察到 busy(max) 峰值，但没有反压证据；需要更长采样确认是否周期性。"
        recommendations.append("增加 --duration 或 --samples 覆盖多个峰值，再看峰值期间吞吐是否下降。")
    if traffic.get("available") and traffic.get("stable"):
        findings.append(
            {
                "type": "traffic_stable_during_window",
                "level": "info",
                "message": "采样窗口内吞吐基本稳定，busy 峰值没有伴随明显吞吐下跌。",
                "evidence": traffic,
            }
        )
    if (series.get("LookupJoin[7].lookupCacheHitRate.max") or {}).get("max") is not None:
        recommendations.append("LookupJoin cache hit 低只说明外部查询占比高；只有同时出现 busy 持续高、反压或 HBase timeout/retry 日志时才判定为 lookup 瓶颈。")
    return conclusion, conclusion_text, findings, dedupe(recommendations)


def analyze_metric_samples(
    samples: list[dict[str, Any]],
    args: argparse.Namespace,
    *,
    checkpoint_data: Any = None,
) -> dict[str, Any]:
    """对一段 metric watch 样本做带记忆的峰值、周期和 checkpoint 关联分析。"""
    peak_key = choose_metric_analyze_peak_key(samples, getattr(args, "peak_metric", None))
    threshold = default_metric_peak_threshold(peak_key, getattr(args, "peak_threshold", None))
    peak_samples, peak_events = group_metric_peak_events(samples, peak_key, threshold) if peak_key else ([], [])
    series = metric_series_stats(samples)
    periodicity = summarize_peak_periodicity(peak_events, tolerance=float(getattr(args, "period_tolerance", 0.35) or 0.35))
    checkpoint_correlation = None
    if checkpoint_data is not None:
        checkpoint_correlation = summarize_checkpoint_correlation(
            samples,
            peak_events,
            checkpoint_data,
            window_seconds=float(getattr(args, "checkpoint_window", 15.0) or 15.0),
        )
    conclusion, conclusion_text, findings, recommendations = summarize_metric_analyze_findings(
        peak_key=peak_key,
        peak_threshold=threshold,
        peak_events=peak_events,
        series=series,
        periodicity=periodicity,
        checkpoint_correlation=checkpoint_correlation,
    )
    result: dict[str, Any] = {
        "type": "metric_analysis",
        "sample_count": len(samples),
        "duration_seconds": round(float(samples[-1].get("elapsed_seconds") or 0.0), 3) if samples else 0.0,
        "peak_metric": peak_key,
        "peak_threshold": threshold,
        "conclusion": conclusion,
        "conclusion_text": conclusion_text,
        "series": series,
        "traffic": traffic_stability(series),
        "peaks": {
            "sample_count": len(peak_samples),
            "event_count": len(peak_events),
            "events": peak_events,
            "top_samples": sorted(peak_samples, key=lambda item: float(item.get("value") or 0), reverse=True)[: int(getattr(args, "max_peak_samples", 10) or 10)],
        },
        "periodicity": periodicity,
        "checkpoint_correlation": checkpoint_correlation,
        "findings": findings,
        "recommendations": recommendations,
    }
    if getattr(args, "include_samples", False):
        result["samples"] = samples
    return result


async def fetch_metric_analyze_checkpoints(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """在 metric analyze 中尽量读取 checkpoint 历史，失败时返回 unavailable 信息。"""
    if getattr(args, "no_checkpoint_correlation", False):
        return None
    has_job_hint = bool(
        parsed.job_id
        or getattr(args, "job_id", "auto") != "auto"
        or getattr(args, "job_name", None)
        or getattr(args, "vertex_id", None)
        or getattr(args, "task_chain_id", None)
        or getattr(args, "vertex_name", None)
        or getattr(args, "task_chain_name", None)
    )
    if not has_job_hint:
        return {"available": False, "reason": "job_id_not_resolved"}
    try:
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id or parsed.job_id, "job_id")
        return await client.get_json(f"jobs/{job_id}/checkpoints")
    except FlinkDiagError as exc:
        return {"available": False, "error": str(exc)}


async def command_metric_analyze(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> dict[str, Any]:
    """采样一段时间的 metric 数据并在内存中做趋势、峰值和 checkpoint 关联分析。"""
    analyze_args = clone_args_namespace(args)
    if not getattr(analyze_args, "get", None):
        analyze_args.get = ",".join(_METRIC_ANALYZE_DEFAULT_GET)
    if not getattr(analyze_args, "agg", None):
        analyze_args.agg = "min,max,avg,sum"
    plan = await prepare_metric_aggregate_request(client, parsed, analyze_args)
    if not plan.get("available"):
        return {"type": "metric_analysis", "available": False, "error": plan.get("error"), "path": plan.get("path")}
    start = time.monotonic()
    max_samples = int(getattr(analyze_args, "samples", 1) or 1)
    if getattr(analyze_args, "duration", None) is None and max_samples == 1:
        max_samples = 6
    interval = max(0.1, float(getattr(analyze_args, "interval", 0.0) or 10.0))
    duration = getattr(analyze_args, "duration", None)
    samples: list[dict[str, Any]] = []
    while True:
        now = time.monotonic()
        result = await fetch_prepared_metric_aggregate(client, plan)
        values = list(result.get("values", [])) if isinstance(result.get("values"), list) else []
        samples.append(
            {
                "sample": len(samples) + 1,
                "elapsed_seconds": now - start,
                "timestamp_ms": int(time.time() * 1000),
                "scope": result.get("scope"),
                "path": result.get("path"),
                "available": result.get("available"),
                "values": values,
                "metric_map": numeric_metric_map(values),
            }
        )
        if duration is not None and time.monotonic() - start >= float(duration):
            break
        if duration is None and len(samples) >= max_samples:
            break
        if duration is not None and len(samples) >= max_samples and max_samples > 1:
            break
        await asyncio.sleep(interval)
    checkpoint_data = await fetch_metric_analyze_checkpoints(client, parsed, analyze_args)
    analysis = analyze_metric_samples(samples, analyze_args, checkpoint_data=checkpoint_data)
    analysis.update(
        {
            "available": True,
            "scope": plan.get("scope"),
            "path": plan.get("path"),
            "metrics": plan.get("metrics"),
            "agg": plan.get("agg"),
            "interval_seconds": interval,
        }
    )
    return analysis


async def command_taskmanager(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle taskmanager subcommands."""
    sub = getattr(args, "subcommand", "list")
    if sub in {None, "list"}:
        return await client.get_json("taskmanagers")
    if sub == "memory-top":
        return summarize_memory_top(await diagnose_memory(client, parsed, args), top=getattr(args, "top", 5))
    context = await resolve_context(client, parsed, args)
    tm_id = require_value(context.taskmanager_id, "taskmanager_id")
    encoded = quote(tm_id, safe="")
    if sub == "show":
        return await client.get_json(f"taskmanagers/{encoded}")
    if sub == "metrics":
        return await fetch_metric_values(
            client,
            f"taskmanagers/{encoded}/metrics",
            split_csv(getattr(args, "get", None)),
            getattr(args, "metric_match", "auto"),
        )
    if sub == "memory":
        detail = await client.get_json(f"taskmanagers/{encoded}")
        metrics = await taskmanager_memory_metrics(client, encoded)
        return summarize_taskmanager_memory(detail, metrics)
    if sub == "stdout":
        return normalize_stdout_resource(
            await client.get_text_status(f"taskmanagers/{encoded}/stdout"),
            tm_id,
            args,
            taskmanager=context.taskmanager,
        )
    if sub == "thread-dump":
        data = await client.get_json(f"taskmanagers/{encoded}/thread-dump")
        return data if getattr(args, "format", "summary") in {"json", "raw"} else summarize_thread_dump(data, split_csv(getattr(args, "top", None)))
    if sub == "logs":
        return await command_logs(client, parsed, args, scope="taskmanager", taskmanager_id=tm_id)
    raise FlinkDiagError(f"Unsupported taskmanager subcommand: {sub}")


async def taskmanager_memory_metrics(client: FlinkClient, encoded_tm_id: str) -> dict[str, Any]:
    """Fetch TaskManager memory and GC metric values."""
    names = [
        "Status.JVM.Memory.Heap.Used",
        "Status.JVM.Memory.Heap.Max",
        "Status.JVM.Memory.NonHeap.Used",
        "Status.JVM.Memory.NonHeap.Max",
        "Status.JVM.Memory.Metaspace.Used",
        "Status.JVM.Memory.Metaspace.Max",
        "Status.JVM.Memory.Direct.MemoryUsed",
        "Status.JVM.Memory.Direct.TotalCapacity",
        "Status.Flink.Memory.Managed.Used",
        "Status.Flink.Memory.Managed.Total",
        "Status.Shuffle.Netty.UsedMemory",
        "Status.Shuffle.Netty.TotalMemory",
        "Status.Network.AvailableMemorySegments",
        "Status.Network.TotalMemorySegments",
        "Status.JVM.GarbageCollector.G1_Young_Generation.Count",
        "Status.JVM.GarbageCollector.G1_Young_Generation.Time",
        "Status.JVM.GarbageCollector.G1_Old_Generation.Count",
        "Status.JVM.GarbageCollector.G1_Old_Generation.Time",
        "Status.JVM.GarbageCollector.Copy.Count",
        "Status.JVM.GarbageCollector.Copy.Time",
        "Status.JVM.GarbageCollector.MarkSweepCompact.Count",
        "Status.JVM.GarbageCollector.MarkSweepCompact.Time",
        "Status.JVM.CPU.Load",
        "Status.JVM.Threads.Count",
    ]
    available = metric_ids(await client.get_json(f"taskmanagers/{encoded_tm_id}/metrics"))
    selected = [name for name in names if name in available]
    values = await fetch_metrics_by_chunks(client, f"taskmanagers/{encoded_tm_id}/metrics", selected)
    return {item.get("id"): item.get("value") for item in values if isinstance(item, dict)}


async def command_jobmanager(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle jobmanager subcommands."""
    sub = getattr(args, "subcommand", "metrics")
    if sub in {None, "metrics"}:
        return await fetch_metric_values(
            client,
            "jobmanager/metrics",
            split_csv(getattr(args, "get", None)),
            getattr(args, "metric_match", "auto"),
        )
    if sub == "config":
        return await client.get_json("jobmanager/config")
    if sub == "environment":
        return await client.get_json("jobmanager/environment")
    if sub == "stdout":
        return normalize_text_resource(await client.get_text_status("jobmanager/stdout"))
    if sub == "thread-dump":
        data = await client.get_json("jobmanager/thread-dump")
        return data if getattr(args, "format", "summary") in {"json", "raw"} else summarize_thread_dump(data, split_csv(getattr(args, "top", None)))
    if sub == "logs":
        return await command_logs(client, parsed, args, scope="jobmanager")
    raise FlinkDiagError(f"Unsupported jobmanager subcommand: {sub}")


async def resolve_log_path(client: FlinkClient, prefix: str, args: argparse.Namespace) -> tuple[str, str | None]:
    """Resolve the REST log path and optional named log file."""
    file_name = await resolve_log_file(client, f"{prefix}/logs", args)
    if file_name:
        return f"{prefix}/logs/{quote(file_name, safe='')}", file_name
    return f"{prefix}/log", None


def log_result_metadata(scope: str, path: str, file_name: str | None, *, mode: str, tail_bytes: int | None = None) -> dict[str, Any]:
    """Build common metadata for log command responses."""
    result: dict[str, Any] = {
        "scope": scope,
        "path": path,
        "file": file_name,
        "mode": mode,
    }
    if tail_bytes is not None:
        result["tail_bytes"] = tail_bytes
    return result


def default_log_download_path(scope: str, file_name: str | None, args: argparse.Namespace) -> Path:
    """Return a local destination for a downloaded log file."""
    output_dir = Path(getattr(args, "output_dir", None) or _DEFAULT_LOG_DOWNLOAD_DIR)
    raw_name = file_name or f"{scope}.log"
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(raw_name).name).strip("._") or f"{scope}.log"
    return output_dir / safe_name


def stdout_rest_path(taskmanager_id: str) -> str:
    """生成 TaskManager stdout 的 REST 相对路径。"""
    return f"taskmanagers/{quote(taskmanager_id, safe='')}/stdout"


def stdout_missing_reason(text: str) -> str | None:
    """识别 Kubernetes 模式下 WebUI stdout 缺失的提示文案。"""
    if _K8S_STDOUT_MISSING_RE.search(text):
        return "kubernetes_stdout_missing"
    return None


def kubectl_logs_recommendation(taskmanager_id: str, args: argparse.Namespace) -> dict[str, Any]:
    """根据当前参数生成安全的 kubectl logs 兜底提示。"""
    command = ["kubectl"]
    kube_context = getattr(args, "kube_context", None) or os.environ.get("KUBE_CONTEXT")
    namespace = getattr(args, "namespace", None) or os.environ.get("KUBE_NAMESPACE")
    container = getattr(args, "container", None) or os.environ.get("KUBE_CONTAINER")
    if kube_context:
        command.extend(["--context", str(kube_context)])
    if namespace:
        command.extend(["-n", str(namespace)])
    command.extend(["logs", taskmanager_id])
    if container:
        command.extend(["-c", str(container)])
    return {
        "provider": "kubectl",
        "pod": taskmanager_id,
        "command": " ".join(shlex.quote(part) for part in command),
        "requires_explicit_context": not bool(kube_context or namespace),
    }


def normalize_stdout_resource(
    resource: dict[str, Any],
    taskmanager_id: str,
    args: argparse.Namespace,
    *,
    taskmanager: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """统一 stdout 响应结构，并把 Kubernetes stdout 缺失转换为可操作提示。"""
    result = normalize_text_resource(resource)
    text = str(result.get("text", ""))
    missing_reason = stdout_missing_reason(text)
    result["taskmanager_id"] = taskmanager_id
    if taskmanager:
        result["taskmanager_path"] = taskmanager.get("path")
    if missing_reason:
        result["available"] = False
        result["reason"] = missing_reason
        result["recommendation"] = kubectl_logs_recommendation(taskmanager_id, args)
    return result


def stdout_result_metadata(action: str, taskmanager_id: str, path: str, *, mode: str, tail_bytes: int | None = None) -> dict[str, Any]:
    """构造 stdout 命令输出里的公共元数据。"""
    result: dict[str, Any] = {
        "source": "stdout",
        "action": action,
        "taskmanager_id": taskmanager_id,
        "path": path,
        "mode": mode,
    }
    if tail_bytes is not None:
        result["tail_bytes"] = tail_bytes
    return result


def default_stdout_download_path(taskmanager_id: str, args: argparse.Namespace) -> Path:
    """返回 stdout 下载到本机时使用的安全文件路径。"""
    output_dir = Path(getattr(args, "output_dir", None) or _DEFAULT_STDOUT_DOWNLOAD_DIR)
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", taskmanager_id).strip("._") or "taskmanager"
    return output_dir / f"{safe_name}.stdout"


async def select_stdout_taskmanagers(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> list[dict[str, Any]]:
    """按 URL 与过滤参数选择需要读取 stdout 的 TaskManager 列表。"""
    data = await client.get_json("taskmanagers")
    taskmanagers = list(data.get("taskmanagers", [])) if isinstance(data, dict) else []
    explicit_id = getattr(args, "taskmanager_id", None)
    use_parsed_id = parsed.taskmanager_id and not getattr(args, "all_taskmanagers", False) and not explicit_id
    target_id = explicit_id or (parsed.taskmanager_id if use_parsed_id else None)
    host = getattr(args, "taskmanager_host", None)
    index = getattr(args, "taskmanager_index", None)
    if target_id:
        taskmanagers = [tm for tm in taskmanagers if str(tm.get("id")) == str(target_id)]
    if host:
        taskmanagers = [
            tm for tm in taskmanagers if host in str(tm.get("id", "")) or host in str(tm.get("path", ""))
        ]
    if index is not None:
        taskmanagers = [pick_one(taskmanagers, index=index, label="taskmanager")]
    if not taskmanagers:
        if target_id:
            return [{"id": str(target_id)}]
        raise FlinkDiagError("No taskmanager matched stdout selection")
    if not getattr(args, "all_taskmanagers", False) and not target_id and not host and index is None and len(taskmanagers) > 1:
        raise FlinkDiagError("Multiple TaskManagers found; pass --all-taskmanagers or a TaskManager filter")
    return taskmanagers


def split_new_stdout_text(previous: str, current: str) -> tuple[str, bool]:
    """比较两轮 stdout 文本，返回新增部分和是否发生重置/窗口跳跃。"""
    if not previous:
        return current, False
    if current.startswith(previous):
        return current[len(previous) :], False
    position = current.find(previous)
    if position >= 0:
        return current[position + len(previous) :], False
    return current, True


def stdout_lines_from_delta(delta: str) -> list[str]:
    """把新增 stdout 文本拆成适合实时输出的行。"""
    if not delta:
        return []
    return delta.splitlines()


def emit_stdout_watch_event(event: dict[str, Any], args: argparse.Namespace) -> None:
    """按普通文本或 JSONL 格式输出 stdout watch 事件。"""
    if getattr(args, "json", False):
        print(json.dumps(redact_sensitive(event), ensure_ascii=False, sort_keys=False), flush=True)
        return
    tm_id = event.get("taskmanager_id", "unknown")
    event_type = event.get("type")
    if event_type == "line":
        print(f"[{tm_id}] {event.get('text', '')}", flush=True)
    elif event_type == "reset":
        print(f"[{tm_id}] reset: {event.get('reason', 'stdout window changed')}", flush=True)
    elif event_type == "unavailable":
        print(f"[{tm_id}] unavailable: {event.get('reason', 'stdout unavailable')}", flush=True)
    elif event_type == "error":
        print(f"[{tm_id}] error: {event.get('error', '')}", flush=True)


def log_diagnose_recommendations(scope: str, file_name: str | None, *, full: bool) -> list[str]:
    """Return safe follow-up commands for large log workflows."""
    scope_args = "--scope taskmanager" if scope == "taskmanager" else "--scope jobmanager"
    file_arg = f" --file {file_name}" if file_name else ""
    scan_mode = "--full" if full else "--tail-bytes 262144"
    return [
        f"logs grep {scope_args}{file_arg} --patterns ERROR,Exception --before 3 --after 8 {scan_mode}",
        f"logs errors {scope_args}{file_arg} --before 3 --after 8 {scan_mode}",
        f"logs download {scope_args}{file_arg} --max-bytes 104857600 --output-dir <local-dir>",
    ]


async def select_log_taskmanagers(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> list[dict[str, Any]]:
    """按日志命令参数选择 TaskManager，支持全量和 id/host/index 过滤。"""
    data = await client.get_json("taskmanagers")
    taskmanagers = list(data.get("taskmanagers", [])) if isinstance(data, dict) else []
    explicit_id = getattr(args, "taskmanager_id", None)
    use_parsed_id = parsed.taskmanager_id and not getattr(args, "all_taskmanagers", False) and not explicit_id
    target_id = explicit_id or (parsed.taskmanager_id if use_parsed_id else None)
    host = getattr(args, "taskmanager_host", None)
    index = getattr(args, "taskmanager_index", None)
    if target_id:
        taskmanagers = [tm for tm in taskmanagers if str(tm.get("id")) == str(target_id)]
    if host:
        taskmanagers = [
            tm for tm in taskmanagers if host in str(tm.get("id", "")) or host in str(tm.get("path", ""))
        ]
    if index is not None:
        taskmanagers = [pick_one(taskmanagers, index=index, label="taskmanager")]
    if not taskmanagers:
        if target_id:
            return [{"id": str(target_id)}]
        raise FlinkDiagError("No taskmanager matched log selection")
    if not getattr(args, "all_taskmanagers", False) and not target_id and not host and index is None and len(taskmanagers) > 1:
        raise FlinkDiagError("Multiple TaskManagers found; pass --all-taskmanagers or a TaskManager filter")
    return taskmanagers


async def run_logs_all_taskmanagers(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
    *,
    action: str,
) -> dict[str, Any]:
    """并行执行 TaskManager 日志 scan/grep/errors，并隔离单个 TM 的失败。"""
    if action not in {"scan", "grep", "errors"}:
        raise FlinkDiagError("--all-taskmanagers is currently supported for logs scan/grep/errors")
    taskmanagers = await select_log_taskmanagers(client, parsed, args)

    async def run_one(taskmanager: dict[str, Any]) -> dict[str, Any]:
        """对单个 TaskManager 执行一次日志诊断。"""
        tm_id = str(taskmanager.get("id"))
        child_args = clone_args_namespace(args)
        child_args.all_taskmanagers = False
        child_args.scope = "taskmanager"
        child_args.logs_action = action
        return await command_logs(client, parsed, child_args, scope="taskmanager", taskmanager_id=tm_id)

    raw_results = await asyncio.gather(*(run_one(tm) for tm in taskmanagers), return_exceptions=True)
    results: list[dict[str, Any]] = []
    for tm, item in zip(taskmanagers, raw_results):
        tm_id = str(tm.get("id"))
        if isinstance(item, Exception):
            results.append(
                {
                    "scope": "taskmanager",
                    "taskmanager_id": tm_id,
                    "available": False,
                    "action": action,
                    "error": str(item),
                }
            )
            continue
        row = dict(item)
        row["taskmanager_id"] = tm_id
        row["available"] = row.get("available", True)
        row["action"] = action
        results.append(row)
    return {
        "scope": "taskmanager",
        "all_taskmanagers": True,
        "action": action,
        "taskmanager_count": len(results),
        "taskmanagers": results,
        "results": results,
    }


async def command_logs(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
    *,
    scope: str | None = None,
    taskmanager_id: str | None = None,
) -> Any:
    """Handle generic and nested logs commands."""
    active_scope = scope or getattr(args, "scope", None) or "jobmanager"
    logs_action = getattr(args, "logs_action", None) or getattr(args, "action", None) or "list"
    if active_scope == "taskmanager" and getattr(args, "all_taskmanagers", False) and logs_action in {"scan", "grep", "errors"}:
        return await run_logs_all_taskmanagers(client, parsed, args, action=logs_action)
    if active_scope == "taskmanager":
        tm_id = taskmanager_id or getattr(args, "taskmanager_id", None) or parsed.taskmanager_id
        if not tm_id:
            context = await resolve_context(client, parsed, args)
            tm_id = require_value(context.taskmanager_id, "taskmanager_id")
        prefix = f"taskmanagers/{quote(tm_id, safe='')}"
    else:
        prefix = "jobmanager"
    if logs_action == "list":
        return await client.get_json(f"{prefix}/logs")
    if logs_action == "tail":
        path, file_name = await resolve_log_path(client, prefix, args)
        return await client.stream_tail(path, getattr(args, "tail_bytes", 65536))
    if logs_action == "scan":
        path, file_name = await resolve_log_path(client, prefix, args)
        tail = await client.stream_tail(path, getattr(args, "tail_bytes", 65536))
        return {**tail, "scan": scan_log_text(str(tail.get("text", "")), split_csv(getattr(args, "patterns", None)))}
    if logs_action == "grep":
        path, file_name = await resolve_log_path(client, prefix, args)
        patterns = split_csv(getattr(args, "patterns", None))
        before = max(0, int(getattr(args, "before", 0) or 0))
        after = max(0, int(getattr(args, "after", 0) or 0))
        max_matches = max(0, int(getattr(args, "max_matches", 50) or 0))
        if getattr(args, "full", False):
            grep = await client.stream_grep(path, patterns, before=before, after=after, max_matches=max_matches)
            return {**log_result_metadata(active_scope, path, file_name, mode="full"), "grep": grep}
        tail_bytes = getattr(args, "tail_bytes", 65536)
        tail = await client.stream_tail(path, tail_bytes)
        grep = grep_log_text(str(tail.get("text", "")), patterns, before=before, after=after, max_matches=max_matches)
        return {
            **log_result_metadata(active_scope, path, file_name, mode="tail", tail_bytes=tail_bytes),
            "bytes_read": tail.get("bytes_read"),
            "truncated": tail.get("truncated"),
            "grep": grep,
        }
    if logs_action == "errors":
        path, file_name = await resolve_log_path(client, prefix, args)
        patterns = split_csv(getattr(args, "patterns", None)) or _DEFAULT_LOG_ERROR_PATTERNS
        before = max(0, int(getattr(args, "before", 2) or 0))
        after = max(0, int(getattr(args, "after", 3) or 0))
        max_signatures = max(0, int(getattr(args, "max_signatures", 20) or 0))
        max_samples = max(0, int(getattr(args, "max_samples_per_signature", 2) or 0))
        if getattr(args, "full", False):
            errors = await client.stream_error_summary(
                path,
                patterns,
                before=before,
                after=after,
                max_signatures=max_signatures,
                max_samples_per_signature=max_samples,
            )
            return {**log_result_metadata(active_scope, path, file_name, mode="full"), "errors": errors}
        tail_bytes = getattr(args, "tail_bytes", 65536)
        tail = await client.stream_tail(path, tail_bytes)
        errors = summarize_log_errors_text(
            str(tail.get("text", "")),
            patterns,
            before=before,
            after=after,
            max_signatures=max_signatures,
            max_samples_per_signature=max_samples,
        )
        return {
            **log_result_metadata(active_scope, path, file_name, mode="tail", tail_bytes=tail_bytes),
            "bytes_read": tail.get("bytes_read"),
            "truncated": tail.get("truncated"),
            "errors": errors,
        }
    if logs_action == "download":
        path, file_name = await resolve_log_path(client, prefix, args)
        max_bytes = getattr(args, "max_bytes", None)
        if max_bytes is not None and max_bytes < 0:
            raise FlinkDiagError("--max-bytes must be non-negative")
        if max_bytes is None and not getattr(args, "full", False):
            raise FlinkDiagError("Refusing unbounded log download; pass --max-bytes <bytes> or explicit --full")
        destination = default_log_download_path(active_scope, file_name, args)
        download = await client.download_text(path, destination, max_bytes=max_bytes, overwrite=getattr(args, "overwrite", False))
        return {**log_result_metadata(active_scope, path, file_name, mode="full" if getattr(args, "full", False) else "bounded"), "download": download}
    if logs_action == "diagnose":
        path, file_name = await resolve_log_path(client, prefix, args)
        tail_bytes = getattr(args, "tail_bytes", 65536)
        patterns = split_csv(getattr(args, "patterns", None)) or _DEFAULT_LOG_ERROR_PATTERNS
        before = max(0, int(getattr(args, "before", 2) or 0))
        after = max(0, int(getattr(args, "after", 3) or 0))
        max_signatures = max(0, int(getattr(args, "max_signatures", 20) or 0))
        max_samples = max(0, int(getattr(args, "max_samples_per_signature", 2) or 0))
        if getattr(args, "full", False):
            errors = await client.stream_error_summary(
                path,
                patterns,
                before=before,
                after=after,
                max_signatures=max_signatures,
                max_samples_per_signature=max_samples,
            )
            mode = "full"
            bytes_read = errors.get("bytes_read")
            truncated = False
        else:
            tail = await client.stream_tail(path, tail_bytes)
            errors = summarize_log_errors_text(
                str(tail.get("text", "")),
                patterns,
                before=before,
                after=after,
                max_signatures=max_signatures,
                max_samples_per_signature=max_samples,
            )
            mode = "tail"
            bytes_read = tail.get("bytes_read")
            truncated = tail.get("truncated")
        return {
            **log_result_metadata(active_scope, path, file_name, mode=mode, tail_bytes=None if mode == "full" else tail_bytes),
            "bytes_read": bytes_read,
            "truncated": truncated,
            "errors": errors,
            "recommendations": log_diagnose_recommendations(active_scope, file_name, full=getattr(args, "full", False)),
        }
    raise FlinkDiagError(f"Unsupported logs action: {logs_action}")


async def stdout_tail_one(client: FlinkClient, taskmanager: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    """读取单个 TaskManager stdout 的尾部内容。"""
    tm_id = str(taskmanager.get("id"))
    path = stdout_rest_path(tm_id)
    tail_bytes = max(0, int(getattr(args, "tail_bytes", 65536) or 0))
    max_bytes = getattr(args, "max_bytes_per_poll", None)
    tail = await client.stream_tail(path, tail_bytes, max_bytes=max_bytes)
    normalized = normalize_stdout_resource(tail, tm_id, args, taskmanager=taskmanager)
    return {
        **stdout_result_metadata("tail", tm_id, path, mode="tail", tail_bytes=tail_bytes),
        **normalized,
    }


async def stdout_grep_one(client: FlinkClient, taskmanager: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    """搜索单个 TaskManager stdout，并只保留命中上下文。"""
    tm_id = str(taskmanager.get("id"))
    path = stdout_rest_path(tm_id)
    patterns = split_csv(getattr(args, "patterns", None))
    before = max(0, int(getattr(args, "before", 0) or 0))
    after = max(0, int(getattr(args, "after", 0) or 0))
    max_matches = max(0, int(getattr(args, "max_matches", 50) or 0))
    max_bytes = getattr(args, "max_bytes_per_poll", None)
    probe = await client.stream_tail(path, min(max(1, int(getattr(args, "tail_bytes", 65536) or 65536)), 4096), max_bytes=4096)
    normalized_probe = normalize_stdout_resource(probe, tm_id, args, taskmanager=taskmanager)
    if not normalized_probe.get("available"):
        return {
            **stdout_result_metadata("grep", tm_id, path, mode="probe"),
            **normalized_probe,
            "grep": {"match_count": 0, "matches": []},
        }
    if getattr(args, "full", False):
        grep = await client.stream_grep(path, patterns, before=before, after=after, max_matches=max_matches, max_bytes=max_bytes)
        return {
            **stdout_result_metadata("grep", tm_id, path, mode="full"),
            "available": bool(grep.get("available")),
            "status_code": grep.get("status_code"),
            "bytes_read": grep.get("bytes_read"),
            "bytes_limited": grep.get("bytes_limited"),
            "grep": grep,
        }
    tail_bytes = max(0, int(getattr(args, "tail_bytes", 65536) or 0))
    tail = await client.stream_tail(path, tail_bytes, max_bytes=max_bytes)
    normalized_tail = normalize_stdout_resource(tail, tm_id, args, taskmanager=taskmanager)
    if not normalized_tail.get("available"):
        return {
            **stdout_result_metadata("grep", tm_id, path, mode="tail", tail_bytes=tail_bytes),
            **normalized_tail,
            "grep": {"match_count": 0, "matches": []},
        }
    grep = grep_log_text(str(tail.get("text", "")), patterns, before=before, after=after, max_matches=max_matches)
    return {
        **stdout_result_metadata("grep", tm_id, path, mode="tail", tail_bytes=tail_bytes),
        "available": True,
        "status_code": tail.get("status_code"),
        "bytes_read": tail.get("bytes_read"),
        "bytes_limited": tail.get("bytes_limited"),
        "truncated": tail.get("truncated"),
        "grep": grep,
    }


async def stdout_download_one(client: FlinkClient, taskmanager: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    """下载单个 TaskManager stdout，且不把缺失提示写成本地日志。"""
    tm_id = str(taskmanager.get("id"))
    path = stdout_rest_path(tm_id)
    max_bytes = getattr(args, "max_bytes", None)
    if max_bytes is not None and max_bytes < 0:
        raise FlinkDiagError("--max-bytes must be non-negative")
    if max_bytes is None and not getattr(args, "full", False):
        raise FlinkDiagError("Refusing unbounded stdout download; pass --max-bytes <bytes> or explicit --full")
    probe = await client.stream_tail(path, 4096, max_bytes=4096)
    normalized_probe = normalize_stdout_resource(probe, tm_id, args, taskmanager=taskmanager)
    if not normalized_probe.get("available"):
        return {
            **stdout_result_metadata("download", tm_id, path, mode="probe"),
            **normalized_probe,
            "download": None,
        }
    destination = default_stdout_download_path(tm_id, args)
    download = await client.download_text(path, destination, max_bytes=max_bytes, overwrite=getattr(args, "overwrite", False))
    return {
        **stdout_result_metadata("download", tm_id, path, mode="full" if getattr(args, "full", False) else "bounded"),
        "available": bool(download.get("available")),
        "status_code": download.get("status_code"),
        "download": download,
    }


async def run_stdout_parallel(
    client: FlinkClient,
    parsed: ParsedUrl,
    args: argparse.Namespace,
    worker: Any,
) -> dict[str, Any]:
    """并行执行 stdout 子命令，并把单个 TaskManager 的失败隔离到结果项。"""
    taskmanagers = await select_stdout_taskmanagers(client, parsed, args)
    tasks = [worker(client, tm, args) for tm in taskmanagers]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)
    results: list[dict[str, Any]] = []
    for tm, item in zip(taskmanagers, raw_results):
        if isinstance(item, Exception):
            results.append(
                {
                    "source": "stdout",
                    "taskmanager_id": str(tm.get("id")),
                    "available": False,
                    "error": str(item),
                }
            )
        else:
            results.append(item)
    return {
        "source": "stdout",
        "provider": getattr(args, "provider", "rest"),
        "taskmanager_count": len(results),
        "taskmanagers": results,
    }


async def command_stdout_watch(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> dict[str, Any]:
    """轮询多个 TaskManager stdout，并实时输出增量行。"""
    taskmanagers = await select_stdout_taskmanagers(client, parsed, args)
    states = {str(tm.get("id")): StdoutWatchState() for tm in taskmanagers}
    start = time.monotonic()
    interval = max(0.1, float(getattr(args, "interval", 2.0) or 2.0))
    duration = getattr(args, "duration", None)
    max_events = getattr(args, "max_events", None)
    max_bytes = getattr(args, "max_bytes_per_poll", None)
    tail_bytes = max(0, int(getattr(args, "tail_bytes", 65536) or 0))
    polls = 0
    event_count = 0
    first_poll = True

    while True:
        poll_results = await asyncio.gather(
            *(stdout_tail_one(client, tm, args) for tm in taskmanagers),
            return_exceptions=True,
        )
        for tm, result in zip(taskmanagers, poll_results):
            tm_id = str(tm.get("id"))
            now = time.time()
            if isinstance(result, Exception):
                emit_stdout_watch_event(
                    {"type": "error", "taskmanager_id": tm_id, "timestamp": now, "error": str(result)},
                    args,
                )
                continue
            if not result.get("available"):
                reason = str(result.get("reason") or result.get("text") or "stdout unavailable")
                if states[tm_id].available:
                    emit_stdout_watch_event(
                        {
                            "type": "unavailable",
                            "taskmanager_id": tm_id,
                            "timestamp": now,
                            "reason": reason,
                            "recommendation": result.get("recommendation"),
                        },
                        args,
                    )
                states[tm_id].available = False
                continue
            text = str(result.get("text", ""))
            state = states[tm_id]
            if first_poll and getattr(args, "since_end", False):
                state.text = text
                state.available = True
                continue
            delta, reset = split_new_stdout_text(state.text, text)
            if reset:
                emit_stdout_watch_event(
                    {
                        "type": "reset",
                        "taskmanager_id": tm_id,
                        "timestamp": now,
                        "reason": "stdout shortened, restarted, or exceeded local tail window",
                        "tail_bytes": tail_bytes,
                        "max_bytes_per_poll": max_bytes,
                    },
                    args,
                )
            state.text = text
            state.available = True
            for line in stdout_lines_from_delta(delta):
                if max_events is not None and event_count >= max_events:
                    break
                emit_stdout_watch_event(
                    {"type": "line", "taskmanager_id": tm_id, "timestamp": now, "text": line},
                    args,
                )
                event_count += 1
        polls += 1
        first_poll = False
        if max_events is not None and event_count >= max_events:
            break
        max_polls = getattr(args, "polls", None)
        if max_polls is not None and polls >= max_polls:
            break
        if duration is not None and time.monotonic() - start >= duration:
            break
        await asyncio.sleep(interval)
    return {
        "_skip_emit": True,
        "source": "stdout",
        "action": "watch",
        "taskmanager_count": len(taskmanagers),
        "polls": polls,
        "events": event_count,
    }


async def command_stdout(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """处理顶层 stdout 子命令。"""
    provider = getattr(args, "provider", "rest")
    if provider not in {"rest", "auto"}:
        raise FlinkDiagError(f"Unsupported stdout provider: {provider}")
    action = getattr(args, "stdout_action", None) or "tail"
    if action == "tail":
        return await run_stdout_parallel(client, parsed, args, stdout_tail_one)
    if action == "grep":
        return await run_stdout_parallel(client, parsed, args, stdout_grep_one)
    if action == "download":
        return await run_stdout_parallel(client, parsed, args, stdout_download_one)
    if action == "watch":
        return await command_stdout_watch(client, parsed, args)
    raise FlinkDiagError(f"Unsupported stdout action: {action}")


async def command_diagnose(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle the diagnose command."""
    playbook = getattr(args, "playbook", None)
    if playbook == "backpressure":
        return await command_backpressure(client, parsed, args)
    if playbook == "checkpoint":
        args.subcommand = "checkpoint-summary"
        return await command_job(client, parsed, args)
    if playbook == "source":
        args.subcommand = "source-stats"
        return await command_task_chain(client, parsed, args)
    if playbook == "sink":
        args.subcommand = "sink-stats"
        return await command_task_chain(client, parsed, args)
    if playbook == "memory":
        return await diagnose_memory(client, parsed, args)
    if playbook == "memory-top":
        return summarize_memory_top(await diagnose_memory(client, parsed, args), top=getattr(args, "top", 5))
    if playbook == "flow":
        args.subcommand = "io-flow"
        return await command_job(client, parsed, args)
    if playbook == "health":
        args.subcommand = "health"
        return await command_job(client, parsed, args)
    if playbook == "skew":
        args.subcommand = "skew"
        return await command_job(client, parsed, args)
    if playbook == "checkpoint-trend":
        args.subcommand = "checkpoint-trend"
        return await command_job(client, parsed, args)
    if playbook == "source-lag":
        args.subcommand = "source-lag"
        return await command_task_chain(client, parsed, args)
    if playbook == "paimon":
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id, "job_id")
        return await diagnose_paimon_job(client, job_id, args)
    if playbook in {"lookup", "hbase-lookup"}:
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id, "job_id")
        return await diagnose_lookup_job(client, job_id, args, parsed=parsed, include_logs=True)
    if playbook in {"capacity", "parallelism"}:
        context = await resolve_context(client, parsed, args)
        job_id = require_value(context.job_id, "job_id")
        return await command_job_capacity(client, parsed, args, job_id)
    context = await resolve_context(client, parsed, args)
    job_id = require_value(context.job_id, "job_id")
    overview_task = asyncio.create_task(client.get_json("overview"))
    graph_task = asyncio.create_task(fetch_job_graph(client, job_id, args))
    checkpoints_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/checkpoints"))
    checkpoint_config_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/checkpoints/config"))
    exceptions_task = asyncio.create_task(client.get_json(f"jobs/{job_id}/exceptions"))
    overview, graph, checkpoints, checkpoint_config, exceptions = await asyncio.gather(
        overview_task, graph_task, checkpoints_task, checkpoint_config_task, exceptions_task
    )
    return {
        "detected_flink_version": context.flink_version,
        "endpoint_profile": context.endpoint_profile,
        "concurrency": getattr(args, "concurrency", 8),
        "overview": overview,
        "job_graph": graph,
        "checkpoints": checkpoints,
        "checkpoint_configuration": checkpoint_config,
        "exceptions": exceptions,
    }


async def diagnose_memory(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> dict[str, Any]:
    """Summarize memory and GC for all or selected TaskManagers."""
    data = await client.get_json("taskmanagers")
    taskmanagers = data.get("taskmanagers", []) if isinstance(data, dict) else []
    results = []
    for tm in taskmanagers:
        tm_id = str(tm.get("id"))
        if getattr(args, "taskmanager_id", None) and args.taskmanager_id != tm_id:
            continue
        metrics = await taskmanager_memory_metrics(client, quote(tm_id, safe=""))
        results.append(summarize_taskmanager_memory(tm, metrics))
    return {"taskmanagers": results}


async def command_inspect(client: FlinkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Inspect a WebUI URL by automatically selecting the matching diagnostic command."""
    if parsed.route_kind == "overview":
        args.command = "overview"
        return await command_overview(client, parsed, args)
    if parsed.route_kind == "jobs_completed":
        args.command = "jobs"
        args.subcommand = "completed"
        return await command_jobs(client, args)
    if parsed.route_kind == "job" and parsed.vertex_id:
        args.command = "task-chain"
        tab = parsed.tab or "detail"
        mapping = {
            "detail": "detail",
            "subtasks": "subtasks",
            "taskmanagers": "taskmanagers",
            "watermarks": "watermarks",
            "accumulators": "accumulators",
            "backpressure": "backpressure",
            "metrics": "metrics",
            "flamegraph": "flamegraph",
        }
        args.subcommand = mapping.get(tab, "detail")
        return await command_task_chain(client, parsed, args)
    if parsed.route_kind == "job":
        args.command = "job"
        tab = parsed.tab or "overview"
        if tab == "overview":
            args.subcommand = "graph"
        elif tab == "configuration":
            args.subcommand = "config"
        elif tab == "checkpoints":
            args.subcommand = "checkpoints"
            args.include_config = True
            args.summary = True
        else:
            args.subcommand = tab
        return await command_job(client, parsed, args)
    if parsed.route_kind == "taskmanager":
        args.command = "taskmanager"
        if not parsed.taskmanager_id:
            args.subcommand = "list"
        else:
            args.subcommand = {"log-list": "logs", "logs": "logs"}.get(parsed.tab or "show", parsed.tab or "show")
        if args.subcommand == "logs":
            args.logs_action = "diagnose" if parsed.tab == "logs" else "list"
        return await command_taskmanager(client, parsed, args)
    if parsed.route_kind == "jobmanager":
        args.command = "jobmanager"
        args.subcommand = {"log": "logs", "logs": "logs"}.get(parsed.tab or "metrics", parsed.tab or "metrics")
        if args.subcommand == "logs":
            args.logs_action = "diagnose" if parsed.tab == "logs" else "list"
        return await command_jobmanager(client, parsed, args)
    return await command_overview(client, parsed, args)


async def fetch_flamegraph(
    client: FlinkClient,
    job_id: str,
    vertex_id: str,
    args: argparse.Namespace,
    context: ResolveResult,
) -> Any:
    """Fetch and optionally summarize a task-chain FlameGraph."""
    flame_type = getattr(args, "type", "mixed") or "mixed"
    rest_type = "full" if flame_type == "mixed" else flame_type
    params: dict[str, str] = {"type": rest_type}
    if context.subtask is not None:
        params["subtask"] = str(context.subtask)
    path = f"jobs/{job_id}/vertices/{vertex_id}/flamegraph?{urlencode(params)}"
    data = await client.get_json(path)
    fmt = getattr(args, "format", "summary")
    if fmt in {"json", "raw"}:
        return data
    return summarize_flamegraph(data, getattr(args, "top", 20) or 20)


async def resolve_log_file(client: FlinkClient, list_path: str, args: argparse.Namespace) -> str | None:
    """Resolve a log file name from flags and log-list endpoint."""
    explicit = getattr(args, "file", None) or getattr(args, "log_file", None)
    if explicit:
        return explicit
    pattern = getattr(args, "file_pattern", None)
    if not pattern:
        return None
    data = await client.get_json(list_path)
    logs = list(data.get("logs", [])) if isinstance(data, dict) else []
    compiled = re.compile(pattern)
    matches = [item for item in logs if compiled.search(str(item.get("name", "")))]
    file_index = getattr(args, "file_index", None)
    selected = pick_one(matches, index=file_index, label="log file")
    return str(selected.get("name"))


def normalize_text_resource(resource: dict[str, Any]) -> dict[str, Any]:
    """Normalize stdout/log text resources into an availability payload."""
    text = str(resource.get("text", ""))
    available = bool(resource.get("available")) and "does not exist" not in text.lower()
    result = dict(resource)
    result["available"] = available
    return result


def infer_single_subtask(detail: dict[str, Any], args: argparse.Namespace) -> int:
    """Infer subtask 0 only when it is unambiguous."""
    if getattr(args, "all_subtasks", False):
        raise FlinkDiagError("--all-subtasks is not valid for this single-subtask operation")
    subtasks = detail.get("subtasks", []) if isinstance(detail, dict) else []
    if len(subtasks) == 1:
        return int(subtasks[0].get("subtask", 0))
    raise FlinkDiagError("Multiple subtasks exist; pass --subtask <index> or use an aggregate scope")


def require_value(value: str | None, name: str) -> str:
    """Return a required string or raise an actionable error."""
    if not value:
        raise FlinkDiagError(f"Unable to resolve {name}; pass it explicitly or run `resolve`/`inventory` first")
    return value


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common CLI options to a parser."""
    parser.add_argument("--url", default=None, help="Flink WebUI URL. Defaults to FLINK_WEB_URL.")
    parser.add_argument("--base-url", default=None, help="REST base URL override.")
    parser.add_argument("--origin", default=None, help="Cookie origin override.")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification.")
    parser.add_argument("--no-cookies", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=8, help="Maximum concurrent HTTP requests.")
    parser.add_argument("--retries", type=int, default=1, help="Retry count for transient GET failures.")
    parser.add_argument("--flink-version", default="auto", help="Flink version or auto.")
    parser.add_argument("--endpoint-profile", default="auto", help="Endpoint profile or auto.")
    parser.add_argument("--target", default=None, help="CDP target for WebUI text fallback.")
    parser.add_argument("--job-id", default="auto", help="Job id or auto.")
    parser.add_argument("--job-name", default=None, help="Select job by name.")
    parser.add_argument("--job-index", type=int, default=None, help="Select job by index among matches.")
    parser.add_argument("--job-state", default=None, choices=["running", "completed", "terminal", "all"], help="Filter job state.")
    parser.add_argument("--vertex-id", default=None, help="Task chain / vertex id.")
    parser.add_argument("--task-chain-id", default=None, help="Alias for --vertex-id.")
    parser.add_argument("--vertex-name", default=None, help="Select vertex by name.")
    parser.add_argument("--task-chain-name", default=None, help="Alias for --vertex-name.")
    parser.add_argument("--taskmanager-id", default=None, help="TaskManager id.")
    parser.add_argument("--taskmanager-host", default=None, help="Select TaskManager by host/path substring.")
    parser.add_argument("--taskmanager-index", type=int, default=None, help="Select TaskManager by index.")
    parser.add_argument("--all-taskmanagers", action="store_true", help="Select every TaskManager where supported.")
    parser.add_argument("--subtask", type=int, default=None, help="Subtask index.")
    parser.add_argument("--all-subtasks", action="store_true", help="Fetch all subtasks where supported.")
    parser.add_argument("--get", default=None, help="Comma-separated metric ids.")
    parser.add_argument("--metric-match", default="auto", choices=["auto", "exact", "suffix", "contains"], help="Metric match mode.")
    parser.add_argument("--file", default=None, help="Log file name.")
    parser.add_argument("--log-file", default=None, help="Alias for --file.")
    parser.add_argument("--file-pattern", default=None, help="Regex for selecting a log file from log-list.")
    parser.add_argument("--file-index", type=int, default=None, help="Log file index among matches.")
    parser.add_argument("--tail-bytes", type=int, default=65536, help="Tail bytes for log/stdout endpoints.")
    parser.add_argument("--full", action="store_true", help="Scan or download the full log stream instead of the bounded tail.")
    parser.add_argument("--before", type=int, default=0, help="Context lines before each log match.")
    parser.add_argument("--after", type=int, default=0, help="Context lines after each log match.")
    parser.add_argument("--max-matches", type=int, default=50, help="Maximum grep matches to keep in output.")
    parser.add_argument("--max-signatures", type=int, default=20, help="Maximum error signatures to keep in output.")
    parser.add_argument("--max-samples-per-signature", type=int, default=2, help="Maximum samples to keep per error signature.")
    parser.add_argument("--output-dir", default=None, help="Local directory for log downloads.")
    parser.add_argument("--max-bytes", type=int, default=None, help="Maximum bytes to download.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing downloaded log file.")
    parser.add_argument("--samples", type=int, default=1, help="Number of samples for trend commands.")
    parser.add_argument("--interval", type=float, default=0.0, help="Seconds between trend samples.")
    parser.add_argument("--patterns", default=None, help="Comma-separated log scan patterns.")
    parser.add_argument("--regex", action="store_true", help="Use regex matching for names.")


def add_stdout_args(parser: argparse.ArgumentParser) -> None:
    """补充 stdout 子命令专用参数。"""
    parser.add_argument("--provider", choices=["rest", "auto"], default="rest", help="stdout provider. auto still starts with WebUI REST.")
    parser.add_argument("--max-bytes-per-poll", type=int, default=None, help="Maximum bytes to read from each stdout endpoint per poll.")
    parser.add_argument("--duration", type=float, default=None, help="Maximum seconds to watch stdout.")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum stdout line events to emit while watching.")
    parser.add_argument("--polls", type=int, default=None, help="Maximum stdout polling rounds for watch.")
    parser.add_argument("--since-end", action="store_true", help="Start watch from the current stdout tail and only emit new lines.")
    parser.add_argument("--kube-context", default=None, help="Kubernetes context used in fallback kubectl logs recommendations.")
    parser.add_argument("--namespace", default=None, help="Kubernetes namespace used in fallback kubectl logs recommendations.")
    parser.add_argument("--container", default=None, help="Kubernetes container used in fallback kubectl logs recommendations.")


def add_metric_aggregate_args(parser: argparse.ArgumentParser) -> None:
    """补充官方 metrics 聚合 endpoint 参数。"""
    parser.add_argument("--scope", choices=["taskmanager", "job", "subtask", "jm-operator"], default="taskmanager")
    parser.add_argument("--agg", default=None, help="Comma-separated aggregations, such as min,max,avg,sum.")
    parser.add_argument("--taskmanagers", default=None, help="Comma-separated TaskManager ids for /taskmanagers/metrics subset.")
    parser.add_argument("--jobs", default=None, help="Comma-separated job ids for /jobs/metrics subset.")
    parser.add_argument("--subtasks", default=None, help="Comma-separated subtask indexes for subtask metrics subset.")


def add_metric_watch_args(parser: argparse.ArgumentParser) -> None:
    """补充 metric watch 的采样和趋势参数。"""
    parser.add_argument("--duration", type=float, default=None, help="Maximum seconds to watch metrics.")
    parser.add_argument("--delta", action="store_true", help="Emit numeric deltas between adjacent samples.")
    parser.add_argument("--rate", action="store_true", help="Emit numeric per-second rates between adjacent samples.")


def add_metric_analyze_args(parser: argparse.ArgumentParser) -> None:
    """补充 metric analyze 的峰值、周期和 checkpoint 关联参数。"""
    parser.add_argument("--duration", type=float, default=None, help="Maximum seconds to sample metrics before analysis.")
    parser.add_argument("--peak-metric", default=None, help="Metric map key used for peak detection, default busyTimeMsPerSecond.max.")
    parser.add_argument("--peak-threshold", type=float, default=None, help="Peak threshold. Defaults to 900 for busyTimeMsPerSecond.")
    parser.add_argument("--period-tolerance", type=float, default=0.35, help="Coefficient-of-variation tolerance for periodic peak detection.")
    parser.add_argument("--checkpoint-window", type=float, default=15.0, help="Seconds around checkpoint trigger/ack counted as correlated.")
    parser.add_argument("--no-checkpoint-correlation", action="store_true", help="Do not fetch checkpoint history for correlation.")
    parser.add_argument("--include-samples", action="store_true", help="Include raw metric samples in the final JSON output.")
    parser.add_argument("--max-peak-samples", type=int, default=10, help="Maximum top peak sample rows to keep.")


def build_parser() -> argparse.ArgumentParser:
    """Build the argparse parser."""
    parser = argparse.ArgumentParser(
        description="Diagnose Flink WebUI / Flink on Kubernetes through REST API and browser cookies.",
        epilog=(
            "Examples:\n"
            "  flink_diag.py inventory --url $FLINK_WEB_URL --json\n"
            "  flink_diag.py job graph --url $FLINK_JOB_OVERVIEW_URL --top-by backpressure\n"
            "  flink_diag.py task-chain metrics --url $FLINK_TASK_URL --scope aggregate --get busyTimeMsPerSecond\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("overview", "inventory", "resolve", "inspect", "diagnose", "backpressure", "metrics"):
        sub = subparsers.add_parser(name)
        add_common_args(sub)
    subparsers.choices["diagnose"].add_argument(
        "playbook",
        nargs="?",
        choices=[
            "backpressure",
            "checkpoint",
            "checkpoint-trend",
            "source",
            "source-lag",
            "sink",
            "memory",
            "memory-top",
            "flow",
            "health",
            "skew",
            "paimon",
            "lookup",
            "hbase-lookup",
            "capacity",
            "parallelism",
        ],
        help="Optional diagnostic playbook.",
    )
    subparsers.choices["backpressure"].add_argument("--top", type=int, default=10, help="Top vertices to show.")

    jobs = subparsers.add_parser("jobs")
    add_common_args(jobs)
    jobs_sub = jobs.add_subparsers(dest="subcommand")
    jobs_completed = jobs_sub.add_parser("completed")
    add_common_args(jobs_completed)

    job = subparsers.add_parser("job")
    add_common_args(job)
    job_sub = job.add_subparsers(dest="subcommand")
    for name in ("show", "exceptions", "exceptions-summary", "timeline", "config", "checkpoint-summary", "checkpoint-trend", "io-flow", "health", "skew", "connectors", "capacity"):
        child = job_sub.add_parser(name)
        add_common_args(child)
    job_sub.choices["exceptions-summary"].add_argument("--limit", type=int, default=10, help="Recent exception history entries to summarize.")
    job_sub.choices["checkpoint-trend"].add_argument("--limit", type=int, default=10, help="Recent checkpoint history count.")
    job_sub.choices["capacity"].add_argument("--limit", type=int, default=10, help="Recent checkpoint history count.")
    checkpoints = job_sub.add_parser("checkpoints")
    add_common_args(checkpoints)
    checkpoints.add_argument("--include-config", action="store_true", help="Include checkpoint configuration.")
    checkpoints.add_argument("--summary", action="store_true", help="Emit a checkpoint summary.")
    graph = job_sub.add_parser("graph")
    add_common_args(graph)
    graph.add_argument("--top-by", choices=["backpressure", "busy", "records"], default=None, help="Sort graph summary.")
    graph.add_argument("--format", choices=["text", "json"], default="text", help="Output format hint.")

    task_chain = subparsers.add_parser("task-chain")
    add_common_args(task_chain)
    tc_sub = task_chain.add_subparsers(dest="subcommand")
    for name in ("detail", "subtasks", "taskmanagers", "watermarks", "accumulators", "backpressure", "source-stats", "source-lag", "sink-stats", "skew"):
        child = tc_sub.add_parser(name)
        add_common_args(child)
    tc_tm_aggregates = tc_sub.add_parser("taskmanager-aggregates")
    add_common_args(tc_tm_aggregates)
    tc_tm_aggregates.add_argument("--sort-by", default=None, help="Aggregated metric to sort rows by, such as read-records.")
    tc_tm_aggregates.add_argument("--top", type=int, default=None, help="Limit returned TaskManager rows.")
    tc_paimon = tc_sub.add_parser("paimon-stats")
    add_common_args(tc_paimon)
    tc_paimon.add_argument("--role", choices=["auto", "writer", "committer", "source"], default="auto", help="Expected Paimon role.")
    tc_metrics = tc_sub.add_parser("metrics")
    add_common_args(tc_metrics)
    tc_metrics.add_argument("--scope", choices=["vertex", "aggregate", "subtask"], default="aggregate", help="Metric scope.")
    tc_flame = tc_sub.add_parser("flamegraph")
    add_common_args(tc_flame)
    tc_flame.add_argument("--type", choices=["mixed", "full", "on_cpu", "off_cpu"], default="mixed", help="FlameGraph type.")
    tc_flame.add_argument("--format", choices=["summary", "json", "raw"], default="summary", help="FlameGraph output format.")
    tc_flame.add_argument("--top", type=int, default=20, help="Top flamegraph nodes.")

    taskmanager = subparsers.add_parser("taskmanager")
    add_common_args(taskmanager)
    tm_sub = taskmanager.add_subparsers(dest="subcommand")
    for name in ("list", "show", "metrics", "memory", "memory-top", "stdout", "thread-dump"):
        child = tm_sub.add_parser(name)
        add_common_args(child)
    tm_sub.choices["memory-top"].add_argument("--top", type=int, default=5)
    tm_sub.choices["thread-dump"].add_argument("--format", choices=["summary", "json", "raw"], default="summary")
    tm_sub.choices["thread-dump"].add_argument("--top", default=None, help="Comma-separated thread states to include.")
    tm_logs = tm_sub.add_parser("logs")
    add_common_args(tm_logs)
    tm_logs_sub = tm_logs.add_subparsers(dest="logs_action", required=True)
    for name in ("list", "tail", "scan", "grep", "errors", "download", "diagnose"):
        child = tm_logs_sub.add_parser(name)
        add_common_args(child)

    jobmanager = subparsers.add_parser("jobmanager")
    add_common_args(jobmanager)
    jm_sub = jobmanager.add_subparsers(dest="subcommand")
    for name in ("metrics", "config", "environment", "stdout", "thread-dump"):
        child = jm_sub.add_parser(name)
        add_common_args(child)
    jm_sub.choices["thread-dump"].add_argument("--format", choices=["summary", "json", "raw"], default="summary")
    jm_sub.choices["thread-dump"].add_argument("--top", default=None, help="Comma-separated thread states to include.")
    jm_logs = jm_sub.add_parser("logs")
    add_common_args(jm_logs)
    jm_logs_sub = jm_logs.add_subparsers(dest="logs_action", required=True)
    for name in ("list", "tail", "scan", "grep", "errors", "download", "diagnose"):
        child = jm_logs_sub.add_parser(name)
        add_common_args(child)

    logs = subparsers.add_parser("logs")
    add_common_args(logs)
    logs.add_argument("--scope", choices=["jobmanager", "taskmanager"], default="jobmanager")
    logs_sub = logs.add_subparsers(dest="logs_action", required=True)
    for name in ("list", "tail", "scan", "grep", "errors", "download", "diagnose"):
        child = logs_sub.add_parser(name)
        add_common_args(child)
        child.add_argument("--scope", choices=["jobmanager", "taskmanager"], default="jobmanager")
    stdout = subparsers.add_parser("stdout")
    add_common_args(stdout)
    add_stdout_args(stdout)
    stdout_sub = stdout.add_subparsers(dest="stdout_action", required=True)
    for name in ("tail", "grep", "download", "watch"):
        child = stdout_sub.add_parser(name)
        add_common_args(child)
        add_stdout_args(child)
    metric = subparsers.add_parser("metric")
    add_common_args(metric)
    metric.add_argument("--scope", choices=["auto", "job", "task-chain", "taskmanager", "jobmanager", "subtask", "jm-operator"], default="auto")
    metric_sub = metric.add_subparsers(dest="subcommand", required=True)
    metric_search = metric_sub.add_parser("search")
    add_common_args(metric_search)
    metric_search.add_argument("keyword", nargs="?", help="Keyword or regex for metric search.")
    metric_search.add_argument("--scope", choices=["auto", "job", "task-chain", "taskmanager", "jobmanager", "jm-operator"], default="auto")
    metric_search.add_argument("--limit", type=int, default=100)
    metric_search.add_argument("--structured", action="store_true", help="Parse metric ids into subtask/operator/metric_name fields.")
    metric_explain = metric_sub.add_parser("explain")
    add_common_args(metric_explain)
    metric_explain.add_argument("metric")
    metric_aggregate = metric_sub.add_parser("aggregate")
    add_common_args(metric_aggregate)
    add_metric_aggregate_args(metric_aggregate)
    metric_watch = metric_sub.add_parser("watch")
    add_common_args(metric_watch)
    add_metric_aggregate_args(metric_watch)
    add_metric_watch_args(metric_watch)
    metric_analyze = metric_sub.add_parser("analyze")
    add_common_args(metric_analyze)
    add_metric_aggregate_args(metric_analyze)
    add_metric_analyze_args(metric_analyze)
    return parser


async def dispatch(args: argparse.Namespace) -> Any:
    """Dispatch parsed CLI arguments."""
    parsed = parse_web_url(getattr(args, "url", None), base_url=getattr(args, "base_url", None), origin=getattr(args, "origin", None))
    cookies = load_browser_cookies(parsed.origin, no_cookies=getattr(args, "no_cookies", False))
    async with FlinkClient(
        parsed.base_url,
        cookies=cookies,
        timeout=getattr(args, "timeout", 10.0),
        verify=not getattr(args, "insecure", False),
        concurrency=getattr(args, "concurrency", 8),
        retries=getattr(args, "retries", 1),
    ) as client:
        command = args.command
        if command == "overview":
            return await command_overview(client, parsed, args)
        if command == "inventory":
            return await build_inventory(client, parsed)
        if command == "resolve":
            return await command_resolve(client, parsed, args)
        if command == "inspect":
            return await command_inspect(client, parsed, args)
        if command == "diagnose":
            return await command_diagnose(client, parsed, args)
        if command == "jobs":
            return await command_jobs(client, args)
        if command == "job":
            return await command_job(client, parsed, args)
        if command == "task-chain":
            return await command_task_chain(client, parsed, args)
        if command == "backpressure":
            return await command_backpressure(client, parsed, args)
        if command == "metrics":
            return await command_metrics(client, parsed, args)
        if command == "metric":
            return await command_metric(client, parsed, args)
        if command == "taskmanager":
            return await command_taskmanager(client, parsed, args)
        if command == "jobmanager":
            return await command_jobmanager(client, parsed, args)
        if command == "logs":
            return await command_logs(client, parsed, args)
        if command == "stdout":
            return await command_stdout(client, parsed, args)
    raise FlinkDiagError(f"Unsupported command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(dispatch(args))
        if isinstance(result, dict) and result.get("_skip_emit"):
            return 0
        emit(result, as_json=getattr(args, "json", False) or getattr(args, "format", None) == "json")
        return 0
    except FlinkDiagError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
