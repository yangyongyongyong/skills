#!/Users/luca/miniforge3/envs/py311/bin/python3.11
"""Spark WebUI diagnostics CLI.

This CLI talks to Spark's REST API behind running Spark Web UI and Spark
History Server pages. It only performs read operations.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

try:
    import httpx
except ImportError as exc:  # pragma: no cover - dependency failure only
    raise SystemExit(
        "Missing dependency: httpx. Install the latest version with "
        "`/Users/luca/miniforge3/envs/py311/bin/python3.11 -m pip install -U httpx`."
    ) from exc

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

_CDP_SCRIPT_DIR = Path("/Users/luca/.cursor/skills/chrome-cdp-ws-daemon/scripts")
if _CDP_SCRIPT_DIR.exists() and str(_CDP_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_CDP_SCRIPT_DIR))

_SENSITIVE_KEY_RE = re.compile(
    r"(token|password|passwd|secret|authorization|cookie|session|credential|access[_-]?key|api[_-]?key)",
    re.IGNORECASE,
)
_TERMINAL_JOB_STATES = {"SUCCEEDED", "FAILED", "KILLED", "UNKNOWN"}
_RUNNING_JOB_STATES = {"RUNNING"}
_DEFAULT_LOG_PATTERNS = ["ERROR", "WARN", "Exception", "OutOfMemoryError", "GC", "Shuffle", "FetchFailed"]
_ENV_KEY_ALIASES = {
    "scalaversion": "runtime.scalaVersion",
    "javaversion": "runtime.javaVersion",
    "javahome": "runtime.javaHome",
}
_PLAN_OPERATORS = [
    "BatchScan",
    "FileScan",
    "Scan",
    "Filter",
    "Project",
    "Exchange",
    "ShuffleQueryStage",
    "BroadcastQueryStage",
    "BroadcastHashJoin",
    "SortMergeJoin",
    "Join",
    "Aggregate",
    "Window",
    "Sort",
    "Repartition",
    "Write",
    "CreateTable",
]


class SparkDiagError(RuntimeError):
    """Raised for actionable Spark diagnostics errors."""


@dataclass
class ParsedUrl:
    """Parsed representation of a Spark Web UI or History Server URL."""

    input_url: str | None
    base_url: str
    origin: str
    ui_kind: str = "running"
    deployment: str | None = None
    app_id: str | None = None
    job_id: int | None = None
    stage_id: int | None = None
    attempt_id: int | None = None
    executor_id: str | None = None
    sql_id: int | None = None
    tab: str | None = None
    path_parts: list[str] = field(default_factory=list)


class SparkClient:
    """Asynchronous Spark REST client with bounded concurrency."""

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
        """Initialize the Spark REST client."""
        self.base_url = ensure_trailing_slash(base_url)
        self.retries = retries
        self._semaphore = asyncio.Semaphore(max(1, concurrency))
        self._client = httpx.AsyncClient(
            cookies=cookies or {},
            timeout=httpx.Timeout(timeout),
            verify=verify,
            follow_redirects=True,
            headers={"Accept": "application/json, text/plain, */*", "Referer": self.base_url},
        )

    async def __aenter__(self) -> "SparkClient":
        """Return this client for async context manager use."""
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the underlying client on context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        await self._client.aclose()

    def url_for(self, path: str) -> str:
        """Build an absolute URL for a REST path."""
        return urljoin(self.base_url, path.lstrip("/"))

    async def get_response(self, path: str, *, allow_error: bool = False) -> httpx.Response:
        """GET a REST path and return the HTTP response."""
        last_error: Exception | None = None
        attempts = max(1, self.retries + 1)
        async with self._semaphore:
            for attempt in range(attempts):
                try:
                    response = await self._client.get(self.url_for(path))
                    if allow_error or response.status_code < 400:
                        return response
                    if response.status_code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                        raise SparkDiagError(format_http_error(path, response))
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_error = exc
                    if attempt == attempts - 1:
                        raise SparkDiagError(f"GET {path} failed: {exc}") from exc
                await asyncio.sleep(0.2 * (attempt + 1))
        raise SparkDiagError(f"GET {path} failed: {last_error}")

    async def get_json(self, path: str, *, allow_error: bool = False) -> Any:
        """GET a REST path and parse JSON."""
        response = await self.get_response(path, allow_error=allow_error)
        if response.status_code >= 400 and allow_error:
            return {"available": False, "status_code": response.status_code, "path": path, "body": response.text}
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise SparkDiagError(f"GET {path} did not return JSON: {response.text[:200]}") from exc

    async def get_text_status(self, path: str) -> dict[str, Any]:
        """GET a REST or log path and return text with availability metadata."""
        response = await self.get_response(path, allow_error=True)
        return {
            "available": response.status_code < 400,
            "status_code": response.status_code,
            "path": path,
            "content_type": response.headers.get("content-type", ""),
            "text": response.text,
        }


def ensure_trailing_slash(value: str) -> str:
    """Return a URL with a trailing slash."""
    return value if value.endswith("/") else value + "/"


def compact_json(value: Any) -> str:
    """Serialize a value as stable UTF-8 JSON."""
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)


def emit(value: Any, *, as_json: bool = False) -> None:
    """Print CLI output with sensitive values redacted."""
    safe = redact_sensitive(value)
    if as_json:
        print(compact_json(safe))
        return
    print(render_text(safe))


def render_text(value: Any) -> str:
    """Render common values as readable text."""
    if isinstance(value, (dict, list)):
        return compact_json(value)
    return str(value)


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
    """Parse a Spark Web UI or History Server URL."""
    raw_url = url or os.environ.get("SPARK_WEB_URL")
    source = raw_url or base_url or ""
    if not source:
        raise SparkDiagError("Missing --url or --base-url. Example: spark_diag.py overview --url <spark-webui-url>")
    split = urlsplit(source)
    if not split.scheme or not split.netloc:
        raise SparkDiagError(f"Invalid URL: {source}")
    detected_origin = origin or f"{split.scheme}://{split.netloc}"
    path_parts = [part for part in split.path.split("/") if part]
    if base_url:
        rest_base = ensure_trailing_slash(base_url)
        parsed = ParsedUrl(raw_url, rest_base, detected_origin, path_parts=path_parts)
        parse_path_parts(parsed, path_parts)
        return parsed
    if path_parts and path_parts[0] == "history":
        rest_base = urlunsplit((split.scheme, split.netloc, "/", "", ""))
        parsed = ParsedUrl(raw_url, rest_base, detected_origin, ui_kind="history", path_parts=path_parts)
    elif path_parts and path_parts[0].startswith("spark-"):
        deployment = path_parts[0]
        rest_base = urlunsplit((split.scheme, split.netloc, f"/{deployment}/", "", ""))
        parsed = ParsedUrl(raw_url, rest_base, detected_origin, ui_kind="running", deployment=deployment, path_parts=path_parts)
    else:
        rest_base = urlunsplit((split.scheme, split.netloc, "/", "", ""))
        parsed = ParsedUrl(raw_url, rest_base, detected_origin, ui_kind="history", path_parts=path_parts)
    parse_path_parts(parsed, path_parts)
    return parsed


def parse_path_parts(parsed: ParsedUrl, path_parts: list[str]) -> None:
    """Populate route fields from URL path parts."""
    parts = path_parts[:]
    if parts and parts[0] == parsed.deployment:
        parts = parts[1:]
    if parts and parts[0] == "history":
        parsed.ui_kind = "history"
        if len(parts) >= 2:
            parsed.app_id = parts[1]
        if len(parts) >= 3:
            parsed.tab = normalize_tab(parts[2])
    elif parts:
        parsed.tab = normalize_tab(parts[0])
    query = parse_qs(urlsplit(parsed.input_url or "").query)
    if "id" in query and parsed.tab == "executors":
        parsed.executor_id = query["id"][0]
    if "jobId" in query:
        parsed.job_id = safe_int(query["jobId"][0])
    if "stageId" in query:
        parsed.stage_id = safe_int(query["stageId"][0])
    if "attempt" in query:
        parsed.attempt_id = safe_int(query["attempt"][0])
    if "id" in query and parsed.tab == "SQL":
        parsed.sql_id = safe_int(query["id"][0])


def normalize_tab(value: str | None) -> str | None:
    """Normalize Spark UI tab names."""
    if not value:
        return None
    return value.rstrip("/")


def safe_int(value: Any) -> int | None:
    """Convert a value to int if possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def split_csv(value: str | None) -> list[str]:
    """Split a comma-separated option into a list."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def to_float(value: Any) -> float | None:
    """Convert a value to float if possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def numeric(value: Any) -> int | float | None:
    """Return a numeric value, preserving integers when possible."""
    number = to_float(value)
    if number is None:
        return None
    return int(number) if number.is_integer() else number


def first_present(*values: Any) -> Any:
    """Return the first value that is not None."""
    for value in values:
        if value is not None:
            return value
    return None


def bytes_human(value: Any) -> str | None:
    """Format bytes with binary units."""
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


def ratio_human(numerator: Any, denominator: Any) -> str | None:
    """Format a percentage from two numbers."""
    top = to_float(numerator)
    bottom = to_float(denominator)
    if top is None or bottom is None or bottom == 0:
        return None
    return f"{top / bottom * 100:.2f}%"


def load_browser_cookies(origin: str, *, no_cookies: bool = False) -> dict[str, str]:
    """Load browser cookies through chrome-cdp-ws-daemon when available."""
    if no_cookies:
        return {}
    try:
        from cdp_client import get_cookies  # type: ignore
    except Exception as exc:
        raise SparkDiagError(f"Unable to import chrome-cdp-ws-daemon cookie client: {exc}") from exc
    cookies = get_cookies(origin)
    if not isinstance(cookies, dict):
        raise SparkDiagError("chrome-cdp-ws-daemon returned an unexpected cookie payload")
    return {str(key): str(value) for key, value in cookies.items()}


async def resolve_app_id(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> str:
    """Resolve a Spark application id from URL, args, or applications endpoint."""
    explicit = getattr(args, "app_id", None)
    if explicit:
        return str(explicit)
    if parsed.app_id:
        return parsed.app_id
    apps = await client.get_json(applications_path(args))
    if not isinstance(apps, list) or not apps:
        raise SparkDiagError("No Spark applications found; pass --app-id explicitly")
    filtered = filter_applications(apps, args)
    if len(filtered) != 1:
        candidates = [{"index": idx, "id": app.get("id"), "name": app.get("name"), "attempts": app.get("attempts")} for idx, app in enumerate(filtered[:20])]
        raise SparkDiagError(f"Multiple Spark applications matched; pass --app-id or --app-index: {compact_json(candidates)}")
    return str(filtered[0].get("id"))


def applications_path(args: argparse.Namespace) -> str:
    """Build the applications list endpoint with limit if requested."""
    limit = getattr(args, "limit", None)
    if limit:
        return f"api/v1/applications?{urlencode({'limit': limit})}"
    return "api/v1/applications"


def filter_applications(apps: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    """Filter Spark applications by CLI selectors."""
    result = apps[:]
    name = getattr(args, "app_name", None)
    if name:
        result = [app for app in result if name in str(app.get("name", ""))]
    completed = getattr(args, "completed", None)
    if completed is not None:
        wanted = str(completed).lower() in {"1", "true", "yes", "completed"}
        result = [app for app in result if any(bool(attempt.get("completed")) is wanted for attempt in app.get("attempts", []) or [])]
    index = getattr(args, "app_index", None)
    if index is not None:
        if index < 0 or index >= len(result):
            raise SparkDiagError(f"--app-index {index} out of range for {len(result)} applications")
        return [result[index]]
    return result


def detect_spark_version_from_app(app: dict[str, Any]) -> str | None:
    """Detect Spark version from an app detail object."""
    attempts = app.get("attempts", []) if isinstance(app, dict) else []
    for attempt in attempts:
        if isinstance(attempt, dict) and attempt.get("appSparkVersion"):
            return str(attempt["appSparkVersion"])
    return None


def select_endpoint_profile(version: str | None, override: str = "auto") -> str:
    """Select an endpoint profile from Spark version."""
    if override and override != "auto":
        return override
    if version:
        match = re.match(r"^(\d+)\.(\d+)", version)
        if match:
            return f"spark-{match.group(1)}.{match.group(2)}"
    return "generic"


def summarize_app(app: dict[str, Any]) -> dict[str, Any]:
    """Summarize a Spark application."""
    attempts = app.get("attempts", []) if isinstance(app, dict) else []
    latest = attempts[-1] if attempts else {}
    return {
        "id": app.get("id"),
        "name": app.get("name"),
        "spark_version": detect_spark_version_from_app(app),
        "completed": latest.get("completed"),
        "duration": millis_human(latest.get("duration")),
        "startTime": latest.get("startTime"),
        "endTime": latest.get("endTime"),
        "sparkUser": latest.get("sparkUser"),
        "attempt_count": len(attempts),
    }


def summarize_jobs(jobs: list[dict[str, Any]], *, brief: bool = False) -> dict[str, Any]:
    """Summarize Spark jobs."""
    states = Counter(str(job.get("status", "UNKNOWN")).upper() for job in jobs)
    return {
        "total_jobs": len(jobs),
        "running_jobs": states.get("RUNNING", 0),
        "succeeded_jobs": states.get("SUCCEEDED", 0),
        "failed_jobs": states.get("FAILED", 0),
        "killed_jobs": states.get("KILLED", 0),
        "total_tasks": sum(int(job.get("numTasks", 0) or 0) for job in jobs),
        "failed_tasks": sum(int(job.get("numFailedTasks", 0) or 0) for job in jobs),
        "jobs": [summarize_job_brief(job) if brief else summarize_job(job) for job in jobs],
    }


def summarize_jobs_timeline(jobs: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a compact job timeline with durations and idle gaps."""
    rows = [summarize_job_brief(job) for job in jobs]
    rows = sorted(rows, key=lambda item: parse_spark_time(item.get("submissionTime")) or datetime.max.replace(tzinfo=timezone.utc))
    gaps: list[dict[str, Any]] = []
    previous_end: datetime | None = None
    previous_job: Any = None
    first_start: datetime | None = None
    last_end: datetime | None = None
    for row in rows:
        start = parse_spark_time(row.get("submissionTime"))
        end = parse_spark_time(row.get("completionTime"))
        if start and first_start is None:
            first_start = start
        if end:
            last_end = end if last_end is None or end > last_end else last_end
        if previous_end and start and start > previous_end:
            gap_ms = int((start - previous_end).total_seconds() * 1000)
            gaps.append({"after_job_id": previous_job, "before_job_id": row.get("jobId"), "gap_ms": gap_ms, "gap": millis_human(gap_ms)})
        if end and (previous_end is None or end > previous_end):
            previous_end = end
            previous_job = row.get("jobId")
    makespan_ms = int((last_end - first_start).total_seconds() * 1000) if first_start and last_end else None
    return {
        "job_count": len(rows),
        "makespan_ms": makespan_ms,
        "makespan": millis_human(makespan_ms),
        "idle_gaps": gaps,
        "jobs": rows,
    }


def summarize_job_idle_gaps(jobs: list[dict[str, Any]], *, top: int = 20) -> dict[str, Any]:
    """Summarize job active time and idle gaps between completed jobs."""
    timeline = summarize_jobs_timeline(jobs)
    rows = timeline.get("jobs", [])
    gaps = sorted(timeline.get("idle_gaps", []), key=lambda item: int(item.get("gap_ms") or 0), reverse=True)
    total_job_wall_time_ms = sum(int(row.get("duration_ms") or 0) for row in rows)
    total_idle_time_ms = sum(int(gap.get("gap_ms") or 0) for gap in gaps)
    makespan_ms = safe_int(timeline.get("makespan_ms"))
    idle_ratio_value = total_idle_time_ms / makespan_ms if makespan_ms else None
    return {
        "job_count": len(rows),
        "total_job_wall_time_ms": total_job_wall_time_ms,
        "total_job_wall_time": millis_human(total_job_wall_time_ms),
        "total_idle_time_ms": total_idle_time_ms,
        "total_idle_time": millis_human(total_idle_time_ms),
        "idle_ratio": f"{idle_ratio_value * 100:.2f}%" if idle_ratio_value is not None else None,
        "idle_ratio_value": round(idle_ratio_value, 6) if idle_ratio_value is not None else None,
        "makespan_ms": makespan_ms,
        "makespan": timeline.get("makespan"),
        "first_job": rows[0].get("jobId") if rows else None,
        "last_job": rows[-1].get("jobId") if rows else None,
        "regular_gap_seconds": detect_regular_gap_seconds(gaps),
        "top_idle_gaps": gaps[:top],
    }


def detect_regular_gap_seconds(gaps: list[dict[str, Any]]) -> int | None:
    """Detect a common rounded idle gap length in seconds."""
    seconds = [int(round(int(gap.get("gap_ms") or 0) / 1000)) for gap in gaps if int(gap.get("gap_ms") or 0) > 0]
    if not seconds:
        return None
    value, count = Counter(seconds).most_common(1)[0]
    return value if count >= 2 else None


def classify_long_app_runtime(
    app: dict[str, Any] | None,
    jobs: list[dict[str, Any]],
    *,
    sql_rows: list[dict[str, Any]] | None = None,
    top: int = 20,
) -> dict[str, Any]:
    """Classify whether a long Spark app duration is compute or idle dominated."""
    idle_summary = summarize_job_idle_gaps(jobs, top=top)
    app_duration_ms = app_duration_from_detail(app or {})
    total_job_wall_time_ms = int(idle_summary.get("total_job_wall_time_ms") or 0)
    denominator_ms = app_duration_ms or safe_int(idle_summary.get("makespan_ms")) or 0
    total_idle_time_ms = max(0, denominator_ms - total_job_wall_time_ms) if denominator_ms else int(idle_summary.get("total_idle_time_ms") or 0)
    idle_ratio_value = total_idle_time_ms / denominator_ms if denominator_ms else None
    failed_sql_count = count_sql_status(sql_rows or [], "FAILED")
    classification = classify_idle_ratio(idle_ratio_value)
    app_kind = classify_app_kind((app or {}).get("name"))
    primary_bottleneck = classify_long_app_bottleneck(classification, failed_sql_count)
    return {
        "app_id": (app or {}).get("id"),
        "app_name": (app or {}).get("name"),
        "app_kind": app_kind,
        "classification": classification,
        "primary_bottleneck": primary_bottleneck,
        "app_duration_ms": app_duration_ms,
        "app_duration": millis_human(app_duration_ms),
        "job_active_time_ms": total_job_wall_time_ms,
        "job_active_time": millis_human(total_job_wall_time_ms),
        "idle_time_ms": total_idle_time_ms if denominator_ms else idle_summary.get("total_idle_time_ms"),
        "idle_time": millis_human(total_idle_time_ms if denominator_ms else idle_summary.get("total_idle_time_ms")),
        "idle_ratio": f"{idle_ratio_value * 100:.2f}%" if idle_ratio_value is not None else idle_summary.get("idle_ratio"),
        "idle_ratio_value": round(idle_ratio_value, 6) if idle_ratio_value is not None else idle_summary.get("idle_ratio_value"),
        "sql_failure_count": failed_sql_count,
        "jobs": idle_summary,
    }


def app_duration_from_detail(app: dict[str, Any]) -> int | None:
    """Read the latest application attempt duration in milliseconds."""
    attempts = app.get("attempts", []) if isinstance(app, dict) else []
    latest = attempts[-1] if attempts else {}
    duration = safe_int(latest.get("duration"))
    if duration is not None:
        return duration
    start = parse_spark_time(latest.get("startTime"))
    end = parse_spark_time(latest.get("endTime"))
    if start and end:
        return max(0, int((end - start).total_seconds() * 1000))
    return None


def classify_idle_ratio(idle_ratio_value: float | None) -> str:
    """Classify idle dominance from an idle ratio."""
    if idle_ratio_value is None:
        return "unknown"
    if idle_ratio_value >= 0.8:
        return "session_idle_dominant"
    if idle_ratio_value <= 0.2:
        return "compute_dominant"
    return "mixed"


def classify_app_kind(name: Any) -> str:
    """Classify app names that look like interactive SQL engine sessions."""
    lowered = str(name or "").lower()
    if any(marker in lowered for marker in ("kyuubi", "thrift", "engine")):
        return "interactive_engine_session"
    return "batch_or_unknown"


def classify_long_app_bottleneck(classification: str, failed_sql_count: int) -> str:
    """Choose the primary bottleneck label for a long-app summary."""
    if failed_sql_count:
        return "metadata_ddl_failures"
    if classification == "session_idle_dominant":
        return "session_idle"
    if classification == "compute_dominant":
        return "compute"
    return "mixed_or_unknown"


def summarize_duration_distribution(rows: list[dict[str, Any]], value_key: str) -> dict[str, Any]:
    """Summarize min, max, average, and percentiles for duration-like values."""
    values = [to_float(get_first_nested(row, [value_key])) for row in rows]
    numbers = sorted(value for value in values if value is not None)
    if not numbers:
        return empty_duration_distribution()
    avg = sum(numbers) / len(numbers)
    median = percentile(numbers, 50)
    max_value = numbers[-1]
    result = {
        "count": len(numbers),
        "min_ms": numeric(numbers[0]),
        "max_ms": numeric(max_value),
        "avg_ms": round(avg, 4),
        "p50_ms": numeric(percentile(numbers, 50)),
        "p75_ms": numeric(percentile(numbers, 75)),
        "p90_ms": numeric(percentile(numbers, 90)),
        "p95_ms": numeric(percentile(numbers, 95)),
        "p99_ms": numeric(percentile(numbers, 99)),
        "min": millis_human(numbers[0]),
        "max": millis_human(max_value),
        "avg": millis_human(avg),
        "p50": millis_human(median),
        "p75": millis_human(percentile(numbers, 75)),
        "p90": millis_human(percentile(numbers, 90)),
        "p95": millis_human(percentile(numbers, 95)),
        "p99": millis_human(percentile(numbers, 99)),
        "max_vs_median": round(max_value / median, 4) if median else None,
        "max_vs_avg": round(max_value / avg, 4) if avg else None,
    }
    return result


def empty_duration_distribution() -> dict[str, Any]:
    """Return an empty duration distribution payload."""
    return {
        "count": 0,
        "min_ms": None,
        "max_ms": None,
        "avg_ms": None,
        "p50_ms": None,
        "p75_ms": None,
        "p90_ms": None,
        "p95_ms": None,
        "p99_ms": None,
        "max_vs_median": None,
        "max_vs_avg": None,
    }


def percentile(sorted_values: list[float], pct: float) -> float:
    """Calculate a linear-interpolated percentile from sorted values."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct / 100
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] * (1 - weight) + sorted_values[high] * weight


def classify_duration_skew(distribution: dict[str, Any]) -> dict[str, Any]:
    """Classify duration long-tail severity from distribution ratios."""
    ratio = to_float(distribution.get("max_vs_median"))
    if ratio is None:
        return {"level": "unknown", "message": "No duration data available"}
    if ratio >= 5:
        return {"level": "critical", "message": "Max duration is at least 5x median; strong long-tail or skew signal"}
    if ratio >= 3:
        return {"level": "warning", "message": "Max duration is at least 3x median; possible long-tail or skew"}
    return {"level": "none", "message": "No obvious duration skew by max-vs-median"}


def summarize_job_durations(jobs: list[dict[str, Any]], *, top: int = 10) -> dict[str, Any]:
    """Summarize job duration distribution and top slow jobs."""
    rows = [summarize_job_brief(job) for job in jobs]
    distribution = summarize_duration_distribution(rows, "duration_ms")
    return {
        "kind": "jobs",
        "distribution": distribution,
        "skew": classify_duration_skew(distribution),
        "top_slow_jobs": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0), reverse=True)[:top],
        "fastest_jobs": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0))[:top],
    }


def summarize_stage_durations(stages: list[dict[str, Any]], *, top: int = 10) -> dict[str, Any]:
    """Summarize stage duration distribution and top slow stages."""
    rows = [summarize_stage(stage) for stage in stages]
    distribution = summarize_duration_distribution(rows, "executorRunTime")
    return {
        "kind": "stages",
        "distribution": distribution,
        "skew": classify_duration_skew(distribution),
        "top_slow_stages": sorted(rows, key=lambda item: int(item.get("executorRunTime") or 0), reverse=True)[:top],
        "fastest_stages": sorted(rows, key=lambda item: int(item.get("executorRunTime") or 0))[:top],
    }


def build_event_timeline(
    sql_rows: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    *,
    sql_id: int | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Build a unified Spark event timeline from REST payloads."""
    selected_sql = [row for row in sql_rows if sql_id is None or safe_int(row.get("id")) == sql_id]
    sql_job_ids = collect_sql_job_ids(selected_sql)
    sql_bounds = [row_time_bounds(row, ("submissionTime", "startTime"), ("completionTime", "endTime"), "duration")[:2] for row in selected_sql]
    if sql_id is not None and sql_job_ids:
        related_jobs = [job for job in jobs if safe_int(job.get("jobId")) in sql_job_ids]
    elif sql_id is not None and selected_sql:
        related_jobs = [job for job in jobs if row_overlaps_bounds(job, ("submissionTime",), ("completionTime",), None, sql_bounds)]
    else:
        related_jobs = jobs
    related_stage_ids = {safe_int(stage_id) for job in related_jobs for stage_id in job.get("stageIds", [])}
    related_stage_ids.discard(None)
    if sql_id is not None and related_stage_ids:
        related_stages = [stage for stage in stages if safe_int(stage.get("stageId")) in related_stage_ids]
    elif sql_id is not None and selected_sql:
        related_stages = [stage for stage in stages if row_overlaps_bounds(stage, ("submissionTime", "firstTaskLaunchedTime"), ("completionTime",), None, sql_bounds)]
    else:
        related_stages = stages
    events: list[dict[str, Any]] = []
    sql_summaries = []
    for row in selected_sql:
        summary, event = summarize_sql_timeline_event(row)
        sql_summaries.append(summary)
        events.append(event)
    events.extend(summarize_job_timeline_event(job) for job in related_jobs)
    events.extend(summarize_stage_timeline_event(stage) for stage in related_stages)
    executor_events = summarize_executor_timeline_events(executors)
    events.extend(executor_events)
    events = sorted(events, key=event_sort_key)
    span = summarize_timeline_span(events)
    type_counts = Counter(str(event.get("type")) for event in events)
    return {
        "event_count": len(events),
        "type_counts": dict(type_counts),
        "span": span,
        "sql_filter": sql_id,
        "sql_executions": sql_summaries,
        "executor_events": {
            "added": sum(1 for event in executor_events if event.get("event") == "executor_added"),
            "removed": sum(1 for event in executor_events if event.get("event") == "executor_removed"),
        },
        "events": events[:limit],
    }


def collect_sql_job_ids(sql_rows: list[dict[str, Any]]) -> set[int]:
    """Collect job ids referenced by SQL execution rows."""
    job_ids: set[int] = set()
    for row in sql_rows:
        for key in ("jobIds", "jobs", "runningJobIds", "succeededJobIds", "failedJobIds"):
            collect_job_ids_from_value(row.get(key), job_ids)
    return job_ids


def collect_job_ids_from_value(value: Any, result: set[int]) -> None:
    """Collect job ids from nested Spark SQL fields."""
    if value is None:
        return
    if isinstance(value, dict):
        for key in ("jobId", "id"):
            parsed = safe_int(value.get(key))
            if parsed is not None:
                result.add(parsed)
        for item in value.values():
            collect_job_ids_from_value(item, result)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            collect_job_ids_from_value(item, result)
        return
    parsed = safe_int(value)
    if parsed is not None:
        result.add(parsed)


def summarize_sql_timeline_event(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Summarize a SQL execution and return its timeline event."""
    start, end, duration_ms = row_time_bounds(row, ("submissionTime", "startTime"), ("completionTime", "endTime"), "duration")
    job_ids = sorted(collect_sql_job_ids([row]))
    summary = {
        "sql_id": row.get("id"),
        "status": row.get("status"),
        "start": format_spark_time(start),
        "end": format_spark_time(end),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "job_ids": job_ids,
        "description": trim_text(row.get("description"), 300),
    }
    event = {
        "type": "sql",
        "event": "sql_execution",
        "at": summary["start"],
        "end": summary["end"],
        "duration_ms": duration_ms,
        "duration": summary["duration"],
        "sql_id": row.get("id"),
        "status": row.get("status"),
        "job_ids": job_ids,
        "description": summary["description"],
    }
    return summary, event


def summarize_job_timeline_event(job: dict[str, Any]) -> dict[str, Any]:
    """Summarize a Spark job timeline event."""
    start, end, duration_ms = row_time_bounds(job, ("submissionTime",), ("completionTime",), None)
    return {
        "type": "job",
        "event": "job_execution",
        "at": format_spark_time(start),
        "end": format_spark_time(end),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "job_id": job.get("jobId"),
        "status": job.get("status"),
        "stage_ids": job.get("stageIds", []),
        "numTasks": job.get("numTasks"),
        "name": job.get("name"),
    }


def summarize_stage_timeline_event(stage: dict[str, Any]) -> dict[str, Any]:
    """Summarize a Spark stage timeline event."""
    start, end, duration_ms = row_time_bounds(stage, ("submissionTime", "firstTaskLaunchedTime"), ("completionTime",), None)
    return {
        "type": "stage",
        "event": "stage_execution",
        "at": format_spark_time(start),
        "end": format_spark_time(end),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "stage_id": stage.get("stageId"),
        "attempt_id": stage.get("attemptId"),
        "status": stage.get("status"),
        "numTasks": stage.get("numTasks"),
        "name": stage.get("name"),
    }


def summarize_executor_timeline_events(executors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build executor add/remove timeline events."""
    events: list[dict[str, Any]] = []
    for executor in executors:
        add_time = parse_first_time(executor, ("addTime", "startTime"))
        if add_time:
            events.append(
                {
                    "type": "executor",
                    "event": "executor_added",
                    "at": format_spark_time(add_time),
                    "executor_id": executor.get("id"),
                    "hostPort": executor.get("hostPort"),
                    "isActive": executor.get("isActive"),
                    "totalCores": executor.get("totalCores"),
                }
            )
        remove_time = parse_first_time(executor, ("removeTime", "endTime"))
        if remove_time:
            events.append(
                {
                    "type": "executor",
                    "event": "executor_removed",
                    "at": format_spark_time(remove_time),
                    "executor_id": executor.get("id"),
                    "hostPort": executor.get("hostPort"),
                    "removeReason": executor.get("removeReason"),
                    "totalTasks": executor.get("totalTasks"),
                    "failedTasks": executor.get("failedTasks"),
                }
            )
    return events


def row_time_bounds(
    row: dict[str, Any],
    start_keys: tuple[str, ...],
    end_keys: tuple[str, ...],
    duration_key: str | None,
) -> tuple[datetime | None, datetime | None, int | None]:
    """Read start, end, and duration from a Spark REST row."""
    start = parse_first_time(row, start_keys)
    end = parse_first_time(row, end_keys)
    duration_ms = safe_int(row.get(duration_key)) if duration_key else None
    if duration_ms is None and start and end:
        duration_ms = max(0, int((end - start).total_seconds() * 1000))
    if end is None and start and duration_ms is not None:
        end = start + timedelta(milliseconds=duration_ms)
    return start, end, duration_ms


def row_overlaps_bounds(
    row: dict[str, Any],
    start_keys: tuple[str, ...],
    end_keys: tuple[str, ...],
    duration_key: str | None,
    bounds: list[tuple[datetime | None, datetime | None]],
) -> bool:
    """Return true when a row's time range overlaps one of the given bounds."""
    start, end, _ = row_time_bounds(row, start_keys, end_keys, duration_key)
    if start is None and end is None:
        return False
    row_start = start or end
    row_end = end or start
    if row_start is None or row_end is None:
        return False
    for bound_start, bound_end in bounds:
        if bound_start is None and bound_end is None:
            continue
        left = bound_start or bound_end
        right = bound_end or bound_start
        if left is not None and right is not None and row_start <= right and row_end >= left:
            return True
    return False


def parse_first_time(row: dict[str, Any], keys: tuple[str, ...]) -> datetime | None:
    """Parse the first available Spark timestamp from a row."""
    for key in keys:
        value = parse_spark_time(row.get(key))
        if value is not None:
            return value
    return None


def format_spark_time(value: datetime | None) -> str | None:
    """Format a UTC timestamp with Spark's GMT suffix."""
    if value is None:
        return None
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "GMT"


def event_sort_key(event: dict[str, Any]) -> tuple[datetime, str, str]:
    """Return a stable sort key for timeline events."""
    timestamp = parse_spark_time(event.get("at")) or datetime.max.replace(tzinfo=timezone.utc)
    return (timestamp, str(event.get("type", "")), str(event.get("event", "")))


def summarize_timeline_span(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize the start, end, and duration of a timeline."""
    timestamps: list[datetime] = []
    for event in events:
        for key in ("at", "end"):
            parsed = parse_spark_time(event.get(key))
            if parsed:
                timestamps.append(parsed)
    if not timestamps:
        return {"start": None, "end": None, "duration_ms": None, "duration": None}
    start = min(timestamps)
    end = max(timestamps)
    duration_ms = max(0, int((end - start).total_seconds() * 1000))
    return {"start": format_spark_time(start), "end": format_spark_time(end), "duration_ms": duration_ms, "duration": millis_human(duration_ms)}


def summarize_job(job: dict[str, Any]) -> dict[str, Any]:
    """Summarize one Spark job."""
    row = summarize_job_brief(job)
    row.update(
        {
            "numActiveTasks": job.get("numActiveTasks"),
            "numCompletedTasks": job.get("numCompletedTasks"),
            "numSkippedTasks": job.get("numSkippedTasks"),
            "description": trim_text(job.get("description"), 500),
        }
    )
    return row


def summarize_job_brief(job: dict[str, Any]) -> dict[str, Any]:
    """Summarize one Spark job without large SQL descriptions."""
    duration_ms = job_duration_ms(job)
    return {
        "jobId": job.get("jobId"),
        "name": job.get("name"),
        "status": job.get("status"),
        "submissionTime": job.get("submissionTime"),
        "completionTime": job.get("completionTime"),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "stageIds": job.get("stageIds", []),
        "numTasks": job.get("numTasks"),
        "numFailedTasks": job.get("numFailedTasks"),
    }


def job_duration_ms(job: dict[str, Any]) -> int | None:
    """Calculate Spark job duration in milliseconds from timestamp fields."""
    start = parse_spark_time(job.get("submissionTime"))
    end = parse_spark_time(job.get("completionTime"))
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def parse_spark_time(value: Any) -> datetime | None:
    """Parse Spark REST timestamps such as 2026-06-01T10:00:00.000GMT."""
    if not value:
        return None
    text = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fGMT", "%Y-%m-%dT%H:%M:%SGMT"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def filter_jobs(jobs: list[dict[str, Any]], status: str | None) -> list[dict[str, Any]]:
    """Filter jobs by Spark status selector."""
    if not status or status == "all":
        return jobs
    wanted = status.upper()
    if wanted == "COMPLETED":
        wanted = "SUCCEEDED"
    if wanted == "RUNNING":
        return [job for job in jobs if str(job.get("status", "")).upper() in _RUNNING_JOB_STATES]
    if wanted == "TERMINAL":
        return [job for job in jobs if str(job.get("status", "")).upper() in _TERMINAL_JOB_STATES]
    return [job for job in jobs if str(job.get("status", "")).upper() == wanted]


def summarize_stages(stages: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize Spark stages."""
    states = Counter(str(stage.get("status", "UNKNOWN")).upper() for stage in stages)
    rows = [summarize_stage(stage) for stage in stages]
    total_shuffle_read = sum(int(row.get("shuffleReadBytes", 0) or 0) for row in rows)
    total_shuffle_write = sum(int(row.get("shuffleWriteBytes", 0) or 0) for row in rows)
    return {
        "total_stages": len(stages),
        "active_stages": states.get("ACTIVE", 0),
        "complete_stages": states.get("COMPLETE", 0),
        "failed_stages": states.get("FAILED", 0),
        "failed_tasks": sum(int(stage.get("numFailedTasks", 0) or 0) for stage in stages),
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "total_shuffle_bytes": total_shuffle_read + total_shuffle_write,
        "total_shuffle_read": bytes_human(total_shuffle_read),
        "total_shuffle_write": bytes_human(total_shuffle_write),
        "total_shuffle": bytes_human(total_shuffle_read + total_shuffle_write),
        "top_duration": sorted(rows, key=lambda item: float(item.get("executorRunTime") or 0), reverse=True)[:10],
        "top_shuffle": sorted(rows, key=lambda item: float(item.get("shuffleTotalBytes") or 0), reverse=True)[:10],
        "stages": rows,
    }


def summarize_stage_shuffle(stages: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a compact shuffle report from Spark stage list rows."""
    summary = summarize_stages(stages)
    rows = [compact_stage_shuffle_row(row) for row in sorted(summary["stages"], key=lambda item: (int(item.get("stageId") or -1), int(item.get("attemptId") or -1)))]
    return {
        "total_stages": summary["total_stages"],
        "complete_stages": summary["complete_stages"],
        "failed_stages": summary["failed_stages"],
        "total_shuffle_read_bytes": summary["total_shuffle_read_bytes"],
        "total_shuffle_write_bytes": summary["total_shuffle_write_bytes"],
        "total_shuffle_bytes": summary["total_shuffle_bytes"],
        "total_shuffle_read": summary["total_shuffle_read"],
        "total_shuffle_write": summary["total_shuffle_write"],
        "total_shuffle": summary["total_shuffle"],
        "top_shuffle": [compact_stage_shuffle_row(row) for row in summary["top_shuffle"]],
        "stages": rows,
    }


def summarize_stages_io(stages: list[dict[str, Any]]) -> dict[str, Any]:
    """Return compact stage input, output, and shuffle movement."""
    rows = [compact_stage_io_row(summarize_stage(stage)) for stage in stages]
    total_input = sum(int(row.get("inputBytesValue", 0) or 0) for row in rows)
    total_output = sum(int(row.get("outputBytesValue", 0) or 0) for row in rows)
    total_shuffle_read = sum(int(row.get("shuffleReadBytes", 0) or 0) for row in rows)
    total_shuffle_write = sum(int(row.get("shuffleWriteBytes", 0) or 0) for row in rows)
    return {
        "total_stages": len(rows),
        "total_input_bytes": total_input,
        "total_output_bytes": total_output,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "total_input": bytes_human(total_input),
        "total_output": bytes_human(total_output),
        "total_shuffle_read": bytes_human(total_shuffle_read),
        "total_shuffle_write": bytes_human(total_shuffle_write),
        "top_input": sorted(rows, key=lambda item: int(item.get("inputBytesValue", 0) or 0), reverse=True)[:10],
        "top_output": sorted(rows, key=lambda item: int(item.get("outputBytesValue", 0) or 0), reverse=True)[:10],
        "top_shuffle": sorted(rows, key=lambda item: int(item.get("shuffleTotalBytes", 0) or 0), reverse=True)[:10],
        "stages": sorted(rows, key=lambda item: (int(item.get("stageId") or -1), int(item.get("attemptId") or -1))),
    }


def compact_stage_io_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return compact stage IO fields."""
    return {
        "stageId": row.get("stageId"),
        "attemptId": row.get("attemptId"),
        "status": row.get("status"),
        "numTasks": row.get("numTasks"),
        "duration": row.get("duration"),
        "inputBytesValue": row.get("inputBytesValue"),
        "outputBytesValue": row.get("outputBytesValue"),
        "shuffleReadBytes": row.get("shuffleReadBytes"),
        "shuffleWriteBytes": row.get("shuffleWriteBytes"),
        "shuffleTotalBytes": row.get("shuffleTotalBytes"),
        "inputBytes": row.get("inputBytes"),
        "outputBytes": row.get("outputBytes"),
        "shuffleRead": row.get("shuffleRead"),
        "shuffleWrite": row.get("shuffleWrite"),
        "shuffleTotal": row.get("shuffleTotal"),
        "name": row.get("name"),
    }


def compact_stage_shuffle_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return the compact fields needed for shuffle reporting."""
    return {
        "stageId": row.get("stageId"),
        "attemptId": row.get("attemptId"),
        "status": row.get("status"),
        "numTasks": row.get("numTasks"),
        "duration": row.get("duration"),
        "shuffleReadBytes": row.get("shuffleReadBytes"),
        "shuffleWriteBytes": row.get("shuffleWriteBytes"),
        "shuffleTotalBytes": row.get("shuffleTotalBytes"),
        "shuffleRead": row.get("shuffleRead"),
        "shuffleWrite": row.get("shuffleWrite"),
        "shuffleTotal": row.get("shuffleTotal"),
        "name": row.get("name"),
    }


def summarize_stage(stage: dict[str, Any]) -> dict[str, Any]:
    """Summarize one Spark stage."""
    input_bytes = first_present(stage.get("inputBytes"), get_nested(stage, "inputMetrics.bytesRead"))
    output_bytes = first_present(stage.get("outputBytes"), get_nested(stage, "outputMetrics.bytesWritten"))
    shuffle_read = first_present(stage.get("shuffleReadBytes"), get_nested(stage, "shuffleReadMetrics.remoteBytesRead"))
    shuffle_write = first_present(stage.get("shuffleWriteBytes"), get_nested(stage, "shuffleWriteMetrics.bytesWritten"))
    shuffle_total = (to_float(shuffle_read) or 0.0) + (to_float(shuffle_write) or 0.0)
    return {
        "stageId": stage.get("stageId"),
        "attemptId": stage.get("attemptId"),
        "name": stage.get("name"),
        "status": stage.get("status"),
        "numTasks": stage.get("numTasks"),
        "numActiveTasks": stage.get("numActiveTasks"),
        "numCompleteTasks": stage.get("numCompleteTasks"),
        "numFailedTasks": stage.get("numFailedTasks"),
        "executorRunTime": stage.get("executorRunTime"),
        "duration": millis_human(stage.get("executorRunTime")),
        "jvmGcTime": millis_human(stage.get("jvmGcTime")),
        "inputBytesValue": numeric(input_bytes),
        "inputBytes": bytes_human(input_bytes),
        "outputBytesValue": numeric(output_bytes),
        "outputBytes": bytes_human(output_bytes),
        "shuffleReadBytes": int(to_float(shuffle_read) or 0),
        "shuffleWriteBytes": int(to_float(shuffle_write) or 0),
        "shuffleTotalBytes": int(shuffle_total),
        "shuffleRead": bytes_human(shuffle_read),
        "shuffleWrite": bytes_human(shuffle_write),
        "shuffleTotal": bytes_human(shuffle_total),
        "memoryBytesSpilled": bytes_human(stage.get("memoryBytesSpilled")),
        "diskBytesSpilled": bytes_human(stage.get("diskBytesSpilled")),
    }


def summarize_task_skew(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize skew across Spark tasks."""
    specs = {
        "duration": ["duration"],
        "input_bytes": ["inputMetrics.bytesRead", "taskMetrics.inputMetrics.bytesRead"],
        "shuffle_read": ["shuffleReadMetrics.remoteBytesRead", "taskMetrics.shuffleReadMetrics.remoteBytesRead"],
        "shuffle_write": ["shuffleWriteMetrics.bytesWritten", "taskMetrics.shuffleWriteMetrics.bytesWritten"],
        "gc_time": ["taskMetrics.jvmGcTime", "jvmGcTime"],
    }
    result: dict[str, Any] = {}
    for name, paths in specs.items():
        values: list[tuple[Any, float]] = []
        for task in tasks:
            value = to_float(get_first_nested(task, paths))
            if value is not None:
                values.append((task.get("taskId") or task.get("index"), value))
        if not values:
            continue
        only = [value for _, value in values]
        positive = [value for value in only if value > 0]
        max_task_id, max_value = max(values, key=lambda item: item[1])
        result[name] = {
            "min": numeric(min(only)),
            "max": numeric(max_value),
            "avg": round(sum(only) / len(only), 4),
            "max_task_id": max_task_id,
            "skew_ratio": round(max_value / min(positive), 4) if positive else None,
            "top_tasks": [{"taskId": task_id, "value": numeric(value)} for task_id, value in sorted(values, key=lambda item: item[1], reverse=True)[:10]],
        }
    return result


def summarize_stage_tasks(tasks: list[dict[str, Any]], *, brief: bool = False, limit: int = 10) -> dict[str, Any]:
    """Summarize task list payloads with optional compact output."""
    skew = summarize_task_skew(tasks)
    top_slow = skew.get("duration", {}).get("top_tasks", [])[:limit]
    result: dict[str, Any] = {"task_count": len(tasks), "skew": skew, "top_slow_tasks": top_slow}
    if not brief:
        result["tasks"] = tasks
    return result


def summarize_task_duration_distribution(tasks: list[dict[str, Any]], *, top: int = 10) -> dict[str, Any]:
    """Summarize task duration distribution with IO, GC, and spill evidence."""
    rows = [compact_task_duration_row(task) for task in tasks]
    distribution = summarize_duration_distribution(rows, "duration_ms")
    metric_skew = summarize_task_skew(tasks)
    duration_skew = classify_task_duration_skew(distribution, metric_skew)
    recommendations = recommend_task_duration_actions(duration_skew, metric_skew, rows)
    return {
        "kind": "tasks",
        "task_count": len(rows),
        "distribution": distribution,
        "skew": duration_skew,
        "metric_skew": metric_skew,
        "top_slow_tasks": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0), reverse=True)[:top],
        "fastest_tasks": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0))[:top],
        "recommendations": recommendations,
    }


def classify_task_duration_skew(distribution: dict[str, Any], metric_skew: dict[str, Any]) -> dict[str, Any]:
    """Classify task duration skew using percentile and max/min task evidence."""
    base = classify_duration_skew(distribution)
    task_ratio = to_float(metric_skew.get("duration", {}).get("skew_ratio"))
    if task_ratio is not None and task_ratio >= 5:
        return {"level": "critical", "message": "Slowest task is at least 5x the fastest positive-duration task"}
    if task_ratio is not None and task_ratio >= 3 and base.get("level") == "none":
        return {"level": "warning", "message": "Slowest task is at least 3x the fastest positive-duration task"}
    return base


def compact_task_duration_row(task: dict[str, Any]) -> dict[str, Any]:
    """Return compact task fields for duration and skew diagnostics."""
    duration_ms = numeric(get_first_nested(task, ["duration"]))
    input_bytes = numeric(get_first_nested(task, ["inputMetrics.bytesRead", "taskMetrics.inputMetrics.bytesRead"]))
    shuffle_read = numeric(get_first_nested(task, ["shuffleReadMetrics.remoteBytesRead", "taskMetrics.shuffleReadMetrics.remoteBytesRead"]))
    shuffle_write = numeric(get_first_nested(task, ["shuffleWriteMetrics.bytesWritten", "taskMetrics.shuffleWriteMetrics.bytesWritten"]))
    gc_ms = numeric(get_first_nested(task, ["taskMetrics.jvmGcTime", "jvmGcTime"]))
    memory_spill = numeric(get_first_nested(task, ["taskMetrics.memoryBytesSpilled", "memoryBytesSpilled"]))
    disk_spill = numeric(get_first_nested(task, ["taskMetrics.diskBytesSpilled", "diskBytesSpilled"]))
    return {
        "taskId": task.get("taskId") or task.get("index"),
        "index": task.get("index"),
        "attempt": task.get("attempt"),
        "executorId": task.get("executorId"),
        "host": task.get("host"),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "input_bytes": input_bytes,
        "input": bytes_human(input_bytes),
        "shuffle_read_bytes": shuffle_read,
        "shuffle_read": bytes_human(shuffle_read),
        "shuffle_write_bytes": shuffle_write,
        "shuffle_write": bytes_human(shuffle_write),
        "gc_ms": gc_ms,
        "gc": millis_human(gc_ms),
        "gc_ratio": ratio_human(gc_ms, duration_ms),
        "memory_spill_bytes": memory_spill,
        "memory_spill": bytes_human(memory_spill),
        "disk_spill_bytes": disk_spill,
        "disk_spill": bytes_human(disk_spill),
        "input_bytes_per_sec": throughput_per_sec(input_bytes, duration_ms),
        "shuffle_read_bytes_per_sec": throughput_per_sec(shuffle_read, duration_ms),
        "shuffle_write_bytes_per_sec": throughput_per_sec(shuffle_write, duration_ms),
    }


def throughput_per_sec(bytes_value: Any, duration_ms: Any) -> int | None:
    """Calculate byte throughput per second from bytes and duration."""
    bytes_number = to_float(bytes_value)
    duration_number = to_float(duration_ms)
    if bytes_number is None or duration_number is None or duration_number <= 0:
        return None
    return int(bytes_number / (duration_number / 1000))


def recommend_task_duration_actions(
    duration_skew: dict[str, Any],
    metric_skew: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[str]:
    """Recommend next checks based on task duration and metric skew evidence."""
    recommendations: list[str] = []
    if duration_skew.get("level") in {"warning", "critical"}:
        recommendations.append("Task duration has a long tail; inspect top_slow_tasks before tuning global parallelism.")
    if (to_float(metric_skew.get("input_bytes", {}).get("skew_ratio")) or 0) >= 3:
        recommendations.append("Input bytes are skewed; check partition pruning, join keys, and source data distribution.")
    if (to_float(metric_skew.get("shuffle_read", {}).get("skew_ratio")) or 0) >= 3 or (to_float(metric_skew.get("shuffle_write", {}).get("skew_ratio")) or 0) >= 3:
        recommendations.append("Shuffle bytes are skewed; inspect Exchange/join keys and consider salting or repartitioning.")
    if any((to_float(row.get("gc_ms")) or 0) > 0 and (to_float(row.get("gc_ratio", "0").rstrip("%")) if isinstance(row.get("gc_ratio"), str) else 0) >= 10 for row in rows):
        recommendations.append("Some slow tasks spend significant time in GC; inspect executor memory, spill, and object pressure.")
    if any((to_float(row.get("memory_spill_bytes")) or 0) > 0 or (to_float(row.get("disk_spill_bytes")) or 0) > 0 for row in rows):
        recommendations.append("Spill is present; check shuffle partitions, memory sizing, and aggregation/join pressure.")
    if not recommendations:
        recommendations.append("No strong skew signal found; compare executor timeline, external writes, and job idle gaps.")
    return recommendations


def summarize_executors(executors: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize Spark executors."""
    rows = [summarize_executor(executor) for executor in executors]
    return {
        "executor_count": len(executors),
        "active_executors": sum(1 for item in executors if item.get("isActive")),
        "inactive_executors": sum(1 for item in executors if not item.get("isActive")),
        "failed_tasks": sum(int(item.get("failedTasks", 0) or 0) for item in executors),
        "top_gc": sorted(rows, key=lambda item: float(item.get("totalGCTime") or 0), reverse=True)[:10],
        "top_memory": sorted(rows, key=lambda item: float(item.get("memory_used_pct") or 0), reverse=True)[:10],
        "top_shuffle_read": sorted(rows, key=lambda item: float(item.get("totalShuffleRead") or 0), reverse=True)[:10],
        "executors": rows,
    }


def summarize_executor_gc(executors: list[dict[str, Any]]) -> dict[str, Any]:
    """Return compact executor GC statistics."""
    rows = [compact_executor_gc_row(executor) for executor in executors]
    total_gc = sum(int(row.get("totalGCTime", 0) or 0) for row in rows)
    total_duration = sum(int(row.get("totalDurationMs", 0) or 0) for row in rows)
    return {
        "executor_count": len(rows),
        "total_gc_ms": total_gc,
        "total_duration_ms": total_duration,
        "total_gc": millis_human(total_gc),
        "total_duration": millis_human(total_duration),
        "gc_ratio": ratio_human(total_gc, total_duration),
        "top_gc": sorted(rows, key=lambda item: int(item.get("totalGCTime", 0) or 0), reverse=True)[:10],
        "executors": rows,
    }


def summarize_executor_health(executors: list[dict[str, Any]], *, top: int = 10) -> dict[str, Any]:
    """Summarize executor health signals such as failures, removals, and GC."""
    rows = [summarize_executor(executor) for executor in executors]
    failed_tasks = sum(int(executor.get("failedTasks", 0) or 0) for executor in executors)
    removed = [executor for executor in executors if executor.get("removeTime") or executor.get("removeReason")]
    reasons = Counter(str(executor.get("removeReason") or "unknown") for executor in removed)
    risks = []
    if failed_tasks:
        risks.append({"level": "critical", "area": "executor_tasks", "message": "Failed tasks reported by executors", "evidence": failed_tasks})
    if removed:
        risks.append({"level": "warning", "area": "executors", "message": "Removed executors found", "evidence": len(removed)})
    if any((to_float(row.get("totalGCTime")) or 0) > 0 and (to_float(str(row.get("totalGCTime"))) or 0) >= 10000 for row in rows):
        risks.append({"level": "warning", "area": "gc", "message": "Executor GC time is elevated", "evidence": rows[0].get("id") if rows else None})
    return {
        "executor_count": len(executors),
        "active_executors": sum(1 for item in executors if item.get("isActive")),
        "inactive_executors": sum(1 for item in executors if not item.get("isActive")),
        "removed_executors": len(removed),
        "failed_tasks": failed_tasks,
        "remove_reasons": dict(reasons),
        "top_failed_executors": sorted(rows, key=lambda item: int(item.get("failedTasks") or 0), reverse=True)[:top],
        "top_gc_executors": sorted(rows, key=lambda item: int(item.get("totalGCTime") or 0), reverse=True)[:top],
        "top_memory_executors": sorted(rows, key=lambda item: float(item.get("memory_used_pct") or 0), reverse=True)[:top],
        "risks": risks,
        "executors": rows,
    }


def summarize_executor_task_health(tasks: list[dict[str, Any]], executors: list[dict[str, Any]] | None = None, *, top: int = 10) -> dict[str, Any]:
    """Aggregate task health by executor id and host."""
    executor_map = {str(executor.get("id")): executor for executor in executors or []}
    groups: dict[str, dict[str, Any]] = {}
    task_rows = [compact_task_health_row(task, executor_map) for task in tasks]
    for row in task_rows:
        executor_id = str(row.get("executorId") or "unknown")
        group = groups.setdefault(
            executor_id,
            {
                "executor_id": executor_id,
                "host": row.get("host"),
                "task_count": 0,
                "failed_tasks": 0,
                "killed_tasks": 0,
                "success_tasks": 0,
                "total_duration_ms": 0,
                "total_gc_ms": 0,
                "memory_spill_bytes": 0,
                "disk_spill_bytes": 0,
                "input_bytes": 0,
                "shuffle_read_bytes": 0,
                "shuffle_write_bytes": 0,
                "top_slow_tasks": [],
                "failed_task_samples": [],
            },
        )
        group["task_count"] += 1
        status = str(row.get("status") or "").upper()
        if status == "FAILED":
            group["failed_tasks"] += 1
            group["failed_task_samples"].append(row)
        elif status == "KILLED":
            group["killed_tasks"] += 1
        elif status in {"SUCCESS", "SUCCEEDED", "COMPLETE"}:
            group["success_tasks"] += 1
        group["total_duration_ms"] += int(row.get("duration_ms") or 0)
        group["total_gc_ms"] += int(row.get("gc_ms") or 0)
        group["memory_spill_bytes"] += int(row.get("memory_spill_bytes") or 0)
        group["disk_spill_bytes"] += int(row.get("disk_spill_bytes") or 0)
        group["input_bytes"] += int(row.get("input_bytes") or 0)
        group["shuffle_read_bytes"] += int(row.get("shuffle_read_bytes") or 0)
        group["shuffle_write_bytes"] += int(row.get("shuffle_write_bytes") or 0)
        group["top_slow_tasks"].append(row)
    executor_rows = []
    for group in groups.values():
        group["total_duration"] = millis_human(group["total_duration_ms"])
        group["total_gc"] = millis_human(group["total_gc_ms"])
        group["gc_ratio"] = ratio_human(group["total_gc_ms"], group["total_duration_ms"])
        group["memory_spill"] = bytes_human(group["memory_spill_bytes"])
        group["disk_spill"] = bytes_human(group["disk_spill_bytes"])
        group["input"] = bytes_human(group["input_bytes"])
        group["shuffle_read"] = bytes_human(group["shuffle_read_bytes"])
        group["shuffle_write"] = bytes_human(group["shuffle_write_bytes"])
        group["top_slow_tasks"] = sorted(group["top_slow_tasks"], key=lambda item: int(item.get("duration_ms") or 0), reverse=True)[:top]
        group["failed_task_samples"] = group["failed_task_samples"][:top]
        executor_rows.append(group)
    return {
        "task_count": len(task_rows),
        "failed_tasks": sum(1 for row in task_rows if str(row.get("status") or "").upper() == "FAILED"),
        "killed_tasks": sum(1 for row in task_rows if str(row.get("status") or "").upper() == "KILLED"),
        "top_failed_tasks": [row for row in task_rows if str(row.get("status") or "").upper() == "FAILED"][:top],
        "top_slow_tasks": sorted(task_rows, key=lambda item: int(item.get("duration_ms") or 0), reverse=True)[:top],
        "executors": sorted(executor_rows, key=lambda item: (int(item.get("failed_tasks") or 0), int(item.get("total_gc_ms") or 0), int(item.get("total_duration_ms") or 0)), reverse=True),
    }


def compact_task_health_row(task: dict[str, Any], executor_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Return compact task health fields with executor context."""
    row = compact_task_duration_row(task)
    executor_id = str(row.get("executorId") or "")
    executor = executor_map.get(executor_id, {})
    row["status"] = first_present(task.get("status"), task.get("taskStatus"), get_nested(task, "taskInfo.status"))
    row["errorMessage"] = first_present(task.get("errorMessage"), task.get("error"), task.get("failureReason"))
    row["host"] = first_present(row.get("host"), executor.get("hostPort"))
    return row


def build_task_health_diagnosis(executor_health: dict[str, Any], task_health: dict[str, Any]) -> dict[str, Any]:
    """Build risks and recommendations from executor and task health summaries."""
    risks: list[dict[str, Any]] = []
    recommendations: list[str] = []
    if int(executor_health.get("failed_tasks", 0) or 0):
        risks.append({"level": "critical", "area": "executors", "message": "Executors reported failed tasks", "evidence": executor_health.get("failed_tasks")})
        recommendations.append("Check top_failed_executors and correlate with failed task samples.")
    if int(task_health.get("failed_tasks", 0) or 0):
        risks.append({"level": "critical", "area": "tasks", "message": "Failed tasks found in taskList", "evidence": task_health.get("failed_tasks")})
        recommendations.append("Inspect failed task errorMessage and executor concentration.")
    if int(executor_health.get("removed_executors", 0) or 0):
        risks.append({"level": "warning", "area": "executors", "message": "Removed executors found", "evidence": executor_health.get("remove_reasons", {})})
        recommendations.append("Review removeReason and executor timeline for resource churn.")
    if executor_health.get("top_gc_executors"):
        risks.append({"level": "info", "area": "gc", "message": "Executor GC evidence available", "evidence": executor_health.get("top_gc_executors", [])[:3]})
    if any((to_float(row.get("gc_ratio", "0").rstrip("%")) if isinstance(row.get("gc_ratio"), str) else 0) >= 10 for row in task_health.get("executors", [])):
        risks.append({"level": "warning", "area": "gc", "message": "Task GC ratio is elevated on some executors", "evidence": True})
        recommendations.append("Check executor memory, spill, and object allocation pressure.")
    if not recommendations:
        recommendations.append("No clear executor/task health issue found; continue with duration, shuffle, and SQL plan checks.")
    return {"risk_count": len(risks), "risks": risks, "recommendations": recommendations}


def compact_executor_gc_row(executor: dict[str, Any]) -> dict[str, Any]:
    """Return compact executor GC fields."""
    total_gc = int(executor.get("totalGCTime", 0) or 0)
    total_duration = int(executor.get("totalDuration", 0) or 0)
    return {
        "id": executor.get("id"),
        "hostPort": executor.get("hostPort"),
        "isActive": executor.get("isActive"),
        "totalTasks": executor.get("totalTasks"),
        "failedTasks": executor.get("failedTasks"),
        "totalDurationMs": total_duration,
        "totalGCTime": total_gc,
        "totalDuration": millis_human(total_duration),
        "totalGCTimeHuman": millis_human(total_gc),
        "gc_ratio": ratio_human(total_gc, total_duration),
    }


def summarize_executor(executor: dict[str, Any]) -> dict[str, Any]:
    """Summarize one Spark executor."""
    memory_used = to_float(executor.get("memoryUsed")) or 0.0
    max_memory = to_float(executor.get("maxMemory")) or 0.0
    return {
        "id": executor.get("id"),
        "hostPort": executor.get("hostPort"),
        "isActive": executor.get("isActive"),
        "totalCores": executor.get("totalCores"),
        "maxTasks": executor.get("maxTasks"),
        "activeTasks": executor.get("activeTasks"),
        "failedTasks": executor.get("failedTasks"),
        "completedTasks": executor.get("completedTasks"),
        "totalTasks": executor.get("totalTasks"),
        "totalDuration": millis_human(executor.get("totalDuration")),
        "totalGCTime": executor.get("totalGCTime"),
        "totalGCTimeHuman": millis_human(executor.get("totalGCTime")),
        "memoryUsed": bytes_human(memory_used),
        "maxMemory": bytes_human(max_memory),
        "memory_used_pct": round(memory_used / max_memory * 100, 2) if max_memory else None,
        "totalInputBytes": bytes_human(executor.get("totalInputBytes")),
        "totalShuffleRead": executor.get("totalShuffleRead"),
        "totalShuffleReadHuman": bytes_human(executor.get("totalShuffleRead")),
        "totalShuffleWrite": executor.get("totalShuffleWrite"),
        "totalShuffleWriteHuman": bytes_human(executor.get("totalShuffleWrite")),
        "executorLogs": executor.get("executorLogs", {}),
    }


def summarize_sql(sql_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize Spark SQL executions."""
    states = Counter(str(item.get("status", "UNKNOWN")).upper() for item in sql_rows)
    rows = [summarize_sql_row(item) for item in sql_rows]
    return {
        "total_sql": len(sql_rows),
        "running_sql": states.get("RUNNING", 0),
        "completed_sql": states.get("COMPLETED", 0),
        "failed_sql": states.get("FAILED", 0),
        "top_duration": sorted(rows, key=lambda item: float(item.get("duration_ms") or 0), reverse=True)[:10],
        "sql": rows,
    }


class _HtmlTableParser(HTMLParser):
    """Collect simple HTML table rows from Spark UI pages."""

    def __init__(self) -> None:
        """Initialize parser state."""
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None
        self._in_cell = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Track table, row, and cell boundaries."""
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
            self._in_cell = True

    def handle_data(self, data: str) -> None:
        """Collect text inside table cells."""
        if self._in_cell and self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        """Flush completed cells, rows, and tables."""
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append(normalize_space("".join(self._current_cell)))
            self._current_cell = None
            self._in_cell = False
        elif tag == "tr" and self._current_row is not None and self._current_table is not None:
            if any(cell for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            self.tables.append(self._current_table)
            self._current_table = None


def parse_sql_failed_html(html: str) -> list[dict[str, Any]]:
    """Extract failed SQL rows and error messages from Spark SQL HTML."""
    parser = _HtmlTableParser()
    parser.feed(html)
    failed_rows: list[dict[str, Any]] = []
    for table in parser.tables:
        if not table:
            continue
        headers = [normalize_header(cell) for cell in table[0]]
        if "error_message" not in headers:
            continue
        for cells in table[1:]:
            row = {headers[index]: cells[index] for index in range(min(len(headers), len(cells)))}
            failed_rows.append(
                {
                    "id": safe_int(first_present(row.get("id"), row.get("sql_id"))),
                    "description": row.get("description") or row.get("details"),
                    "status": row.get("status") or "FAILED",
                    "error_message": row.get("error_message"),
                }
            )
    return failed_rows


def normalize_header(value: Any) -> str:
    """Normalize HTML table headers to stable snake-case keys."""
    text = normalize_space(value)
    aliases = {
        "error message": "error_message",
        "sql id": "sql_id",
        "id": "id",
        "description": "description",
        "details": "details",
        "status": "status",
    }
    return aliases.get(text.lower(), re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_"))


def normalize_space(value: Any) -> str:
    """Collapse whitespace in display text."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def summarize_sql_failures(
    sql_rows: list[dict[str, Any]],
    *,
    html_errors: list[dict[str, Any]] | None = None,
    top: int = 20,
) -> dict[str, Any]:
    """Summarize failed SQL executions with HTML error-message enrichment."""
    error_by_id = {row.get("id"): row for row in html_errors or [] if row.get("id") is not None}
    failed_rows = [row for row in sql_rows if sql_status(row) == "FAILED"]
    completed_rows = [row for row in sql_rows if sql_status(row) == "COMPLETED"]
    failed_by_table: Counter[str] = Counter()
    failed_by_statement_type: Counter[str] = Counter()
    failed_by_error_message: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for row in failed_rows:
        sql_id = row.get("id")
        html_row = error_by_id.get(sql_id, {})
        description = first_present(row.get("description"), html_row.get("description"), "")
        error_message = first_present(html_row.get("error_message"), row.get("errorMessage"), row.get("error_message"))
        table = extract_sql_table(description)
        statement_type = classify_sql_statement_type(description)
        if table:
            failed_by_table[table] += 1
        if statement_type:
            failed_by_statement_type[statement_type] += 1
        if error_message:
            failed_by_error_message[normalize_space(error_message)] += 1
        samples.append(
            {
                "id": sql_id,
                "table": table,
                "statement_type": statement_type,
                "description": trim_text(description, 500),
                "error_message": trim_text(error_message, 500),
            }
        )
    return {
        "total_sql": len(sql_rows),
        "failed_sql": len(failed_rows),
        "completed_sql": len(completed_rows),
        "failed_by_table": dict(failed_by_table.most_common(top)),
        "failed_by_statement_type": dict(failed_by_statement_type.most_common(top)),
        "failed_by_error_message": dict(failed_by_error_message.most_common(top)),
        "top_failed_sql_samples": samples[:top],
        "recommendations": recommend_sql_failures(failed_by_error_message),
    }


def summarize_ddl_failures(
    sql_rows: list[dict[str, Any]],
    *,
    html_errors: list[dict[str, Any]] | None = None,
    top: int = 20,
) -> dict[str, Any]:
    """Summarize DDL SQL executions and partition-oriented failures."""
    error_by_id = {row.get("id"): row for row in html_errors or [] if row.get("id") is not None}
    ddl_rows = [row for row in sql_rows if is_ddl_sql(row.get("description"))]
    tables: Counter[str] = Counter()
    statement_types: Counter[str] = Counter()
    partition_values: dict[str, Counter[str]] = {}
    samples: list[dict[str, Any]] = []
    rows_without_jobs = 0
    for row in ddl_rows:
        description = str(row.get("description") or "")
        table = extract_sql_table(description)
        statement_type = classify_sql_statement_type(description)
        if table:
            tables[table] += 1
        if statement_type:
            statement_types[statement_type] += 1
        for key, value in extract_partition_values(description).items():
            partition_values.setdefault(key, Counter())[value] += 1
        if not collect_sql_job_ids([row]):
            rows_without_jobs += 1
        error_message = error_by_id.get(row.get("id"), {}).get("error_message")
        samples.append(
            {
                "id": row.get("id"),
                "status": row.get("status"),
                "table": table,
                "statement_type": statement_type,
                "description": trim_text(description, 500),
                "error_message": trim_text(error_message, 500),
            }
        )
    failed = [row for row in ddl_rows if sql_status(row) == "FAILED"]
    completed = [row for row in ddl_rows if sql_status(row) == "COMPLETED"]
    return {
        "ddl_total": len(ddl_rows),
        "ddl_failed": len(failed),
        "ddl_completed": len(completed),
        "tables": dict(tables.most_common(top)),
        "statement_types": dict(statement_types.most_common(top)),
        "partition_values": {key: dict(counter.most_common(top)) for key, counter in partition_values.items()},
        "mostly_without_spark_jobs": rows_without_jobs >= max(1, len(ddl_rows) // 2) if ddl_rows else False,
        "top_ddl_samples": samples[:top],
        "recommendations": recommend_ddl_failures(ddl_rows, failed),
    }


def sql_status(row: dict[str, Any]) -> str:
    """Return a normalized SQL status."""
    return str(row.get("status", "UNKNOWN")).upper()


def count_sql_status(rows: list[dict[str, Any]], status: str) -> int:
    """Count SQL rows matching one status."""
    wanted = status.upper()
    return sum(1 for row in rows if sql_status(row) == wanted)


def classify_sql_statement_type(description: Any) -> str | None:
    """Classify a SQL statement into a compact operation type."""
    text = normalize_space(description).upper()
    if not text:
        return None
    if text.startswith("ALTER TABLE"):
        if "DROP" in text and "PARTITION" in text:
            return "ALTER TABLE DROP PARTITION"
        if "ADD" in text and "PARTITION" in text:
            return "ALTER TABLE ADD PARTITION"
        return "ALTER TABLE"
    if text.startswith("CREATE TABLE"):
        return "CREATE TABLE"
    if text.startswith("DROP TABLE"):
        return "DROP TABLE"
    if text.startswith("TRUNCATE TABLE"):
        return "TRUNCATE TABLE"
    if text.startswith("MSCK REPAIR TABLE"):
        return "MSCK REPAIR TABLE"
    return text.split(" ", 1)[0]


def is_ddl_sql(description: Any) -> bool:
    """Return true for DDL statements that often have no Spark jobs."""
    statement_type = classify_sql_statement_type(description)
    return bool(statement_type and statement_type in {"ALTER TABLE", "ALTER TABLE DROP PARTITION", "ALTER TABLE ADD PARTITION", "CREATE TABLE", "DROP TABLE", "TRUNCATE TABLE", "MSCK REPAIR TABLE"})


def extract_sql_table(description: Any) -> str | None:
    """Extract the principal table name from a SQL description."""
    text = normalize_space(description)
    patterns = [
        r"\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(`?[\w.\-]+`?)",
        r"\bMSCK\s+REPAIR\s+TABLE\s+(`?[\w.\-]+`?)",
        r"\b(?:DROP|TRUNCATE)\s+TABLE\s+(?:IF\s+EXISTS\s+)?(`?[\w.\-]+`?)",
        r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`?[\w.\-]+`?)",
        r"\bINSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?TABLE\s+(`?[\w.\-]+`?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip("`")
    return None


def extract_partition_values(description: Any) -> dict[str, str]:
    """Extract partition key-value pairs from a SQL PARTITION clause."""
    text = normalize_space(description)
    match = re.search(r"\bPARTITION\s*\((.*?)\)", text, flags=re.IGNORECASE)
    if not match:
        return {}
    values: dict[str, str] = {}
    for part in match.group(1).split(","):
        key_value = part.split("=", 1)
        if len(key_value) != 2:
            continue
        key = normalize_space(key_value[0]).strip("`")
        value = normalize_space(key_value[1]).strip("'\"`")
        if key and value:
            values[key] = value
    return values


def recommend_sql_failures(failed_by_error_message: Counter[str]) -> list[str]:
    """Build SQL failure recommendations from dominant error messages."""
    recommendations: list[str] = []
    combined = "\n".join(failed_by_error_message.keys()).lower()
    if "partition metadata is not stored in the hive metastore" in combined or "msck repair table" in combined:
        recommendations.append("大量 DDL 失败指向 Hive metastore 分区元数据缺失，先对目标表执行或补齐 `msck repair table` 流程，再重试 DROP/ADD PARTITION。")
    if not recommendations and failed_by_error_message:
        recommendations.append("优先按 failed_by_error_message 中的最高频错误归因，而不是按 Spark stage 耗时归因。")
    return recommendations


def recommend_ddl_failures(ddl_rows: list[dict[str, Any]], failed_rows: list[dict[str, Any]]) -> list[str]:
    """Build DDL-focused recommendations."""
    if not ddl_rows:
        return []
    if failed_rows and len(failed_rows) / len(ddl_rows) >= 0.5:
        return ["DDL 批量失败占比较高，且这类 SQL 可能没有 Spark job/stage；先处理元数据或权限问题，避免误判为计算性能慢。"]
    return ["DDL 执行存在但失败占比不高，结合 SQL failure error message 和表分区分布继续排查。"]


def summarize_sql_operators(sql_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize important physical plan operators for SQL executions."""
    rows = []
    for item in sql_rows:
        plan = str(item.get("planDescription") or "")
        rows.append(
            {
                "id": item.get("id"),
                "status": item.get("status"),
                "duration": millis_human(item.get("duration")),
                "duration_ms": item.get("duration"),
                "description": trim_text(item.get("description"), 300),
                "operators": count_plan_operators(plan),
                "operator_lines": extract_plan_operator_lines(plan),
            }
        )
    return {"total_sql": len(rows), "sql": rows}


def count_plan_operators(plan: str) -> dict[str, int]:
    """Count important Spark physical plan operators."""
    return {operator: len(re.findall(re.escape(operator), plan, flags=re.IGNORECASE)) for operator in _PLAN_OPERATORS}


def extract_plan_operator_lines(plan: str, *, limit: int = 80) -> list[str]:
    """Extract readable plan lines containing important operators."""
    lines = []
    for line in plan.splitlines():
        if any(operator.lower() in line.lower() for operator in _PLAN_OPERATORS):
            lines.append(line[:300])
    return lines[:limit]


def analyze_sql_performance(
    sql_rows: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
) -> dict[str, Any]:
    """Analyze likely Spark SQL bottlenecks from plan and runtime summaries."""
    operators = summarize_sql_operators(sql_rows)
    stage_io = summarize_stages_io(stages)
    shuffle = summarize_stage_shuffle(stages)
    executor_gc = summarize_executor_gc(executors)
    jobs_summary = summarize_jobs(jobs, brief=True)
    total_input = int(stage_io.get("total_input_bytes", 0) or 0)
    total_shuffle = int(shuffle.get("total_shuffle_bytes", 0) or 0)
    gc_ratio_text = executor_gc.get("gc_ratio")
    gc_ratio = float(str(gc_ratio_text).rstrip("%")) if gc_ratio_text else 0.0
    op_totals = Counter()
    for row in operators["sql"]:
        op_totals.update(row.get("operators", {}))
    recommendations: list[str] = []
    primary = "unknown"
    if total_input > max(total_shuffle * 10, 1024**3):
        primary = "scan"
        recommendations.append("优先检查源表分区裁剪、增量读取、谓词下推，减少大表扫描。")
    elif total_shuffle > 1024**3:
        primary = "shuffle"
        recommendations.append("优先检查 Exchange、join key、shuffle partitions 和数据倾斜。")
    elif gc_ratio > 10:
        primary = "gc"
        recommendations.append("GC 占比较高，优先检查 executor 内存、缓存和对象膨胀。")
    if op_totals.get("Window", 0) or op_totals.get("Sort", 0):
        recommendations.append("计划包含 Window/Sort，若是维表取最新记录，考虑预计算或物化最新维表。")
    if op_totals.get("Repartition", 0) or op_totals.get("Exchange", 0):
        recommendations.append("计划包含 repartition/exchange，确认分区数是否与数据规模和目标表 bucket 匹配。")
    if not recommendations:
        recommendations.append("未发现明显单点瓶颈，建议进一步查看 task-level skew 和写入端日志。")
    return {
        "primary_bottleneck": primary,
        "recommendations": recommendations,
        "operator_totals": dict(op_totals),
        "jobs": jobs_summary,
        "stage_io": stage_io,
        "shuffle": shuffle,
        "executor_gc": executor_gc,
        "sql_operators": operators,
    }


def build_speed_diagnosis(
    jobs: list[dict[str, Any]],
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    sql_rows: list[dict[str, Any]],
    *,
    app: dict[str, Any] | None = None,
    task_failure_groups: list[dict[str, Any]] | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    """Build a one-shot speed diagnosis report."""
    stage_io = compact_top_report(summarize_stages_io(stages), limit, drop_keys={"stages"})
    shuffle = compact_top_report(summarize_stage_shuffle(stages), limit, drop_keys={"stages"})
    sql_analysis = compact_sql_analysis(analyze_sql_performance(sql_rows, jobs, stages, executors), limit)
    jobs_summary = summarize_jobs(jobs, brief=True)
    jobs_summary["jobs"] = jobs_summary["jobs"][:limit]
    timeline = summarize_jobs_timeline(jobs)
    timeline["jobs"] = timeline["jobs"][:limit]
    timeline["idle_gaps"] = timeline["idle_gaps"][:limit]
    long_app = classify_long_app_runtime(app, jobs, sql_rows=sql_rows, top=limit)
    executor_capacity = build_executor_capacity_summary(stages, executors, task_failure_groups or [], limit=limit)
    primary_bottleneck = executor_capacity.get("primary_bottleneck")
    if primary_bottleneck == "unknown":
        primary_bottleneck = long_app.get("primary_bottleneck")
    return {
        "primary_bottleneck": primary_bottleneck,
        "long_app": long_app,
        "jobs": jobs_summary,
        "timeline": timeline,
        "job_durations": summarize_job_durations(jobs, top=limit),
        "stage_durations": summarize_stage_durations(stages, top=limit),
        "stage_wall_times": summarize_stage_wall_times(stages, top=limit),
        "executor_capacity": executor_capacity,
        "stage_io": stage_io,
        "shuffle": shuffle,
        "executor_gc": compact_top_report(summarize_executor_gc(executors), limit, drop_keys={"executors"}),
        "sql_analysis": sql_analysis,
    }


def build_executor_capacity_summary(
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    task_failure_groups: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> dict[str, Any]:
    """Summarize executor capacity and churn evidence for speed diagnosis."""
    wall_times = summarize_stage_wall_times(stages, top=limit)
    top_wall_stage = (wall_times.get("top_wall_time_stages") or [{}])[0]
    stage_id = safe_int(top_wall_stage.get("stageId"))
    churn = summarize_executor_churn(executors, stages, stage_id=stage_id, attempt_id=safe_int(top_wall_stage.get("attemptId")), top=limit)
    reason_counts = Counter()
    for group in task_failure_groups:
        reason_counts.update(group.get("error_reasons", {}))
    estimated_parallelism = to_float(top_wall_stage.get("estimated_parallelism"))
    num_tasks = safe_int(top_wall_stage.get("numTasks")) or 0
    wall_ms = safe_int(top_wall_stage.get("wall_duration_ms")) or 0
    primary = "unknown"
    if estimated_parallelism is not None and estimated_parallelism < 2 and num_tasks >= 100 and wall_ms >= 60000:
        primary = "low_executor_parallelism"
    elif churn.get("removed_during_stage") and reason_counts.get("executor_lost"):
        primary = "executor_churn"
    return {
        "primary_bottleneck": primary,
        "top_wall_time_stage": top_wall_stage,
        "estimated_parallelism": estimated_parallelism,
        "executor_churn_count_during_top_stage": churn.get("removed_during_stage"),
        "failed_task_reasons": dict(reason_counts.most_common(limit)),
        "churn": {key: value for key, value in churn.items() if key not in {"timeline", "executors"}},
    }


def compact_top_report(report: dict[str, Any], limit: int, *, drop_keys: set[str] | None = None) -> dict[str, Any]:
    """Drop full lists and trim top lists for one-shot diagnostics."""
    result = {key: value for key, value in report.items() if key not in (drop_keys or set())}
    for key, value in list(result.items()):
        if key.startswith("top_") and isinstance(value, list):
            result[key] = value[:limit]
    return result


def compact_sql_analysis(report: dict[str, Any], limit: int) -> dict[str, Any]:
    """Trim verbose SQL analysis internals for speed diagnostics."""
    result = {key: value for key, value in report.items() if key not in {"jobs", "stage_io", "shuffle", "executor_gc"}}
    operators = result.get("sql_operators", {})
    if isinstance(operators, dict):
        sql_rows = []
        for row in operators.get("sql", [])[:limit]:
            compact_row = dict(row)
            compact_row.pop("operator_lines", None)
            sql_rows.append(compact_row)
        result["sql_operators"] = {"total_sql": operators.get("total_sql"), "sql": sql_rows}
    return result


def summarize_sql_row(item: dict[str, Any]) -> dict[str, Any]:
    """Summarize one SQL execution row."""
    return {
        "id": item.get("id"),
        "status": item.get("status"),
        "submissionTime": item.get("submissionTime"),
        "duration": millis_human(item.get("duration")),
        "duration_ms": item.get("duration"),
        "runningJobIds": item.get("runningJobIds", []),
        "successJobIds": item.get("successJobIds", []),
        "failedJobIds": item.get("failedJobIds", []),
        "description": trim_text(item.get("description"), 800),
        "planDescription": trim_text(item.get("planDescription"), 800),
    }


def scan_log_text(text: str, patterns: list[str] | None = None, *, limit: int = 20) -> dict[str, Any]:
    """Scan log text for diagnostic patterns."""
    wanted = patterns or _DEFAULT_LOG_PATTERNS
    lines = text.splitlines()
    matches: dict[str, Any] = {}
    for pattern in wanted:
        expression = rf"\b{re.escape(pattern)}\b" if re.fullmatch(r"[A-Za-z]+", pattern) else pattern
        compiled = re.compile(expression, re.IGNORECASE)
        selected = [line[:500] for line in lines if compiled.search(line)]
        matches[pattern] = {"count": len(selected), "samples": selected[:limit]}
    return {"line_count": len(lines), "patterns": wanted, "matches": matches}


def get_environment_value(environment: dict[str, Any], key: str) -> dict[str, Any]:
    """Resolve an environment value by page label, REST field, or property key."""
    alias = _ENV_KEY_ALIASES.get(normalize_lookup_key(key))
    candidates = [alias, key] if alias else [key]
    flattened = flatten_environment(environment)
    normalized = {normalize_lookup_key(path): (path, value) for path, value in flattened.items()}
    for candidate in candidates:
        if not candidate:
            continue
        direct = flattened.get(candidate)
        if direct is not None:
            return {"key": key, "path": candidate, "value": direct, "available": True}
        match = normalized.get(normalize_lookup_key(candidate))
        if match:
            return {"key": key, "path": match[0], "value": match[1], "available": True}
    return {"key": key, "available": False}


def flatten_environment(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten Spark environment payload dictionaries and key-value arrays."""
    result: dict[str, Any] = {}
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(item, (dict, list)):
                result.update(flatten_environment(item, path))
            else:
                result[path] = item
    elif isinstance(value, list):
        for index, item in enumerate(value):
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                result[str(item[0])] = item[1]
                result[f"{prefix}.{item[0]}" if prefix else str(item[0])] = item[1]
            else:
                result.update(flatten_environment(item, f"{prefix}[{index}]"))
    return result


def normalize_lookup_key(value: Any) -> str:
    """Normalize display labels and property keys for lookup."""
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def build_health_report(parts: dict[str, Any]) -> dict[str, Any]:
    """Build a compact health report from diagnostic parts."""
    app = parts.get("app", {})
    jobs = parts.get("jobs", {})
    stages = parts.get("stages", {})
    executors = parts.get("executors", {})
    sql = parts.get("sql", {})
    logs = parts.get("logs", {})
    risks: list[dict[str, Any]] = []
    if jobs.get("failed_jobs") or jobs.get("failed_tasks"):
        risks.append({"level": "critical", "area": "jobs", "message": "Failed jobs or tasks found", "evidence": {"failed_jobs": jobs.get("failed_jobs"), "failed_tasks": jobs.get("failed_tasks")}})
    if stages.get("failed_stages") or stages.get("failed_tasks"):
        risks.append({"level": "critical", "area": "stages", "message": "Failed stages or tasks found", "evidence": {"failed_stages": stages.get("failed_stages"), "failed_tasks": stages.get("failed_tasks")}})
    if executors.get("inactive_executors"):
        risks.append({"level": "warning", "area": "executors", "message": "Inactive executors found", "evidence": executors.get("inactive_executors")})
    if executors.get("failed_tasks"):
        risks.append({"level": "warning", "area": "executors", "message": "Executor failed tasks found", "evidence": executors.get("failed_tasks")})
    if sql.get("failed_sql"):
        risks.append({"level": "critical", "area": "sql", "message": "Failed SQL executions found", "evidence": sql.get("failed_sql")})
    log_matches = logs.get("scan", {}).get("matches", {}) if isinstance(logs, dict) else {}
    error_count = sum(int(log_matches.get(key, {}).get("count", 0) or 0) for key in ("ERROR", "Exception", "OutOfMemoryError", "FetchFailed"))
    if error_count:
        risks.append({"level": "warning", "area": "logs", "message": "Risky log patterns found", "evidence": error_count})
    return {
        "app": app,
        "risk_count": len(risks),
        "risks": risks,
        "jobs": jobs,
        "stages": stages,
        "executors": executors,
        "sql": sql,
    }


def get_nested(data: dict[str, Any], path: str) -> Any:
    """Read a dotted path from nested dictionaries."""
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def get_first_nested(data: dict[str, Any], paths: list[str]) -> Any:
    """Read the first available dotted path from nested dictionaries."""
    for path in paths:
        value = get_nested(data, path)
        if value is not None:
            return value
    return None


def trim_text(value: Any, limit: int) -> str | None:
    """Return a trimmed string if present."""
    if value is None:
        return None
    text = str(value).strip()
    return text if len(text) <= limit else text[:limit] + "..."


def app_path(app_id: str, suffix: str = "") -> str:
    """Build an app-scoped Spark REST path."""
    return f"api/v1/applications/{app_id}/{suffix}".rstrip("/")


from lib.stages import (  # noqa: E402
    build_input_distribution_diagnosis,
    build_executor_loss_diagnosis,
    build_parallelism_diagnosis,
    build_failure_diagnosis,
    build_retry_diagnosis,
    classify_task_duration_skew,
    classify_failure_reason,
    classify_stage_failure,
    compact_failed_task_row,
    compact_stage_io_row,
    compact_stage_shuffle_row,
    compact_task_duration_row,
    group_stage_attempts,
    recommend_task_duration_actions,
    stage_wall_time_ms,
    summarize_executor_churn,
    summarize_stage,
    summarize_stage_durations,
    summarize_stage_executor_io,
    summarize_stage_retries,
    summarize_stage_shuffle,
    summarize_stage_tasks,
    summarize_stage_wall_times,
    summarize_stages,
    summarize_stages_io,
    summarize_task_duration_distribution,
    summarize_task_failures,
    summarize_task_skew,
    throughput_per_sec,
)


async def command_applications(client: SparkClient, args: argparse.Namespace) -> Any:
    """Handle applications listing."""
    apps = await client.get_json(applications_path(args))
    if not isinstance(apps, list):
        return apps
    return {"applications": [summarize_app(app) for app in filter_applications(apps, args)]}


async def command_resolve(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> dict[str, Any]:
    """Handle the resolve command."""
    app_id: str | None = None
    app: dict[str, Any] | None = None
    try:
        app_id = await resolve_app_id(client, parsed, args)
        app_data = await client.get_json(app_path(app_id), allow_error=True)
        app = app_data if isinstance(app_data, dict) and app_data.get("id") else None
    except SparkDiagError:
        app_id = parsed.app_id or getattr(args, "app_id", None)
    version = getattr(args, "spark_version", "auto")
    detected_version = None if version != "auto" else detect_spark_version_from_app(app or {})
    return {
        "input_url": parsed.input_url,
        "base_url": parsed.base_url,
        "origin": parsed.origin,
        "ui_kind": parsed.ui_kind,
        "deployment": parsed.deployment,
        "tab": parsed.tab,
        "app_id": app_id,
        "job_id": first_present(parsed.job_id, getattr(args, "job_id", None)),
        "stage_id": first_present(parsed.stage_id, getattr(args, "stage_id", None)),
        "attempt_id": first_present(parsed.attempt_id, getattr(args, "attempt_id", None)),
        "executor_id": first_present(parsed.executor_id, getattr(args, "executor_id", None)),
        "detected_spark_version": detected_version,
        "endpoint_profile": select_endpoint_profile(detected_version, getattr(args, "endpoint_profile", "auto")),
    }


async def command_overview(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle overview command."""
    app_id = await resolve_app_id(client, parsed, args)
    app, jobs, stages, executors, sql_rows = await asyncio.gather(
        client.get_json(app_path(app_id)),
        client.get_json(app_path(app_id, "jobs")),
        client.get_json(app_path(app_id, "stages")),
        client.get_json(app_path(app_id, "executors")),
        client.get_json(app_path(app_id, "sql"), allow_error=True),
    )
    sql_summary = summarize_sql(sql_rows) if isinstance(sql_rows, list) else sql_rows
    return {
        "app": summarize_app(app),
        "jobs": summarize_jobs(jobs),
        "stages": summarize_stages(stages),
        "executors": summarize_executors(executors),
        "sql": sql_summary,
    }


async def command_jobs(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle jobs command."""
    app_id = await resolve_app_id(client, parsed, args)
    jobs = await client.get_json(app_path(app_id, "jobs"))
    filtered = filter_jobs(jobs, getattr(args, "status", None))
    if getattr(args, "job_id", None) is not None:
        filtered = [job for job in filtered if int(job.get("jobId", -1)) == int(args.job_id)]
    if getattr(args, "subcommand", None) == "timeline":
        return summarize_jobs_timeline(filtered)
    if getattr(args, "subcommand", None) == "idle-gaps":
        return summarize_job_idle_gaps(filtered, top=getattr(args, "top", None) or 20)
    return summarize_jobs(filtered, brief=getattr(args, "brief", False))


async def command_job(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle singular job command aliases."""
    sub = getattr(args, "subcommand", None)
    if sub != "show":
        return await command_jobs(client, parsed, args)
    args.status = "all"
    result = await command_jobs(client, parsed, args)
    jobs = result.get("jobs", []) if isinstance(result, dict) else []
    return jobs[0] if jobs else {"available": False, "job_id": getattr(args, "job_id", None)}


def filter_stage_rows_for_args(stages: list[dict[str, Any]], parsed: ParsedUrl, args: argparse.Namespace, *, require_stage: bool = False) -> list[dict[str, Any]]:
    """Filter stage rows using stage and attempt ids from CLI args or URL context."""
    stage_id = first_present(getattr(args, "stage_id", None), parsed.stage_id)
    attempt_id = first_present(getattr(args, "attempt_id", None), parsed.attempt_id)
    if require_stage and stage_id is None:
        raise SparkDiagError("Missing --stage-id")
    selected = stages
    if stage_id is not None:
        selected = [stage for stage in selected if safe_int(stage.get("stageId")) == int(stage_id)]
    if attempt_id is not None:
        selected = [stage for stage in selected if safe_int(first_present(stage.get("attemptId"), 0)) == int(attempt_id)]
    return selected


def select_stages_for_failure_scan(stages: list[dict[str, Any]], parsed: ParsedUrl, args: argparse.Namespace) -> list[dict[str, Any]]:
    """Select stage attempts for bounded failure taskList scans."""
    command = getattr(args, "command", None)
    if command == "stage" or getattr(args, "stage_id", None) is not None or parsed.stage_id is not None:
        return filter_stage_rows_for_args(stages, parsed, args, require_stage=(command == "stage"))
    limit = getattr(args, "limit_stages", None) or 20
    if getattr(args, "all_stages", False):
        return sorted(stages, key=lambda stage: int(stage.get("executorRunTime") or 0), reverse=True)[:limit]
    failed = [stage for stage in stages if str(stage.get("status", "")).upper() in {"FAILED", "KILLED"} or int(stage.get("numFailedTasks", 0) or 0) > 0]
    slow = sorted(stages, key=lambda stage: int(stage.get("executorRunTime") or 0), reverse=True)
    selected: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any]] = set()
    for stage in failed + slow:
        key = (stage.get("stageId"), first_present(stage.get("attemptId"), 0))
        if key in seen:
            continue
        selected.append(stage)
        seen.add(key)
        if len(selected) >= limit:
            break
    return selected


async def collect_stage_task_failure_groups(
    client: SparkClient,
    app_id: str,
    selected_stages: list[dict[str, Any]],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    """Fetch selected taskList payloads and summarize task failures by stage."""
    task_results = await asyncio.gather(
        *[
            client.get_json(stage_task_list_path(app_id, stage), allow_error=True)
            for stage in selected_stages
            if stage.get("stageId") is not None
        ]
    )
    groups = []
    for stage, tasks in zip(selected_stages, task_results):
        if not isinstance(tasks, list):
            continue
        summary = summarize_task_failures(tasks, stage, top=getattr(args, "top", None) or 20)
        if (
            summary.get("failed_tasks")
            or summary.get("killed_tasks")
            or summary.get("classification") != "none"
            or getattr(args, "include_successful_retries", False)
        ):
            groups.append(summary)
    return groups


def stage_task_list_path(app_id: str, stage: dict[str, Any]) -> str:
    """Build a taskList path large enough to include failed tasks beyond page one."""
    stage_id = stage.get("stageId")
    attempt_id = first_present(stage.get("attemptId"), 0)
    length = stage_task_list_length(stage)
    return app_path(app_id, f"stages/{stage_id}/{attempt_id}/taskList?length={length}")


def stage_task_list_length(stage: dict[str, Any]) -> int:
    """Estimate taskList length needed for a complete stage attempt scan."""
    candidates = [
        safe_int(stage.get("numTasks")),
        safe_int(stage.get("numCompleteTasks")),
        safe_int(stage.get("numCompletedTasks")),
        safe_int(stage.get("numCompletedIndices")),
    ]
    base = max([value for value in candidates if value is not None] or [20])
    failed = safe_int(stage.get("numFailedTasks")) or 0
    killed = safe_int(stage.get("numKilledTasks")) or 0
    return max(20, base + failed + killed + 20)


async def fetch_stage_tasks_for_args(
    client: SparkClient,
    app_id: str,
    parsed: ParsedUrl,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Fetch one stage's complete taskList using stage summary row counts."""
    stages = await client.get_json(app_path(app_id, "stages"))
    selected = filter_stage_rows_for_args(stages, parsed, args, require_stage=True)
    if not selected:
        raise SparkDiagError("No stage matched the requested --stage-id/--attempt-id")
    stage = selected[0]
    tasks = await client.get_json(stage_task_list_path(app_id, stage))
    if not isinstance(tasks, list):
        raise SparkDiagError(f"Stage {stage.get('stageId')} taskList was unavailable")
    return stage, tasks


async def command_stages(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle stages command."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None) or "list"
    if sub == "retries":
        stages = await client.get_json(app_path(app_id, "stages"))
        selected = filter_stage_rows_for_args(stages, parsed, args, require_stage=getattr(args, "command", None) == "stage")
        jobs = await client.get_json(app_path(app_id, "jobs"), allow_error=True)
        if not isinstance(jobs, list):
            jobs = []
        return {"app_id": app_id, **summarize_stage_retries(selected, jobs, top=getattr(args, "top", None) or 20)}
    if sub == "failures":
        stages = await client.get_json(app_path(app_id, "stages"))
        jobs = await client.get_json(app_path(app_id, "jobs"), allow_error=True)
        if not isinstance(jobs, list):
            jobs = []
        selected = select_stages_for_failure_scan(stages, parsed, args)
        task_failures = await collect_stage_task_failure_groups(client, app_id, selected, args)
        attempts_by_stage_id = group_stage_attempts(stages)
        stage_failures = [
            impact
            for impact in (classify_stage_failure(stage, attempts_by_stage_id, jobs) for stage in selected)
            if impact.get("classification") != "none"
        ]
        return {
            "app_id": app_id,
            "scanned_stage_count": len(selected),
            "stage_failures": stage_failures[: getattr(args, "top", None) or 20],
            "task_failures": task_failures,
            "diagnosis": build_failure_diagnosis(stages, jobs, task_failures),
        }
    if sub == "wall-time":
        stages = await client.get_json(app_path(app_id, "stages"))
        return {"app_id": app_id, **summarize_stage_wall_times(stages, top=getattr(args, "top", None) or 10)}
    if sub == "tasks":
        stage_id = require_int(first_present(getattr(args, "stage_id", None), parsed.stage_id), "stage_id")
        attempt_id = require_int(first_present(getattr(args, "attempt_id", None), parsed.attempt_id, 0), "attempt_id")
        _, tasks = await fetch_stage_tasks_for_args(client, app_id, parsed, args)
        summary = summarize_stage_tasks(tasks, brief=getattr(args, "brief", False), limit=getattr(args, "limit", None) or 10)
        summary.update({"app_id": app_id, "stage_id": stage_id, "attempt_id": attempt_id})
        if not getattr(args, "brief", False) and getattr(args, "limit", None):
            summary["tasks"] = summary["tasks"][: args.limit]
        return summary
    if sub == "show":
        stage_id = require_int(first_present(getattr(args, "stage_id", None), parsed.stage_id), "stage_id")
        data = await client.get_json(app_path(app_id, f"stages/{stage_id}"), allow_error=True)
        return data
    stages = await client.get_json(app_path(app_id, "stages"))
    status = getattr(args, "status", None)
    if status:
        stages = [stage for stage in stages if str(stage.get("status", "")).upper() == status.upper()]
    if sub == "shuffle":
        summary = summarize_stage_shuffle(stages)
        limit = getattr(args, "limit", None)
        if limit:
            summary["top_shuffle"] = summary["top_shuffle"][:limit]
            summary["stages"] = summary["stages"][:limit]
        return summary
    if sub == "io":
        if getattr(args, "by_executor", False):
            stage_id = require_int(first_present(getattr(args, "stage_id", None), parsed.stage_id), "stage_id")
            attempt_id = require_int(first_present(getattr(args, "attempt_id", None), parsed.attempt_id, 0), "attempt_id")
            stage, tasks = await fetch_stage_tasks_for_args(client, app_id, parsed, args)
            summary = summarize_stage_executor_io(stage, tasks, top=getattr(args, "limit", None) or 20)
            summary.update({"app_id": app_id, "stage_id": stage_id, "attempt_id": attempt_id})
            return summary
        summary = summarize_stages_io(stages)
        limit = getattr(args, "limit", None)
        if limit:
            summary["top_input"] = summary["top_input"][:limit]
            summary["top_output"] = summary["top_output"][:limit]
            summary["top_shuffle"] = summary["top_shuffle"][:limit]
            summary["stages"] = summary["stages"][:limit]
        return summary
    return summarize_stages(stages)


async def command_stage(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle singular stage command aliases."""
    return await command_stages(client, parsed, args)


async def command_executors(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle executors command."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None) or "list"
    endpoint = "allexecutors" if getattr(args, "all", False) or sub == "churn" else "executors"
    executors = await client.get_json(app_path(app_id, endpoint), allow_error=(endpoint == "allexecutors"))
    if not isinstance(executors, list):
        executors = await client.get_json(app_path(app_id, "executors"))
    executor_id = str(getattr(args, "executor_id", None) or parsed.executor_id or "")
    if sub == "show":
        if not executor_id:
            raise SparkDiagError("Missing --executor-id")
        matches = [executor for executor in executors if str(executor.get("id")) == executor_id]
        return matches[0] if matches else {"available": False, "executor_id": executor_id}
    summary = summarize_executors(executors)
    if sub == "top":
        return {
            "top_gc": summary["top_gc"][: getattr(args, "limit", 10)],
            "top_memory": summary["top_memory"][: getattr(args, "limit", 10)],
            "top_shuffle_read": summary["top_shuffle_read"][: getattr(args, "limit", 10)],
        }
    if sub == "gc":
        result = summarize_executor_gc(executors)
        limit = getattr(args, "limit", None)
        if limit:
            result["top_gc"] = result["top_gc"][:limit]
            result["executors"] = result["executors"][:limit]
        return result
    if sub == "health":
        selected = [executor for executor in executors if not executor_id or str(executor.get("id")) == executor_id]
        return {"app_id": app_id, **summarize_executor_health(selected, top=getattr(args, "top", None) or 10)}
    if sub == "churn":
        stages = await client.get_json(app_path(app_id, "stages"))
        stage_id = first_present(getattr(args, "stage_id", None), parsed.stage_id)
        attempt_id = first_present(getattr(args, "attempt_id", None), parsed.attempt_id)
        return {
            "app_id": app_id,
            **summarize_executor_churn(
                executors,
                stages,
                stage_id=safe_int(stage_id),
                attempt_id=safe_int(attempt_id) if attempt_id is not None else None,
                top=getattr(args, "top", None) or 20,
            ),
        }
    if sub in {"failed-tasks", "tasks"}:
        if sub == "tasks" and not executor_id:
            raise SparkDiagError("Missing --executor-id")
        stages = await client.get_json(app_path(app_id, "stages"))
        task_rows = await collect_executor_tasks(client, app_id, stages, args)
        if executor_id:
            task_rows = [task for task in task_rows if str(task.get("executorId")) == executor_id]
        task_health = summarize_executor_task_health(task_rows, executors, top=getattr(args, "top", None) or 10)
        executor_health = summarize_executor_health(executors, top=getattr(args, "top", None) or 10)
        result = {
            "app_id": app_id,
            "executor_id": executor_id or None,
            "executor_health": executor_health,
            "task_health": task_health,
            "diagnosis": build_task_health_diagnosis(executor_health, task_health),
        }
        if getattr(args, "include_logs", False):
            result["logs"] = await scan_executor_logs(client, executors, executor_id=executor_id, args=args)
        return result
    return summary


async def command_executor(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle singular executor command aliases."""
    return await command_executors(client, parsed, args)


async def fetch_sql_rows(client: SparkClient, app_id: str, *, limit: int | None = None) -> Any:
    """Fetch SQL execution rows, optionally expanding Spark's default page length."""
    path = app_path(app_id, "sql")
    if limit:
        path = f"{path}?{urlencode({'length': limit})}"
    return await client.get_json(path, allow_error=True)


async def fetch_sql_failed_html(client: SparkClient, parsed: ParsedUrl, app_id: str, *, limit: int | None = None) -> dict[str, Any]:
    """Fetch and parse the Spark SQL HTML page for failed execution errors."""
    path = sql_failed_html_path(parsed, app_id, limit=limit)
    html = await client.get_text_status(path)
    if not html.get("available"):
        return {"available": False, "status_code": html.get("status_code"), "path": path, "rows": []}
    rows = parse_sql_failed_html(str(html.get("text") or ""))
    return {"available": True, "status_code": html.get("status_code"), "path": path, "row_count": len(rows), "rows": rows}


def sql_failed_html_path(parsed: ParsedUrl, app_id: str, *, limit: int | None = None) -> str:
    """Build the Spark SQL HTML page path used for failed execution tables."""
    query = {"failed.pageSize": limit} if limit else {}
    suffix = f"?{urlencode(query)}" if query else ""
    if parsed.ui_kind == "history":
        return f"history/{app_id}/SQL/{suffix}"
    return f"SQL/{suffix}"


async def command_sql(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle SQL command."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None)
    rows = await fetch_sql_rows(client, app_id, limit=getattr(args, "limit", None))
    if not isinstance(rows, list):
        return rows
    if sub in {"failures", "ddl-summary"}:
        html = await fetch_sql_failed_html(client, parsed, app_id, limit=getattr(args, "limit", None))
        html_rows = html.get("rows", []) if html.get("available") else []
        summary = (
            summarize_sql_failures(rows, html_errors=html_rows, top=getattr(args, "top", None) or 20)
            if sub == "failures"
            else summarize_ddl_failures(rows, html_errors=html_rows, top=getattr(args, "top", None) or 20)
        )
        summary["app_id"] = app_id
        summary["html_errors"] = {key: value for key, value in html.items() if key != "rows"}
        return summary
    if sub == "analyze":
        jobs, stages, executors = await asyncio.gather(
            client.get_json(app_path(app_id, "jobs")),
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "executors")),
        )
        return analyze_sql_performance(rows, jobs, stages, executors)
    if sub == "plan" and getattr(args, "operators", False):
        return summarize_sql_operators(rows)
    sql_id = first_present(getattr(args, "sql_id", None), parsed.sql_id)
    if sql_id is not None:
        matches = [row for row in rows if int(row.get("id", -1)) == int(sql_id)]
        return summarize_sql_row(matches[0]) if matches else {"available": False, "sql_id": sql_id}
    return summarize_sql(rows)


async def command_environment(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle environment command."""
    app_id = await resolve_app_id(client, parsed, args)
    data = await client.get_json(app_path(app_id, "environment"))
    if getattr(args, "subcommand", None) == "get":
        key = getattr(args, "key", None)
        if not key:
            raise SparkDiagError("Missing --key. Example: environment get --key 'Scala Version'")
        return get_environment_value(data, key)
    return redact_sensitive(data)


async def command_storage(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle storage command."""
    app_id = await resolve_app_id(client, parsed, args)
    return await client.get_json(app_path(app_id, "storage/rdd"), allow_error=True)


async def command_logs(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle log scan command from executor log URLs when available."""
    app_id = await resolve_app_id(client, parsed, args)
    executors = await client.get_json(app_path(app_id, "executors"))
    executor_id = str(getattr(args, "executor_id", None) or parsed.executor_id or "")
    candidates = [executor for executor in executors if not executor_id or str(executor.get("id")) == executor_id]
    if not candidates:
        return {"available": False, "reason": "No executor matched"}
    limit = getattr(args, "limit", 3)
    results = []
    for executor in candidates[:limit]:
        logs = executor.get("executorLogs", {}) or {}
        log_url = logs.get("stderr") or logs.get("stdout")
        if not log_url:
            results.append({"executor_id": executor.get("id"), "available": False, "reason": "No executor log URL exposed"})
            continue
        try:
            response = await client._client.get(log_url)
            text = response.text[-getattr(args, "tail_bytes", 65536) :]
            results.append({"executor_id": executor.get("id"), "available": response.status_code < 400, "status_code": response.status_code, "scan": scan_log_text(text, split_csv(getattr(args, "patterns", None)))})
        except Exception as exc:
            results.append({"executor_id": executor.get("id"), "available": False, "error": str(exc)})
    return {"app_id": app_id, "logs": results}


async def command_health(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle health command."""
    app_id = await resolve_app_id(client, parsed, args)
    app, jobs, stages, executors, sql_rows = await asyncio.gather(
        client.get_json(app_path(app_id)),
        client.get_json(app_path(app_id, "jobs")),
        client.get_json(app_path(app_id, "stages")),
        client.get_json(app_path(app_id, "executors")),
        client.get_json(app_path(app_id, "sql"), allow_error=True),
    )
    parts = {
        "app": summarize_app(app),
        "jobs": summarize_jobs(jobs),
        "stages": summarize_stages(stages),
        "executors": summarize_executors(executors),
        "sql": summarize_sql(sql_rows) if isinstance(sql_rows, list) else {"available": False},
    }
    return build_health_report(parts)


async def command_skew(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle skew command."""
    app_id = await resolve_app_id(client, parsed, args)
    if getattr(args, "subcommand", None) == "duration":
        stage_id = require_int(first_present(getattr(args, "stage_id", None), parsed.stage_id), "stage_id")
        attempt_id = require_int(first_present(getattr(args, "attempt_id", None), parsed.attempt_id, 0), "attempt_id")
        _, tasks = await fetch_stage_tasks_for_args(client, app_id, parsed, args)
        return {
            "app_id": app_id,
            "stage_id": stage_id,
            "attempt_id": attempt_id,
            "duration": summarize_task_duration_distribution(
                tasks,
                top=getattr(args, "top", None) or 10,
                include_failed_attempts=getattr(args, "include_failed_attempts", False),
            ),
        }
    stage_id = first_present(getattr(args, "stage_id", None), parsed.stage_id)
    if stage_id is not None:
        attempt_id = first_present(getattr(args, "attempt_id", None), parsed.attempt_id, 0)
        _, tasks = await fetch_stage_tasks_for_args(client, app_id, parsed, args)
        return {"app_id": app_id, "stage_id": stage_id, "attempt_id": attempt_id, "skew": summarize_task_skew(tasks)}
    stages = await client.get_json(app_path(app_id, "stages"))
    ranked = sorted([summarize_stage(stage) for stage in stages], key=lambda item: float(item.get("executorRunTime") or 0), reverse=True)
    return {"app_id": app_id, "top_stages": ranked[: getattr(args, "limit", 10)]}


async def command_duration(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle duration distribution commands."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None)
    top = getattr(args, "top", None) or 10
    if sub == "jobs":
        jobs = await client.get_json(app_path(app_id, "jobs"))
        return {"app_id": app_id, **summarize_job_durations(jobs, top=top)}
    if sub == "stages":
        stages = await client.get_json(app_path(app_id, "stages"))
        return {"app_id": app_id, **summarize_stage_durations(stages, top=top)}
    if sub == "tasks":
        if getattr(args, "all_stages", False):
            stages = await client.get_json(app_path(app_id, "stages"))
            selected = sorted(stages, key=lambda stage: int(stage.get("executorRunTime") or 0), reverse=True)[: getattr(args, "limit_stages", None) or 20]
            task_results = await asyncio.gather(
                *[
                    client.get_json(stage_task_list_path(app_id, stage), allow_error=True)
                    for stage in selected
                    if stage.get("stageId") is not None
                ]
            )
            per_stage = []
            for stage, tasks in zip(selected, task_results):
                if isinstance(tasks, list):
                    per_stage.append(
                        {
                            "stage_id": stage.get("stageId"),
                            "attempt_id": first_present(stage.get("attemptId"), 0),
                            "stage_duration": millis_human(stage.get("executorRunTime")),
                            "tasks": summarize_task_duration_distribution(
                                tasks,
                                top=top,
                                include_failed_attempts=getattr(args, "include_failed_attempts", False),
                            ),
                        }
                    )
            return {"app_id": app_id, "stage_count": len(per_stage), "stages": per_stage}
        stage_id = require_int(first_present(getattr(args, "stage_id", None), parsed.stage_id), "stage_id")
        attempt_id = require_int(first_present(getattr(args, "attempt_id", None), parsed.attempt_id, 0), "attempt_id")
        _, tasks = await fetch_stage_tasks_for_args(client, app_id, parsed, args)
        return {
            "app_id": app_id,
            "stage_id": stage_id,
            "attempt_id": attempt_id,
            **summarize_task_duration_distribution(
                tasks,
                top=top,
                include_failed_attempts=getattr(args, "include_failed_attempts", False),
            ),
        }
    raise SparkDiagError(f"Unsupported duration subcommand: {sub}")


async def collect_executor_tasks(client: SparkClient, app_id: str, stages: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    """Fetch taskList payloads for selected stages used by task health checks."""
    selected = select_stages_for_task_health(stages, args)
    task_results = await asyncio.gather(
        *[
            client.get_json(stage_task_list_path(app_id, stage), allow_error=True)
            for stage in selected
            if stage.get("stageId") is not None
        ]
    )
    tasks: list[dict[str, Any]] = []
    for stage, result in zip(selected, task_results):
        if not isinstance(result, list):
            continue
        for task in result:
            if isinstance(task, dict):
                row = dict(task)
                row.setdefault("stageId", stage.get("stageId"))
                row.setdefault("attemptId", first_present(stage.get("attemptId"), 0))
                tasks.append(row)
    return tasks


def select_stages_for_task_health(stages: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    """Select stages for task health scans with bounded default cost."""
    limit = getattr(args, "limit_stages", None) or 20
    if getattr(args, "all_stages", False):
        return sorted(stages, key=lambda stage: int(stage.get("executorRunTime") or 0), reverse=True)[:limit]
    failed = [stage for stage in stages if str(stage.get("status", "")).upper() == "FAILED" or int(stage.get("numFailedTasks", 0) or 0) > 0]
    slow = sorted(stages, key=lambda stage: int(stage.get("executorRunTime") or 0), reverse=True)
    selected: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any]] = set()
    for stage in failed + slow:
        key = (stage.get("stageId"), first_present(stage.get("attemptId"), 0))
        if key in seen:
            continue
        selected.append(stage)
        seen.add(key)
        if len(selected) >= limit:
            break
    return selected


async def scan_executor_logs(client: SparkClient, executors: list[dict[str, Any]], *, executor_id: str, args: argparse.Namespace) -> list[dict[str, Any]]:
    """Scan logs for selected executors when explicitly requested."""
    selected = [executor for executor in executors if not executor_id or str(executor.get("id")) == executor_id]
    results = []
    for executor in selected[: getattr(args, "top", None) or 10]:
        logs = executor.get("executorLogs", {}) or {}
        log_url = logs.get("stderr") or logs.get("stdout")
        if not log_url:
            results.append({"executor_id": executor.get("id"), "available": False, "reason": "No executor log URL exposed"})
            continue
        response = await client._client.get(log_url)
        text = response.text[-getattr(args, "tail_bytes", 65536) :]
        results.append({"executor_id": executor.get("id"), "available": response.status_code < 400, "scan": scan_log_text(text, split_csv(getattr(args, "patterns", None)))})
    return results


async def command_diagnose(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle diagnose playbooks."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None)
    if sub == "speed":
        app, jobs, stages, executors, all_executors, sql_rows = await asyncio.gather(
            client.get_json(app_path(app_id)),
            client.get_json(app_path(app_id, "jobs")),
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "executors")),
            client.get_json(app_path(app_id, "allexecutors"), allow_error=True),
            fetch_sql_rows(client, app_id, limit=1000),
        )
        executor_rows = all_executors if isinstance(all_executors, list) else executors
        top_wall = summarize_stage_wall_times(stages, top=1).get("top_wall_time_stages") or []
        top_stage_ids = {(item.get("stageId"), item.get("attemptId")) for item in top_wall}
        selected = [
            stage
            for stage in stages
            if (stage.get("stageId"), first_present(stage.get("attemptId"), 0)) in top_stage_ids
            or int(stage.get("numFailedTasks", 0) or 0) > 0
        ][: getattr(args, "limit", None) or 5]
        task_failures = await collect_stage_task_failure_groups(client, app_id, selected, args)
        return build_speed_diagnosis(
            jobs,
            stages,
            executor_rows,
            sql_rows if isinstance(sql_rows, list) else [],
            app=app,
            task_failure_groups=task_failures,
            limit=getattr(args, "limit", None) or 5,
        )
    if sub == "long-app":
        app, jobs, sql_rows = await asyncio.gather(
            client.get_json(app_path(app_id)),
            client.get_json(app_path(app_id, "jobs")),
            fetch_sql_rows(client, app_id, limit=getattr(args, "limit", None) or 1000),
        )
        return classify_long_app_runtime(app, jobs, sql_rows=sql_rows if isinstance(sql_rows, list) else [], top=getattr(args, "top", None) or 20)
    if sub == "task-health":
        stages, executors = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "allexecutors" if getattr(args, "all", False) else "executors"), allow_error=True),
        )
        if not isinstance(executors, list):
            executors = await client.get_json(app_path(app_id, "executors"))
        tasks = await collect_executor_tasks(client, app_id, stages, args)
        executor_health = summarize_executor_health(executors, top=getattr(args, "top", None) or 10)
        task_health = summarize_executor_task_health(tasks, executors, top=getattr(args, "top", None) or 10)
        result = {
            "app_id": app_id,
            "executor_health": executor_health,
            "task_health": task_health,
            "diagnosis": build_task_health_diagnosis(executor_health, task_health),
        }
        if getattr(args, "include_logs", False):
            result["logs"] = await scan_executor_logs(client, executors, executor_id="", args=args)
        return result
    if sub == "executor-loss":
        stages, jobs, executors = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "jobs"), allow_error=True),
            client.get_json(app_path(app_id, "allexecutors"), allow_error=True),
        )
        if not isinstance(jobs, list):
            jobs = []
        if not isinstance(executors, list):
            executors = await client.get_json(app_path(app_id, "executors"))
        selected = filter_stage_rows_for_args(stages, parsed, args, require_stage=True)
        if not selected:
            raise SparkDiagError("No stage matched the requested --stage-id/--attempt-id")
        task_results = await asyncio.gather(*[client.get_json(stage_task_list_path(app_id, stage), allow_error=True) for stage in selected])
        tasks: list[dict[str, Any]] = []
        for stage, result in zip(selected, task_results):
            if not isinstance(result, list):
                continue
            for task in result:
                if isinstance(task, dict):
                    row = dict(task)
                    row.setdefault("stageId", stage.get("stageId"))
                    row.setdefault("attemptId", first_present(stage.get("attemptId"), 0))
                    tasks.append(row)
        stage_id = first_present(getattr(args, "stage_id", None), parsed.stage_id)
        attempt_id = first_present(getattr(args, "attempt_id", None), parsed.attempt_id)
        return {
            "app_id": app_id,
            "scanned_stage_count": len(selected),
            "diagnosis": build_executor_loss_diagnosis(
                stages,
                executors,
                tasks,
                stage_id=safe_int(stage_id),
                attempt_id=safe_int(attempt_id) if attempt_id is not None else None,
                jobs=jobs,
                top=getattr(args, "top", None) or 20,
            ),
        }
    if sub == "input-distribution":
        stages, executors, environment = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "allexecutors"), allow_error=True),
            client.get_json(app_path(app_id, "environment"), allow_error=True),
        )
        if not isinstance(executors, list):
            executors = await client.get_json(app_path(app_id, "executors"))
        return {
            "app_id": app_id,
            "diagnosis": build_input_distribution_diagnosis(
                stages,
                executors,
                environment=environment if isinstance(environment, dict) else None,
                top=getattr(args, "top", None) or 10,
            ),
        }
    if sub == "parallelism":
        stages, executors, environment = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "allexecutors"), allow_error=True),
            client.get_json(app_path(app_id, "environment"), allow_error=True),
        )
        if not isinstance(executors, list):
            executors = await client.get_json(app_path(app_id, "executors"))
        return {
            "app_id": app_id,
            "diagnosis": build_parallelism_diagnosis(
                stages,
                executors,
                environment=environment if isinstance(environment, dict) else None,
                top=getattr(args, "top", None) or 10,
            ),
        }
    if sub == "retries":
        stages, jobs = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "jobs"), allow_error=True),
        )
        if not isinstance(jobs, list):
            jobs = []
        retry_summary = summarize_stage_retries(stages, jobs, top=getattr(args, "top", None) or 20)
        return {
            "app_id": app_id,
            "retries": retry_summary,
            "diagnosis": build_retry_diagnosis(stages, jobs, retry_summary),
        }
    if sub == "failures":
        stages, jobs = await asyncio.gather(
            client.get_json(app_path(app_id, "stages")),
            client.get_json(app_path(app_id, "jobs"), allow_error=True),
        )
        if not isinstance(jobs, list):
            jobs = []
        selected = select_stages_for_failure_scan(stages, parsed, args)
        task_failures = await collect_stage_task_failure_groups(client, app_id, selected, args)
        attempts_by_stage_id = group_stage_attempts(stages)
        stage_failures = [
            impact
            for impact in (classify_stage_failure(stage, attempts_by_stage_id, jobs) for stage in selected)
            if impact.get("classification") != "none"
        ]
        return {
            "app_id": app_id,
            "scanned_stage_count": len(selected),
            "stage_failures": stage_failures[: getattr(args, "top", None) or 20],
            "task_failures": task_failures,
            "diagnosis": build_failure_diagnosis(stages, jobs, task_failures),
        }
    raise SparkDiagError(f"Unsupported diagnose subcommand: {sub}")


async def command_timeline(client: SparkClient, parsed: ParsedUrl, args: argparse.Namespace) -> Any:
    """Handle timeline commands."""
    app_id = await resolve_app_id(client, parsed, args)
    sub = getattr(args, "subcommand", None)
    if sub != "events":
        raise SparkDiagError(f"Unsupported timeline subcommand: {sub}")
    sql_rows, jobs, stages, executors = await asyncio.gather(
        client.get_json(app_path(app_id, "sql"), allow_error=True),
        client.get_json(app_path(app_id, "jobs")),
        client.get_json(app_path(app_id, "stages")),
        client.get_json(app_path(app_id, "allexecutors"), allow_error=True),
    )
    if not isinstance(executors, list):
        executors = await client.get_json(app_path(app_id, "executors"))
    return build_event_timeline(
        sql_rows if isinstance(sql_rows, list) else [],
        jobs,
        stages,
        executors,
        sql_id=getattr(args, "sql_id", None),
        limit=getattr(args, "limit", None) or 100,
    )


def require_int(value: Any, name: str) -> int:
    """Return a required integer value."""
    parsed = safe_int(value)
    if parsed is None:
        raise SparkDiagError(f"Missing or invalid {name}")
    return parsed


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common CLI arguments to a parser."""
    parser.add_argument("--url", default=None, help="Spark WebUI or History Server URL. Defaults to SPARK_WEB_URL.")
    parser.add_argument("--base-url", default=None, help="REST base URL override.")
    parser.add_argument("--origin", default=None, help="Cookie origin override.")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification.")
    parser.add_argument("--no-cookies", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=8, help="Maximum concurrent HTTP requests.")
    parser.add_argument("--retries", type=int, default=1, help="Retry count for transient GET failures.")
    parser.add_argument("--spark-version", default="auto", help="Spark version or auto.")
    parser.add_argument("--endpoint-profile", default="auto", help="Endpoint profile or auto.")
    parser.add_argument("--app-id", default=None, help="Spark application id.")
    parser.add_argument("--app-name", default=None, help="Filter applications by name substring.")
    parser.add_argument("--app-index", type=int, default=None, help="Select app by index after filtering.")
    parser.add_argument("--completed", default=None, help="Filter history applications by completed true/false.")
    parser.add_argument("--job-id", type=int, default=None, help="Spark job id.")
    parser.add_argument("--stage-id", type=int, default=None, help="Spark stage id.")
    parser.add_argument("--attempt-id", type=int, default=None, help="Spark stage attempt id.")
    parser.add_argument("--executor-id", default=None, help="Spark executor id.")
    parser.add_argument("--sql-id", type=int, default=None, help="Spark SQL execution id.")
    parser.add_argument("--limit", type=int, default=None, help="Limit rows returned.")
    parser.add_argument("--brief", action="store_true", help="Emit a compact summary for fast statistics.")
    parser.add_argument("--verbose", action="store_true", help="Emit full detail payloads when a command supports compact output.")
    parser.add_argument("--tail-bytes", type=int, default=65536, help="Tail bytes for logs.")
    parser.add_argument("--patterns", default=None, help="Comma-separated log scan patterns.")


def add_duration_args(parser: argparse.ArgumentParser) -> None:
    """Add duration diagnostic arguments to a parser."""
    parser.add_argument("--top", type=int, default=10, help="Number of fastest/slowest rows to return.")
    parser.add_argument("--all-stages", action="store_true", help="Fetch task duration summaries for top stages.")
    parser.add_argument("--limit-stages", type=int, default=20, help="Maximum stages to fetch when --all-stages is set.")
    parser.add_argument("--include-failed-attempts", action="store_true", help="Include failed/killed task attempts in duration skew math.")


def add_idle_gap_args(parser: argparse.ArgumentParser) -> None:
    """Add idle-gap diagnostic arguments to a parser."""
    parser.add_argument("--top", type=int, default=20, help="Number of largest idle gaps or samples to return.")


def add_task_health_args(parser: argparse.ArgumentParser) -> None:
    """Add task health diagnostic arguments to a parser."""
    parser.add_argument("--top", type=int, default=10, help="Number of top executor/task rows to return.")
    parser.add_argument("--all-stages", action="store_true", help="Fetch task lists for all selected stages.")
    parser.add_argument("--limit-stages", type=int, default=20, help="Maximum stages to scan for task health.")
    parser.add_argument("--include-logs", action="store_true", help="Also scan selected executor logs.")


def add_stage_retry_failure_args(parser: argparse.ArgumentParser) -> None:
    """Add stage retry and failure diagnostic arguments to a parser."""
    parser.add_argument("--top", type=int, default=20, help="Maximum retry, failure, or sample rows to return.")
    parser.add_argument("--all-stages", action="store_true", help="Scan top stages beyond failed stages.")
    parser.add_argument("--limit-stages", type=int, default=20, help="Maximum stage attempts to scan when taskList is needed.")
    parser.add_argument("--include-successful-retries", action="store_true", help="Include stage/task groups without failed tasks when scanning broadly.")


def build_parser() -> argparse.ArgumentParser:
    """Build the argparse parser."""
    parser = argparse.ArgumentParser(description="Diagnose Spark WebUI / Spark on Kubernetes through REST API and browser cookies.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("resolve", "overview", "applications", "environment", "storage", "health", "skew"):
        sub = subparsers.add_parser(name)
        add_common_args(sub)
    applications = subparsers.choices["applications"]
    applications.add_argument("--history", action="store_true", help="Hint that URL is a History Server URL.")

    skew = subparsers.choices["skew"]
    skew_sub = skew.add_subparsers(dest="subcommand")
    skew_duration = skew_sub.add_parser("duration")
    add_common_args(skew_duration)
    add_duration_args(skew_duration)

    history = subparsers.add_parser("history")
    add_common_args(history)
    history_sub = history.add_subparsers(dest="subcommand", required=True)
    history_apps = history_sub.add_parser("applications")
    add_common_args(history_apps)
    history_apps.add_argument("--history", action="store_true", default=True, help="Hint that URL is a History Server URL.")

    jobs = subparsers.add_parser("jobs")
    add_common_args(jobs)
    jobs.add_argument("--status", choices=["running", "completed", "succeeded", "failed", "terminal", "all"], default="all")
    jobs_sub = jobs.add_subparsers(dest="subcommand")
    jobs_timeline = jobs_sub.add_parser("timeline")
    add_common_args(jobs_timeline)
    jobs_timeline.add_argument("--status", choices=["running", "completed", "succeeded", "failed", "terminal", "all"], default="all")
    jobs_idle = jobs_sub.add_parser("idle-gaps")
    add_common_args(jobs_idle)
    jobs_idle.add_argument("--status", choices=["running", "completed", "succeeded", "failed", "terminal", "all"], default="all")
    add_idle_gap_args(jobs_idle)

    job = subparsers.add_parser("job")
    add_common_args(job)
    job_sub = job.add_subparsers(dest="subcommand", required=True)
    job_show = job_sub.add_parser("show")
    add_common_args(job_show)

    stages = subparsers.add_parser("stages")
    add_common_args(stages)
    stage_sub = stages.add_subparsers(dest="subcommand")
    for name in ("list", "show", "tasks", "shuffle", "io"):
        child = stage_sub.add_parser(name)
        add_common_args(child)
        child.add_argument("--status", default=None)
        if name == "io":
            child.add_argument("--by-executor", action="store_true", help="Fetch one stage taskList and group IO by executor.")
    stages_wall_time = stage_sub.add_parser("wall-time")
    add_common_args(stages_wall_time)
    add_idle_gap_args(stages_wall_time)
    for name in ("retries", "failures"):
        child = stage_sub.add_parser(name)
        add_common_args(child)
        add_stage_retry_failure_args(child)

    stage = subparsers.add_parser("stage")
    add_common_args(stage)
    stage_alias_sub = stage.add_subparsers(dest="subcommand", required=True)
    for name in ("show", "tasks", "shuffle", "io"):
        child = stage_alias_sub.add_parser(name)
        add_common_args(child)
        child.add_argument("--status", default=None)
        if name == "io":
            child.add_argument("--by-executor", action="store_true", help="Fetch one stage taskList and group IO by executor.")
    for name in ("retries", "failures"):
        child = stage_alias_sub.add_parser(name)
        add_common_args(child)
        add_stage_retry_failure_args(child)

    executors = subparsers.add_parser("executors")
    add_common_args(executors)
    executor_sub = executors.add_subparsers(dest="subcommand")
    for name in ("list", "show", "top", "gc", "health", "failed-tasks", "churn"):
        child = executor_sub.add_parser(name)
        add_common_args(child)
        child.add_argument("--all", action="store_true", help="Use allexecutors endpoint.")
        if name in {"health", "failed-tasks"}:
            add_task_health_args(child)
        if name == "churn":
            add_idle_gap_args(child)

    executor = subparsers.add_parser("executor")
    add_common_args(executor)
    executor_alias_sub = executor.add_subparsers(dest="subcommand", required=True)
    for name in ("show", "top", "gc", "health", "tasks"):
        child = executor_alias_sub.add_parser(name)
        add_common_args(child)
        child.add_argument("--all", action="store_true", help="Use allexecutors endpoint.")
        if name in {"health", "tasks"}:
            add_task_health_args(child)

    sql = subparsers.add_parser("sql")
    add_common_args(sql)
    sql_sub = sql.add_subparsers(dest="subcommand")
    for name in ("list", "show", "plan", "analyze", "failures", "ddl-summary"):
        child = sql_sub.add_parser(name)
        add_common_args(child)
        if name == "plan":
            child.add_argument("--operators", action="store_true", help="Summarize physical plan operators.")
        if name in {"failures", "ddl-summary"}:
            add_idle_gap_args(child)

    env = subparsers.choices["environment"]
    env_sub = env.add_subparsers(dest="subcommand")
    env_get = env_sub.add_parser("get")
    add_common_args(env_get)
    env_get.add_argument("--key", required=True, help="Environment key or page label, e.g. 'Scala Version'.")

    diagnose = subparsers.add_parser("diagnose")
    add_common_args(diagnose)
    diagnose_sub = diagnose.add_subparsers(dest="subcommand", required=True)
    diagnose_speed = diagnose_sub.add_parser("speed")
    add_common_args(diagnose_speed)
    diagnose_long_app = diagnose_sub.add_parser("long-app")
    add_common_args(diagnose_long_app)
    add_idle_gap_args(diagnose_long_app)
    diagnose_task_health = diagnose_sub.add_parser("task-health")
    add_common_args(diagnose_task_health)
    diagnose_task_health.add_argument("--all", action="store_true", help="Use allexecutors endpoint.")
    add_task_health_args(diagnose_task_health)
    diagnose_executor_loss = diagnose_sub.add_parser("executor-loss")
    add_common_args(diagnose_executor_loss)
    add_stage_retry_failure_args(diagnose_executor_loss)
    diagnose_retries = diagnose_sub.add_parser("retries")
    add_common_args(diagnose_retries)
    add_stage_retry_failure_args(diagnose_retries)
    diagnose_failures = diagnose_sub.add_parser("failures")
    add_common_args(diagnose_failures)
    add_stage_retry_failure_args(diagnose_failures)
    diagnose_input_distribution = diagnose_sub.add_parser("input-distribution")
    add_common_args(diagnose_input_distribution)
    diagnose_input_distribution.add_argument("--top", type=int, default=10, help="Number of stage/executor evidence rows to return.")
    diagnose_parallelism = diagnose_sub.add_parser("parallelism")
    add_common_args(diagnose_parallelism)
    diagnose_parallelism.add_argument("--top", type=int, default=10, help="Number of stage parallelism rows to return.")

    timeline = subparsers.add_parser("timeline")
    add_common_args(timeline)
    timeline_sub = timeline.add_subparsers(dest="subcommand", required=True)
    timeline_events = timeline_sub.add_parser("events")
    add_common_args(timeline_events)

    duration = subparsers.add_parser("duration")
    add_common_args(duration)
    duration_sub = duration.add_subparsers(dest="subcommand", required=True)
    for name in ("jobs", "stages", "tasks"):
        child = duration_sub.add_parser(name)
        add_common_args(child)
        add_duration_args(child)

    logs = subparsers.add_parser("logs")
    add_common_args(logs)
    logs_sub = logs.add_subparsers(dest="subcommand", required=True)
    scan = logs_sub.add_parser("scan")
    add_common_args(scan)
    return parser


async def dispatch(args: argparse.Namespace) -> Any:
    """Dispatch parsed CLI args."""
    parsed = parse_web_url(getattr(args, "url", None), base_url=getattr(args, "base_url", None), origin=getattr(args, "origin", None))
    cookies = load_browser_cookies(parsed.origin, no_cookies=getattr(args, "no_cookies", False))
    async with SparkClient(
        parsed.base_url,
        cookies=cookies,
        timeout=getattr(args, "timeout", 10.0),
        verify=not getattr(args, "insecure", False),
        concurrency=getattr(args, "concurrency", 8),
        retries=getattr(args, "retries", 1),
    ) as client:
        command = args.command
        if command == "resolve":
            return await command_resolve(client, parsed, args)
        if command in {"overview"}:
            return await command_overview(client, parsed, args)
        if command == "applications" or (command == "history" and getattr(args, "subcommand", None) == "applications"):
            return await command_applications(client, args)
        if command == "jobs":
            return await command_jobs(client, parsed, args)
        if command == "job":
            return await command_job(client, parsed, args)
        if command == "stages":
            return await command_stages(client, parsed, args)
        if command == "stage":
            return await command_stage(client, parsed, args)
        if command == "executors":
            return await command_executors(client, parsed, args)
        if command == "executor":
            return await command_executor(client, parsed, args)
        if command == "sql":
            return await command_sql(client, parsed, args)
        if command == "environment":
            return await command_environment(client, parsed, args)
        if command == "storage":
            return await command_storage(client, parsed, args)
        if command == "logs":
            return await command_logs(client, parsed, args)
        if command == "health":
            return await command_health(client, parsed, args)
        if command == "skew":
            return await command_skew(client, parsed, args)
        if command == "diagnose":
            return await command_diagnose(client, parsed, args)
        if command == "timeline":
            return await command_timeline(client, parsed, args)
        if command == "duration":
            return await command_duration(client, parsed, args)
    raise SparkDiagError(f"Unsupported command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(dispatch(args))
        emit(result, as_json=getattr(args, "json", False))
        return 0
    except SparkDiagError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
