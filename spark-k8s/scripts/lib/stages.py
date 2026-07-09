"""Stage and task helper functions for Spark diagnostics."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any


def first_present(*values: Any) -> Any:
    """Return the first value that is not None."""
    for value in values:
        if value is not None:
            return value
    return None


def to_float(value: Any) -> float | None:
    """Convert a value to float if possible."""
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def numeric(value: Any) -> int | float | None:
    """Return a numeric value, preserving integers when possible."""
    number = to_float(value)
    if number is None:
        return None
    return int(number) if number.is_integer() else number


def bytes_human(value: Any) -> str | None:
    """Format bytes with binary units."""
    number = to_float(value)
    if number is None:
        return None
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    unit = 0
    while abs(number) >= 1024 and unit < len(units) - 1:
        number /= 1024
        unit += 1
    return f"{number:.2f} {units[unit]}"


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
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.2f} min"
    return f"{minutes / 60:.2f} h"


def ratio_human(numerator: Any, denominator: Any) -> str | None:
    """Format a ratio as a percentage."""
    num = to_float(numerator)
    den = to_float(denominator)
    if num is None or den is None or den == 0:
        return None
    return f"{num / den * 100:.2f}%"


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


def percentile(values: list[float], pct: float) -> float | None:
    """Calculate a percentile using linear interpolation."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


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
    }


def summarize_duration_distribution(rows: list[dict[str, Any]], value_key: str) -> dict[str, Any]:
    """Summarize min, max, average, and percentiles for duration-like values."""
    values = [to_float(row.get(value_key)) for row in rows]
    numbers = [value for value in values if value is not None]
    if not numbers:
        return empty_duration_distribution()
    p50 = percentile(numbers, 0.50)
    max_value = max(numbers)
    return {
        "count": len(numbers),
        "min_ms": numeric(min(numbers)),
        "max_ms": numeric(max_value),
        "avg_ms": round(sum(numbers) / len(numbers), 4),
        "p50_ms": numeric(p50),
        "p75_ms": numeric(percentile(numbers, 0.75)),
        "p90_ms": numeric(percentile(numbers, 0.90)),
        "p95_ms": numeric(percentile(numbers, 0.95)),
        "p99_ms": numeric(percentile(numbers, 0.99)),
        "max_vs_median": round(max_value / p50, 4) if p50 and p50 > 0 else None,
    }


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


def compact_stage_executor_io_row(executor_id: str, group: dict[str, Any]) -> dict[str, Any]:
    """Return compact per-executor task IO for one stage."""
    duration_ms = group.get("duration_ms", 0)
    input_bytes = group.get("input_bytes", 0)
    shuffle_read = group.get("shuffle_read_bytes", 0)
    shuffle_write = group.get("shuffle_write_bytes", 0)
    return {
        "executor_id": executor_id,
        "host": group.get("host"),
        "tasks": group.get("tasks", 0),
        "successful_tasks": group.get("successful_tasks", 0),
        "failed_tasks": group.get("failed_tasks", 0),
        "killed_tasks": group.get("killed_tasks", 0),
        "duration_ms": duration_ms,
        "duration": millis_human(duration_ms),
        "input_bytes": input_bytes,
        "input": bytes_human(input_bytes),
        "shuffle_read_bytes": shuffle_read,
        "shuffle_read": bytes_human(shuffle_read),
        "shuffle_write_bytes": shuffle_write,
        "shuffle_write": bytes_human(shuffle_write),
        "gc_ms": group.get("gc_ms", 0),
        "gc": millis_human(group.get("gc_ms", 0)),
    }


def summarize_stage_executor_io(stage: dict[str, Any], tasks: list[dict[str, Any]], *, top: int = 20) -> dict[str, Any]:
    """Summarize task input and shuffle metrics by executor for one stage."""
    groups: dict[str, dict[str, Any]] = {}
    for task in tasks:
        executor_id = str(task.get("executorId") or "unknown")
        group = groups.setdefault(
            executor_id,
            {
                "host": task.get("host"),
                "tasks": 0,
                "successful_tasks": 0,
                "failed_tasks": 0,
                "killed_tasks": 0,
                "duration_ms": 0,
                "input_bytes": 0,
                "shuffle_read_bytes": 0,
                "shuffle_write_bytes": 0,
                "gc_ms": 0,
            },
        )
        row = compact_task_duration_row(task)
        status = str(first_present(task.get("status"), task.get("taskStatus"), get_nested(task, "taskInfo.status")) or "UNKNOWN").upper()
        group["tasks"] += 1
        group["successful_tasks"] += 1 if status in {"SUCCESS", "SUCCEEDED"} else 0
        group["failed_tasks"] += 1 if status == "FAILED" else 0
        group["killed_tasks"] += 1 if status == "KILLED" else 0
        group["duration_ms"] += int(row.get("duration_ms") or 0)
        group["input_bytes"] += int(row.get("input_bytes") or 0)
        group["shuffle_read_bytes"] += int(row.get("shuffle_read_bytes") or 0)
        group["shuffle_write_bytes"] += int(row.get("shuffle_write_bytes") or 0)
        group["gc_ms"] += int(row.get("gc_ms") or 0)
        if not group.get("host") and row.get("host"):
            group["host"] = row.get("host")
    rows = [compact_stage_executor_io_row(executor_id, group) for executor_id, group in groups.items()]
    rows = sorted(
        rows,
        key=lambda item: (
            int(item.get("input_bytes") or 0) + int(item.get("shuffle_read_bytes") or 0) + int(item.get("shuffle_write_bytes") or 0),
            int(item.get("duration_ms") or 0),
        ),
        reverse=True,
    )
    total_input = sum(int(row.get("input_bytes") or 0) for row in rows)
    total_shuffle_read = sum(int(row.get("shuffle_read_bytes") or 0) for row in rows)
    total_shuffle_write = sum(int(row.get("shuffle_write_bytes") or 0) for row in rows)
    return {
        "stage_id": stage.get("stageId"),
        "attempt_id": first_present(stage.get("attemptId"), 0),
        "stage": compact_stage_io_row(summarize_stage(stage)),
        "executor_count": len(rows),
        "task_count": len(tasks),
        "failed_tasks": sum(int(row.get("failed_tasks") or 0) for row in rows),
        "total_input_bytes": total_input,
        "total_shuffle_read_bytes": total_shuffle_read,
        "total_shuffle_write_bytes": total_shuffle_write,
        "total_input": bytes_human(total_input),
        "total_shuffle_read": bytes_human(total_shuffle_read),
        "total_shuffle_write": bytes_human(total_shuffle_write),
        "executors": rows[:top],
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


def classify_task_duration_skew(distribution: dict[str, Any], metric_skew: dict[str, Any]) -> dict[str, Any]:
    """Classify task duration skew using percentile and max/min task evidence."""
    base = classify_duration_skew(distribution)
    task_ratio = to_float(metric_skew.get("duration", {}).get("skew_ratio"))
    if task_ratio is not None and task_ratio >= 5:
        return {"level": "critical", "message": "Slowest task is at least 5x the fastest positive-duration task"}
    if task_ratio is not None and task_ratio >= 3 and base.get("level") == "none":
        return {"level": "warning", "message": "Slowest task is at least 3x the fastest positive-duration task"}
    return base


def is_failed_task_attempt(task: dict[str, Any]) -> bool:
    """Return true when a task attempt should be excluded from duration skew by default."""
    status = str(first_present(task.get("status"), task.get("taskStatus"), get_nested(task, "taskInfo.status")) or "").upper()
    return status in {"FAILED", "KILLED"}


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


def summarize_task_duration_distribution(
    tasks: list[dict[str, Any]],
    *,
    top: int = 10,
    include_failed_attempts: bool = False,
) -> dict[str, Any]:
    """Summarize task duration distribution with IO, GC, and spill evidence."""
    analyzed_tasks = tasks if include_failed_attempts else [task for task in tasks if not is_failed_task_attempt(task)]
    excluded_failed_attempts = len(tasks) - len(analyzed_tasks)
    rows = [compact_task_duration_row(task) for task in analyzed_tasks]
    distribution = summarize_duration_distribution(rows, "duration_ms")
    metric_skew = summarize_task_skew(analyzed_tasks)
    duration_skew = classify_task_duration_skew(distribution, metric_skew)
    recommendations = recommend_task_duration_actions(duration_skew, metric_skew, rows)
    return {
        "kind": "tasks",
        "task_count": len(tasks),
        "analyzed_task_count": len(rows),
        "excluded_failed_attempts": excluded_failed_attempts,
        "include_failed_attempts": include_failed_attempts,
        "distribution": distribution,
        "skew": duration_skew,
        "metric_skew": metric_skew,
        "top_slow_tasks": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0), reverse=True)[:top],
        "fastest_tasks": sorted(rows, key=lambda item: int(item.get("duration_ms") or 0))[:top],
        "recommendations": recommendations,
    }


def group_stage_attempts(stages: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    """Group stage rows by stage id and sort attempts deterministically."""
    groups: dict[int, list[dict[str, Any]]] = {}
    for stage in stages:
        stage_id = safe_int(stage.get("stageId"))
        if stage_id is None:
            continue
        groups.setdefault(stage_id, []).append(stage)
    for attempts in groups.values():
        attempts.sort(key=stage_attempt_sort_key)
    return groups


def stage_attempt_sort_key(stage: dict[str, Any]) -> tuple[int, str]:
    """Return a stable sort key for stage attempts."""
    attempt_id = safe_int(stage.get("attemptId"))
    return (attempt_id if attempt_id is not None else -1, str(first_present(stage.get("submissionTime"), stage.get("firstTaskLaunchedTime"), "")))


def safe_int(value: Any) -> int | None:
    """Convert a value to int if possible."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def format_spark_time(value: datetime | None) -> str | None:
    """Format a UTC timestamp with Spark's GMT suffix."""
    if value is None:
        return None
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "GMT"


def stage_time_bounds(stage: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    """Return stage start and end timestamps."""
    start = parse_spark_time(first_present(stage.get("submissionTime"), stage.get("firstTaskLaunchedTime")))
    end = parse_spark_time(stage.get("completionTime"))
    return start, end


def stage_wall_time_ms(stage: dict[str, Any]) -> int | None:
    """Calculate stage wall-clock duration from submission to completion."""
    start, end = stage_time_bounds(stage)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def compact_stage_wall_time_row(stage: dict[str, Any]) -> dict[str, Any]:
    """Return compact stage wall-time and executor-runtime fields."""
    wall_ms = stage_wall_time_ms(stage)
    executor_ms = numeric(stage.get("executorRunTime"))
    num_tasks = safe_int(stage.get("numTasks"))
    estimated_parallelism = None
    avg_task_duration_ms = None
    stage_throughput_tasks_per_sec = None
    if wall_ms and executor_ms is not None:
        estimated_parallelism = round(float(executor_ms) / wall_ms, 4)
    if num_tasks and num_tasks > 0 and executor_ms is not None:
        avg_task_duration_ms = numeric(float(executor_ms) / num_tasks)
    if wall_ms and num_tasks is not None:
        stage_throughput_tasks_per_sec = round(num_tasks / (wall_ms / 1000), 4)
    return {
        "stageId": stage.get("stageId"),
        "attemptId": first_present(stage.get("attemptId"), 0),
        "name": stage.get("name"),
        "status": stage.get("status"),
        "submissionTime": stage.get("submissionTime"),
        "completionTime": stage.get("completionTime"),
        "wall_duration_ms": wall_ms,
        "wall_duration": millis_human(wall_ms),
        "executorRunTime_ms": executor_ms,
        "executorRunTime": millis_human(executor_ms),
        "estimated_parallelism": estimated_parallelism,
        "numTasks": num_tasks,
        "avg_task_duration_ms": avg_task_duration_ms,
        "avg_task_duration": millis_human(avg_task_duration_ms),
        "stage_throughput_tasks_per_sec": stage_throughput_tasks_per_sec,
    }


def summarize_stage_wall_times(stages: list[dict[str, Any]], *, top: int = 10) -> dict[str, Any]:
    """Summarize stage wall-clock time independently from executor runtime."""
    rows = [compact_stage_wall_time_row(stage) for stage in stages]
    distribution = summarize_duration_distribution(rows, "wall_duration_ms")
    sortable = [row for row in rows if row.get("wall_duration_ms") is not None]
    return {
        "kind": "stage_wall_time",
        "stage_count": len(rows),
        "distribution": distribution,
        "top_wall_time_stages": sorted(sortable, key=lambda item: int(item.get("wall_duration_ms") or 0), reverse=True)[:top],
        "fastest_stages": sorted(sortable, key=lambda item: int(item.get("wall_duration_ms") or 0))[:top],
    }


def executor_time_bounds(executor: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    """Return executor add and remove timestamps."""
    start = parse_spark_time(first_present(executor.get("addTime"), executor.get("startTime")))
    end = parse_spark_time(first_present(executor.get("removeTime"), executor.get("endTime")))
    return start, end


def interval_overlap_ms(
    left_start: datetime | None,
    left_end: datetime | None,
    right_start: datetime | None,
    right_end: datetime | None,
) -> int | None:
    """Calculate overlap duration between two bounded intervals."""
    if left_start is None or left_end is None or right_start is None or right_end is None:
        return None
    start = max(left_start, right_start)
    end = min(left_end, right_end)
    if end < start:
        return 0
    return max(0, int((end - start).total_seconds() * 1000))


def time_in_interval(value: datetime | None, start: datetime | None, end: datetime | None) -> bool:
    """Return true when a timestamp is inside a bounded interval."""
    if value is None or start is None or end is None:
        return False
    return start <= value <= end


def executor_stage_relation(
    add_time: datetime | None,
    remove_time: datetime | None,
    stage_start: datetime | None,
    stage_end: datetime | None,
) -> str:
    """Classify how an executor lifecycle relates to a stage interval."""
    if stage_start is None or stage_end is None or add_time is None:
        return "unknown"
    effective_end = remove_time or stage_end
    if effective_end < stage_start:
        return "before_stage"
    if add_time > stage_end:
        return "after_stage"
    if remove_time is not None and stage_start <= remove_time <= stage_end:
        return "removed_during_stage"
    if stage_start <= add_time <= stage_end:
        return "added_during_stage"
    if add_time <= stage_start and effective_end >= stage_end:
        return "spans_stage"
    return "overlaps_stage"


def stage_row_for_id(stages: list[dict[str, Any]], stage_id: int | None, attempt_id: int | None = None) -> dict[str, Any] | None:
    """Find a stage row by stage id and optional attempt id."""
    if stage_id is None:
        return None
    for stage in stages:
        if safe_int(stage.get("stageId")) != stage_id:
            continue
        if attempt_id is not None and safe_int(first_present(stage.get("attemptId"), 0)) != attempt_id:
            continue
        return stage
    return None


def executor_host(executor: dict[str, Any]) -> str | None:
    """Extract a host name from an executor row."""
    host_port = first_present(executor.get("hostPort"), executor.get("host"))
    if host_port is None:
        return None
    return str(host_port).split(":", 1)[0]


def framework_deleted_reason(reason: Any) -> bool:
    """Return true for Spark framework/user executor delete reasons."""
    text = str(reason or "").lower()
    return "deleted by a user or the framework" in text or ("deleted" in text and "framework" in text)


def compact_executor_churn_row(
    executor: dict[str, Any],
    stage_start: datetime | None,
    stage_end: datetime | None,
) -> dict[str, Any]:
    """Return executor lifecycle fields used by churn diagnostics."""
    add_time, remove_time = executor_time_bounds(executor)
    lifetime_ms = interval_overlap_ms(add_time, remove_time, add_time, remove_time)
    stage_overlap_end = remove_time or stage_end
    stage_overlap_ms = interval_overlap_ms(add_time, stage_overlap_end, stage_start, stage_end)
    removed_during_stage = time_in_interval(remove_time, stage_start, stage_end)
    stage_relation = executor_stage_relation(add_time, remove_time, stage_start, stage_end)
    return {
        "id": executor.get("id"),
        "host": executor_host(executor),
        "hostPort": executor.get("hostPort"),
        "isActive": executor.get("isActive"),
        "addTime": format_spark_time(add_time),
        "removeTime": format_spark_time(remove_time),
        "removeReason": executor.get("removeReason"),
        "lifetime_ms": lifetime_ms,
        "lifetime": millis_human(lifetime_ms),
        "stage_overlap_ms": stage_overlap_ms,
        "stage_overlap": millis_human(stage_overlap_ms),
        "removed_during_stage": removed_during_stage,
        "stage_relation": stage_relation,
        "totalTasks": executor.get("totalTasks"),
        "failedTasks": executor.get("failedTasks"),
        "totalCores": executor.get("totalCores"),
    }


def build_executor_churn_timeline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build executor add/remove lifecycle events from compact churn rows."""
    events: list[dict[str, Any]] = []
    for row in rows:
        if row.get("addTime"):
            events.append({"event": "executor_added", "at": row.get("addTime"), "executor_id": row.get("id"), "host": row.get("host")})
        if row.get("removeTime"):
            events.append(
                {
                    "event": "executor_removed",
                    "at": row.get("removeTime"),
                    "executor_id": row.get("id"),
                    "host": row.get("host"),
                    "removeReason": row.get("removeReason"),
                    "removed_during_stage": row.get("removed_during_stage"),
                }
            )
    return sorted(events, key=lambda item: parse_spark_time(item.get("at")) or datetime.max.replace(tzinfo=timezone.utc))


def classify_executor_churn(rows: list[dict[str, Any]], removed_during_stage: int) -> str:
    """Classify executor churn from lifecycle and remove reason evidence."""
    removed_rows = [row for row in rows if row.get("removeTime")]
    if not removed_rows:
        return "unknown"
    if removed_during_stage and any(framework_deleted_reason(row.get("removeReason")) for row in removed_rows):
        return "framework_deleted_executors"
    host_counts = Counter(row.get("host") or "unknown" for row in removed_rows)
    if removed_rows and host_counts.most_common(1)[0][1] >= max(2, len(removed_rows) // 2 + 1):
        return "node_hotspot"
    if any(int(row.get("failedTasks") or 0) > 0 for row in removed_rows):
        return "executor_lost_recovered"
    return "unknown"


def summarize_executor_churn(
    executors: list[dict[str, Any]],
    stages: list[dict[str, Any]] | None = None,
    *,
    stage_id: int | None = None,
    attempt_id: int | None = None,
    top: int = 20,
) -> dict[str, Any]:
    """Summarize executor add/remove lifecycle and churn during a stage."""
    stage = stage_row_for_id(stages or [], stage_id, attempt_id)
    stage_start, stage_end = stage_time_bounds(stage) if stage else (None, None)
    rows = [compact_executor_churn_row(executor, stage_start, stage_end) for executor in executors]
    removed_rows = [row for row in rows if row.get("removeTime")]
    removed_during_stage = sum(1 for row in removed_rows if row.get("removed_during_stage"))
    reason_counts = Counter(str(row.get("removeReason") or "unknown") for row in removed_rows)
    host_counts = Counter(str(row.get("host") or "unknown") for row in removed_rows)
    relation_counts = Counter(str(row.get("stage_relation") or "unknown") for row in rows)
    return {
        "executor_count": len(executors),
        "active_executors": sum(1 for executor in executors if bool(executor.get("isActive"))),
        "removed_executors": len(removed_rows),
        "removed_during_stage": removed_during_stage,
        "stage_id": stage_id,
        "attempt_id": attempt_id,
        "stage": compact_stage_wall_time_row(stage) if stage else None,
        "stage_interval": {"start": format_spark_time(stage_start), "end": format_spark_time(stage_end), "duration_ms": stage_wall_time_ms(stage) if stage else None},
        "stage_relations": dict(relation_counts),
        "remove_reasons": dict(reason_counts.most_common(top)),
        "removed_hosts": dict(host_counts.most_common(top)),
        "classification": classify_executor_churn(rows, removed_during_stage),
        "timeline": build_executor_churn_timeline(rows)[:top],
        "executors": sorted(removed_rows, key=lambda item: int(item.get("stage_overlap_ms") or 0), reverse=True)[:top],
    }


def environment_spark_properties(environment: dict[str, Any] | None) -> dict[str, str]:
    """Extract Spark properties from an environment REST payload."""
    if not isinstance(environment, dict):
        return {}
    result: dict[str, str] = {}
    props = environment.get("sparkProperties")
    if isinstance(props, list):
        for item in props:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                result[str(item[0])] = str(item[1])
            elif isinstance(item, dict):
                key = item.get("name") or item.get("key")
                value = item.get("value")
                if key is not None and value is not None:
                    result[str(key)] = str(value)
    elif isinstance(props, dict):
        result.update({str(key): str(value) for key, value in props.items()})
    return result


def bool_spark_conf(props: dict[str, str], key: str) -> bool | None:
    """Parse a Spark boolean configuration value."""
    value = props.get(key)
    if value is None:
        return None
    lowered = value.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def int_spark_conf(props: dict[str, str], key: str) -> int | None:
    """Parse a Spark integer configuration value."""
    return safe_int(props.get(key))


def compact_executor_io_row(executor: dict[str, Any]) -> dict[str, Any]:
    """Return compact executor input and shuffle counters."""
    input_bytes = numeric(first_present(executor.get("totalInputBytes"), executor.get("inputBytes")))
    shuffle_read = numeric(first_present(executor.get("totalShuffleRead"), executor.get("shuffleReadBytes")))
    shuffle_write = numeric(first_present(executor.get("totalShuffleWrite"), executor.get("shuffleWriteBytes")))
    return {
        "id": executor.get("id"),
        "host": executor_host(executor),
        "isActive": executor.get("isActive"),
        "totalTasks": executor.get("totalTasks"),
        "failedTasks": executor.get("failedTasks"),
        "totalInputBytes": int(input_bytes or 0),
        "input": bytes_human(input_bytes or 0),
        "totalShuffleRead": int(shuffle_read or 0),
        "shuffleRead": bytes_human(shuffle_read or 0),
        "totalShuffleWrite": int(shuffle_write or 0),
        "shuffleWrite": bytes_human(shuffle_write or 0),
        "addTime": executor.get("addTime"),
        "removeTime": executor.get("removeTime"),
    }


def build_input_distribution_diagnosis(
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    *,
    environment: dict[str, Any] | None = None,
    top: int = 10,
) -> dict[str, Any]:
    """Diagnose executor input distribution versus shuffle-only later stages."""
    stage_rows = [compact_stage_io_row(summarize_stage(stage)) for stage in stages]
    executor_rows = [compact_executor_io_row(executor) for executor in executors if str(executor.get("id")) != "driver"]
    external_input_stages = [row for row in stage_rows if int(row.get("inputBytesValue") or 0) > 0]
    shuffle_read_stages = [row for row in stage_rows if int(row.get("inputBytesValue") or 0) == 0 and int(row.get("shuffleReadBytes") or 0) > 0]
    input_executors = [row for row in executor_rows if int(row.get("totalInputBytes") or 0) > 0]
    shuffle_read_only = [
        row
        for row in executor_rows
        if int(row.get("totalInputBytes") or 0) == 0 and int(row.get("totalShuffleRead") or 0) > 0
    ]
    props = environment_spark_properties(environment)
    classification = "balanced_or_unknown"
    if external_input_stages and shuffle_read_stages and shuffle_read_only:
        classification = "shuffle_only_later_stage"
    elif external_input_stages and len(input_executors) < len(executor_rows):
        classification = "input_stage_limited_executors"
    evidence = [
        f"external_input_stages={len(external_input_stages)}",
        f"shuffle_read_stages={len(shuffle_read_stages)}",
        f"executors_with_input={len(input_executors)}",
        f"executors_with_shuffle_read_only={len(shuffle_read_only)}",
    ]
    for key in ("spark.executor.instances", "spark.executor.cores", "spark.dynamicAllocation.enabled"):
        if key in props:
            evidence.append(f"{key}={props[key]}")
    recommendations: list[str] = []
    if classification == "shuffle_only_later_stage":
        recommendations.append("Executor Input only counts external source reads; inspect shuffle read/write for later reduce or write stages.")
    if props.get("spark.dynamicAllocation.enabled", "").lower() == "false" and int_spark_conf(props, "spark.executor.instances"):
        recommendations.append("Static executor allocation limits how many executors can participate in the first input stage.")
    if not recommendations:
        recommendations.append("Compare stage input/shuffle columns with executor input/shuffle columns before treating zero Input as idle executors.")
    return {
        "classification": classification,
        "stage_count": len(stage_rows),
        "executor_count": len(executor_rows),
        "total_input_bytes": sum(int(row.get("inputBytesValue") or 0) for row in stage_rows),
        "total_input": bytes_human(sum(int(row.get("inputBytesValue") or 0) for row in stage_rows)),
        "total_shuffle_read_bytes": sum(int(row.get("shuffleReadBytes") or 0) for row in stage_rows),
        "total_shuffle_read": bytes_human(sum(int(row.get("shuffleReadBytes") or 0) for row in stage_rows)),
        "executors_with_input": len(input_executors),
        "executors_with_shuffle_read_only": len(shuffle_read_only),
        "external_input_stages": sorted(external_input_stages, key=lambda item: int(item.get("inputBytesValue") or 0), reverse=True)[:top],
        "shuffle_read_stages": sorted(shuffle_read_stages, key=lambda item: int(item.get("shuffleReadBytes") or 0), reverse=True)[:top],
        "top_executor_input": sorted(executor_rows, key=lambda item: int(item.get("totalInputBytes") or 0), reverse=True)[:top],
        "top_executor_shuffle_read": sorted(executor_rows, key=lambda item: int(item.get("totalShuffleRead") or 0), reverse=True)[:top],
        "config": {key: props.get(key) for key in ("spark.executor.instances", "spark.executor.cores", "spark.dynamicAllocation.enabled") if key in props},
        "evidence": evidence,
        "recommendations": recommendations,
    }


def build_parallelism_diagnosis(
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    *,
    environment: dict[str, Any] | None = None,
    top: int = 10,
) -> dict[str, Any]:
    """Diagnose effective stage parallelism from wall time, executor runtime, and config."""
    props = environment_spark_properties(environment)
    wall = summarize_stage_wall_times(stages, top=top)
    top_stages = wall.get("top_wall_time_stages", [])
    executor_instances = int_spark_conf(props, "spark.executor.instances")
    executor_cores = int_spark_conf(props, "spark.executor.cores")
    dynamic_allocation = bool_spark_conf(props, "spark.dynamicAllocation.enabled")
    configured_total_cores = executor_instances * executor_cores if executor_instances is not None and executor_cores is not None else None
    observed_total_cores = sum(safe_int(executor.get("totalCores")) or 0 for executor in executors if str(executor.get("id")) != "driver")
    max_effective_parallelism = max([to_float(row.get("estimated_parallelism")) or 0 for row in top_stages] or [0])
    classification = "unknown"
    if dynamic_allocation is False and configured_total_cores is not None and configured_total_cores <= 4:
        classification = "low_static_parallelism"
    elif max_effective_parallelism and configured_total_cores and max_effective_parallelism < configured_total_cores * 0.5:
        classification = "underutilized_capacity"
    elif max_effective_parallelism:
        classification = "parallelism_observed"
    recommendations: list[str] = []
    if classification == "low_static_parallelism":
        recommendations.append("Executor allocation is static and small; increase executor instances/cores or enable dynamic allocation when the cluster supports it.")
    if top_stages:
        recommendations.append("Use stages wall-time and executors churn together to separate low parallelism from executor loss or external write latency.")
    if not recommendations:
        recommendations.append("No stage wall-time evidence available; fetch stages with submission and completion timestamps.")
    return {
        "classification": classification,
        "configured_executor_instances": executor_instances,
        "configured_executor_cores": executor_cores,
        "configured_total_cores": configured_total_cores,
        "dynamic_allocation_enabled": dynamic_allocation,
        "observed_executor_count": len([executor for executor in executors if str(executor.get("id")) != "driver"]),
        "observed_total_cores": observed_total_cores,
        "max_effective_parallelism": round(max_effective_parallelism, 4) if max_effective_parallelism else None,
        "top_stages": top_stages,
        "recommendations": recommendations,
    }


def stage_recovered(stage: dict[str, Any] | None, jobs: list[dict[str, Any]]) -> bool:
    """Return true when a stage with task failures appears to have recovered."""
    if stage and str(stage.get("status") or "").upper() in {"COMPLETE", "SUCCEEDED"}:
        return True
    stage_id = safe_int(stage.get("stageId")) if stage else None
    if stage_id is None:
        return False
    statuses = stage_job_statuses(stage_id, jobs)
    return bool(statuses) and all(status not in {"FAILED", "KILLED"} for status in statuses)


def build_executor_loss_diagnosis(
    stages: list[dict[str, Any]],
    executors: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    *,
    stage_id: int | None = None,
    attempt_id: int | None = None,
    jobs: list[dict[str, Any]] | None = None,
    top: int = 20,
) -> dict[str, Any]:
    """Diagnose whether failed tasks came from executor loss and removal churn."""
    stage = stage_row_for_id(stages, stage_id, attempt_id)
    task_rows = [compact_failed_task_row(task) for task in tasks]
    failed_rows = [row for row in task_rows if str(row.get("status") or "").upper() in {"FAILED", "KILLED"}]
    reasons = Counter(row.get("error_reason") or "unknown" for row in failed_rows)
    removed_executor_ids = {str(executor.get("id")) for executor in executors if executor_time_bounds(executor)[1] is not None}
    failed_on_removed = [row for row in failed_rows if str(row.get("executorId")) in removed_executor_ids]
    host_counts = Counter(str(row.get("host") or "unknown") for row in failed_rows)
    churn = summarize_executor_churn(executors, stages, stage_id=stage_id, attempt_id=attempt_id, top=top)
    recovered = stage_recovered(stage, jobs or [])
    classification = "unknown"
    if reasons.get("executor_lost") and recovered:
        classification = "executor_lost_recovered"
    elif reasons.get("executor_lost"):
        classification = "executor_lost"
    elif failed_rows and host_counts.most_common(1)[0][1] >= max(2, len(failed_rows) // 2 + 1):
        classification = "node_hotspot"
    elif churn.get("classification") == "framework_deleted_executors":
        classification = "framework_deleted_executors"
    recommendations = recommend_executor_loss_actions(classification, churn, reasons)
    return {
        "stage_id": stage_id,
        "attempt_id": attempt_id,
        "classification": classification,
        "recovered": recovered,
        "failed_task_count": len(failed_rows),
        "executor_lost_tasks": reasons.get("executor_lost", 0),
        "failed_on_removed_executor": len(failed_on_removed),
        "remove_events_during_stage": churn.get("removed_during_stage"),
        "error_reasons": dict(reasons.most_common(top)),
        "failed_executors": dict(Counter(str(row.get("executorId") or "unknown") for row in failed_rows).most_common(top)),
        "failed_hosts": dict(host_counts.most_common(top)),
        "failed_task_samples": failed_rows[:top],
        "churn": churn,
        "recommendations": recommendations,
    }


def recommend_executor_loss_actions(classification: str, churn: dict[str, Any], reasons: Counter[str]) -> list[str]:
    """Build recommendations for executor-loss and churn diagnoses."""
    recommendations: list[str] = []
    if classification == "executor_lost_recovered":
        recommendations.append("Failed tasks were recovered after executor loss; inspect resource reclaim, dynamic allocation, and Kubernetes pod deletion before SQL logic.")
    if churn.get("classification") == "framework_deleted_executors":
        recommendations.append("Executor removals were reported as framework/user deletes during the stage; correlate with cluster autoscaling or Spark dynamic allocation.")
    if classification == "node_hotspot":
        recommendations.append("Failed tasks concentrate on one host; check node health and pod eviction events for that host.")
    if reasons and not recommendations:
        recommendations.append("Use the dominant failed task reason and executor/host distribution for the next drilldown.")
    if not recommendations:
        recommendations.append("No executor-loss signal found in the scanned task rows.")
    return recommendations


def stage_job_ids(stage_id: int | None, jobs: list[dict[str, Any]]) -> list[int]:
    """Return job ids that reference a stage id."""
    if stage_id is None:
        return []
    result = []
    for job in jobs:
        stage_ids = {safe_int(item) for item in job.get("stageIds", [])}
        if stage_id in stage_ids:
            job_id = safe_int(job.get("jobId"))
            if job_id is not None:
                result.append(job_id)
    return sorted(result)


def stage_job_statuses(stage_id: int | None, jobs: list[dict[str, Any]]) -> list[str]:
    """Return job statuses for jobs that reference a stage id."""
    if stage_id is None:
        return []
    statuses = []
    for job in jobs:
        stage_ids = {safe_int(item) for item in job.get("stageIds", [])}
        if stage_id in stage_ids:
            statuses.append(str(job.get("status") or "UNKNOWN").upper())
    return statuses


def classify_stage_failure(
    stage: dict[str, Any],
    attempts_by_stage_id: dict[int, list[dict[str, Any]]],
    jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Classify whether a stage failure is fatal, recovered, or non-fatal."""
    stage_id = safe_int(stage.get("stageId"))
    attempts = attempts_by_stage_id.get(-1 if stage_id is None else stage_id, [stage])
    final_attempt = attempts[-1] if attempts else stage
    status = str(stage.get("status") or "UNKNOWN").upper()
    final_status = str(final_attempt.get("status") or "UNKNOWN").upper()
    job_ids = stage_job_ids(stage_id, jobs)
    job_statuses = stage_job_statuses(stage_id, jobs)
    failed_jobs = [status for status in job_statuses if status in {"FAILED", "KILLED"}]
    if status == "KILLED":
        classification = "non_fatal_killed"
    elif status == "FAILED" and final_status in {"COMPLETE", "SUCCEEDED"}:
        classification = "recovered_stage_attempt"
    elif status == "FAILED" and (final_status == "FAILED" or failed_jobs):
        classification = "fatal_stage_failure"
    elif status in {"COMPLETE", "SUCCEEDED"} and int(stage.get("numFailedTasks", 0) or 0) > 0:
        classification = "recovered_task_failure"
    elif len(attempts) > 1:
        classification = "stage_retry_without_failure"
    else:
        classification = "none"
    return {
        "classification": classification,
        "stage_id": stage_id,
        "attempt_id": safe_int(stage.get("attemptId")),
        "status": status,
        "final_status": final_status,
        "job_ids": job_ids,
        "job_statuses": job_statuses,
        "recovered": classification in {"recovered_stage_attempt", "recovered_task_failure", "stage_retry_without_failure"},
        "fatal": classification == "fatal_stage_failure",
    }


def summarize_stage_retries(stages: list[dict[str, Any]], jobs: list[dict[str, Any]] | None = None, *, top: int = 20) -> dict[str, Any]:
    """Summarize retried stage attempts and whether they recovered."""
    attempts_by_stage_id = group_stage_attempts(stages)
    rows = []
    for stage_id, attempts in sorted(attempts_by_stage_id.items()):
        if len(attempts) <= 1:
            continue
        failed_attempts = [stage for stage in attempts if str(stage.get("status") or "").upper() == "FAILED"]
        final_attempt = attempts[-1]
        classified_stage = failed_attempts[-1] if failed_attempts else final_attempt
        impact = classify_stage_failure(classified_stage, attempts_by_stage_id, jobs or [])
        rows.append(
            {
                "stage_id": stage_id,
                "attempt_count": len(attempts),
                "attempt_ids": [safe_int(stage.get("attemptId")) for stage in attempts],
                "statuses": [str(stage.get("status") or "UNKNOWN").upper() for stage in attempts],
                "failed_attempts": [safe_int(stage.get("attemptId")) for stage in failed_attempts],
                "final_status": str(final_attempt.get("status") or "UNKNOWN").upper(),
                "classification": impact["classification"],
                "job_ids": impact["job_ids"],
                "name": final_attempt.get("name"),
            }
        )
    return {
        "retry_stage_count": len(rows),
        "retry_attempt_count": sum(max(0, row["attempt_count"] - 1) for row in rows),
        "recovered_retries": sum(1 for row in rows if row.get("classification") in {"recovered_stage_attempt", "stage_retry_without_failure"}),
        "final_failed_retries": sum(1 for row in rows if row.get("classification") == "fatal_stage_failure"),
        "stages": rows[:top],
    }


def classify_failure_reason(error_text: Any) -> str:
    """Classify a Spark task or stage error string into a stable reason."""
    text = str(error_text or "").lower()
    if "fetchfailed" in text or "fetch failed" in text:
        return "fetch_failed"
    if "executorlost" in text or "executor lost" in text or "container killed" in text:
        return "executor_lost"
    if "outofmemory" in text or "out of memory" in text or "java heap space" in text:
        return "out_of_memory"
    if "pythonexception" in text or "python exception" in text:
        return "python_exception"
    if "filenotfound" in text or "file not found" in text or "no such file" in text:
        return "file_not_found"
    if "taskkilled" in text or "task killed" in text or "killed" in text:
        return "task_killed"
    if "shuffle" in text:
        return "shuffle_error"
    if "timeout" in text:
        return "timeout"
    if not text:
        return "unknown"
    return "other"


def compact_failed_task_row(task: dict[str, Any]) -> dict[str, Any]:
    """Return compact task failure fields for retry/failure diagnostics."""
    row = compact_task_duration_row(task)
    error_message = first_present(task.get("errorMessage"), task.get("error"), task.get("failureReason"))
    row["status"] = first_present(task.get("status"), task.get("taskStatus"), get_nested(task, "taskInfo.status"))
    row["errorMessage"] = error_message
    row["error_reason"] = classify_failure_reason(error_message or row.get("status"))
    return row


def summarize_task_failures(tasks: list[dict[str, Any]], stage: dict[str, Any], *, top: int = 20) -> dict[str, Any]:
    """Summarize failed and killed tasks for one stage attempt."""
    rows = [compact_failed_task_row(task) for task in tasks]
    failed = [row for row in rows if str(row.get("status") or "").upper() == "FAILED"]
    killed = [row for row in rows if str(row.get("status") or "").upper() == "KILLED"]
    successful = [row for row in rows if str(row.get("status") or "").upper() in {"SUCCESS", "SUCCEEDED", "COMPLETE"}]
    reasons = Counter(row.get("error_reason") or "unknown" for row in failed + killed)
    executors = Counter(str(row.get("executorId") or "unknown") for row in failed + killed)
    hosts = Counter(str(row.get("host") or "unknown") for row in failed + killed)
    stage_status = str(stage.get("status") or "").upper()
    if stage_status == "FAILED":
        classification = "fatal_stage_failure"
    elif stage_status == "KILLED":
        classification = "non_fatal_killed"
    elif failed:
        classification = "recovered_task_failure"
    elif killed:
        classification = "non_fatal_killed"
    else:
        classification = "none"
    return {
        "stage_id": stage.get("stageId"),
        "attempt_id": first_present(stage.get("attemptId"), 0),
        "status": stage.get("status"),
        "classification": classification,
        "task_count": len(rows),
        "failed_tasks": len(failed),
        "killed_tasks": len(killed),
        "successful_tasks": len(successful),
        "error_reasons": dict(reasons),
        "executors": dict(executors),
        "hosts": dict(hosts),
        "failed_task_samples": failed[:top],
        "killed_task_samples": killed[:top],
    }


def build_retry_diagnosis(
    stages: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    retry_groups: dict[str, Any],
) -> dict[str, Any]:
    """Build retry risks and recommendations from stage retry evidence."""
    risks: list[dict[str, Any]] = []
    recommendations: list[str] = []
    if retry_groups.get("final_failed_retries"):
        risks.append({"level": "critical", "area": "stage_retries", "message": "Some retried stages still failed", "evidence": retry_groups.get("final_failed_retries")})
        recommendations.append("Inspect the failed stage attempts and task error reasons before treating retries as noise.")
    if retry_groups.get("recovered_retries"):
        risks.append({"level": "warning", "area": "stage_retries", "message": "Recovered stage retries found", "evidence": retry_groups.get("recovered_retries")})
        recommendations.append("Recovered retries may indicate executor loss, fetch failures, speculation, or transient resource churn.")
    if not risks:
        recommendations.append("No stage retry signal found in the scanned stages.")
    return {
        "risk_count": len(risks),
        "risks": risks,
        "recommendations": recommendations,
        "failed_jobs": [job.get("jobId") for job in jobs if str(job.get("status") or "").upper() in {"FAILED", "KILLED"}],
        "stage_count": len(stages),
    }


def build_failure_diagnosis(
    stages: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    task_failure_groups: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build failure risks and recommendations from stage and task failures."""
    risks: list[dict[str, Any]] = []
    recommendations: list[str] = []
    fatal_groups = [group for group in task_failure_groups if group.get("classification") == "fatal_stage_failure"]
    recovered_groups = [group for group in task_failure_groups if group.get("classification") == "recovered_task_failure"]
    reason_counts = Counter()
    for group in task_failure_groups:
        reason_counts.update(group.get("error_reasons", {}))
    if fatal_groups or any(str(job.get("status") or "").upper() in {"FAILED", "KILLED"} for job in jobs):
        risks.append({"level": "critical", "area": "stage_failures", "message": "Fatal stage or job failure evidence found", "evidence": len(fatal_groups)})
        recommendations.append("Start with fatal stage failed_task_samples and correlate executor/host concentration.")
    if recovered_groups:
        risks.append({"level": "warning", "area": "task_failures", "message": "Recovered task failures found", "evidence": len(recovered_groups)})
        recommendations.append("Recovered task failures can still explain slowness; check dominant error_reasons and executors.")
    if reason_counts:
        top_reason = reason_counts.most_common(1)[0][0]
        recommendations.append(f"Dominant task failure reason is {top_reason}; inspect matching executor logs if available.")
    if not recommendations:
        recommendations.append("No failed task details found in the scanned stages.")
    return {
        "risk_count": len(risks),
        "risks": risks,
        "recommendations": recommendations,
        "error_reasons": dict(reason_counts),
        "failed_jobs": [job.get("jobId") for job in jobs if str(job.get("status") or "").upper() in {"FAILED", "KILLED"}],
        "stage_count": len(stages),
    }
