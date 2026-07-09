---
name: flink-k8s
description: Diagnose Apache Flink WebUI and Flink on Kubernetes jobs through the Flink REST API. Use when the user mentions Flink WebUI, Flink on K8s, task status, backpressure, checkpoint, TaskManager, JobManager, metrics, logs, thread dump, FlameGraph, or wants silent monitoring with browser cookie authentication.
---

# Flink WebUI Diagnostics

## Purpose

Use this skill to inspect Flink WebUI deployments through the same REST API used by the dashboard. The bundled CLI reuses Chrome cookies through `chrome-cdp-ws-daemon`, so it can silently query internal Flink WebUI URLs that require browser SSO authentication.

Interpreter:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11
```

CLI entrypoint:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 ~/.cursor/skills/flink-k8s/scripts/flink_diag.py --help
```

## Common Workflow

1. Start with `resolve` when the user gives a WebUI URL. It extracts REST base URL, job id, task chain id, and TaskManager id when present.
2. Use `inventory` to discover dynamic ids when the URL does not include them.
3. Use `overview`, `jobs`, `job graph`, and `task-chain detail` to understand the job shape.
4. Use `inspect` on a copied WebUI tab URL when you want the CLI to choose the matching high-level command automatically.
5. Use `job health` for a one-shot report across graph, IO flow, checkpoint, exceptions, backpressure, memory, and risks.
6. Use `job io-flow` or `diagnose flow` to compare every task chain's input/output records and find where records are filtered, expanded, or stop.
7. Use `job skew`, `task-chain skew`, and `backpressure --samples` to distinguish skew, current load, and persistent vs transient backpressure.
8. Use `job capacity` or `diagnose parallelism` when the question is whether parallelism is reasonable, whether a task chain is limiting throughput, whether there is backpressure, and whether sink tuning is more likely than increasing parallelism.
9. Use `task-chain source-lag`, `task-chain source-stats`, `task-chain sink-stats`, `task-chain taskmanager-aggregates`, `job checkpoint-trend`, `taskmanager memory-top`, and `logs diagnose` / `logs errors` / `logs grep` for focused checks.
10. For Paimon sink jobs, use `job connectors`, `task-chain paimon-stats`, `diagnose paimon`, or `job capacity` to connect writer, committer, compaction, commit latency, and source-absence evidence.
11. For HBase / dimension table lookup joins, use `diagnose lookup` or `diagnose hbase-lookup`; do not treat the surrounding task-chain input rate as HBase lookup QPS.
12. Read logs, stdout, thread dumps, and FlameGraphs only when needed; these can be large. For print-connector debugging, prefer bounded `stdout tail` / `stdout watch` across selected TaskManagers.

## Examples

```bash
python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py resolve \
  --url "https://flink-k8s-aliyun-cn.tuya-inc.com:7799/app/#/job/running/<jobid>/overview/<vertexid>/detail" \
  --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py inventory \
  --url "$FLINK_WEB_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job graph \
  --url "$FLINK_JOB_OVERVIEW_URL" --top-by backpressure --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job health \
  --url "$FLINK_JOB_OVERVIEW_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job io-flow \
  --url "$FLINK_JOB_OVERVIEW_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job exceptions-summary \
  --url "$FLINK_JOB_EXCEPTIONS_URL" --limit 10 --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job connectors \
  --url "$FLINK_JOB_OVERVIEW_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain paimon-stats \
  --url "$PAIMON_WRITER_OR_COMMITTER_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py diagnose paimon \
  --url "$FLINK_JOB_OVERVIEW_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py diagnose lookup \
  --url "$FLINK_JOB_OVERVIEW_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job capacity \
  --url "$FLINK_JOB_OVERVIEW_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py diagnose parallelism \
  --url "$FLINK_JOB_OVERVIEW_URL" --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job skew \
  --url "$FLINK_JOB_OVERVIEW_URL" \
  --get numRecordsInPerSecond,numRecordsOutPerSecond,busyTimeMsPerSecond,backPressuredTimeMsPerSecond \
  --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py backpressure \
  --url "$FLINK_JOB_OVERVIEW_URL" --samples 3 --interval 10 --top 5 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain metrics \
  --url "$FLINK_TASK_CHAIN_URL" \
  --get busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsOutPerSecond \
  --scope aggregate --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain metrics \
  --url "$FLINK_TASK_CHAIN_SUBTASKS_URL" \
  --scope subtask --all-subtasks \
  --get numRecordsIn,numRecordsOut,numRecordsInPerSecond,numRecordsOutPerSecond \
  --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain taskmanager-aggregates \
  --url "$FLINK_TASK_CHAIN_TASKMANAGERS_URL" \
  --get read-records,accumulated-busy-time,accumulated-backpressured-time \
  --sort-by read-records --top 5 --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain source-stats \
  --url "$FLINK_SOURCE_TASK_CHAIN_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain source-lag \
  --url "$FLINK_SOURCE_TASK_CHAIN_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain sink-stats \
  --url "$FLINK_SINK_TASK_CHAIN_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py job checkpoint-trend \
  --url "$FLINK_JOB_OVERVIEW_URL" --limit 10 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py taskmanager memory-top \
  --url "$FLINK_WEB_URL" --top 5 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs scan \
  --url "$FLINK_WEB_URL" --patterns ERROR,WARN,Exception,OutOfMemoryError,Checkpoint --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs diagnose \
  --url "$FLINK_TM_LOG_URL" --scope taskmanager --tail-bytes 262144 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs grep \
  --url "$FLINK_TM_LOG_URL" --scope taskmanager \
  --patterns ERROR,Exception --before 3 --after 8 --tail-bytes 262144 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs grep \
  --url "$FLINK_JOB_OVERVIEW_URL" --scope taskmanager --all-taskmanagers \
  --patterns hbase-lookup --tail-bytes 262144 --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs errors \
  --url "$FLINK_TM_LOG_URL" --scope taskmanager \
  --before 3 --after 8 --max-signatures 20 --tail-bytes 262144 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs download \
  --url "$FLINK_TM_LOG_URL" --scope taskmanager \
  --file-pattern '.*\.log$' --max-bytes 104857600 --output-dir ./flink-logs --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py inspect \
  --url "$CURRENT_FLINK_WEBUI_TAB_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric search \
  numRecordsIn --url "$FLINK_TASK_CHAIN_URL" --scope task-chain --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric search \
  numRecordsIn --url "$FLINK_TASK_CHAIN_URL" --scope auto --structured --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric explain \
  sink.records_send --url "$FLINK_WEB_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric aggregate \
  --url "$FLINK_WEB_URL" --scope taskmanager \
  --get Status.JVM.CPU.Load,Status.JVM.Memory.Heap.Used \
  --agg min,max,avg --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric aggregate \
  --url "$FLINK_TASK_CHAIN_URL" --scope subtask \
  --get busyTimeMsPerSecond,numRecordsInPerSecond \
  --agg min,max,avg,sum --subtasks 0,1,2 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric watch \
  --url "$FLINK_TASK_CHAIN_URL" --scope subtask \
  --get numRecordsInPerSecond,numRecordsOutPerSecond \
  --agg sum --samples 6 --interval 10 --delta --rate --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric analyze \
  --url "$FLINK_TASK_CHAIN_URL" --scope subtask \
  --get busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond,checkpointStartDelayNanos \
  --agg min,max,avg,sum --samples 12 --interval 10 --peak-threshold 900 --json --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py diagnose backpressure \
  --url "$FLINK_JOB_OVERVIEW_URL" --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py taskmanager logs tail \
  --url "$FLINK_TM_LOG_URL" --tail-bytes 65536 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py stdout tail \
  --url "$FLINK_TM_STDOUT_URL" --all-taskmanagers \
  --tail-bytes 65536 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py stdout grep \
  --url "$FLINK_TM_STDOUT_URL" --all-taskmanagers \
  --patterns 'print connector,ERROR,Exception' \
  --before 2 --after 4 --tail-bytes 262144 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py stdout download \
  --url "$FLINK_TM_STDOUT_URL" --all-taskmanagers \
  --max-bytes 104857600 --output-dir ./flink-stdout --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py stdout watch \
  --url "$FLINK_TM_STDOUT_URL" --all-taskmanagers \
  --interval 2 --since-end --max-bytes-per-poll 10485760 --insecure
```

## URL And Dynamic Ids

The CLI treats URL path and hash segments as variables:

- `/<deployment>/#/overview` gives the REST base.
- `#/job/running/<jobid>/...` gives `job_id`.
- `#/job/running/<jobid>/overview/<vertexid>/...` gives task chain / vertex id.
- `#/task-manager/<taskmanager_id>/...` gives TaskManager id.

When an id is not present, the CLI can discover it from upstream REST resources:

- jobs from `jobs/overview`
- task chains from `jobs/<jobid>`
- TaskManagers from `taskmanagers`
- log files from `jobmanager/logs` or `taskmanagers/<tmid>/logs`
- metrics from the relevant `metrics` endpoint

## Semantic Metrics

Prefer semantic aliases for Source/Sink questions. Flink WebUI labels can be misleading: task-chain `Records Sent` means records emitted to downstream Flink operators, so a terminal sink can show `0` while it is still writing to Kafka or another external system.

Useful aliases:

- `source.records_in`
- `source.records_in_rate`
- `source.records_out`
- `sink.records_send`
- `sink.records_send_rate`
- `sink.records_send_errors`
- `sink.bytes_send`
- `taskchain.backpressure`
- `source.records_lag`
- `source.current_offset`
- `source.committed_offset`
- `paimon.writer.records_in`
- `paimon.writer.records_in_rate`
- `paimon.writer.buffer_writers`
- `paimon.writer.buffer_preempt_count`
- `paimon.compaction.busy`
- `paimon.compaction.level0_file_count`
- `paimon.commit.duration_p99`
- `paimon.commit.files_added`
- `paimon.commit.files_appended`
- `paimon.commit.files_deleted`
- `paimon.commit.partitions_written`
- `paimon.commit.buckets_written`
- `paimon.commit.snapshots`
- `paimon.commit.attempts`

Use `metric search` to discover concrete metric ids and `metric explain` to understand aliases or known metrics.

## Official Metric REST Integration

The Flink Monitoring REST API supports aggregate metric endpoints. Use `metric aggregate` when you need cluster-wide or subset summaries without manually fetching every entity:

- `--scope taskmanager`: `/taskmanagers/metrics`, with optional `--taskmanagers A,B`.
- `--scope job`: `/jobs/metrics`, with optional `--jobs J1,J2`.
- `--scope subtask`: `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics`, with optional `--subtasks 0,1`.
- `--scope jm-operator`: `/jobs/<jobid>/vertices/<vertexid>/jm-operator-metrics`; this endpoint is feature-detected and may be unavailable on some Flink versions.

Use `--agg min,max,avg,sum` to request specific aggregate fields. Metric ids are escaped per value before joining with commas, so ids containing `#`, `$`, `&`, `+`, `/`, `;`, `=`, `?`, or `@` can be queried safely.

`metric watch` repeatedly calls the same aggregate endpoint and can emit adjacent-sample `--delta` or per-second `--rate`. Prefer bounded runs with `--samples` or `--duration`.

`metric analyze` runs a bounded watch, keeps the sampled metric maps in memory, and emits a post-run analysis. It detects `busyTimeMsPerSecond.max` peaks by default, groups consecutive peak samples into events, estimates peak periodicity, checks whether peaks correlate with recent checkpoint trigger/ack windows, and reports whether the shape looks like `checkpoint_state_spike`, `backpressure_spike`, `periodic_busy_spike_without_backpressure`, or `busy_spike_without_backpressure`.

`metric search --structured` parses dashboard-style ids into `subtask`, `operator`, and `metric_name`. With `--scope auto` on a task-chain URL, it also probes `jm-operator-metrics` and includes it when available.

For connector debugging, prefer user-defined Counters/Gauges/Meters such as `debug.records_in`, `debug.filtered`, and `debug.sent` over stdout prints when the job may run for more than a short test session. Latency and state-access tracking metrics can be useful, but they have documented performance impact and should only be enabled explicitly for focused debugging.

## Capacity / Parallelism Diagnosis

Use `job capacity` or `diagnose parallelism` for a one-shot capacity report. The command combines:

- job graph parallelism and maxParallelism
- backpressure REST samples per task chain
- subtask metrics: `numRecordsInPerSecond`, `numRecordsOutPerSecond`, bytes rate, `busyTimeMsPerSecond`, `idleTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, and `checkpointStartDelayNanos`
- TaskManager row aggregates from the task-chain `taskmanagers` endpoint
- checkpoint trend
- Paimon writer/committer detection and sink tuning risks when Paimon metrics are present
- LookupJoin detection and HBase lookup risk signals when operator metrics are present

Interpretation rules:

- high `backPressuredTimeMsPerSecond` or WebUI backpressure ratio means downstream blockage should be investigated before changing parallelism.
- high `busyTimeMsPerSecond` with low idle means that task chain may need more parallelism or cheaper per-record logic.
- high idle and no backpressure means increasing parallelism is unlikely to help current throughput.
- high subtask skew means first check source partitions, key distribution, bucket/partition design, or data skew.
- Paimon Writer `Records Out` is treated as internal committable flow, not as business record pass-through. A low writer `Records Out` is not automatically a filter/drop diagnosis.
- Global Committer parallelism of 1 is common; tune only when commit metrics show sustained latency, retries, or file-count pressure.
- LookupJoin reports `chain_records_in_rate_sum` separately from `actual_lookup_records_in_rate_sum`; only the latter should be used as HBase lookup QPS.
- Lookup cache hit rate near 0 is a risk signal, but `job capacity` only treats LookupJoin as a likely bottleneck when it also sees busy, backpressure, or HBase/lookup log anomalies.

## HBase LookupJoin Diagnostics

Use `diagnose lookup` or `diagnose hbase-lookup` for jobs with dimension table joins. The command scans the job graph for `LookupJoin`, reads operator-level metrics such as `LookupJoin[*].numRecordsInPerSecond`, `LookupJoin[*].numRecordsOutPerSecond`, and `lookupCacheHitRate`, then combines them with busy/idle, backpressure, checkpoint trend, and bounded HBase log grep.

Semantic aliases:

- `lookup.records_in_rate`: actual LookupJoin input rate, useful as lookup QPS.
- `lookup.records_out_rate`: records emitted after lookup.
- `lookup.cache_hit_rate`: connector cache hit ratio when exposed.

HBase/lookup log preset:

```bash
python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs grep \
  --url "$FLINK_JOB_OVERVIEW_URL" --scope taskmanager --all-taskmanagers \
  --patterns hbase-lookup --tail-bytes 262144 --json --insecure
```

The preset expands to `HBaseConfigurationUtil`, `TimeoutException`, `RetriesExhausted`, `ScannerTimeout`, and `CallTimeout`. `ERROR` matching is case-sensitive so a WARN line containing the word `Error` is not counted as an ERROR-level event.

## TaskManager Aggregated Metrics

On a task-chain `taskmanagers` WebUI tab, each TaskManager row's More menu can show aggregated metrics. The CLI exposes the same read-only row data with:

```bash
python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py task-chain taskmanager-aggregates \
  --url "$FLINK_TASK_CHAIN_TASKMANAGERS_URL" \
  --get read-records,write-records,accumulated-busy-time,accumulated-backpressured-time \
  --sort-by read-records --top 10 --json --insecure
```

Use this to spot TaskManager-local imbalance or pressure for one task chain. It complements `task-chain skew`: `taskmanager-aggregates` groups by TaskManager, while `task-chain skew` ranks individual subtasks.

## Paimon Connector Diagnostics

Paimon metrics are usually split across sink-side task chains:

- Writer vertices expose `paimon.table...writerBuffer.*`, `paimon.table...compaction.*`, and writer input/rate metrics.
- Global Committer vertices expose `paimon.table...commit.*`, including commit duration, attempts, files, partitions, buckets, and snapshots.
- A job can write to Paimon while still reading from Kafka or another source. In that case `diagnose paimon` reports `source_absent` for Paimon source metrics and lists the actual source vertices to inspect with source playbooks.

Risk labels:

- `commit_slow`: high commit duration or commit attempts greater than 1.
- `small_files_risk`: many files, partitions, or buckets touched in one commit.
- `compaction_busy`: high compaction busy percentage or large level-0 file backlog.
- `writer_skew`: high per-subtask skew in writer throughput or compaction metrics.
- `source_absent`: no Paimon source metrics found in the job graph.

## Troubleshooting Playbooks

`diagnose` accepts focused playbooks:

- `diagnose backpressure`: graph-level backpressure ranking.
- `diagnose checkpoint`: checkpoint backend, interval, timeout, and latest completed checkpoint summary.
- `diagnose source`: Source semantic metric summary for a task chain.
- `diagnose sink`: Sink writer/KafkaProducer metric summary for a task chain.
- `diagnose memory`: TaskManager memory and GC summary.
- `diagnose flow`: Job-level input/output summary for every task chain, including pass-through percentage and largest filtering stage.
- `diagnose health`: One-shot job health report.
- `diagnose skew`: Job-level subtask skew summary.
- `diagnose source-lag`: Source offset/lag summary for a task chain.
- `diagnose checkpoint-trend`: Recent checkpoint duration and state-size trend.
- `diagnose memory-top`: Top TaskManagers by heap/direct/managed/GC.
- `diagnose paimon`: Paimon writer, committer, compaction, commit, small-file, skew, and source-absence summary.
- `diagnose lookup` / `diagnose hbase-lookup`: HBase/LookupJoin lookup QPS, cache hit, skew, busy/idle, backpressure, checkpoint, and HBase warning summary.

## Job Exceptions

Use `job exceptions` when you need the raw Flink REST payload from `jobs/<jobid>/exceptions`. Use `job exceptions-summary` when you need a compact view of both root exception and exception history.

The CLI accepts both route shapes without changing the REST endpoint:

- Flink 1.18 style: `#/job/running/<jobid>/exceptions`
- Flink 1.15 style: `#/job/<jobid>/exceptions`

The summary preserves version-specific field names and reports `fields_present` so callers can tell whether `root-exception`, `all-exceptions`, and `exceptionHistory` were present. In Flink 1.15, `all-exceptions` can be empty while `exceptionHistory.entries` contains the history rows.

## Large Log Workflow

For TaskManager or JobManager log pages such as `#/task-manager/<tmid>/logs`, start with bounded summaries:

```bash
python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py inspect \
  --url "$FLINK_TM_LOG_URL" --tail-bytes 262144 --insecure

python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py logs errors \
  --url "$FLINK_TM_LOG_URL" --scope taskmanager \
  --patterns ERROR,Exception,Caused\ by,OutOfMemoryError,TimeoutException \
  --before 3 --after 8 --tail-bytes 262144 --insecure
```

Use `--all-taskmanagers` with `logs grep`, `logs scan`, or `logs errors` when a problem may appear on any TaskManager. The command shares the same async client and bounded concurrency, and each TaskManager failure is isolated in its own result row.

Use `--full` only when you intentionally want to stream the complete log. `logs grep --full` still keeps only `--max-matches` plus context in CLI output. `logs download` refuses an unbounded download unless you pass `--max-bytes` or explicit `--full`; it writes only to a local path and reports `output_path`, `bytes_written`, `truncated`, and `sha256`.

## Metric Analyze Workflow

Use `metric analyze` when a WebUI chart shows periodic spikes and you need the CLI to remember samples and explain the shape after the window ends. Typical task-chain busy analysis:

```bash
python ~/.cursor/skills/flink-k8s/scripts/flink_diag.py metric analyze \
  --url "$FLINK_TASK_CHAIN_URL" --scope subtask \
  --get busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond,checkpointStartDelayNanos \
  --agg min,max,avg,sum --samples 18 --interval 10 \
  --peak-threshold 900 --checkpoint-window 15 --json --insecure
```

Performance notes:

- The command resolves metric ids once, then reuses the prepared aggregate request for each sample.
- Samples are stored in memory only; use `--include-samples` when you need raw sample rows in the final JSON.
- Prefer enough samples to cover at least two expected periods; for checkpoint-related spikes, cover at least two checkpoint intervals.

## Stdout Watch Workflow

Use `stdout` commands when test code writes print-connector content to TaskManager stdout. The CLI reads the same REST endpoint as the WebUI stdout tab, supports `--all-taskmanagers`, and keeps one shared `httpx.AsyncClient` connection pool with bounded async concurrency.

- `stdout tail` reads bounded tails for one or many TaskManagers.
- `stdout grep` searches a bounded tail by default; use `--full` only when necessary.
- `stdout download` refuses unbounded downloads unless `--max-bytes` or `--full` is explicit.
- `stdout watch` is the smooth refresh path: it polls the stdout REST endpoint, diffs each TaskManager's local tail state, and prints only new lines with `[taskmanager-id]` prefixes. With `--json`, watch emits one compact JSON object per event.

Performance guardrails:

- Keep `--interval` conservative for large stdout; default polling uses the existing async HTTP client and `--concurrency`.
- Use `--max-bytes-per-poll` to cap per-TaskManager reads when a proxy may not support byte ranges.
- Use `--duration`, `--polls`, or `--max-events` for bounded debug sessions.

In Kubernetes mode, `taskmanagers/<tmid>/stdout` may return a small explanatory message instead of real stdout. The CLI reports `available: false`, marks `reason: kubernetes_stdout_missing`, and includes a `kubectl logs <pod>` recommendation. It does not save that explanatory message as a downloaded stdout file.

## Safety Notes

- The CLI never prints cookies, tokens, or full authentication headers.
- Use `--concurrency` to control parallel requests. Default is conservative.
- Logs, stdout, thread dumps, and FlameGraphs are size-limited or summarized by default.
- Log grep/error diagnostics do not print full logs by default; they summarize bounded tails unless `--full` is explicitly requested.
- Log downloads are local-only and default to a `.ferryignore`-excluded `.downloads/` directory when no `--output-dir` is provided.
- Stdout downloads are local-only and default to `.downloads/stdout/` when no `--output-dir` is provided.
- In Kubernetes mode stdout may not exist; this is reported as an unavailable resource, not a CLI crash.

## Reference

Read `references/API_DISCOVERY.md` for the verified Flink 1.18 route mappings, endpoint profiles, dynamic id sources, and diagnostic metric guidance.
