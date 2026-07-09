# Flink WebUI API Discovery

This reference records the verified Flink 1.18 WebUI routes and REST endpoints used by `scripts/flink_diag.py`.

## Base URL

For a WebUI URL like:

```text
https://flink-k8s-aliyun-cn.tuya-inc.com:7799/smart-energy-saving-v250704/#/overview
```

the REST base is:

```text
https://flink-k8s-aliyun-cn.tuya-inc.com:7799/smart-energy-saving-v250704/
```

## Version Detection

Use this order:

1. `config` field `flink-version`
2. `overview` field `flink-version`
3. WebUI top text, matching `Version: x.y.z`
4. Explicit `--flink-version`

Unknown versions use the `generic` profile. Verified profile is `flink-1.18`; the CLI also groups 1.15 through 1.20 into conservative profiles for endpoint selection and falls back to `generic` when an endpoint is not known.

## Route Mapping

| WebUI route | REST endpoint |
| --- | --- |
| `#/overview` | `overview` + `jobs/overview` |
| `#/job/completed` | `jobs/overview`, filtered to terminal jobs |
| `#/job/running/<jobid>/exceptions` | `jobs/<jobid>/exceptions` |
| `#/job/<jobid>/exceptions` | `jobs/<jobid>/exceptions` for Flink 1.15-style routes |
| `#/job/running/<jobid>/timeline` | `jobs/<jobid>` |
| `#/job/running/<jobid>/checkpoints` | `jobs/<jobid>/checkpoints` + `jobs/<jobid>/checkpoints/config` |
| `#/job/running/<jobid>/configuration` | `jobs/<jobid>/config` |
| `#/job/running/<jobid>/overview/<vertexid>/detail` | `jobs/<jobid>/vertices/<vertexid>` |
| `#/job/running/<jobid>/overview/<vertexid>/subtasks` | `jobs/<jobid>/vertices/<vertexid>` + `jobs/<jobid>/vertices/<vertexid>/subtasktimes` |
| `#/job/running/<jobid>/overview/<vertexid>/taskmanagers` | `jobs/<jobid>/vertices/<vertexid>/taskmanagers` |
| `#/job/running/<jobid>/overview/<vertexid>/watermarks` | `jobs/<jobid>/vertices/<vertexid>/watermarks` |
| `#/job/running/<jobid>/overview/<vertexid>/accumulators` | `jobs/<jobid>/vertices/<vertexid>/accumulators` |
| `#/job/running/<jobid>/overview/<vertexid>/backpressure` | `jobs/<jobid>/vertices/<vertexid>/backpressure` |
| `#/job/running/<jobid>/overview/<vertexid>/metrics` | `jobs/<jobid>/vertices/<vertexid>/metrics` and subtask metrics |
| `#/job/running/<jobid>/overview/<vertexid>/flamegraph` | `jobs/<jobid>/vertices/<vertexid>/flamegraph?type=<type>&subtask=<index>` |
| `#/task-manager` | `taskmanagers` |
| `#/task-manager/<tmid>/metrics` | `taskmanagers/<tmid>/metrics` |
| `#/task-manager/<tmid>/logs` | `taskmanagers/<tmid>/log` |
| `#/task-manager/<tmid>/stdout` | `taskmanagers/<tmid>/stdout` |
| `#/task-manager/<tmid>/log-list` | `taskmanagers/<tmid>/logs` |
| `#/task-manager/<tmid>/thread-dump` | `taskmanagers/<tmid>/thread-dump` |
| `#/job-manager/metrics` | `jobmanager/metrics` |
| `#/job-manager/config` | `jobmanager/config` |
| `#/job-manager/logs` | `jobmanager/log` |
| `#/job-manager/stdout` | `jobmanager/stdout` |
| `#/job-manager/log` | `jobmanager/logs` |
| `#/job-manager/thread-dump` | `jobmanager/thread-dump` |

## High-Level Commands

| Need | CLI command |
| --- | --- |
| Auto-map any copied WebUI tab URL | `inspect --url <webui-tab-url>` |
| One-shot health report | `job health --url <job-url>` or `diagnose health` |
| Source input / output totals | `task-chain source-stats --url <source-task-url>` |
| Source Kafka offset / lag | `task-chain source-lag --url <source-task-url>` |
| Sink actual external writes | `task-chain sink-stats --url <sink-task-url>` |
| Find filtering or non-transmission stage | `job io-flow --url <job-url>` or `diagnose flow` |
| Parallelism / throughput / sink capacity diagnosis | `job capacity --url <job-url>` or `diagnose parallelism --url <job-url>` |
| Subtask skew across job | `job skew --url <job-url>` |
| Subtask skew for one task chain | `task-chain skew --url <task-chain-url>` |
| Persistent vs transient backpressure | `backpressure --samples 3 --interval 10 --url <job-url>` |
| Recent checkpoint trend | `job checkpoint-trend --url <job-url> --limit 10` |
| Top TaskManager memory / GC | `taskmanager memory-top --url <webui-url>` |
| Scan JobManager or TaskManager logs | `logs scan --url <webui-url> --patterns ERROR,WARN,Exception` |
| Diagnose a large TaskManager log page | `logs diagnose --url <tm-logs-url> --scope taskmanager --tail-bytes 262144` |
| Grep bounded log context | `logs grep --url <logs-url> --patterns ERROR,Exception --before 3 --after 8` |
| Summarize repeated log errors | `logs errors --url <logs-url> --max-signatures 20` |
| Download logs locally with a ceiling | `logs download --url <logs-url> --max-bytes 104857600 --output-dir <dir>` |
| Tail TaskManager stdout across pods | `stdout tail --url <stdout-url> --all-taskmanagers --tail-bytes 65536` |
| Watch print connector output | `stdout watch --url <stdout-url> --all-taskmanagers --interval 2 --since-end` |
| Download stdout with a ceiling | `stdout download --url <stdout-url> --all-taskmanagers --max-bytes 104857600 --output-dir <dir>` |
| Metric discovery | `metric search <keyword> --url <url> --scope task-chain` |
| Structured metric discovery | `metric search <keyword> --url <task-url> --scope auto --structured` |
| Metric alias explanation | `metric explain sink.records_send --url <url>` |
| Official TaskManager metric aggregate | `metric aggregate --scope taskmanager --get <metrics> --agg min,max,avg` |
| Official subtask metric aggregate | `metric aggregate --scope subtask --url <task-url> --get <metrics> --agg min,max,avg,sum --subtasks 0,1` |
| Metric trend sampling | `metric watch --scope subtask --get <metrics> --agg sum --samples 6 --interval 10 --delta --rate --json` |
| Metric peak / periodicity analysis | `metric analyze --scope subtask --get busyTimeMsPerSecond,... --samples 18 --interval 10 --peak-threshold 900 --json` |
| Checkpoint summary | `job checkpoint-summary --url <job-url>` or `diagnose checkpoint` |
| TaskManager memory / GC | `taskmanager memory --url <tm-url>` or `diagnose memory` |
| Connector role discovery | `job connectors --url <job-url>` |
| Paimon task-chain summary | `task-chain paimon-stats --url <writer-or-committer-url>` |
| Paimon job diagnosis | `diagnose paimon --url <job-url>` |
| HBase / LookupJoin diagnosis | `diagnose lookup --url <job-url>` or `diagnose hbase-lookup --url <job-url>` |

## Dynamic Variables

| Variable | Primary source | Fallback |
| --- | --- | --- |
| `deployment` | URL path first segment | `--base-url` |
| `job_id` | URL hash `#/job/running/<jobid>/...` or `#/job/<jobid>/...` | `jobs/overview` filtered by job name/state/index |
| `vertex_id` | URL hash `#/job/running/<jobid>/overview/<vertexid>/...` or `#/job/<jobid>/overview/<vertexid>/...` | `jobs/<jobid>.vertices` filtered by task chain name |
| `taskmanager_id` | URL hash `#/task-manager/<tmid>/...` | `taskmanagers` filtered by id/host/index |
| `subtask` | `--subtask` | auto `0` only when parallelism is 1 |
| `metric_id` | requested `--get` | relevant `metrics` list with exact/suffix/contains matching |
| `log_file` | `--file` | `logs` list with regex/index selection |

## Metrics Strategy

Use three levels for task chain metrics:

- `jobs/<jobid>/vertices/<vertexid>/metrics`: full task chain metric list, including prefixed ids like `0.busyTimeMsPerSecond`.
- `jobs/<jobid>/vertices/<vertexid>/subtasks/metrics?get=...`: aggregated `min/max/avg/sum` across subtasks.
- `jobs/<jobid>/vertices/<vertexid>/subtasks/<index>/metrics?get=...`: concrete value for a single subtask.
- `jobs/<jobid>/vertices/<vertexid>/taskmanagers`: TaskManager rows for this task chain, including each row's `aggregated.metrics`.

CLI mapping:

- Aggregated metrics: `task-chain metrics --scope aggregate --get <metrics>`
- One subtask: `task-chain metrics --scope subtask --subtask <index> --get <metrics>`
- Every subtask table: `task-chain metrics --scope subtask --all-subtasks --get <metrics>`
- TaskManager row aggregated metrics: `task-chain taskmanager-aggregates --get <metrics> --sort-by <metric>`

`--all-subtasks` requires `--get` so the CLI does not accidentally pull every metric for every subtask.

Metric value requests are chunked automatically to avoid long `get=` URLs when a semantic alias expands into many per-subtask operator metrics.

Official aggregate endpoints are available through `metric aggregate`:

- `/taskmanagers/metrics`, optionally `?taskmanagers=A,B`
- `/jobs/metrics`, optionally `?jobs=D,E`
- `/jobs/<jobid>/vertices/<vertexid>/subtasks/metrics`, optionally `?subtask=0,1`
- `/jobs/<jobid>/vertices/<vertexid>/jm-operator-metrics`, feature-detected because not every deployment exposes it

Use `--agg min,max,avg,sum` to request specific aggregate fields. Metric query values are escaped individually before comma-joining, matching the official REST API requirement for special characters such as `#`, `$`, `&`, `+`, `/`, `;`, `=`, `?`, and `@`.

`metric watch` repeatedly samples an aggregate endpoint and can calculate adjacent-sample `--delta` or per-second `--rate`. It is intended for bounded debugging sessions; prefer `--samples` or `--duration`.

`metric analyze` uses the same official aggregate endpoint but keeps the sample window in memory and analyzes it after collection. It resolves metric ids once, then reuses the prepared request for every sample. Default analysis uses `busyTimeMsPerSecond.max` with threshold `900` when present, groups consecutive peak samples into peak events, estimates event periodicity, and correlates peak timestamps with recent checkpoint trigger/ack windows when a job id is available.

Dashboard-style metric ids can be parsed with `metric search --structured`: task metrics look like `<subtask>.<metric>`, and operator metrics like `<subtask>.<operator>.<metric>`. In task-chain auto scope, search also probes `jm-operator-metrics` when available.

The WebUI task-chain `taskmanagers` tab has a row More action named "View aggregated metrics". In Flink 1.18 this data is already present in the `taskmanagers` endpoint response under each row's `aggregated.metrics`; tested per-TaskManager `/metrics` REST paths returned 404 for the company deployment. Use `task-chain taskmanager-aggregates` to summarize and rank those row aggregates.

Useful task diagnostic metrics:

- `busyTimeMsPerSecond`
- `idleTimeMsPerSecond`
- `backPressuredTimeMsPerSecond`
- `softBackPressuredTimeMsPerSecond`
- `hardBackPressuredTimeMsPerSecond`
- `isBackPressured`
- `numRecordsInPerSecond`
- `numRecordsOutPerSecond`
- `numBytesInPerSecond`
- `numBytesOutPerSecond`
- `checkpointStartDelayNanos`
- `currentInputWatermark`
- `currentOutputWatermark`

Semantic aliases:

- `source.records_in`: prefixed Source operator `numRecordsIn` metrics.
- `source.records_in_rate`: prefixed Source operator `numRecordsInPerSecond` metrics.
- `sink.records_send`: sink writer `numRecordsSend` or KafkaProducer `record-send-total`.
- `sink.records_send_rate`: sink writer/KafkaProducer send rate metrics.
- `sink.records_send_errors`: sink writer/KafkaProducer send error metrics.
- `sink.bytes_send`: sink writer/KafkaProducer outgoing bytes.
- `taskchain.backpressure`: current backpressure time metrics.
- `source.current_offset`: Kafka source current offset metrics.
- `source.committed_offset`: Kafka source committed offset metrics.
- `source.records_lag`: Kafka consumer lag metrics, when exposed.
- `source.assigned_partitions`: assigned partition count, when exposed.
- `lookup.records_in`: LookupJoin operator input counter.
- `lookup.records_in_rate`: LookupJoin operator input rate; use this as actual lookup QPS.
- `lookup.records_out`: LookupJoin operator output counter.
- `lookup.records_out_rate`: LookupJoin operator output rate.
- `lookup.cache_hit_rate`: LookupJoin cache hit ratio when the connector exposes it.
- `paimon.writer.records_in`: records entering a Paimon writer.
- `paimon.writer.records_in_rate`: writer input rate.
- `paimon.writer.buffer_writers`: Paimon writer buffer writer count.
- `paimon.writer.buffer_preempt_count`: writer buffer preempt count.
- `paimon.compaction.busy`: compaction thread busy percentage.
- `paimon.compaction.completed_count`: completed compaction count.
- `paimon.compaction.level0_file_count`: level-0 file backlog.
- `paimon.compaction.total_file_size`: total compaction file size.
- `paimon.compaction.input_size`: compaction input size.
- `paimon.compaction.output_size`: compaction output size.
- `paimon.compaction.time`: compaction time.
- `paimon.commit.duration`: commit duration max/mean style metrics.
- `paimon.commit.duration_p99`: commit duration p99.
- `paimon.commit.files_added`: files added in the last commit.
- `paimon.commit.files_appended`: files appended in the last commit.
- `paimon.commit.files_deleted`: files deleted in the last commit.
- `paimon.commit.partitions_written`: partitions written in the last commit.
- `paimon.commit.buckets_written`: buckets written in the last commit.
- `paimon.commit.snapshots`: snapshots generated in the last commit.
- `paimon.commit.attempts`: commit attempts for the last commit.

Use sink semantic metrics for terminal sinks. WebUI `Records Sent` on a sink task chain can be `0` because there is no downstream Flink operator; it is not the same as records written to Kafka or an external sink.

## Capacity / Parallelism Diagnosis

`job capacity` and `diagnose parallelism` combine the REST endpoints that are usually needed for "is this parallelism enough?" questions:

- `jobs/<jobid>` for vertices, configured parallelism, maxParallelism, and task-chain totals.
- `jobs/<jobid>/vertices/<vertexid>/backpressure` for current WebUI backpressure ratios.
- `jobs/<jobid>/vertices/<vertexid>/subtasks/<index>/metrics` for per-subtask throughput, busy, idle, backpressure time, bytes rate, and checkpoint start delay.
- `jobs/<jobid>/vertices/<vertexid>/taskmanagers` for TaskManager row aggregates and subtask placement.
- `jobs/<jobid>/checkpoints` for recent checkpoint trend.
- Paimon metric endpoints when Paimon writer/committer metrics are detected.
- LookupJoin operator metrics and bounded TaskManager log grep when HBase/lookup joins are detected.

Capacity interpretation:

- `busyTimeMsPerSecond` near 1000 with low idle means the task chain may be CPU/operator limited.
- `backPressuredTimeMsPerSecond` or WebUI backpressure ratio means downstream pressure; follow the graph downstream before increasing upstream parallelism.
- low busy, high idle, and zero backpressure means increasing parallelism is unlikely to improve current throughput.
- subtask skew ratio above 3 is reported as skew and should be traced to source partitions, key distribution, bucket/partition design, or data skew.
- Paimon Writer `write-records` is treated as internal committable flow. `job io-flow` reports it as `sink_writer_internal_records` instead of `filters_or_drops_records`.
- Paimon Global Committer with parallelism 1 is expected unless commit duration, commit attempts, or file-count metrics show sustained pressure.
- LookupJoin reports both `chain_records_in_rate_sum` and `actual_lookup_records_in_rate_sum`; use the latter as HBase lookup QPS.
- Lookup cache hit near 0 is a risk signal only. Treat it as a bottleneck only when busy, backpressure, or HBase timeout/retry/log evidence is present.

## HBase / LookupJoin Metrics

`diagnose lookup` and `diagnose hbase-lookup` are read-only playbooks for HBase dimension table joins:

- scan `jobs/<jobid>.vertices[]` and `jobs/<jobid>/vertices/<vertexid>/metrics` for `LookupJoin`.
- read operator metrics such as `<subtask>.LookupJoin[7].numRecordsInPerSecond`, `numRecordsOutPerSecond`, and `lookupCacheHitRate`.
- keep task-chain input rate separate from actual LookupJoin input rate so a high upstream chain input is not misreported as HBase QPS.
- combine lookup QPS, cache hit, subtask skew, busy/idle, backpressure, checkpoint trend, and HBase warning patterns into one conclusion.

Risk labels:

- `lookup_cache_miss`: cache hit rate is near zero; this is a warning but not a bottleneck by itself.
- `lookup_skew`: LookupJoin QPS skew ratio is at least 3.
- `lookup_bottleneck`: actual lookup traffic is present and there is busy, backpressure, or HBase/lookup log anomaly evidence.
- `lookup_metrics_missing`: task chain looks like LookupJoin but operator-level metrics were not exposed.

## Paimon Connector Metrics

Paimon diagnosis uses the same read-only REST endpoints as generic metric commands:

- `jobs/<jobid>` discovers all vertices.
- `jobs/<jobid>/vertices/<vertexid>/metrics` lists metric ids and returns aggregate values with `?get=...`.
- `jobs/<jobid>/vertices/<vertexid>/subtasks/<index>/metrics` supports per-subtask skew checks for writer throughput and compaction metrics.

Role detection is metric-driven:

- Writer: `paimon.table...writerBuffer.*`, `paimon.table...compaction.*`, or matching writer input/rate metrics.
- Committer: `paimon.table...commit.*`.
- Source: `paimon.table...source.*` or `paimon.table...scan.*`.
- Unknown: no Paimon metric family matched.

`job connectors` reports every vertex with `paimon_role`, metric evidence, and whether it appears to be an actual upstream source. `diagnose paimon` combines the first detected writer and committer summaries, then emits risk labels:

- `commit_slow`: `commitDuration_p99` or max/mean duration is at least 30 seconds, or `lastCommitAttempts` is greater than 1.
- `small_files_risk`: many files, partitions, or buckets are touched by the last commit.
- `compaction_busy`: `compactionThreadBusy` is at least 80 or `level0FileCount` is at least 50.
- `writer_skew`: a writer or compaction metric has subtask skew ratio at least 3.
- `source_absent`: no Paimon source metric was found; use source commands on the listed actual source vertex, often Kafka.

## Output Summaries

Summary commands format bytes and durations into human-readable units:

- bytes: `B`, `KiB`, `MiB`, `GiB`, `TiB`
- durations: `ms`, `s`, `min`

Checkpoint summaries include state backend, storage, interval, timeout, max concurrency, latest completed checkpoint id, size, duration, and external path.

TaskManager memory summaries include heap, non-heap, metaspace, direct, managed, shuffle/netty, network segment, GC, CPU, and thread-count fields when the deployment exposes those metrics.

## IO Flow / Filtering Diagnosis

`job io-flow` reads `jobs/<jobid>.vertices[].metrics` and summarizes every task chain:

- `records_in`: `read-records`
- `records_out`: `write-records`
- `records_delta`: `records_in - records_out`
- `pass_through_pct`: `records_out / records_in * 100`
- `diagnosis`: `filters_or_drops_records`, `expands_records`, `source_or_generated_records`, `sink_writer_internal_records`, `sink_committer_terminal`, `stops_here_or_terminal_sink`, `passes_through`, or `missing_metrics`
- `largest_filter_drop`: largest non-terminal filtering/drop stage

Use this before drilling into a specific task chain. If the suspicious row is a terminal sink, follow with `task-chain sink-stats` or `task-chain paimon-stats`; task-chain `Records Sent = 0` is expected for many terminal sinks. Paimon Writer rows with much lower `write-records` than `read-records` should not be treated as business drops without connector-specific evidence.

## Health, Skew, Trend, And Logs

`job health` aggregates:

- job status and id
- `job io-flow` largest filtering/drop stage
- backpressure ranking
- checkpoint summary
- exception history
- `taskmanager memory-top`

`job skew` and `task-chain skew` fetch selected metrics for all subtasks and compute `min`, `max`, `avg`, `max_subtask`, `skew_ratio`, and top subtasks per metric.

`backpressure --samples N --interval S` repeats backpressure sampling and reports how many samples each vertex was high in. Use this when a WebUI backpressure page shows high but current metrics look idle.

`job checkpoint-trend` summarizes recent checkpoint history: completed/failed count, average/max duration, first/last state size, and state-size growth.

`metric analyze` is for chart-like questions that need memory across a time window:

- `checkpoint_state_spike`: busy peaks have no backpressure and correlate with checkpoint or state-related signals.
- `backpressure_spike`: busy peaks also show `backPressuredTimeMsPerSecond`.
- `periodic_busy_spike_without_backpressure`: repeated busy peaks without downstream pressure.
- `busy_spike_without_backpressure`: a short busy peak was observed, but the window is too short to prove periodicity.

`logs scan` tails the chosen JobManager or TaskManager log and counts configured patterns, with sample lines. Keep `--tail-bytes` conservative for large logs.

Large-log diagnostics use the same read-only endpoints and preserve bounded output by default:

- `logs diagnose`: resolves the log path, reads only the bounded tail unless `--full` is set, summarizes repeated error signatures, and returns recommended next commands.
- `logs grep`: streams full logs only with explicit `--full`; otherwise it greps the bounded tail and returns matching lines plus `--before` / `--after` context, capped by `--max-matches`.
- `logs errors`: groups `ERROR`, `WARN`, `Exception`, `Caused by`, `OutOfMemoryError`, `CheckpointException`, `TimeoutException`, and backpressure-style lines by normalized signature, capped by `--max-signatures`.
- `logs download`: writes to a local file only. It refuses an unbounded download unless `--max-bytes` or explicit `--full` is provided, and outputs only path, byte count, truncation flag, and sha256.

`logs grep`, `logs scan`, and `logs errors` support `--scope taskmanager --all-taskmanagers`. TaskManagers are enumerated through `taskmanagers`, fetched with the shared `httpx.AsyncClient` connection pool, and bounded by `--concurrency` plus `--tail-bytes`.

Use `--patterns hbase-lookup` for HBase lookup joins. The preset expands to `HBaseConfigurationUtil`, `TimeoutException`, `RetriesExhausted`, `ScannerTimeout`, and `CallTimeout`. The `ERROR` pattern is case-sensitive, so a WARN line containing `Error while ...` is not counted as an ERROR-level event.

When no `--output-dir` is supplied, downloads go under the skill's `.downloads/` directory, which is excluded by `.ferryignore`.

## Job Graph Summary

The job overview graph should be reconstructed from REST data, not OCR:

- `jobs/<jobid>` for vertices, task counts, duration, status, and vertex metrics
- `jobs/<jobid>/plan` for graph nodes and edges
- `jobs/<jobid>/vertices/<vertexid>/backpressure` for backpressure ratios
- selected subtask metrics for busy/idle/backpressured throughput

## Logs And Stdout

The current internal proxy does not honor `Range` or `start/length` for log file endpoints. Always stream and keep only the last `--tail-bytes` bytes locally.

Stdout uses the same TaskManager route as the WebUI tab:

- `taskmanagers/<tmid>/stdout` for one TaskManager.
- `stdout tail --all-taskmanagers` first lists `taskmanagers`, then fetches all selected stdout endpoints concurrently through the shared `httpx.AsyncClient`.
- `stdout watch` does not click the WebUI refresh button. It repeats REST reads, keeps a small local state per TaskManager, diffs the new response against the prior tail window, and emits only new lines with the TaskManager id.
- `--max-bytes-per-poll` caps each poll when range requests are unavailable; this protects the client and network at the cost of possibly seeing only the bounded prefix/tail window returned by the proxy.
- `--duration`, `--polls`, and `--max-events` bound watch sessions.

In Kubernetes mode stdout can be unavailable:

- `taskmanagers/<tmid>/stdout` may return explanatory text.
- `jobmanager/stdout` may return 404.

Report this as `available: false` with `reason: kubernetes_stdout_missing` when the endpoint returns the Kubernetes explanatory text. `stdout download` must not save this placeholder as a local stdout file; instead return a `kubectl logs <pod>` recommendation.

## FlameGraph

Verified Flink 1.18 endpoint:

```text
jobs/<jobid>/vertices/<vertexid>/flamegraph?type=full|on_cpu|off_cpu&subtask=<index>
```

WebUI `mixed` maps to REST `type=full`. Subtask selection is a query parameter, not a nested path. Default CLI output should summarize top stack nodes.
