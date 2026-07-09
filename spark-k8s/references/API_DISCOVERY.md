# Spark Web UI API Discovery

This document records the REST API mapping used by `spark-k8s/scripts/spark_diag.py`.
The target baseline is Spark 3.5.2, but URLs and versions must stay parameterized.

## Verified Shapes

Running Spark UI:

- Page example: `https://spark-k8s-us.tuya-inc.com:7799/spark-2061228901727735842/jobs/`
- REST base: `https://spark-k8s-us.tuya-inc.com:7799/spark-2061228901727735842/`
- Applications endpoint: `api/v1/applications`

Spark History Server:

- Root example: `https://spark-k8s-historyserver-us.tuya-inc.com:7799/`
- App page shape: `/history/<appId>/jobs/`, `/history/<appId>/stages/`, `/history/<appId>/executors/`
- REST base: `https://spark-k8s-historyserver-us.tuya-inc.com:7799/`
- REST app endpoint shape: `api/v1/applications/<appId>/...`

Some History Server HTML pages can redirect to an `http://...:7799/...` URL and
then fail with `400 The plain HTTP request was sent to HTTPS port`. The CLI
therefore keeps the original scheme/host from the user-provided URL and relies
on REST JSON for diagnostics.

## Core REST Endpoints

All paths are relative to the resolved REST base:

- `api/v1/applications`
- `api/v1/applications/<appId>`
- `api/v1/applications/<appId>/jobs`
- `api/v1/applications/<appId>/jobs?status=running`
- `api/v1/applications/<appId>/stages`
- `api/v1/applications/<appId>/stages?status=active`
- `api/v1/applications/<appId>/stages/<stageId>`
- `api/v1/applications/<appId>/stages/<stageId>/<attemptId>`
- `api/v1/applications/<appId>/stages/<stageId>/<attemptId>/taskList`
- `api/v1/applications/<appId>/stages/<stageId>/<attemptId>/taskSummary`
- `api/v1/applications/<appId>/executors`
- `api/v1/applications/<appId>/allexecutors`
- `api/v1/applications/<appId>/storage/rdd`
- `api/v1/applications/<appId>/environment`
- `api/v1/applications/<appId>/sql`
- `api/v1/applications/<appId>/streaming/statistics`

If an endpoint is unavailable, commands should return an `available:false` style
payload or degrade to a partial summary instead of crashing.

## Dynamic Identifiers

- `appId`: parsed from `/history/<appId>/...`, provided by `--app-id`, or discovered from `api/v1/applications`.
- `jobId`: provided by `--job-id` or parsed from query parameters such as `?jobId=...`.
- `stageId`: provided by `--stage-id` or parsed from `?stageId=...`.
- `attemptId`: provided by `--attempt-id`, parsed from `?attempt=...`, or defaults to `0` for taskList requests.
- `executorId`: provided by `--executor-id` or parsed from executor page query context when present.
- `sqlId`: provided by `--sql-id` or parsed from SQL query context when present.

When a History Server root has many applications, require a selector such as
`--app-id`, `--app-name`, or `--app-index`.

## Version Detection

Spark version is detected from:

- `api/v1/applications[].attempts[].appSparkVersion`
- `api/v1/applications/<appId>.attempts[].appSparkVersion`

The CLI supports `--spark-version auto|3.5.2|...` and maps detected minor
versions to conservative endpoint profiles like `spark-3.5`. For unknown
versions, use the generic profile and rely on graceful endpoint fallback.

## Diagnostics Mapping

`overview` gathers:

- application detail
- jobs
- stages
- executors
- SQL executions

`jobs` maps to job summaries:

- status counts
- failed job/task counts
- task counts
- stage id references
- `--brief` should be used for fast job count/duration questions. It excludes
  long SQL descriptions and returns only status, submission/completion time,
  calculated duration, stage ids, and task counts.
- `jobs timeline` adds ordering, makespan, and idle gaps from submission and
  completion timestamps.
- `jobs idle-gaps` focuses on long app sessions: total job wall time, total idle
  time, idle ratio, top idle gaps, and regular gap detection such as repeated
  5/10/15 minute submissions.

`duration jobs` summarizes job duration distribution:

- duration is calculated from `submissionTime` and `completionTime`
- output includes min, max, avg, p50, p75, p90, p95, p99
- `max_vs_median` is used as a long-tail signal
- returns top slow and fastest jobs

`stages` maps to stage summaries:

- status counts
- failed task counts
- executor runtime
- input bytes
- shuffle read/write
- memory/disk spill
- GC time

`stages shuffle` is the preferred fast path for page-level shuffle statistics:

- it reads the already aggregated fields in `api/v1/applications/<appId>/stages`
- it uses top-level `shuffleReadBytes` and `shuffleWriteBytes`
- it returns total shuffle read/write, per-stage read/write, and top shuffle stages
- it does not fetch `taskList`; use `stage tasks` only when task-level skew is needed
- use `--limit <n>` on very large applications when only top stages are needed

`stages io` is the preferred fast path for data movement:

- it reads stage-list fields such as `inputBytes`, `outputBytes`,
  `shuffleReadBytes`, and `shuffleWriteBytes`
- it returns total input/output/shuffle and top stages by each dimension
- `stages io --by-executor --stage-id <id>` fetches that stage attempt's
  `taskList?length=<estimated>` and groups input bytes, shuffle read/write,
  duration, GC, and failed/killed task counts by executor
- use `--by-executor` when executor-page `Input` looks sparse and you need to
  confirm whether later executors were actually doing shuffle-read work

`duration stages` summarizes stage duration distribution:

- duration uses stage-list `executorRunTime`
- output includes duration percentiles, slowest stages, fastest stages, and a
  long-tail classification

`stages wall-time` summarizes elapsed stage time separately:

- wall duration is calculated from `completionTime - submissionTime`
- executor runtime remains the stage-list `executorRunTime`
- estimated parallelism is `executorRunTime / wall_duration`
- output includes numTasks, average task duration, throughput, top wall-time
  stages, and fastest stages

`stage tasks --brief` keeps task-list diagnostics compact:

- it returns task count, skew metrics, and top slow tasks
- it omits full task payloads unless the caller requests a full command without `--brief`

`duration tasks` and `skew duration` use `taskList` for one stage attempt:

- duration distribution: min, max, avg, p50, p90, p95, p99
- slow task evidence: task id, executor id, host, input bytes, shuffle read/write,
  GC time, GC ratio, memory/disk spill, and byte throughput
- recommendations distinguish data skew, shuffle skew, GC pressure, spill, and
  possible external IO or write-side tail latency
- failed and killed attempts are excluded from duration/skew math by default to
  avoid false positives from recovered `ExecutorLostFailure` attempts; use
  `--include-failed-attempts` when investigating failed attempts themselves
- the CLI requests `taskList?length=<estimated>` using stage summary counts so
  rows beyond Spark's default first page are included
- `--all-stages` fetches multiple `taskList` endpoints and should be used with
  `--limit-stages` because task-level payloads are larger than page summaries

`stages tasks` maps to `taskList`:

- duration skew
- input byte skew
- shuffle read/write skew
- GC time skew
- slowest or largest tasks

`stages retries`, `stage retries`, and `diagnose retries` use stage-list rows and
job rows:

- stage attempts are grouped by `stageId` and sorted by `attemptId`
- repeated attempts are classified as `recovered_stage_attempt`,
  `stage_retry_without_failure`, or `fatal_stage_failure`
- job `stageIds` and terminal job statuses help identify whether a failed retry
  is fatal
- these commands do not fetch `taskList`, so they are the preferred fast path
  when the page-level question is whether retries recovered

`stages failures`, `stage failures`, and `diagnose failures` combine stage-list
rows with bounded `taskList` scans:

- default selection scans failed/killed stages, stages with `numFailedTasks > 0`,
  and then top slow stages up to `--limit-stages`
- `--all-stages` broadens the scan and should be paired with `--limit-stages`
- failed task rows keep task id, index, attempt, executor id, host, duration,
  error message, GC, spill, and shuffle metrics
- error details are classified into stable reasons such as `fetch_failed`,
  `executor_lost`, `out_of_memory`, `python_exception`, `file_not_found`, and
  `task_killed`
- the Spark HTML stage page can use `task.sort=Status` to bring failed tasks to
  the top, but the REST `taskList` endpoint does not reliably apply that UI sort;
  the CLI requests enough rows from stage summary counts and filters failures
  locally
- use these commands when the question starts from failed stage attempts or task
  error details; use `executors health` first for executor capacity, memory, GC,
  removed executor, or executor-level failed task counts

`executors` maps to executor summaries:

- active/inactive executor counts
- cores and task counts
- failed tasks
- GC time
- memory usage percentage
- shuffle read/write
- exposed log URLs

`executors gc` is the preferred fast path for GC questions:

- total GC time
- total executor duration
- GC ratio
- top GC executors
- add `--all` to use `allexecutors`, which includes removed executors in History
  Server apps and avoids missing GC or duration from executor churn

`executors health` is the preferred fast path for executor-page health:

- reads `executors` or `allexecutors`
- reports total failed tasks, inactive executors, removed executors, remove
  reason counts, top failed executors, top GC executors, and top memory executors
- does not fetch taskList by default

`executors failed-tasks`, `executor tasks`, and `diagnose task-health` correlate
executor health with taskList:

- stage selection defaults to failed stages plus top slow stages
- selected stages use `taskList?length=<estimated>` from `numTasks`,
  completed-task counts, failed tasks, and killed tasks; this avoids missing
  failed rows when the REST endpoint's default page returns only the first 20
  tasks
- `--all-stages` intentionally broadens the scan and should be paired with
  `--limit-stages`
- task health is grouped by `executorId` and includes failed/killed/success task
  counts, slow task samples, GC ratio, spill, input, and shuffle totals
- `--include-logs` scans exposed executor logs only when explicitly requested

`executors churn` uses `allexecutors` first, then falls back to `executors`:

- executor add events come from `addTime`
- executor remove events come from `removeTime` and `removeReason`
- `--stage-id` and optional `--attempt-id` add stage-overlap calculations
- output includes removed executor count, removed-during-stage count, remove
  reason distribution, host distribution, lifecycle timeline, per-executor
  lifetime, and stage overlap
- each executor gets a `stage_relation` classification such as
  `removed_during_stage`, `added_during_stage`, `spans_stage`, `before_stage`,
  `after_stage`, or `overlaps_stage`
- classifications include `framework_deleted_executors`,
  `executor_lost_recovered`, `node_hotspot`, and `unknown`

`diagnose input-distribution` combines stage-list IO, `allexecutors`, and
environment properties:

- explains sparse executor `Input` by comparing external-input stages with
  shuffle-read-only stages
- reports executors with nonzero external input versus executors with zero input
  but nonzero shuffle read
- includes allocation evidence such as `spark.executor.instances`,
  `spark.executor.cores`, and `spark.dynamicAllocation.enabled`
- classify common cases such as `shuffle_only_later_stage`, where only the early
  source-read stage contributes executor input bytes and later stages only read
  shuffle

`diagnose parallelism` combines stage wall time, `allexecutors`, and environment
properties:

- calculates effective stage parallelism from `executorRunTime / wall_duration`
- compares it with configured executor instances and cores
- flags `low_static_parallelism` when static executor allocation is small, for
  example `spark.executor.instances=2`, `spark.executor.cores=1`, and dynamic
  allocation disabled
- use it after `duration tasks` shows no strong successful-task skew but a stage
  still has long wall time

`diagnose executor-loss` combines stage rows, jobs, all executors, and a complete
taskList for the requested stage:

- failed task errors are classified into stable reasons such as
  `executor_lost`
- failed tasks are matched to removed executor ids and host distribution
- executor remove events are checked against the stage interval
- recovered status is inferred from a complete/succeeded stage or succeeded
  related jobs
- recommendations point to resource reclaim, dynamic allocation, Kubernetes pod
  deletion, or node health before treating recovered executor loss as SQL logic
  failure

`timeline events` uses `allexecutors` first, then falls back to `executors`:

- executor add events come from `addTime`
- executor remove events come from `removeTime` and `removeReason`
- these events are useful for explaining resource churn during long SQL runs

`sql` maps to SQL execution rows:

- status
- duration
- description and plan snippets
- running/succeeded/failed job IDs
- `--limit <n>` maps to `api/v1/applications/<appId>/sql?length=<n>` because
  Spark's default SQL REST page can return only a small first page.

`sql failures` combines REST SQL rows with the Spark SQL HTML failed table:

- REST rows provide status, SQL id, description, duration, and related job ids.
- The HTML SQL tab can expose `Error Message`, which Spark REST details may not
  include.
- Failed rows are grouped by table, statement type, and error message.
- Hive metastore partition metadata failures are called out with `msck repair
  table` guidance.

`sql ddl-summary` focuses on DDL batches:

- DDL count, failed count, completed count
- table distribution
- partition key/value distribution such as `dt` and `hour`
- whether DDL rows mostly have no Spark job ids, avoiding a false compute
  slowness diagnosis

`sql plan --operators` extracts physical plan structure:

- counts `Scan`, `Filter`, `Exchange`, `BroadcastHashJoin`, `Window`, `Sort`,
  `Repartition`, and related operators
- returns key plan lines instead of dumping full plans

`sql analyze` combines SQL plan, jobs, stages, executors, and shuffle data to
classify likely bottlenecks such as scan, shuffle, GC, window/sort, or write-side
tail latency.

`timeline events --sql-id <id>` merges event sources into one sorted line:

- SQL execution start/end/duration from `sql` rows
- related job durations from SQL job id fields such as `succeededJobIds`
- if SQL job id fields are unavailable, jobs and stages are matched by time overlap with the SQL execution window
- related stage durations from job `stageIds` or SQL time overlap
- executor add/remove events from `allexecutors`

`environment` maps to environment/config payloads and redacts sensitive keys.
`environment get --key <name>` resolves a single value by REST path or UI label;
for example `Scala Version` maps to `runtime.scalaVersion`.

`storage` maps to cached RDD information.

`logs scan` uses exposed executor log URLs from executor payloads when present.
It scans for `ERROR`, `WARN`, `Exception`, `OutOfMemoryError`, `GC`, `Shuffle`,
and `FetchFailed` by default.

`health` combines app, jobs, stages, executors, SQL, and optional logs into a
risk list. Initial risk heuristics flag failed jobs/stages/tasks, inactive
executors, failed SQL executions, and risky log patterns.

`diagnose speed` is the default lightweight performance playbook. It runs:

- `jobs --brief`
- `jobs timeline`
- long-app summary with app duration, job active time, idle ratio, SQL failure
  count, and primary bottleneck such as `session_idle` or
  `metadata_ddl_failures`
- `duration jobs`
- `duration stages`
- `stages wall-time`
- executor capacity summary with top wall-time stage, estimated parallelism,
  churn during that stage, and failed task reason counts when available
- `stages io`
- `stages shuffle`
- `executors gc`
- `sql analyze`
- `timeline events` separately when event ordering or executor churn matters

`diagnose long-app` is the default playbook when app duration looks much longer
than actual Spark work:

- compares app attempt duration with summed job wall time
- reports top inter-job idle gaps and regular gap seconds
- requests an extended SQL REST page by default for SQL failure count
- classifies runtime as `session_idle_dominant`, `compute_dominant`, or `mixed`
- flags Kyuubi/Thrift/engine application names as `interactive_engine_session`

`diagnose task-health` is the default executor/task health playbook. It runs:

- `executors health`
- bounded `taskList` scans from failed/top slow stages
- task grouping by executor and host
- optional executor log scan via `--include-logs`

`diagnose retries` is the default stage retry playbook. It runs:

- stage attempt grouping from `stages`
- final status classification for retried stage ids
- retry risk recommendations without fetching task lists

`diagnose failures` is the default stage/task failure playbook. It runs:

- bounded `taskList` scans from failed/top slow stages
- fatal vs recovered stage/task failure classification
- dominant error reason aggregation and next-step recommendations

## Read-Only Boundary

The CLI must not expose any side-effect command. Forbidden verbs include:

- stop
- kill
- cancel
- delete
- submit
- update
- modify
- write

Only read REST JSON, environment/config data, and exposed logs.
