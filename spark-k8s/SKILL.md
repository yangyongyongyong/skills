---
name: spark-k8s
description: Diagnose Spark on Kubernetes Web UI and Spark History Server jobs through read-only Spark REST APIs. Use when the user mentions Spark UI, Spark History Server, Spark on K8s, jobs, stages, SQL executions, executors, skew, logs, duration, retries, failures, or resource health.
---

# Spark K8s Diagnostics

Use this skill when diagnosing Spark on Kubernetes Web UI or Spark History Server
pages, especially for job status, stages, SQL executions, executors, resource
usage, skew, logs, and read-only health checks.

## What This Provides

- A local CLI: `spark-k8s/scripts/spark_diag.py`
- Spark REST API access through running Spark UI or History Server.
- Browser cookie reuse through `chrome-cdp-ws-daemon` when the internal UI needs authentication.
- Parameterized URLs and Spark versions; do not hardcode company domains or app ids.
- Read-only diagnostics only. Do not add or run stop, kill, cancel, delete, submit, or config mutation commands.

## Common Workflow

Start by resolving the URL:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py resolve \
  --url "https://spark-k8s-us.tuya-inc.com:7799/spark-2061228901727735842/jobs/" \
  --json
```

Inspect a running or historical application:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py overview \
  --url "$SPARK_WEB_URL" \
  --json
```

List History Server applications:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py applications \
  --url "https://spark-k8s-historyserver-us.tuya-inc.com:7799/" \
  --limit 10 \
  --json
```

## Fast Statistics

Prefer these lightweight commands for quick questions. They read the same REST
summary data that backs the Spark UI pages and avoid printing long SQL plans or
task lists.

Job count and duration:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py jobs \
  --url "$SPARK_WEB_URL" \
  --brief \
  --json
```

Stage shuffle totals:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages shuffle \
  --url "$SPARK_WEB_URL" \
  --json
```

End-to-end speed diagnosis:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose speed \
  --url "$SPARK_WEB_URL" \
  --json
```

Long-duration app diagnosis:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose long-app \
  --url "$SPARK_WEB_URL" \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py jobs idle-gaps \
  --url "$SPARK_WEB_URL" \
  --top 20 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py sql failures \
  --url "$SPARK_WEB_URL" \
  --limit 1000 \
  --json
```

When an app duration is very long, first check whether `diagnose long-app`
classifies it as `session_idle_dominant`, especially for Kyuubi/Thrift/engine
application names. If SQL failures dominate, use `sql failures` and `sql
ddl-summary` before treating the case as Spark compute slowness.

Event timeline for SQL, jobs, stages, and executor add/remove events:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py timeline events \
  --url "$SPARK_WEB_URL" \
  --sql-id 5 \
  --limit 100 \
  --json
```

Use `timeline events` when the question is about a SQL execution's start/end
time, whether executor add/remove happened during the job, or whether the task
was delayed by serial waits between jobs/stages.

Duration distribution and skew checks:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py duration jobs \
  --url "$SPARK_WEB_URL" \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py duration stages \
  --url "$SPARK_WEB_URL" \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages wall-time \
  --url "$SPARK_WEB_URL" \
  --top 20 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose parallelism \
  --url "$SPARK_WEB_URL" \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py duration tasks \
  --url "$SPARK_WEB_URL" \
  --stage-id 6 \
  --attempt-id 0 \
  --json
```

Use `stages wall-time` when the question is elapsed stage time versus cumulative
executor runtime; it reports wall duration, executor runtime, estimated
parallelism, average task duration, and throughput. Use `duration tasks` or
`skew duration` when checking task long tail. Failed and killed task attempts
are excluded from duration skew by default so recovered executor-loss attempts
do not create false skew; add `--include-failed-attempts` only when failed
attempt duration is itself the target. These commands request enough `taskList`
rows from stage summary counts before filtering locally, so failed tasks beyond
Spark's default first page are not missed. Use `--all-stages --limit-stages N`
only when you intentionally want to fetch task lists for multiple stages. Use
`diagnose parallelism` when wall time is long but task durations are not skewed;
it combines stage wall time, effective parallelism, executor count, and Spark
allocation settings.

Executor/task health checks:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py executors health \
  --url "$SPARK_WEB_URL" \
  --all \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py executors failed-tasks \
  --url "$SPARK_WEB_URL" \
  --limit-stages 10 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py executors churn \
  --url "$SPARK_WEB_URL" \
  --stage-id 6 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose input-distribution \
  --url "$SPARK_WEB_URL" \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose executor-loss \
  --url "$SPARK_WEB_URL" \
  --stage-id 6 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose task-health \
  --url "$SPARK_WEB_URL" \
  --limit-stages 10 \
  --json
```

Use `executors health` for fast executor-page signals such as failed tasks, GC,
memory, inactive/removed executors, and remove reasons. When many executor-page
failed tasks appear with inactive or removed executors, run `executors churn`
with the slow or failed `--stage-id`, then `diagnose executor-loss` to check
whether task failures are `ExecutorLostFailure`, whether the executor was
removed during the stage, whether failures recovered, and whether they
concentrate on one host. Use `executors failed-tasks`, `executor tasks`, or
`diagnose task-health` when you need broader taskList correlation by
executor/host. Add `--include-logs` only when log samples are needed.

When only a few executors show nonzero `Input`, run `diagnose
input-distribution` first. Spark executor `Input` counts external source reads,
while later reduce/write stages often show `Input=0` and nonzero `Shuffle Read`.
For task-level confirmation, run:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages io \
  --url "$SPARK_WEB_URL" \
  --stage-id 6 \
  --by-executor \
  --json
```

`executors churn` also reports each executor's relation to the selected stage
(`removed_during_stage`, `added_during_stage`, `spans_stage`, etc.), which helps
distinguish late executor replacement from executors that were available during
the input stage.

Stage retry and failure checks:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages retries \
  --url "$SPARK_WEB_URL" \
  --limit-stages 20 \
  --top 20 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stage failures \
  --url "$SPARK_WEB_URL" \
  --stage-id 12 \
  --attempt-id 0 \
  --top 20 \
  --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py diagnose failures \
  --url "$SPARK_WEB_URL" \
  --limit-stages 20 \
  --top 20 \
  --json
```

Use `stages retries`, `stage retries`, or `diagnose retries` when the stage page
shows repeated attempts and you need to know whether retries recovered or ended
in a final failed stage. Use `stages failures`, `stage failures`, or `diagnose
failures` when failed tasks/stages need error reason aggregation. Prefer
`executors health` first when the question is executor capacity, GC, memory, or
removed executors; use stage failure commands when the question starts from
stage attempts, failed tasks, or task error details.

Spark's stage page can sort the task table by Status to bring failed tasks to
the top. The REST `taskList` endpoint does not reliably honor that page sort, so
the CLI requests enough task rows from stage summary counts and filters failed
tasks locally.

For large jobs, add `--limit 20` when only the top shuffle stages or timeline
events are needed.

Use the full `jobs`, `stages`, `stage tasks`, or `sql` commands only when the
user asks for SQL text, plan details, task-level skew, or full diagnostic
payloads.

Check jobs, stages, and executors:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py jobs --url "$SPARK_WEB_URL" --status all --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages --url "$SPARK_WEB_URL" --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py executors top --url "$SPARK_WEB_URL" --json
```

The singular aliases also work when they read more naturally:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py job show --url "$SPARK_WEB_URL" --job-id 3 --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stage tasks --url "$SPARK_WEB_URL" --stage-id 12 --attempt-id 0 --brief --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py executor top --url "$SPARK_WEB_URL" --json
```

Analyze a specific stage's task skew:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py stages tasks \
  --url "$SPARK_WEB_URL" \
  --stage-id 12 \
  --attempt-id 0 \
  --json
```

Get SQL and environment information:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py sql --url "$SPARK_WEB_URL" --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py sql plan --url "$SPARK_WEB_URL" --operators --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py sql analyze --url "$SPARK_WEB_URL" --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py environment --url "$SPARK_WEB_URL" --json
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py environment get --url "$SPARK_WEB_URL" --key "Scala Version" --json
```

Run a one-shot health report:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py health --url "$SPARK_WEB_URL" --json
```

Scan exposed executor logs:

```bash
/Users/luca/miniforge3/envs/py311/bin/python3.11 spark-k8s/scripts/spark_diag.py logs scan \
  --url "$SPARK_WEB_URL" \
  --patterns "ERROR,WARN,Exception,OutOfMemoryError,GC,Shuffle,FetchFailed" \
  --json
```

## URL And ID Handling

The CLI accepts:

- Running UI URLs like `https://host:7799/<deployment>/jobs/`.
- History Server roots like `https://history-host:7799/`.
- History app URLs like `https://history-host:7799/history/<appId>/jobs/`.
- Explicit `--base-url` and `--app-id` overrides.

For running UI pages, REST endpoints are resolved under `/<deployment>/api/v1/...`.
For History Server, REST endpoints are resolved under `/api/v1/...`. History page
URLs are used only to parse app id and tab context; diagnostic data should come
from REST JSON.

If the URL does not contain an app id, the CLI uses `api/v1/applications` and
requires the result to identify exactly one app. Use `--app-id`, `--app-name`,
or `--app-index` when multiple applications match.

## Diagnostics

- `jobs`: counts running, succeeded, failed, killed jobs and task failures.
- `jobs --brief`: quickly returns job count, status, duration, stage ids, and task counts without long descriptions.
- `jobs timeline`: returns compact job ordering, makespan, and idle gaps.
- `jobs idle-gaps`: summarizes job active time, idle time, idle ratio, top idle gaps, and regular submission gaps.
- `duration jobs`: returns job duration min/max/avg/percentiles and slowest/fastest jobs.
- `stages`: summarizes active/failed stages, task counts, shuffle/input/output, spill, and GC.
- `stages shuffle`: quickly returns stage-level shuffle read/write totals and top shuffle stages.
- `stages io`: quickly returns stage-level input/output/shuffle totals and top IO stages.
- `stages wall-time`: separates stage wall duration from cumulative executor runtime and estimates parallelism.
- `stages retries`: summarizes retried stage attempts, recovered retries, and final failed retries.
- `stages failures`: scans bounded taskList payloads for failed/killed tasks and aggregates error reasons.
- `stage retries`: drills into retry attempts for one stage id.
- `stage failures`: drills into failed/killed task details for one stage attempt or all attempts of a stage.
- `duration stages`: returns stage duration min/max/avg/percentiles and slowest/fastest stages.
- `duration tasks`: returns task duration distribution, top slow tasks, GC ratio, spill, throughput, and skew recommendations.
- `skew duration`: focused task duration skew report for one stage attempt.
- `stages tasks --brief`: fetches taskList and reports skew/top slow tasks without returning full task payloads.
- `executors`: summarizes active/inactive executors, failed tasks, memory use, GC, shuffle, and exposed log URLs.
- `executors gc`: quickly returns executor GC totals, GC ratio, and top GC executors.
- `executors health`: returns failed task totals, inactive/removed executors, remove reasons, top failed/GC/memory executors, and risks.
- `executors failed-tasks`: scans bounded stage task lists and groups failed/slow tasks by executor.
- `executors churn`: summarizes executor add/remove timeline, remove reasons, stage overlap, and framework/user deletes.
- `executor health`: focused executor health report for one `--executor-id`.
- `executor tasks`: focused task health report for one `--executor-id`.
- `diagnose task-health`: one-shot executor + taskList health diagnosis with optional log scan.
- `diagnose executor-loss`: correlates failed tasks with removed executors and classifies recovered executor lost failures.
- `diagnose retries`: explains whether stage retries are recovered noise or final failures.
- `diagnose failures`: explains fatal/recovered stage and task failures with dominant error reasons.
- `diagnose long-app`: explains whether long app duration is session idle, compute, or mixed, and flags interactive engine sessions.
- `sql`: lists SQL executions, status, duration, related jobs, and plan snippets.
- `sql --limit N`: requests `sql?length=N` to avoid Spark's default short SQL page.
- `sql failures`: groups failed SQL by table, statement type, and HTML error message.
- `sql ddl-summary`: summarizes partition DDL batches that may not create Spark jobs or stages.
- `sql plan --operators`: extracts operator counts and key physical plan lines.
- `sql analyze`: combines SQL plan, jobs, stages, shuffle, and executor GC into bottleneck recommendations.
- `environment get --key <name>`: fetches one environment value, including page labels such as `Scala Version`.
- `diagnose speed`: one-shot speed playbook using jobs timeline, stage wall time, executor capacity/churn, stage IO/shuffle, executor GC, and SQL analysis.
- `timeline events`: merges SQL execution, job, stage, and executor add/remove events into one sorted event line.
- `storage`: reads RDD/cache usage.
- `environment`: reads Spark environment and redacts sensitive keys.
- `logs scan`: tails exposed executor log URLs and scans diagnostic patterns.
- `health`: aggregates app, jobs, stages, executors, and SQL risks.

## Safety Notes

This skill is intentionally read-only:

- Allowed: REST JSON reads, exposed log reads, environment/config reads with redaction.
- Forbidden: stopping tasks, killing executors, cancelling jobs, deleting data, submitting jobs, or modifying configuration.
- Never print cookies, tokens, passwords, secrets, access keys, or authorization headers.

## Reference

See `spark-k8s/references/API_DISCOVERY.md` for endpoint mapping and known Spark
3.5.2 behavior.
