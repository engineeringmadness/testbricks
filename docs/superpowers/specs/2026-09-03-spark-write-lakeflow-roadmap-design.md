# Spark Write + Lakeflow Control-Flow Enhancement Roadmap

**Date:** 2026-09-03
**Status:** Proposed TODO backlog (design only, no implementation)
**Scope:** Two clusters only — (1) Lakeflow/Jobs control-flow fidelity in `LocalWorkflowRunner`, (2) `spark.write` / `DataFrameWriter` fidelity in `SparkProxy`. CSV + temp-view storage unchanged.

**Explicitly out of scope for this library (per owner decision):**
- `MERGE INTO`, `UPDATE`, `DELETE`
- Time travel (`VERSION AS OF`, `@v`), CDF, Delta log
- `spark.sql` DDL (`CREATE TABLE`, `CTAS`, `DROP TABLE`, `INSERT INTO ... SELECT`)
- UDFs / higher-order functions / new SQL function coverage

**Optimization goal:** Databricks fidelity first — local/CI runs should behave like real Lakeflow + Delta writes for the covered APIs, even where that costs a little speed.

## Current baseline

| Area | Today |
|---|---|
| `df.write.mode("overwrite"/"append").saveAsTable("schema.table")` | Works via `TableCatalog.save_dataframe`; default (no mode) = overwrite; append validates exact column-set match |
| `format()`, `partitionBy()`, `option("overwriteSchema"/"mergeSchema")` | Accepted, stored, mostly ignored; `partitionBy` is a silent no-op; `overwriteSchema` untested truth table |
| `insertInto`, `mode("error"/"errorIfExists"/"ignore")`, `bucketBy/sortBy`, `replaceWhere`, `writeTo` | Missing (AttributeError or wrong semantics) |
| File writes `df.write.csv(path)` | Works via native Spark under `base_path`; `parquet`/`json`/`save`/`format().save()` dispatch missing |
| `LocalWorkflowRunner` | DAG via `depends_on` + topological order; `base_parameters` seeded into `os.environ` + widget overrides; single shared namespace; no `run_if`, no condition/for_each tasks, no taskValues, no retries |
| `dbutils.notebook.run()` / `exit()` | Isolated vs shared namespaces exist; no `dbutils.jobs.taskValues` bridge |

Storage stays `{base_path}/{schema}/{table}.csv` + temp views `{schema}_{table}`. All TODOs below are thin shims on `DataFrameWriter`, `TableCatalog.save_dataframe`, and `LocalWorkflowRunner`.

## TODO list

### Cluster 1 — Lakeflow control flow (top priority, in order)

- [ ] **T1. `taskValues` propagation between tasks** — add `dbutils.jobs.taskValues.set(key, value)` / `.get(task_key, key)` backed by a runner-scoped dict, seeded from and visible to `base_parameters`; define semantics for shared-namespace `run_workflow` (visible immediately) vs isolated `dbutils.notebook.run()` (visible after return). Small dict + widget/env bridge, no persistence.
- [ ] **T2. `run_if` / `depends_on` outcome conditions** — support `depends_on[].outcome` / task-level `run_if` (`ALL_SUCCESS`, `ALL_FAILED`, `AT_LEAST_ONE_SUCCESS`, `ALL_DONE`, `NONE_FAILED`); skip non-eligible tasks with a clear `SKIPPED` status instead of running them; keep topological order. Pure scheduler logic in `_build_graphs` + `run_workflow` loop.
- [ ] **T3. `if/else` condition tasks** — support `condition_task` (`op`: `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, etc. over left/right operands referencing `taskValues`/params); route to the matching branch and mark the other branch skipped. Reuses T1 values + T2 skip machinery.
- [ ] **T4. `for_each` parameter expansion** — support `for_each_task` with `inputs` (literal list or `taskValues` reference) + `concurrency`; execute the nested notebook once per input with per-iteration params/env isolation; iterate sequentially first, parallel as a later stretch. Builds on executor `run_isolated` semantics.
- [ ] **T5. Repair-and-rerun + idempotent re-execution** — allow `run_workflow(only=["task_a"])` / `from_task` to re-run a subgraph; document that `overwrite` saves are naturally idempotent while `append` saves need a `repair` policy (skip, dedupe-key, or explicit overwrite flag). Runner-only change + docs, no catalog change.
- [ ] **T6. Per-task retries / timeouts** — parse `max_retries` / `min_retry_interval_millis` / `timeout_seconds` from workflow JSON; retry failed notebook tasks N times, accept-but-log timeout (matching current `dbutils.notebook.run(timeout)` behavior of accept-not-enforce, or enforce via signals as a stretch). Loop wrapper in `run_workflow`.

### Cluster 2 — `spark.write` fidelity (in order)

- [ ] **W1. Mode fidelity: `error` / `errorIfExists` + `ignore`** — `saveAsTable` with `error`/`errorIfExists` raises when CSV exists; `ignore` silently skips the write (no file touch, no view refresh). Few lines in `save_dataframe`; completes the Spark save-mode truth table alongside existing overwrite/append/default-overwrite.
- [ ] **W2. `insertInto(table, overwrite=False)`** — DataFrame API twin of append/overwrite `saveAsTable`; honor writer `mode` (`append` vs `overwrite`); require the table (CSV) to exist and raise a Spark-like AnalysisException message when missing; accept the `overwrite=True` kwarg for full-refresh semantics.
- [ ] **W3. `partitionBy` validation + `replaceWhere` dynamic overwrite** — validate `partitionBy` cols exist in the DataFrame (raise, don't silently ignore typos); support `.option("replaceWhere", "<predicate>")` with `mode("overwrite")` as overwrite-where-predicate: delete matching rows from stored CSV via pandas query, append the new frame. Full partition-overwrite layout stays out of scope (still one CSV).
- [ ] **W4. CSV write options** — honor `delimiter`/`sep`, `quote`, `escape`, `nullValue`, `dateFormat`, `timestampFormat` on both `saveAsTable` (persist options per table for round-trip reads) and `csv(path)` passthrough; store the effective options so `read.table` round-trips without callers repeating them.
- [ ] **W5. File-write dispatch: `parquet` / `json` / `save` + `format().save()`** — route `df.write.parquet(path)` / `.json(path)` / `.save(path)` and `.format("parquet"|"json"|"csv"|"delta").save(path)` under `base_path` using native Spark writers; `format("delta").save(path)` maps to parquet-on-disk (documented, no Delta log). Table APIs stay CSV; file APIs use real formats.
- [ ] **W6. `overwriteSchema` / `mergeSchema` truth table** — `overwriteSchema=true` + overwrite = replace file even on schema change (already true, add tests); `mergeSchema=true` + append = union missing columns with nulls via pandas instead of raising `SchemaMismatchError`; `overwriteSchema=false` + incompatible change = raise. Document the matrix in README.
- [ ] **W7. `bucketBy` / `sortBy` accepted no-ops** — accept and ignore (like `partitionBy` today) with an explicit log/docstring that bucketing/sorting is not simulated; prevents AttributeError on production chains that call `.bucketBy(n, col).sortBy(col)`.
- [ ] **W8. `writeTo` (DataFrameWriterV2) decision** — either a minimal `writeTo(table).using(...).partitionedBy(...).option(...).create()/replace()/append()` façade over `save_dataframe`, or an explicit `NotImplementedError` with a migration hint to `saveAsTable`. Decide once real notebooks show which V2 verbs appear; do not build full V2 (overwritePartitions, createOrReplace) preemptively.

## Architecture

```
src/testbricks/
  data_frame_wrapper.py  # W1-W3, W6-W8: DataFrameWriter verbs/options -> TableCatalog.save_dataframe
  catalog/table_catalog.py  # W1, W3, W4, W6: mode truth table, replaceWhere delete+append, stored CSV options
  local_workflow_runner.py  # T2-T6: outcome/condition/for_each/retry scheduling around NotebookExecutor
  notebook_executor.py  # T1, T4: taskValues store + per-iteration isolation
  dbutils/jobs.py (new, tiny)  # T1: taskValues.set/get façade over runner store
```

No new persistence layer, no Delta/parquet dependency for tables, no SQL parser beyond the existing `replaceWhere` predicate via pandas and the existing `depends_on` graph.

## Data flow

- **Write path:** `df.write[.format][.mode][.partitionBy][.option]*.saveAsTable/insertInto/csv/parquet/json/save` → `DataFrameWriter` normalizes mode + options → `TableCatalog.save_dataframe(ident, df, mode, header, csv_options, replace_where)` → atomic CSV replace/append + `createOrReplaceTempView`.
- **Control-flow path:** workflow JSON → `_parse_tasks` (now also reads `run_if`, `condition_task`, `for_each_task`, `max_retries`, `timeout_seconds`) → `_build_graphs` with outcome edges → `run_workflow` loop checks eligibility (T2/T3), expands iterations (T4), retries (T6), threads `taskValues` (T1) through shared namespace + env/widget bridge.

## Error handling

| Case | Behavior |
|---|---|
| `mode("error"/"errorIfExists")` on existing table | Raise (Spark-like message with `schema.table`) |
| `mode("ignore")` on existing table | No-op success; file mtime + view unchanged |
| `insertInto` on missing table | Raise (matches Spark requiring the table to exist) |
| `partitionBy` unknown column | Raise at write time listing available columns |
| `replaceWhere` unparsable predicate | Raise `ValueError` naming the predicate; never half-write (read-modify-write stays atomic via existing temp-file swap) |
| `mergeSchema` type conflict (same col, incompatible types) | Raise `SchemaMismatchError`; only additive merges succeed |
| Unknown `format()` on table APIs | Ignore (CSV-backed), as today; unknown format on file `save()` raises |
| Skipped tasks (T2/T3) | Marked `SKIPPED`, not failed; downstream `run_if` sees the skip |
| `for_each` child failure | Fail the task (no partial-success aggregation in v1) |
| Retry exhaustion (T6) | Re-raise last exception; per-attempt logging |

## Testing

- **Writer:** per-mode matrix (overwrite/append/ignore/error/errorIfExists/default) on missing + existing tables; `insertInto` append/overwrite/missing-table; `partitionBy` typo raises; `replaceWhere` partial overwrite; CSV option round-trips (pipe delimiter, custom nullValue, date format); `parquet`/`json` file writes readable back; `mergeSchema` union vs conflict; `bucketBy/sortBy` chain smoke test; `writeTo` decision test (façade or clear NotImplementedError).
- **Control flow:** `run_if` skip matrix; `condition_task` true/false branch routing; `for_each` fan-out with per-iteration params; `taskValues` set/get across shared vs isolated runs; repair-and-rerun subgraph; retry-succeeds-on-second-attempt and retry-exhaustion tests.
- Keep new tests next to existing suites (`tests/test_basic.py`, `tests/test_catalog.py`, `tests/test_local_workflow_runner.py` + notebook fixtures); full suite must stay green with no new heavy dependencies.

## Success criteria

1. A production chain like `df.write.format("delta").mode("overwrite").option("replaceWhere", "dt = '2026-09-01'").partitionBy("dt").saveAsTable("silver.t")` runs locally with correct partial-overwrite semantics.
2. `mode("ignore")` / `mode("error")` / `insertInto` behave like Spark instead of AttributeError/silent-wrong-write.
3. A Lakeflow JSON using `taskValues` + `run_if` + `condition_task` + `for_each` executes locally with the same branch/skip outcomes as Databricks (modulo documented no-ops).
4. No `MERGE`/time-travel/DDL/UDF surface added; no new required dependencies; existing suites green.
