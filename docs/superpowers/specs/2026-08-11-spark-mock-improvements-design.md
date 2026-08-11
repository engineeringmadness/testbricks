# SparkMock Improvements Design

**Date:** 2026-08-11  
**Status:** Proposed (review)  
**Scope:** `SparkMock` and its CSV-backed DataFrame read/write/SQL proxies only.  
**Out of scope:** `DbutilsMock`, `LocalWorkflowRunner`, notebook executor, `%run` / `%sh`.

## Goal

Extend and harden the Spark proxy so more real Databricks / Delta-table notebook patterns run locally against CSV files, while making the mock easier to read, test, and evolve.

Today the proxy covers a thin slice:

| Databricks API | Current mock behavior |
|---|---|
| `spark.read.table("schema.table")` | Reads `{base}/{schema}/{table}.csv` |
| `df.write.saveAsTable("schema.table")` | Writes CSV + registers temp view `schema_table` |
| `spark.sql("…")` | Blanket `.` → `_` rewrite, then session SQL |
| `df.write.csv(path)` | Native Spark CSV writer under `base_path` |

That is enough for simple ETL, but common Delta / UC patterns fail or silently diverge from Databricks.

---

## Current Architecture (as-is)

```
src/mock/
  spark_mock.py          # session + CSV discovery → temp views
  data_frame_reader.py   # read.option*.table
  data_frame_writer.py   # write.mode/option*.csv|saveAsTable
  data_frame_wrapper.py  # __getattr__ proxy around pyspark DataFrame
```

### Mapping convention

- Filesystem: `{base_path}/{schema_name}/{table_name}.csv`
- SQL temp view: `{schema_name}_{table_name}`
- Identifier format enforced as exactly two parts: `schema.table`

### Data flow today

```
Startup
  SparkMock(base_path)
    → walk schema folders
    → read each *.csv (header=True, inferSchema=True)
    → createOrReplaceTempView(schema_table)

Notebook
  spark.read.table("a.b")  → open CSV again (options from caller)
  df.write.saveAsTable("a.b")
    → createOrReplaceTempView("a_b")
    → pandas.to_csv(a/b.csv)   # mode() ignored
  spark.sql("SELECT * FROM a.b")
    → query.replace('.', '_') → SELECT * FROM a_b
```

---

## Problems Found

### P1 — Correctness bugs that break real notebooks

1. **Naive SQL rewriting** (`SparkMock.sql`)  
   `query.replace('.', '_')` rewrites *every* dot, including:
   - qualified columns (`c.customer_id` → `c_customer_id`)
   - decimal literals (`1.5` → `1_5`)
   - string contents that happen to contain dots  
   The E2E design already works around this (`USING` + unqualified columns). Notebooks that use `schema.table` *and* `alias.column` cannot run unchanged.

2. **`DataFrameWrapper` drops the mock on non-`DataFrame` returns**  
   `groupBy` / `rollup` / `cube` return `GroupedData`. `.agg(...)` then returns a raw Spark `DataFrame`, so `.write.saveAsTable(...)` hits the *native* Spark writer and bypasses CSV + temp-view sync. Same class of bug for any API that returns an intermediate non-`DataFrame` type that later yields a `DataFrame`.

3. **Write `mode()` is ignored for `saveAsTable`**  
   `DataFrameWriter.mode` is stored but only applied to `.csv()`. `saveAsTable` always overwrites via pandas `to_csv`. `append` / `ignore` / `errorifexists` / `overwrite` do not match Databricks semantics.

4. **Dual sources of truth (CSV vs temp view) can diverge**  
   - Startup loads views with `header=True, inferSchema=True`.  
   - `read.table` uses caller options (often without `inferSchema`), so schema/types can differ from `sql()`.  
   - `saveAsTable` updates both, but there is no shared registry; future DDL/DML that touches only one side will desync silently.

5. **`parallelize` wraps an RDD in `DataFrameWrapper`**  
   Callers expect an RDD (or should use `createDataFrame`). Attribute forwarding will misbehave.

### P2 — Missing Delta / table operations (CSV-compatible)

These are the highest-value APIs to cover next, ranked by how often they appear in Databricks pipelines and how cleanly they map to CSV:

| Priority | Operation | Suggested CSV mapping |
|---|---|---|
| High | `spark.table("s.t")` | Alias of `read.table` |
| High | `spark.write.table` / `df.write.saveAsTable` parity | Same path; honor mode |
| High | `df.write.mode(...).insertInto("s.t")` | Append/overwrite rows in CSV + refresh view |
| High | SQL `CREATE OR REPLACE TABLE s.t AS SELECT …` | Write query result CSV + register view |
| High | SQL `INSERT INTO` / `INSERT OVERWRITE` | Append or replace CSV |
| Medium | SQL `DROP TABLE` / `CREATE SCHEMA` / `DROP SCHEMA` | Delete file / mkdir / rmtree |
| Medium | SQL `DELETE FROM` / `UPDATE` (row predicates) | Filter/update pandas or Spark DF → rewrite CSV |
| Medium | `MERGE INTO` (simple match/update/insert) | Implement via DataFrame ops, then rewrite CSV |
| Medium | Three-part names `catalog.schema.table` | Map to `{base}/{catalog}/{schema}/{table}.csv` or ignore catalog segment with a flag |
| Medium | `spark.catalog.listTables` / `tableExists` / `dropTempView` | Backed by registry |
| Low | `partitionBy` on write | Optional `{table}/col=val/*.csv` layout (document as best-effort) |
| Low | `format("delta").save` / path-based Delta | Map path under `base_path` to a single CSV or folder; no real Delta log |
| Low | Time travel / `OPTIMIZE` / `VACUUM` / CDF | Explicitly unsupported; raise clear `NotImplementedError` |

**Non-goal:** Emulating the Delta transaction log, optimistic concurrency, or Z-ORDER. The mock should stay “CSV files + temp views,” with clear errors for unsupported Delta features.

### P3 — Readability and maintainability

| Issue | Location | Fix |
|---|---|---|
| Debug `print` while walking folders | `spark_mock._load_tables_into_temp_views` | Use `logging` at DEBUG, or remove |
| Duplicated `schema.table` parsing | Reader + writer | Shared `TableIdentifier` / `parse_table_name()` |
| Path building scattered | Reader, writer, loader | Shared `TableCatalog.path_for(id)` |
| Unused import `SparkSession` | `data_frame_reader.py` | Remove |
| No type hints / docstrings on public APIs | All four modules | Add gradual typing + short module docs |
| `__getattr__` rebuilds wrappers every access | `DataFrameWrapper` | Cache callables or wrap known return types explicitly |
| Inconsistent write path (pandas vs Spark CSV) | `saveAsTable` vs `.csv` | Prefer one strategy (Spark write to temp then atomic replace, or pandas for both table ops) |
| Tests assert underscore view names | `test_basic.py` | Prefer dotted `schema.table` in SQL once rewriter is fixed |
| No dedicated errors | Raises bare `ValueError` / Spark exceptions | `SparkMockError` hierarchy for missing table, bad identifier, unsupported op |
| Package export | `mock/__init__.py` only exports runner | Export `SparkMock` as primary public API |

---

## Recommended Target Architecture

Introduce a small **catalog layer** so every read, write, and SQL mutation goes through one registry. Keep the public Spark-shaped API thin.

```
src/mock/
  spark_mock.py              # public Spark-like façade
  catalog/
    __init__.py
    identifier.py            # parse catalog.schema.table / schema.table
    table_catalog.py         # register, resolve path, sync view ↔ CSV
    errors.py                # SparkMockError, TableNotFound, UnsupportedOperation
  sql/
    __init__.py
    rewriter.py              # rewrite only table identifiers in SQL
    ddl_dml.py               # optional: intercept CREATE/INSERT/DELETE/UPDATE/MERGE
  dataframe/
    reader.py                # was data_frame_reader.py
    writer.py                # was data_frame_writer.py
    wrapper.py               # DataFrame + GroupedData wrapping
```

Migration can be incremental (new packages + re-exports) so existing imports keep working.

### Component responsibilities

| Component | Responsibility |
|---|---|
| `TableIdentifier` | Parse 2- or 3-part names; produce view name + relative CSV path |
| `TableCatalog` | List/register tables; load CSV → view; persist DF → CSV; refresh after writes |
| `SqlRewriter` | Replace *table* references only (`FROM`/`JOIN`/`INTO`/`TABLE`/`MERGE`), leave column dots and literals alone |
| `DdlDmlHandler` | Detect supported DDL/DML; execute via catalog; fall through to Spark SQL for pure SELECT |
| `DataFrameWrapper` | Wrap `DataFrame` **and** intermediates (`GroupedData`) so `.write` always returns mock writer |
| `DataFrameWriter` | Honor `mode`; implement `saveAsTable`, `insertInto`, `table` (if exposed), `csv` |
| `SparkMock` | Own session + catalog; expose `read`, `sql`, `table`, `createDataFrame`, `stop` |

### Unified read/write flow

```
                    ┌──────────────────┐
  spark.read.table  │                  │  path = base/schema/table.csv
  spark.table       │  TableCatalog    │  view = schema_table
  saveAsTable       │                  │  sync: CSV ←→ temp view
  insertInto        │  - resolve()     │
  SQL DDL/DML       │  - load()        │
  sql SELECT *      │  - save()        │──▶ SparkSession temp views
                    │  - drop()        │──▶ filesystem CSVs
                    └──────────────────┘
```

Rules:

1. **Catalog is source of truth** for “which tables exist.”  
2. After every mutating API, both CSV and temp view are updated in one catalog method.  
3. `read.table` / `spark.table` / SQL SELECT all resolve through the catalog (same defaults: `header=True`, `inferSchema=True` unless overridden).  
4. Unsupported Delta features raise `UnsupportedOperation` with a short message pointing at the CSV limitation.

---

## SQL Identifier Rewriting

Do **not** replace every `.`. Prefer a focused rewriter:

**Option A (recommended for v1):** regex / simple tokenizer that only rewrites dotted names in table-position contexts:

- `FROM schema.table` / `JOIN schema.table`
- `INTO schema.table` / `TABLE schema.table`
- `MERGE INTO schema.table`
- optional backticks: `` `schema`.`table` ``

Leave `alias.column`, decimals, and string literals untouched.

**Option B (later):** parse with `sqlglot` (or Spark’s parser if accessible) and rewrite AST table nodes. Heavier dependency, more correct for nested queries.

Acceptance examples:

| Input | Output |
|---|---|
| `SELECT * FROM bronze.customers` | `SELECT * FROM bronze_customers` |
| `SELECT c.id FROM bronze.customers c` | `SELECT c.id FROM bronze_customers c` |
| `SELECT * FROM t WHERE amount > 1.5` | unchanged decimals |
| `` SELECT * FROM `silver`.`orders` `` | `SELECT * FROM silver_orders` |

---

## Write Modes (CSV semantics)

| Mode | `saveAsTable` / `insertInto` / SQL INSERT |
|---|---|
| `overwrite` | Replace CSV contents; replace temp view |
| `append` | Concatenate rows (schema must match); refresh view |
| `ignore` | No-op if table/CSV exists |
| `error` / `errorifexists` | Raise if table exists |
| default (None) | Match Spark: treat as `errorifexists` for saveAsTable |

Implementation note: prefer writing to a temp file then `os.replace` so failed writes do not truncate the table.

---

## DataFrameWrapper Fix

```text
groupBy → MockGroupedData (wraps GroupedData)
  .agg / .count / .sum / … → DataFrameWrapper
DataFrame methods returning DataFrame → DataFrameWrapper
.write → always DataFrameWriter(spark_mock, df)
```

Also add `spark.createDataFrame` on `SparkMock` so tests and notebooks do not reach into `_spark_session`.

Deprecate or fix `parallelize` (return raw RDD, or remove from public surface).

---

## Phased Delivery

### Phase 0 — Maintainability (no behavior change intended)

- Extract `parse_table_name` + path helpers
- Remove debug prints; drop unused imports
- Export `SparkMock` from `mock`
- Add `SparkMockError` types
- Add type hints on public methods
- Tests: keep green

### Phase 1 — Correctness

- SQL table-only rewriter; update tests to use `schema.table` in SQL
- Wrap `GroupedData` (and similar) so `groupBy().agg().write.saveAsTable` works
- Honor write `mode` on `saveAsTable`
- Unify default CSV options via catalog
- Fix / narrow `parallelize`

### Phase 2 — Broader table API (CSV)

- `spark.table(...)`
- `insertInto`
- SQL: `CREATE OR REPLACE TABLE … AS`, `INSERT INTO` / `INSERT OVERWRITE`, `DROP TABLE`
- `spark.catalog.tableExists` / `listTables` backed by registry

### Phase 3 — Delta-ish DML (still CSV)

- `DELETE FROM` / `UPDATE` with simple predicates
- Best-effort `MERGE INTO` (equality match keys only)
- Optional 3-level UC names
- Explicit `UnsupportedOperation` for time travel, `OPTIMIZE`, CDF, etc.

Each phase ships with unit tests under `tests/test_spark_mock_*.py` (split from the growing `test_basic.py`).

---

## Testing Plan

| Area | Assertions |
|---|---|
| Identifier parsing | 2-part OK; 1-part error; 3-part (phase 3) |
| SQL rewriter | Column dots and decimals preserved; table dots rewritten |
| Round-trip | `saveAsTable` → `read.table` → `sql` same row count and schema |
| Modes | overwrite replaces; append grows; ignore/error behave |
| GroupBy write | `df.groupBy(...).agg(...).write.mode("overwrite").saveAsTable(...)` writes CSV |
| Missing table | Clear `TableNotFound` (or wrapped AnalysisException) |
| Unsupported | `MERGE` with complex clauses / time travel raises `UnsupportedOperation` |
| Regression | Existing `tests/test_basic.py` + E2E workflow still pass |

---

## Alternatives Considered

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| Keep blanket `.` → `_` | Simple | Breaks real SQL | Reject |
| Full Delta Lake local (delta-rs / delta) | Real semantics | Heavy; diverges from “CSV mock” goal | Reject for core path |
| DuckDB instead of Spark SQL | Fast, rich SQL | Different engine; notebooks import pyspark | Reject |
| sqlglot-based rewrite from day one | Correct AST | Extra dependency | Defer to Option B if regex fails |
| Only document workarounds | No code churn | Users rewrite notebooks | Reject as primary strategy |

---

## Success Criteria

1. Notebook SQL can use `schema.table` **and** `alias.column` / decimals without manual rewriting.  
2. `groupBy → agg → write.saveAsTable` persists CSV under `base_path` and is readable via `read.table` / `sql`.  
3. Write modes behave as documented for table saves.  
4. New table ops (at least Phase 2) go through one catalog, with no duplicated path logic.  
5. Public modules are typed, free of debug prints, and export a clear `SparkMock` entry point.  
6. Full suite (`coverage run -m pytest tests/ -v`) stays green.

---

## Open Questions

1. **Default catalog segment:** When notebooks use `main.bronze.customers`, should `main` become a directory, or be stripped with a `default_catalog=` setting?  
2. **Schema enforcement on append:** Fail hard on column mismatch, or align columns with null fill like Spark sometimes does?  
3. **Empty tables:** Represent as header-only CSV vs missing file + registered empty view?  
4. **Dependency budget:** Is `sqlglot` acceptable if the regex rewriter hits edge cases?

---

## References

- `src/mock/spark_mock.py`, `data_frame_*.py` — current implementation  
- `docs/superpowers/specs/2026-08-09-e2e-workflow-test-design.md` — documents the SQL-dot and GroupedData workarounds  
- `AGENTS.md` — target APIs: `read.table`, `write.table`, `sql`  
- `README.md` — product framing: Delta/UC → local CSV proxy  
