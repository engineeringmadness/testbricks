# SparkMock Catalog + Subset DML Design

**Date:** 2026-08-11  
**Status:** Draft for review  
**Scope:** Refactor `SparkMock` around a shared `TableCatalog`, expand core Delta-style table I/O against CSV files, and add a subset of SQL DML (`MERGE` / `DELETE` / `UPDATE` / `INSERT` / `CTAS`). Ignore `DbutilsMock` and `LocalWorkflowRunner` except where existing SparkMock tests/fixtures must stay green.

## Goal

Make `SparkMock` a maintainable CSV-backed stand-in for the Databricks table operations notebooks actually use:

1. Core table I/O parity — `spark.read.table`, `df.write.saveAsTable` / `df.write.table` / `insertInto`, write modes, `INSERT INTO`, `CREATE TABLE AS SELECT`.
2. Subset SQL DML — simple `DELETE` / `UPDATE`, and a common-pattern `MERGE` (`WHEN MATCHED THEN UPDATE` + `WHEN NOT MATCHED THEN INSERT`).
3. Readability / maintainability — one place owns `schema.table` ↔ CSV ↔ temp view sync; remove duplicated path logic and fragile blanket SQL dot-rewriting.

## Requirements Summary

| Decision | Choice |
|---|---|
| Architecture | Catalog-centric refactor (`TableCatalog` + `SqlGateway` + thin facades) |
| Table names | Two-part only: `schema.table` |
| On-disk format | One CSV per table at `{base_path}/{schema}/{table}.csv` |
| Temp views | `schema_table` (unchanged convention) |
| Write modes | `overwrite`, `append`, `ignore`, `error` / `errorifexists` (default) |
| Reader defaults | `header=true`, `inferSchema=true` unless overridden |
| MERGE fidelity | Subset: one matched-update + one not-matched-insert; reject richer clauses |
| SQL identifier rewrite | Rewrite only two-part table identifiers; leave literals and `alias.col` alone |
| Out of scope | Unity Catalog three-part names, `spark.catalog` polish, partitions, OPTIMIZE/VACUUM, dbutils, LocalWorkflowRunner |

## Motivation / Current Gaps

Today's SparkMock already supports basic `read.table`, `saveAsTable`, and `sql`, but several issues block broader notebook coverage:

| Gap | Impact |
|---|---|
| No `df.write.table` (saveAsTable alias) | AGENTS.md write-to-table goal only partially covered via `saveAsTable` |
| `DataFrameWriter.mode` ignored for `saveAsTable` | Overwrite/append/error semantics missing |
| Blanket `query.replace('.', '_')` | Breaks `alias.col`, decimals, string literals; forces awkward SQL in e2e notebooks |
| Duplicated name/path logic in reader + writer | Harder to extend safely |
| Startup `print` noise | Noisy tests / unclear API surface |
| `GroupedData` not wrapped | `groupBy().agg().write` bypasses mock CSV writer |
| No `MERGE` / `DELETE` / `UPDATE` / `INSERT` / `CTAS` | Common Delta notebook patterns cannot be unit-tested |

## Approaches Considered

| Approach | Trade-off |
|---|---|
| 1. Incremental patches on current classes | Smallest diff; further tangles path/SQL/DML logic |
| **2. Catalog-centric refactor** *(chosen)* | Larger first change; clear boundaries; A and B share one persist path |
| 3. Temp-view-first + dirty flush | DML can lean on Spark more; CSV sync easier to get wrong |

**Decision: Approach 2.** Core I/O and DML both need reliable “mutate then persist CSV,” and duplicated path/`schema_table` logic is already the main maintainability drag.

## Architecture

```text
Notebook / tests
       │
       ▼
   SparkMock  ──read──► DataFrameReader
       │                      │
       ├──write──► DataFrameWriter (saveAsTable / table / insertInto)
       │
       ├──sql──► SqlGateway  (SELECT + subset DML)
       │
       └──► TableCatalog ◄── shared by reader/writer/sql
                 │
                 ├── resolve names (schema.table)
                 ├── CSV load/save (modes)
                 └── createOrReplaceTempView (schema_table)
```

### Modules (`src/mock/`)

| Module | Role |
|---|---|
| `spark_mock.py` | Facade: session, `.read`, `.write`, `.sql`, startup `catalog.load_all()` |
| `table_catalog.py` | Name parsing, paths, load/save CSV, register/refresh temp views |
| `sql_gateway.py` | Safe SQL rewrite for two-part names; route mutating SQL |
| `data_frame_reader.py` | Thin `option` / `options` / `table`; persistence via catalog |
| `data_frame_writer.py` | Thin `mode` / `option` / `saveAsTable` / `table` (alias) / `insertInto` / `csv` |
| `data_frame_wrapper.py` | Wrap `DataFrame` and `GroupedData` so write paths stay on the mock |

**Non-goals:** Unity Catalog three-part names, full Delta MERGE dialect, partitions, OPTIMIZE/VACUUM, changes to dbutils or LocalWorkflowRunner.

## TableCatalog & Core I/O

`TableCatalog` is the only component that touches CSV paths and temp views.

### Naming

- Accept `schema.table` only; reject 1-part / 3-part with `ValueError`.
- Internal view name: `schema_table`.
- Path: `{base_path}/{schema}/{table}.csv`.

### Conceptual API

- `parse_table_name(name) -> (schema, table)`
- `csv_path(schema, table) -> Path`
- `view_name(schema, table) -> str`
- `load_all()` — scan `base_path/*/*.csv`, register views (quiet; no debug prints)
- `read_table(name, options) -> DataFrame` — read CSV, refresh view, return DF
- `write_table(name, df, mode, options)` — apply mode, write single CSV, refresh view
- `table_exists(name) -> bool`
- `drop_table(name)` — delete CSV + drop temp view

### Write modes

| Mode | Behavior |
|---|---|
| `overwrite` | Replace CSV contents; refresh view |
| `append` | Concatenate rows (compatible columns required); refresh view |
| `ignore` | No-op if table exists |
| `error` / `errorifexists` (default) | Raise if table exists |

### Facades

- Table writes always start from a DataFrame writer: `df.write.saveAsTable(...)`, `df.write.table(...)` (alias of `saveAsTable`), and `df.write.insertInto(...)` all go through `catalog.write_table` (`insertInto` uses append semantics). There is no top-level `spark.write` without a DataFrame — that matches PySpark and still covers the AGENTS.md “write table” goal.
- `spark.read.table` calls `catalog.read_table` with defaults `header=true`, `inferSchema=true` unless overridden. This is an intentional behavior change vs today (where omitting `header` could treat the header row as data); update tests accordingly.
- `DataFrameWriter.mode(...)` is honored for table writes.
- Reader/writer hold a catalog reference instead of re-implementing path joins / name splits.
- Direct `df.write.csv(...)` remains available for path-based writes (existing behavior), still rooted at `base_path`.

## SqlGateway & Subset DML

`spark.sql(query)` goes through `SqlGateway`, not a blanket `query.replace('.', '_')`.

### Routing

1. Strip leading comments; classify statement case-insensitively: `SELECT`/`WITH`, `INSERT`, `CREATE TABLE ... AS`, `DELETE`, `UPDATE`, `MERGE`, else pass-through attempt or clear unsupported error.
2. Rewrite only two-part identifiers `schema.table` → `schema_table` (never dots inside strings, decimals, or `alias.column`).
3. After any successful mutating statement, persist the target table via `catalog.write_table(..., mode="overwrite")` (or `append` for `INSERT INTO`) and refresh its view.

### Supported DML subset

| Statement | Supported shape | Execution strategy |
|---|---|---|
| `DELETE FROM schema.t WHERE <cond>` | Optional `WHERE`; no delete-join | Filter DF / temp view; overwrite CSV |
| `UPDATE schema.t SET c=expr [, ...] WHERE <cond>` | Simple assignments; no multi-table | Update matching rows; overwrite CSV |
| `MERGE INTO target t USING source s ON <cond> WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...)` | One matched-update + one not-matched-insert | Join-based DataFrame merge; catalog overwrite |
| `INSERT INTO schema.t SELECT ...` / `VALUES ...` | Append semantics | `write_table(..., mode="append")` |
| `CREATE TABLE schema.t AS SELECT ...` | `USING` clause optional/ignored | Create via catalog; fail if table exists |
| `CREATE OR REPLACE TABLE schema.t AS SELECT ...` | Same as CTAS | Overwrite via catalog |

### Explicitly unsupported (clear error)

- `WHEN MATCHED THEN DELETE`
- Multiple `WHEN` branches / `*` update-insert sugar (until explicitly added later)
- `UPDATE` / `DELETE` with joins
- Three-part names
- Full Delta Lake utilities (`OPTIMIZE`, `VACUUM`, etc.)

### SELECT path

- Rewrite two-part table names only; execute on the Spark session; wrap in `DataFrameWrapper`.
- Enables qualified column references in joins without the current e2e `USING(...)` + unqualified-column workaround.

### Wrapper fix

- `DataFrameWrapper.__getattr__`: if a method returns `GroupedData`, wrap it so subsequent `.agg()` / aggregations that return a `DataFrame` still expose the mock `.write`.

## Error Handling & Edge Cases

| Case | Behavior |
|---|---|
| Invalid table name format | `ValueError` expecting `schema.table` |
| Missing table on read / DML target | Clear not-found error (`AnalysisException` when practical, else dedicated error tests can match) |
| Write mode `error` and table exists | Fail before mutating disk/view |
| `append` / `INSERT` column mismatch | Explicit schema/column mismatch error |
| Mode `ignore` and table exists | Silent success; no view churn |
| Unsupported DML dialect | `NotImplementedError` / dedicated unsupported error naming the clause |
| Identifier rewriter regressions | Must not touch `'a.b'`, `1.5`, `t.col` |
| Mutating success invariant | CSV on disk matches temp view for that table |
| Mutating failure invariant | Best-effort: prior CSV/view unchanged (validate before write; atomic temp-file replace for CSV) |
| Logging | Optional debug logging; default quiet (no startup prints) |

## Testing

Keep existing SparkMock behaviors in `tests/test_basic.py` green. Extend coverage for catalog and DML. Do not expand dbutils / LocalWorkflowRunner scope.

### Unit tests (prefer isolated temp `base_path`)

- **Catalog:** parse names; path/view helpers; `load_all`; exists/drop; modes `overwrite` / `append` / `ignore` / `error`.
- **Reader defaults:** header + inferSchema without callers repeating options.
- **`df.write.table`** parity with `saveAsTable`.
- **`insertInto` / `INSERT INTO`** append.
- **CTAS** creates CSV + queryable view.
- **SQL rewriter:** `schema.table` rewritten; `'a.b'`, `1.5`, `alias.col` preserved; qualified joins work.
- **DML subset:** simple `DELETE` / `UPDATE`; happy-path `MERGE`; unsupported `MERGE` clause raises clearly.
- **Wrapper:** `df.groupBy(...).agg(...).write.saveAsTable(...)` persists via catalog.

### Regression

- Re-run current SparkMock tests and the existing e2e workflow test.
- Optionally simplify e2e SQL later once the rewriter is safe; not required for the first implementation PR if behavior remains compatible.

## Implementation Notes (for later planning)

Suggested build order once the spec is approved:

1. Extract `TableCatalog` and migrate existing read/write/load_all paths (behavior-preserving).
2. Honor write modes; add `df.write.table` alias + `insertInto`.
3. Fix `DataFrameWrapper` `GroupedData` wrapping.
4. Replace SQL entrypoint with `SqlGateway` rewriter + SELECT path.
5. Add `DELETE` / `UPDATE` / `INSERT` / `CTAS`.
6. Add subset `MERGE`.
7. Expand tests as each slice lands.

## Success Criteria

- Notebooks can use `read.table`, `df.write.table` / `saveAsTable`, write modes, `INSERT`, `CTAS` / `CREATE OR REPLACE`, simple `DELETE`/`UPDATE`, and subset `MERGE` against local CSVs.
- All table persistence goes through `TableCatalog` (no duplicated path logic in reader/writer).
- `spark.sql` no longer mangles non-table dots.
- Existing SparkMock unit tests and e2e workflow test still pass.
- Dbutils and LocalWorkflowRunner remain untouched aside from incidental fixture compatibility.
