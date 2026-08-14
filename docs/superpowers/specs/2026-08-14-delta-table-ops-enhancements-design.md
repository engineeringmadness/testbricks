# Incremental Delta Table Operation Support (No Major Refactor)

**Date:** 2026-08-14  
**Status:** Proposed (ideas + recommended first slice)  
**Scope:** Further `SparkMock` / `TableCatalog` support for Databricks-style Delta table usage, without a new storage format, transaction log, or large rewrite.  
**Explicitly excluded:** Delta `MERGE`, `UPDATE`, `DELETE` (SQL and DataFrame equivalents). Also excluded: time travel, CDF, Z-ORDER, real OPTIMIZE/VACUUM semantics.

## Current baseline

Tables are CSV files at `{base_path}/{schema}/{table}.csv` plus Spark temp views named `{schema}_{table}`.

| API | Today |
|---|---|
| `spark.read.table("schema.table")` | Reads CSV via `TableCatalog.read_csv`; options pass through; no default `header`/`inferSchema` (unlike `load_all`) |
| `df.write.mode(...).saveAsTable("schema.table")` | `overwrite` (default) or `append`; schema-name mismatch on append raises `SchemaMismatchError` |
| `spark.sql(...)` | Replaces **every** `.` with `_`, then runs against temp views |
| `spark.write.table(...)` | Does not exist (`SparkMock` has no `write`) |
| `df.write.format("delta")` | Does not exist (AttributeError on real notebooks) |
| `df.groupBy(...).agg(...).write.saveAsTable(...)` | Bypasses mock writer: `GroupedData` is not wrapped, so `.write` is native Spark |

`TableCatalog` already centralizes path + temp-view sync. Enhancements should add thin methods on that catalog and Spark-shaped shims on reader/writer/`sql()`, not a new persistence layer.

## Goals

1. Let typical notebook **write chains** and **SQL reads** against two-part names run locally without rewriting production code.
2. Stay on CSV + temp views; no Delta log, no major module split.
3. Prefer no-ops and identifier rewrites over simulating Delta internals.
4. Do not implement row-level `MERGE` / `UPDATE` / `DELETE`.

## Idea catalog (incremental, small surface)

Ideas are grouped by payoff. Each item is implementable as a localized change on existing types (`DataFrameWriter`, `DataFrameReader`, `SparkMock.sql`, `TableCatalog`, `DataFrameWrapper`).

### A. Writer/reader compatibility shims (highest “notebooks stop crashing” value)

These match Databricks call chains that currently fail before any data is written.

1. **`format(source)` no-op on reader and writer**  
   Honor `format("delta")` (and ignore other formats for table APIs). `saveAsTable` / `table()` stay CSV-backed. Real notebooks almost always write `.format("delta").mode("overwrite").saveAsTable(...)`.

2. **`partitionBy(*cols)` no-op on writer**  
   Partitioning is ignored; still one CSV per table. Prevents AttributeError; documents that partition layout is not simulated.

3. **`option("overwriteSchema", "true")` / `"mergeSchema"`**  
   Overwrite already replaces the file (schema can change). Treat `overwriteSchema` as already-satisfied. `mergeSchema` on append can union missing columns with nulls via pandas — small, optional, still no Delta log.

4. **Path saves that claim Delta**  
   `df.write.format("delta").save(path)` and `spark.read.format("delta").load(path)` can reuse the existing CSV `csv(path)` / `read.csv` path under `base_path`, or no-op-map `.save` to the same directory writer used today. Keep this thinner than table APIs if time is tight.

5. **SQL maintenance no-ops**  
   `OPTIMIZE`, `VACUUM`, `ANALYZE TABLE`, `REFRESH TABLE`, `FSCK REPAIR TABLE` → succeed and return an empty DataFrame (or `OK` row). Notebooks that “compact” Delta tables then continue. No file rewriting beyond current CSV.

### B. SQL identifier handling (highest correctness for `spark.sql`)

6. **Rewrite only table identifiers, not every `.`**  
   Today `query.replace(".", "_")` breaks literals (`1.5` → `1_5`), aliases, and `schema.table.column`. Replace known `schema.table` names (from catalog discovery + backtick forms) with `schema_table` view names. Unknown two-part names can still be rewritten if they match `ident.view_name` pattern, or fail with a clear “table not registered” error.

7. **Accept Databricks SQL that uses dotted names**  
   `SELECT * FROM schema.table` and `FROM \`schema\`.\`table\`` should work without authors switching to `schema_table`. Tests today query `f1_data_drivers`; production notebooks query `f1_data.drivers`.

8. **Three-part names as a thin alias**  
   `spark.read.table("main.schema.table")` and `FROM catalog.schema.table`: parse optional catalog prefix, ignore catalog (or require a configured default), map to the same `{schema}/{table}.csv`. Avoid nested `catalog/schema/table.csv` unless a later spec needs Unity Catalog layout.

### C. Missing SparkSession table APIs (small façade)

9. **`spark.table("schema.table")`**  
   Alias of `spark.read.table(...)`. Common in notebooks.

10. **Forward `createDataFrame` / `range` / `sparkContext` from `SparkMock`**  
    Wrap returned `DataFrame`s in `DataFrameWrapper` so `.write.saveAsTable` stays on the mock. Today tests use `_spark_session.createDataFrame` internally; notebooks call `spark.createDataFrame`.

11. **`spark.catalog.tableExists` / `listTables` / `listDatabases`**  
    Thin wrappers over `TableCatalog.exists` and directory listing. Databricks jobs often gate writes with `tableExists`. Do not proxy the full PySpark `Catalog` object unless needed; a small `SparkMock.catalog` façade (today it returns `TableCatalog`, which is **not** PySpark `Catalog`) is a compatibility trap — either rename the internal catalog or add `spark._table_catalog` and expose Spark-like methods on a wrapper. **Prefer:** keep `TableCatalog` internal-ish and add `tableExists` on `SparkMock` or a thin `CatalogFacade` without renaming everything in one go.

### D. SQL DDL / insert-only DML (still not MERGE/UPDATE/DELETE)

These persist through existing `save_dataframe` / filesystem deletes.

12. **`CREATE OR REPLACE TABLE schema.t AS SELECT ...` (CTAS)**  
    Run the SELECT (after identifier rewrite), `save_dataframe(..., mode="overwrite")`.

13. **`CREATE TABLE IF NOT EXISTS` / empty `CREATE TABLE (... types)`**  
    IF NOT EXISTS: no-op if CSV exists. Schema-only create: write a header-only CSV and register an empty temp view (types approximated via Spark schema → pandas). Skip CHECK constraints and generated columns.

14. **`DROP TABLE [IF EXISTS] schema.t`**  
    Delete CSV; `DROP VIEW IF EXISTS schema_t`. Small and useful for test isolation.

15. **`CREATE SCHEMA` / `CREATE DATABASE` / `IF NOT EXISTS`**  
    `os.makedirs` on `{base_path}/{schema}`.

16. **`INSERT INTO schema.t [SELECT ... \| VALUES ...]`**  
    Map to `save_dataframe(..., mode="append")`. This is the SQL twin of append `saveAsTable`, not row-level mutation. **In scope.** Column lists (`INSERT INTO t (a,b) SELECT ...`) can be a follow-up.

17. **`INSERT OVERWRITE [TABLE] schema.t SELECT ...`**  
    Map to overwrite `save_dataframe`. Distinct from `UPDATE`/`DELETE`. Databricks uses this for full-refresh facts.

18. **`df.write.insertInto("schema.table")`**  
    DataFrame API for INSERT INTO; honor writer `mode` (`append` vs `overwrite`). Spark’s `insertInto` requires the table to exist — match that if cheap (`error` if missing).

19. **Write modes `errorIfExists` and `ignore`**  
    Previously omitted. Few lines in `save_dataframe`. Completes Spark save modes except path-based `overwrite` partition replace.

### E. Wrapper holes that break table writes after transforms

20. **Wrap `GroupedData` (and `DataFrame` from `.agg`)**  
    `DataFrameWrapper.__getattr__` only re-wraps `DataFrame`. `groupBy().agg()` returns a native DataFrame; `.write.saveAsTable` then writes a real Spark table, not CSV. Fix: wrap `GroupedData` so chained DataFrame results stay mocked. Called out in the E2E spec as why notebooks used `spark.sql` instead of DataFrame aggregations.

21. **Wrap other Spark relational types if they appear in the wild**  
    `DataFrameNaFunctions` / `DataFrameStatFunctions` already return DataFrames from methods — those methods go through `__getattr__` and get wrapped. Lowest priority after GroupedData.

### F. Read-path consistency and errors

22. **Default `header=true`, `inferSchema=true` on `read.table`**  
    Match `load_all`. Today `read.table` without options depends on Spark CSV defaults; `load_all` always infers. Aligning removes a class of “SQL sees types, DataFrame reader sees strings” bugs.

23. **Missing-table errors**  
    Map missing CSV to a message that includes `schema.table` (AnalysisException-like `ValueError` or existing `SparkMockError` subclass). Tests currently accept generic `Exception`.

24. **Quoted / mixed-case identifiers**  
    Strip backticks in `TableIdentifier.parse`; keep case as on disk (CSV names are case-sensitive on Linux).

### Explicitly not in this enhancement wave

- `MERGE INTO`, `UPDATE`, `DELETE`, `replaceWhere` as delete-subset, Change Data Feed  
- Delta transaction log, `_delta_log`, time travel (`VERSION AS OF`, `@v`)  
- Real `OPTIMIZE` / `VACUUM` / Z-ORDER file layout  
- Streaming `writeStream` / `readStream` to Delta  
- Full Unity Catalog (grants, volumes, foreign catalogs)  
- Replacing CSV with parquet/Delta Lake Python (`delta-spark`) — that is a major storage change

## Approaches

### Approach 1 — Compatibility shims only (recommended first slice)

Implement **A + B + C9–C10 + E20 + F22**.

- `format` / `partitionBy` no-ops  
- SQL rewrite limited to catalog table names + backticks  
- `spark.table`, wrap `createDataFrame`  
- Wrap `GroupedData`  
- Default CSV read options  

**Trade-off:** Notebooks that only read/overwrite/append tables work with dotted SQL and `.format("delta")`. No `INSERT INTO` / `DROP` / `CTAS` yet. Smallest diff, no new SQL parser beyond identifier substitution.

### Approach 2 — Shims plus insert/DDL subset

Approach 1, then **D12, D14, D16–D18, D19** (`CTAS`, `DROP TABLE`, `INSERT INTO` / `INSERT OVERWRITE`, `insertInto`, `errorIfExists`/`ignore`).

**Trade-off:** Covers full-refresh and append SQL pipelines. Needs a small SQL prefix detector (regex or simple parse of leading keywords), not a general SQL engine. Risk: incomplete SQL (comments, leading CTEs) — document supported statement shapes.

### Approach 3 — Catalog façade + three-part names

Approach 1 or 2, plus **C11 + B8** (Spark-like `catalog.tableExists` / `listTables`, optional `catalog.schema.table`).

**Trade-off:** Better Unity Catalog *naming* without UC features. Easy to over-build if `SparkMock.catalog` currently *is* `TableCatalog` — a façade rename can sprawl. Do this only if notebooks call `spark.catalog.*`.

## Recommendation

Ship **Approach 1** first. It fixes the two largest practical gaps (`format("delta")` crash, `spark.sql` dotted names / decimal mangling, GroupedData write bypass) without pretending to be Delta Lake.

Add **Approach 2** as a second slice if pipelines use `INSERT INTO` / `CREATE OR REPLACE TABLE AS SELECT` / `DROP TABLE` instead of only `saveAsTable`.

Do **not** start Approach 3 until `spark.catalog.tableExists` (or three-part names) shows up in target notebooks.

## Design for Approach 1 (first implementation)

### Architecture (unchanged layout)

```
src/testbricks/
  spark_mock.py           # sql() identifier rewrite; table(); __getattr__ wrap DataFrames
  data_frame_reader.py    # format(); default header/inferSchema
  data_frame_writer.py    # format(); partitionBy()
  data_frame_wrapper.py   # wrap GroupedData so .agg() returns DataFrameWrapper
  catalog/
    identifier.py         # optional backticks; optional 3-part later
    table_catalog.py      # unchanged persist semantics; maybe list identifiers for SQL rewrite
```

No new packages. No Delta files.

### SQL rewrite

1. Collect registered identifiers from `{schema}/{table}.csv` plus views already created this session (same as `load_all` + saves).  
2. In the query string, replace (longest-first)  
   `` `schema`.`table` ``, `schema.table`  
   with `schema_table`.  
3. Do **not** globally replace `.`.  
4. If the statement is a no-op maintenance command (Approach 1 optional stretch: `OPTIMIZE`/`VACUUM`/`REFRESH`), return empty DF without running Spark SQL.

CTEs and column `schema.table.col` remain a known limitation unless the rewrite only targets `FROM`/`JOIN`/`TABLE` positions. Prefer **FROM/JOIN/TABLE-token replacement** over whole-string replace so `SELECT schema.table.col` can stay as view column access (`schema_table.col` after FROM rewrite only). Document: qualified column names may still need the view name or unqualified columns after `USING`.

### Writer/reader

- `format(self, source)` stores `_format` and returns `self`. `saveAsTable` ignores it.  
- `partitionBy(self, *cols)` stores columns, ignored on save.  
- `read.table` defaults: `header=true`, `inferSchema=true`, then apply explicit options (explicit wins).

### GroupedData

If `getattr` result is callable and the inner result is `DataFrame`, wrap as today. If the attribute itself is a `GroupedData` (or the callable returns `GroupedData`), wrap in a tiny `GroupedDataWrapper` that re-wraps DataFrame results. Do not wrap RDD/Column.

### `spark.table`

`SparkMock.table(name)` → `self.read.table(name)`.

### `createDataFrame`

`SparkMock.createDataFrame(*args, **kwargs)` → wrap `self._spark_session.createDataFrame(...)`.

### Error handling

- Unknown `format` for `saveAsTable`: ignore (table path is always CSV).  
- `format("delta").csv(path)` is contradictory; keep current `csv()` behavior (native CSV write).  
- Invalid table name: existing `InvalidTableNameError`.

### Testing

| Area | Tests |
|---|---|
| `format("delta").mode("overwrite").saveAsTable` | CSV + temp view as today |
| `partitionBy` chain then `saveAsTable` | same as unpartitioned |
| `spark.sql("SELECT * FROM schema.table")` | row count; decimal literal `SELECT 1.5 AS x` unchanged |
| Backticks | `` SELECT * FROM `schema`.`table` `` |
| `spark.table("schema.table")` | same as `read.table` |
| `createDataFrame` then `saveAsTable` | uses mock writer |
| `groupBy().agg().write.saveAsTable` | CSV catalog, not Spark metastore |
| Default read options | types inferred without calling `.option("header","true")` |

Out of scope for Approach 1 tests: INSERT/CTAS/DROP, three-part names, `spark.catalog.listTables`.

### Success criteria (Approach 1)

1. A notebook write chain `df.write.format("delta").mode("overwrite").option("overwriteSchema","true").partitionBy("dt").saveAsTable("silver.t")` persists CSV + temp view.  
2. `spark.sql("SELECT * FROM silver.t")` works (dotted name).  
3. `SELECT 1.5 AS v` does not become `1_5`.  
4. Aggregation then `saveAsTable` hits `TableCatalog`.  
5. Full suite green; no Delta/Parquet dependency added.

## Follow-up (Approach 2 sketch)

Detect statement kind with a normalized prefix (`CREATE OR REPLACE TABLE`, `INSERT INTO`, `INSERT OVERWRITE`, `DROP TABLE`). Extract target `schema.table`, run SELECT remainder with the same identifier rewrite, `save_dataframe` append or overwrite, or delete file. Unsupported: `MERGE`/`UPDATE`/`DELETE` still raise a clear `NotImplementedError`.

## Open choice (does not block Approach 1)

Whether `SparkMock.catalog` should remain `TableCatalog` or become a Spark-like façade is deferred to Approach 3 so this wave does not rename a public attribute.
