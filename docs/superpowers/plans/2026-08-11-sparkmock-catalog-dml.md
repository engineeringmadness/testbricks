# SparkMock Catalog + Subset DML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor SparkMock around `TableCatalog`, honor write modes / `df.write.table` / `insertInto`, add safe SQL rewriting plus subset DML (`DELETE`/`UPDATE`/`INSERT`/`CTAS`/`MERGE`), and wrap `GroupedData` so aggregations still use the mock writer.

**Architecture:** `TableCatalog` owns `schema.table` ↔ CSV ↔ temp view sync. `SqlGateway` classifies/rewrites SQL and routes DML. Reader/writer/wrapper stay thin facades over the catalog.

**Tech Stack:** Python 3.14, PySpark, pandas, pytest, coverage

## Global Constraints

- Two-part names only: `schema.table` (reject 1-part / 3-part with `ValueError`)
- CSV path: `{base_path}/{schema}/{table}.csv`; view name: `schema_table`
- Reader defaults: `header=true`, `inferSchema=true` unless overridden
- MERGE subset only: one `WHEN MATCHED THEN UPDATE` + one `WHEN NOT MATCHED THEN INSERT`
- Do not modify dbutils or LocalWorkflowRunner except incidental fixture compatibility
- TDD: failing test before production code for each behavior slice
- Run tests with: `python3.14 -m coverage run -m pytest tests/ -v` (or targeted files during tasks)

## File Structure

| File | Responsibility |
|---|---|
| Create `src/mock/table_catalog.py` | Name parse, paths, load/save CSV, views, exists/drop, modes |
| Create `src/mock/sql_gateway.py` | Statement classify, safe identifier rewrite, DML handlers |
| Create `src/mock/grouped_data_wrapper.py` | Wrap PySpark `GroupedData` so agg returns `DataFrameWrapper` |
| Modify `src/mock/spark_mock.py` | Own catalog + sql gateway; remove inline load/prints |
| Modify `src/mock/data_frame_reader.py` | Delegate `table()` to catalog |
| Modify `src/mock/data_frame_writer.py` | Modes, `table` alias, `insertInto`, catalog writes |
| Modify `src/mock/data_frame_wrapper.py` | Wrap `GroupedData` |
| Modify `tests/test_basic.py` | Update header-default expectation; keep regressions |
| Create `tests/test_table_catalog.py` | Catalog unit tests |
| Create `tests/test_sql_gateway.py` | Rewriter + DML tests |
| Create `tests/test_dataframe_wrapper.py` | GroupedData write path |

---

### Task 1: TableCatalog extract (behavior-preserving load/read/write)

**Files:**
- Create: `src/mock/table_catalog.py`
- Modify: `src/mock/spark_mock.py`
- Modify: `src/mock/data_frame_reader.py`
- Modify: `src/mock/data_frame_writer.py`
- Test: `tests/test_table_catalog.py`
- Modify: `tests/test_basic.py` (reader default header expectation)

**Interfaces:**
- Produces:
  - `class TableCatalog`
  - `parse_table_name(name: str) -> tuple[str, str]`
  - `csv_path(schema: str, table: str) -> str`
  - `view_name(schema: str, table: str) -> str`
  - `load_all() -> None`
  - `table_exists(name: str) -> bool`
  - `read_table(name: str, options: dict | None = None) -> DataFrame`
  - `write_table(name: str, dataframe, mode: str | None = None, options: dict | None = None) -> None`
  - `register_view(name: str, dataframe) -> None`
  - `drop_table(name: str) -> None`

- [ ] **Step 1: Write failing catalog tests**

```python
# tests/test_table_catalog.py
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession
from mock.table_catalog import TableCatalog


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("catalog-tests").getOrCreate()


@pytest.fixture
def catalog(tmp_path, spark):
    return TableCatalog(str(tmp_path), spark)


def test_parse_table_name_valid(catalog):
    assert catalog.parse_table_name("schema.table") == ("schema", "table")


def test_parse_table_name_rejects_one_and_three_part(catalog):
    with pytest.raises(ValueError, match="schema_name.table_name"):
        catalog.parse_table_name("table")
    with pytest.raises(ValueError, match="schema_name.table_name"):
        catalog.parse_table_name("a.b.c")


def test_view_and_csv_path(catalog, tmp_path):
    assert catalog.view_name("s", "t") == "s_t"
    assert catalog.csv_path("s", "t") == os.path.join(str(tmp_path), "s", "t.csv")


def test_write_overwrite_and_read_roundtrip(catalog, spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    catalog.write_table("default.sample", df, mode="overwrite")
    assert catalog.table_exists("default.sample")
    out = catalog.read_table("default.sample")
    assert out.count() == 1
    assert set(out.columns) == {"id", "name"}
    # temp view registered
    assert spark.sql("SELECT * FROM default_sample").count() == 1
```

- [ ] **Step 2: Run tests — expect ImportError / fail**

Run: `python3.14 -m pytest tests/test_table_catalog.py -v`

- [ ] **Step 3: Implement `TableCatalog`**

Implement `src/mock/table_catalog.py`:
- Constructor `(base_path, spark_session)`
- `parse_table_name` as above
- `write_table`: create schema dir; honor modes (`overwrite`/`append`/`ignore`/`error`/`errorifexists`; default `error` if mode is None); pandas `to_csv` with header default True; atomic write via temp file + replace; call `register_view`
- `read_table`: merge defaults `{"header": "true", "inferSchema": "true"}` with options; raise clear error if missing; register view; return DF
- `load_all`: scan `base_path/*/*.csv`, register each view quietly (logging only, no print)
- `drop_table`: delete CSV if present; `spark.catalog.dropTempView(view)` best-effort
- `append`: require same columns (order-insensitive name match); concat pandas; fail with clear message on mismatch

- [ ] **Step 4: Wire SparkMock / reader / writer to catalog**

- `SparkMock.__init__`: create `self._catalog = TableCatalog(base_path, session)`; `self._catalog.load_all()`; remove `_load_tables_into_temp_views` / prints
- Keep `_get_full_path` for path-based `csv()` writes OR move to catalog helper used by writer
- `DataFrameReader.table`: `return DataFrameWrapper(self._spark, self._spark._catalog.read_table(table_name, self._options))`
- `DataFrameWriter.saveAsTable`: `self._spark._catalog.write_table(table_name, self._dataframe, mode=self._mode, options=self._options)`

- [ ] **Step 5: Update basic test for reader defaults**

In `tests/test_basic.py`, change `test_read_table_without_header_option_treats_header_as_data` to assert default header behavior (21 data rows, not header-as-data). Rename accordingly.

- [ ] **Step 6: Run catalog + basic SparkMock tests**

Run: `python3.14 -m pytest tests/test_table_catalog.py tests/test_basic.py -v`  
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/mock/table_catalog.py src/mock/spark_mock.py src/mock/data_frame_reader.py src/mock/data_frame_writer.py tests/test_table_catalog.py tests/test_basic.py
git commit -m "feat: extract TableCatalog for CSV-backed SparkMock tables"
```

---

### Task 2: Write modes, `table` alias, `insertInto`

**Files:**
- Modify: `src/mock/data_frame_writer.py`
- Modify: `tests/test_table_catalog.py` and/or `tests/test_basic.py`

**Interfaces:**
- Consumes: `TableCatalog.write_table`, `table_exists`
- Produces: `DataFrameWriter.table`, `DataFrameWriter.insertInto`

- [ ] **Step 1: Write failing tests**

```python
def test_write_mode_error_if_exists(catalog, spark):
    df = spark.createDataFrame([(1,)], ["id"])
    catalog.write_table("s.t", df, mode="overwrite")
    with pytest.raises(Exception):
        catalog.write_table("s.t", df, mode="error")


def test_write_mode_ignore(catalog, spark):
    df1 = spark.createDataFrame([(1,)], ["id"])
    df2 = spark.createDataFrame([(1,), (2,)], ["id"])
    catalog.write_table("s.t", df1, mode="overwrite")
    catalog.write_table("s.t", df2, mode="ignore")
    assert catalog.read_table("s.t").count() == 1


def test_write_mode_append(catalog, spark):
    df1 = spark.createDataFrame([(1,)], ["id"])
    df2 = spark.createDataFrame([(2,)], ["id"])
    catalog.write_table("s.t", df1, mode="overwrite")
    catalog.write_table("s.t", df2, mode="append")
    assert catalog.read_table("s.t").count() == 2


def test_df_write_table_alias(temp_spark):
    df = _make_df(temp_spark, [("A", 1)], ["c1", "c2"])
    df.write.mode("overwrite").table("schema1.table1")
    assert temp_spark.read.table("schema1.table1").count() == 1


def test_insert_into_appends(temp_spark):
    df = _make_df(temp_spark, [("A", 1)], ["c1", "c2"])
    df.write.mode("overwrite").saveAsTable("schema1.table1")
    df.write.insertInto("schema1.table1")
    assert temp_spark.read.table("schema1.table1").count() == 2
```

- [ ] **Step 2: Run — expect failures for alias/insertInto/modes as needed**

- [ ] **Step 3: Implement**

```python
# data_frame_writer.py
def table(self, table_name):
    return self.saveAsTable(table_name)

def insertInto(self, table_name, overwrite=False):
    mode = "overwrite" if overwrite else "append"
    self._spark._catalog.write_table(
        table_name, self._dataframe, mode=mode, options=self._options
    )
```

Ensure catalog mode handling is complete from Task 1.

- [ ] **Step 4: Run tests — PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: honor write modes and add table/insertInto APIs"
```

---

### Task 3: GroupedData wrapper

**Files:**
- Create: `src/mock/grouped_data_wrapper.py`
- Modify: `src/mock/data_frame_wrapper.py`
- Test: `tests/test_dataframe_wrapper.py`

**Interfaces:**
- Produces: `GroupedDataWrapper` forwarding to underlying `GroupedData`; callable results that are `DataFrame` become `DataFrameWrapper`

- [ ] **Step 1: Failing test**

```python
def test_groupby_agg_write_save_as_table(temp_spark):
    df = _make_df(temp_spark, [("a", 1), ("a", 2), ("b", 3)], ["k", "v"])
    result = df.groupBy("k").agg({"v": "sum"})
    result.write.mode("overwrite").saveAsTable("default.agg_out")
    out = temp_spark.read.table("default.agg_out")
    assert out.count() == 2
```

- [ ] **Step 2: Run — expect AttributeError / native writer path failure**

- [ ] **Step 3: Implement wrapper**

```python
# grouped_data_wrapper.py
from pyspark.sql import DataFrame
from .data_frame_wrapper import DataFrameWrapper

class GroupedDataWrapper:
    def __init__(self, spark_mock, grouped):
        self._spark = spark_mock
        self._grouped = grouped

    def __getattr__(self, name):
        attr = getattr(self._grouped, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    return DataFrameWrapper(self._spark, result)
                return result
            return wrapper
        return attr
```

In `DataFrameWrapper.__getattr__`, if result is GroupedData (import from `pyspark.sql.group`), return `GroupedDataWrapper`.

Avoid circular import: import `DataFrameWrapper` inside method or lazy import in grouped wrapper.

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "fix: wrap GroupedData so agg().write uses mock writer"
```

---

### Task 4: SqlGateway — safe rewrite + SELECT

**Files:**
- Create: `src/mock/sql_gateway.py`
- Modify: `src/mock/spark_mock.py` (`sql` delegates to gateway)
- Test: `tests/test_sql_gateway.py`

**Interfaces:**
- Produces: `SqlGateway.execute(query) -> DataFrameWrapper | None`
- Produces: `rewrite_two_part_identifiers(sql: str) -> str`

- [ ] **Step 1: Failing rewriter tests**

```python
from mock.sql_gateway import rewrite_two_part_identifiers

def test_rewrites_schema_table_only():
    q = "SELECT t.col, 1.5, 'a.b' FROM schema.table t"
    out = rewrite_two_part_identifiers(q)
    assert "schema_table" in out
    assert "t.col" in out
    assert "1.5" in out
    assert "'a.b'" in out
    assert "schema.table" not in out


def test_select_via_spark_sql(temp_spark):
    # presuppose catalog has f1 or write a small table first
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    out = temp_spark.sql("SELECT t.id FROM s.t t WHERE t.id = 1")
    assert out.count() == 1
```

- [ ] **Step 2: Implement rewriter**

Use a tokenizer-aware approach:
- Scan SQL; track single/double quoted strings
- Replace identifiers matching `\b([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b` only when not inside strings AND when the left token is not a known column alias ambiguity… Practical rule from spec: rewrite pairs that look like table refs used after `FROM`/`JOIN`/`INTO`/`UPDATE`/`TABLE`/`MERGE INTO`, **or** rewrite all two-part names that are not preceded by another identifier-dot (i.e. not `a.b.c`) and prefer: rewrite `schema.table` tokens when schema folder exists / registered OR always rewrite two-part names that appear as table positions.

Simplest correct approach matching tests:
- Tokenize respecting quotes
- For unquoted tokens of form `ident.ident`, rewrite to `ident_ident` **unless** the match is clearly `alias.column` in SELECT list… Spec requires `t.col` preserved.

Better rule:
1. Find table-introducing clauses: `FROM`, `JOIN`, `INTO`, `UPDATE`, `TABLE`, `MERGE INTO`, `USING` (for MERGE source).
2. Rewrite only the immediately following `schema.table` (optional alias follows).
3. Also rewrite target in `DELETE FROM`, `INSERT INTO`, `CREATE TABLE`.

Do **not** globally replace every `x.y`.

For SELECT convenience, also allow `FROM schema.table` rewrite which is enough for most notebooks; qualified columns stay.

- [ ] **Step 3: `SparkMock.sql` uses gateway for SELECT**

```python
def sql(self, query):
    return self._sql_gateway.execute(query)
```

SELECT/WITH: rewrite table refs, run session.sql, wrap DF.

- [ ] **Step 4: Run tests — PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat: add SqlGateway with safe two-part table identifier rewrite"
```

---

### Task 5: SQL DML — DELETE, UPDATE, INSERT, CTAS

**Files:**
- Modify: `src/mock/sql_gateway.py`
- Modify: `tests/test_sql_gateway.py`

**Interfaces:**
- Consumes: `TableCatalog.read_table` / `write_table` / `table_exists`
- Mutating statements return `None` or empty wrapper consistently — choose: return `DataFrameWrapper` of empty DF or `None`; tests should not rely on return for DML. Prefer returning `None` for DML and DF for SELECT.

- [ ] **Step 1: Failing DML tests**

```python
def test_delete_where(temp_spark):
    df = _make_df(temp_spark, [(1,), (2,), (3,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("DELETE FROM s.t WHERE id = 2")
    assert [r.id for r in temp_spark.read.table("s.t").collect()] == [1, 3]


def test_update_where(temp_spark):
    df = _make_df(temp_spark, [(1, "a"), (2, "b")], ["id", "name"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("UPDATE s.t SET name = 'z' WHERE id = 1")
    rows = {r.id: r.name for r in temp_spark.read.table("s.t").collect()}
    assert rows == {1: "z", 2: "b"}


def test_insert_into_select(temp_spark):
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.t")
    temp_spark.sql("INSERT INTO s.t SELECT id FROM s.t")
    assert temp_spark.read.table("s.t").count() == 2


def test_ctas_and_create_or_replace(temp_spark):
    df = _make_df(temp_spark, [(1,)], ["id"])
    df.write.mode("overwrite").saveAsTable("s.src")
    temp_spark.sql("CREATE TABLE s.dst AS SELECT * FROM s.src")
    assert temp_spark.read.table("s.dst").count() == 1
    with pytest.raises(Exception):
        temp_spark.sql("CREATE TABLE s.dst AS SELECT * FROM s.src")
    temp_spark.sql("CREATE OR REPLACE TABLE s.dst AS SELECT * FROM s.src WHERE id = 1")
    assert temp_spark.read.table("s.dst").count() == 1
```

- [ ] **Step 2: Implement handlers**

- `DELETE`: parse table + optional WHERE; `df = read_table`; `df.filter(f"NOT ({where})")` or all rows if no where delete all; `write_table(..., overwrite)`
- `UPDATE`: parse assignments; for matching rows use Spark SQL against temp view then overwrite, e.g. create updated DF via `SELECT CASE...` or filter+union pattern; simplest: register view, run `SELECT` with replaced columns via Spark SQL on rewritten view name, overwrite
- `INSERT INTO ... SELECT`: run select, `write_table(..., append)`
- `CREATE TABLE ... AS SELECT`: fail if exists; overwrite for `CREATE OR REPLACE`

Unsupported joins in DELETE/UPDATE → `NotImplementedError`

- [ ] **Step 3: Run — PASS**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: support DELETE UPDATE INSERT and CTAS via SqlGateway"
```

---

### Task 6: Subset MERGE

**Files:**
- Modify: `src/mock/sql_gateway.py`
- Modify: `tests/test_sql_gateway.py`

- [ ] **Step 1: Failing MERGE tests**

```python
def test_merge_matched_update_not_matched_insert(temp_spark):
    target = _make_df(temp_spark, [(1, "old"), (2, "keep")], ["id", "name"])
    target.write.mode("overwrite").saveAsTable("s.target")
    source = _make_df(temp_spark, [(1, "new"), (3, "ins")], ["id", "name"])
    source.write.mode("overwrite").saveAsTable("s.source")
    temp_spark.sql("""
        MERGE INTO s.target t
        USING s.source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
    """)
    rows = {r.id: r.name for r in temp_spark.read.table("s.target").collect()}
    assert rows == {1: "new", 2: "keep", 3: "ins"}


def test_merge_matched_delete_unsupported(temp_spark):
    # seed minimal tables...
    with pytest.raises(NotImplementedError):
        temp_spark.sql("""
            MERGE INTO s.target t USING s.source s ON t.id = s.id
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
        """)
```

- [ ] **Step 2: Implement subset MERGE**

Parse with regex/structured parse for the supported shape only; otherwise `NotImplementedError`.
Execution:
1. Load target + source DFs (or use views)
2. Alias columns; join on ON condition via Spark SQL
3. Build updated matched rows + unmatched target rows + inserted rows
4. Union and `write_table(target, overwrite)`

- [ ] **Step 3: Run — PASS**

- [ ] **Step 4: Commit**

```bash
git commit -m "feat: add subset MERGE INTO support for SparkMock SQL"
```

---

### Task 7: Full verification + docs touch-up

- [ ] **Step 1: Run full suite**

```bash
python3.14 -m coverage run -m pytest tests/ -v
python3.14 -m coverage report
```

Expected: all pass (e2e included).

- [ ] **Step 2: Fix any regressions** (especially e2e SQL still valid under new rewriter)

- [ ] **Step 3: Commit any fixes; push; update PR**

---

## Spec Coverage Check

| Spec item | Task |
|---|---|
| TableCatalog extract + load/read/write | 1 |
| Write modes / table alias / insertInto | 2 |
| GroupedData wrapper | 3 |
| Safe SQL rewrite + SELECT | 4 |
| DELETE/UPDATE/INSERT/CTAS | 5 |
| Subset MERGE | 6 |
| Regression suite | 7 |
| Ignore dbutils / workflow runner | Global constraint |

## Execution

User requested implementation immediately → execute inline with executing-plans (this session), TDD per task, commit after each task, push and update PR continuously.
