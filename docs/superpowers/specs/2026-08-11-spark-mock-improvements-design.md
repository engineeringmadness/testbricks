# SparkProxy Catalog Refactor + `saveAsTable` Append Mode

**Date:** 2026-08-11  
**Status:** Implemented  
**Scope:**
1. Shared CSV/temp-view **catalog** for SparkProxy table I/O
2. `DataFrameWriter.saveAsTable()` honors `mode("append")`

**Out of scope:**
- `DbutilsMock`, `LocalWorkflowRunner`, notebook executor
- SQL DDL/DML: `DELETE`, `MERGE`, `UPDATE`, `INSERT INTO`, `CREATE TABLE`, `DROP TABLE`
- Delta Lake features: time travel, `OPTIMIZE`, CDF, Z-ORDER, transaction log
- SQL identifier rewriting, `GroupedData` wrapping

## Goals

1. Centralize `schema.table` → CSV path / temp-view mapping so reader, writer, and startup load share one code path.
2. Keep append-mode `saveAsTable` behavior working through that catalog.

## Architecture

```
src/mock/
  spark_proxy.py              # façade; owns TableCatalog
  data_frame_reader.py       # read.table → catalog.read_csv
  data_frame_writer.py       # saveAsTable → catalog.save_dataframe
  data_frame_wrapper.py
  catalog/
    __init__.py
    identifier.py            # TableIdentifier.parse("schema.table")
    table_catalog.py         # load_all / read_csv / save_dataframe
    errors.py                # SparkProxyError, InvalidTableNameError, SchemaMismatchError
```

### Responsibilities

| Component | Role |
|---|---|
| `TableIdentifier` | Parse two-part names; expose `view_name`, `relative_csv_path` |
| `TableCatalog` | Resolve paths; discover CSVs at startup; read/write CSV; refresh temp views |
| `SparkProxy` | Build session + catalog; expose `read` / `sql` / `catalog` |
| `DataFrameReader` / `DataFrameWriter` | Thin Spark-shaped API over the catalog |

### Data flow

```
Startup:  SparkProxy(base) → TableCatalog.load_all() → temp views
Read:     spark.read.table("s.t") → TableIdentifier → catalog.read_csv
Write:    df.write.mode(...).saveAsTable("s.t") → catalog.save_dataframe
          → atomic CSV write + createOrReplaceTempView
```

## `saveAsTable` modes

| `mode` | CSV exists? | Behavior |
|---|---|---|
| `"append"` | No | Create CSV + temp view from the DataFrame |
| `"append"` | Yes | Append rows; refresh temp view with **combined** data |
| `"overwrite"` / `None` / other | any | Replace CSV + temp view |

Append requires matching column names (order may differ). Mismatch raises `SchemaMismatchError` (subclass of `ValueError`).

## Explicitly ignored

- Delta time travel, `OPTIMIZE`, `VACUUM`, CDF
- SQL `INSERT INTO` / `MERGE` / `DELETE` / `UPDATE`
- `ignore` / `errorifexists` write modes

## Testing

| Suite | Coverage |
|---|---|
| `tests/test_catalog.py` | Identifier parsing, load_all, path/exists, overwrite/append, schema mismatch |
| `tests/test_basic.py` | Existing read/sql/write + append integration |
| E2E workflow | Unchanged; still uses `saveAsTable` overwrite |

## Success criteria

1. No duplicated `schema.table` parsing or path joins in reader/writer/startup.
2. `mode("append").saveAsTable` still appends CSV + refreshes the temp view.
3. Invalid names / append schema mismatch raise catalog errors (compatible with `ValueError`).
4. Full suite green: `python3.14 -m coverage run -m pytest tests/ -v`
