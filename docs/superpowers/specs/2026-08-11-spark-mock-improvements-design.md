# SparkMock Improvements Design — `saveAsTable` Append Mode

**Date:** 2026-08-11  
**Status:** Approved for implementation  
**Scope:** Enhance `DataFrameWriter.saveAsTable()` to honor `mode("append")` against CSV + temp views.  
**Out of scope:**
- `DbutilsMock`, `LocalWorkflowRunner`, notebook executor
- SQL DDL/DML: `DELETE`, `MERGE`, `UPDATE`, `INSERT INTO`, `CREATE TABLE`, `DROP TABLE`
- Delta Lake features: time travel, `OPTIMIZE`, CDF, Z-ORDER, transaction log
- SQL identifier rewriting, `GroupedData` wrapping, catalog refactor (deferred)

## Goal

Notebooks commonly write with:

```python
df.write.mode("append").saveAsTable("schema.table")
df.write.mode("overwrite").saveAsTable("schema.table")
```

Today `saveAsTable` always overwrites the CSV via pandas and ignores `mode()`. This change makes **`append`** work. Other modes keep today’s overwrite behavior (explicit `overwrite` and the default when mode is unset).

## Current Behavior

```python
# data_frame_writer.py — mode is stored but unused in saveAsTable
pandas_df.to_csv(csv_path, ...)  # always replaces file
self._dataframe.createOrReplaceTempView(f"{schema}_{table}")  # view = new rows only
```

## Target Behavior

| `mode` | CSV exists? | Behavior |
|---|---|---|
| `"append"` | No | Create CSV + temp view from the DataFrame (same as first write) |
| `"append"` | Yes | Append rows to existing CSV; refresh temp view with **combined** data |
| `"overwrite"` / `None` / other | any | Replace CSV + temp view (unchanged from today) |

### Append rules

1. Read existing CSV with `header=True` (same convention as mock table writes).
2. Require column names to match (order may differ — align to existing column order before concat).
3. On schema mismatch (missing/extra columns), raise `ValueError` with a clear message.
4. Write a single CSV (header once) via pandas; then `createOrReplaceTempView` from the combined Spark DataFrame so `spark.sql` and later reads see all rows.
5. Atomicity: write to a temp file in the schema directory, then `os.replace` onto the target CSV.

### Explicitly ignored

- Delta time travel (`VERSION AS OF`, `@v`, etc.)
- `OPTIMIZE`, `VACUUM`, CDF
- `INSERT INTO` / `MERGE` / `DELETE` / `UPDATE` SQL
- `ignore` / `errorifexists` write modes (not implemented; treated like overwrite if somehow passed, or left as today’s overwrite path)

## Implementation

Single-file change in `src/mock/data_frame_writer.py` inside `saveAsTable`:

```text
parse schema.table
ensure schema directory
csv_path = base/schema/table.csv
new_pdf = dataframe.toPandas()

if mode == "append" and csv exists:
    existing_pdf = pandas.read_csv(csv_path)
    validate columns
    combined = concat([existing, new aligned to existing columns])
else:
    combined = new_pdf

write combined to csv_path (temp + replace)
spark.createDataFrame(combined) or read back → createOrReplaceTempView(schema_table)
```

Prefer refreshing the temp view from the combined pandas frame via `spark_session.createDataFrame` so append does not leave the view showing only the latest batch.

## Testing Plan

Add to `tests/test_basic.py` (or a focused `tests/test_save_as_table.py`):

| Test | Expectation |
|---|---|
| Append to missing table | Creates CSV with new rows; sql/read sees them |
| Append to existing table | Row count = old + new; values from both batches present |
| Append then read via `read.table` | Same row count as sql temp view |
| Overwrite still replaces | Second `mode("overwrite")` write → only new rows |
| Default (no mode) still replaces | Same as overwrite today |
| Append schema mismatch | `ValueError` |

Regression: existing `TestWriteTable` / E2E workflow tests remain green.

## Success Criteria

1. `df.write.mode("append").saveAsTable("s.t")` appends rows to `{base}/s/t.csv` and updates `s_t` temp view.
2. `mode("overwrite")` and unset mode continue to replace the table.
3. No new SQL DML/DDL or Delta-feature surface area.
4. Full suite passes: `python3.14 -m coverage run -m pytest tests/ -v`
