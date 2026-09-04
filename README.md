# Testbricks

<img width="150" height="auto" alt="image" src="https://github.com/engineeringmadness/testbricks-website/blob/main/public/favicon.svg" />



![workflow](https://github.com/engineeringmadness/testbricks/actions/workflows/tests.yml/badge.svg)

![workflow](https://github.com/engineeringmadness/testbricks/actions/workflows/release.yml/badge.svg)


Databricks notebooks are awsome way for interactive development. The inbuilt IDE is fantastic, stable, fully featured and has the right AI assistance levels. So what's the problem ?

Testing is the main problem, more specifically Unit testing.

Testbricks is my effort to decouple a typical Databricks Stack - 
- Delta lake and Unity Catalog etc for storing data
- Notebooks with Pyspark code to transform said data
- Databricks jobs to orchestrate a bunch of notebooks

## Quickstart

```bash
pip install testbricks
```

PySpark needs a JDK on `PATH` (Java 8+). Create a `SparkProxy`, import `dbutils`, and run a Databricks workflow JSON with `LocalWorkflowRunner`:

```python
from testbricks import SparkProxy, LocalWorkflowRunner
from testbricks.dbutils import dbutils

# CSV tables live under {base_path}/{schema}/{table}.csv
# e.g. ./data/bronze/customers.csv  →  spark.read.table("bronze.customers")
spark = SparkProxy("./data")

# Optional: use dbutils in the driver process (notebooks get it automatically)
dbutils.widgets.text("filter_country", "USA")
print(dbutils.widgets.get("filter_country"))

runner = LocalWorkflowRunner(
    source_dir="./notebooks",          # local .py files named after the notebook
    workflow_json_path="./workflow.json",
    base_path="./data",                # same catalog root as SparkProxy
)
runner.run_workflow(extra_globals={"spark": spark})
```

`workflow.json` is a Databricks job export with a `tasks` list. Notebook paths are resolved to `{source_dir}/{last_path_segment}.py`:

```json
{
  "tasks": [
    {
      "task_key": "enrich_customers",
      "notebook_task": {
        "notebook_path": "/Workspace/jobs/enrich_customers",
        "base_parameters": { "filter_country": "USA" }
      }
    },
    {
      "task_key": "build_summary",
      "depends_on": [{ "task_key": "enrich_customers" }],
      "notebook_task": {
        "notebook_path": "/Workspace/jobs/build_summary"
      }
    }
  ]
}
```

A notebook such as `notebooks/enrich_customers.py` can use the usual Databricks names — `spark` is injected via `extra_globals`, and `dbutils` is injected by the runner:

```python
dbutils.widgets.text("filter_country", "ALL")
country = dbutils.widgets.get("filter_country")

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.customers")
if country != "ALL":
    df = df.filter(df.country == country)

df.write.mode("overwrite").saveAsTable("silver.customers_enriched")
```

## Spark write modes and schema options

Table writes (`saveAsTable` / `insertInto`) stay CSV-backed. File writes (`csv` / `parquet` / `json` / `save`) use native Spark under `base_path`. `format("delta").save(path)` is stored as parquet (no Delta log).

| Write | Missing table | Existing table |
|---|---|---|
| default / `overwrite` | create | replace rows |
| `append` | create | append rows (exact column set, unless `mergeSchema`) |
| `error` / `errorIfExists` | create | raise `AnalysisException` |
| `ignore` | create | no-op (file and temp view unchanged) |
| `insertInto` | raise `AnalysisException` | append, or replace when `overwrite=True` / `mode("overwrite")` |

Schema flags:

| Option | Effect |
|---|---|
| `overwriteSchema=true` + overwrite | replace the CSV even when columns change |
| `overwriteSchema=false` (default) + overwrite | raise `SchemaMismatchError` on incompatible schema change |
| `mergeSchema=true` + append | union missing columns with nulls |
| `mergeSchema=false` (default) + append | raise `SchemaMismatchError` if columns differ |
| same column, incompatible types | always raise `SchemaMismatchError` (merge only adds columns) |

`partitionBy` columns must exist on the DataFrame. `option("replaceWhere", "<predicate>")` with `mode("overwrite")` deletes matching stored rows then appends the new frame. `bucketBy` / `sortBy` are accepted no-ops (bucketing is not simulated). `df.writeTo(table).using(...).create()` / `.replace()` / `.append()` maps onto the same table writer; `createOrReplace` and `overwritePartitions` raise `NotImplementedError` with a `saveAsTable` hint.

## Key Modules
1. `SparkProxy` - A Spark proxy that manipulates incoming Delta table reads and writes and redirects them to interactions with CSV files stored locally
2. `LocalWorkflowRunner` - A notebook orchestrator that takes the notebook .py files as defined in a Databricks Workflow JSON file and executes them as per the DAG definition. Databricks comment magics `%run`, `%sh` (`# %sh` / `# MAGIC %sh`, including `%sh -e`), and `%fs` (`# %fs` / `# MAGIC %fs`) work in those notebooks.
3. `dbutils` - A drop in replacement for Databricks `dbutils`. Supports `fs` (`ls`/`put`/`cp`/`mv`/`rm`/`mkdirs`), `widgets` (including `combobox`/`multiselect`/`getAll`), `secrets` (`get`/`list`/`listScopes`), `notebook` (`exit` / `%run`), `library.restartPython`, and `data.summarize`.
