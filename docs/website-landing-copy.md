# Landing page copy for [testbricks.netlify.app](https://testbricks.netlify.app/)

Drop-in files for `engineeringmadness/testbricks-website` (this library repo cannot push that repository):

- `docs/website/index.html` → website `index.html`
- `docs/website/App.tsx` → website `src/App.tsx`

The live site still uses `SparkMock` and undersells Lakeflow / `dbutils` coverage from master.

## Meta (`index.html`)

**title:** `testbricks — Run Databricks workflows locally`

**description / og:description:**

> testbricks runs Databricks workflows on your laptop: SparkProxy maps `schema.table` to local CSV, a drop-in dbutils (widgets, fs, secrets, notebook, taskValues), and a workflow runner with DAG, run_if, and repair-and-rerun.

## Hero

**h1:** Run Databricks Workflows E2E / **in your Local Environment** (unchanged)

**body:**

> testbricks is a Python library with genuinely useful mocks — SparkProxy routes `spark.read.table` / `saveAsTable` to CSV on disk, a drop-in `dbutils` for widgets, files, secrets, notebooks, and task values, and a runner that executes a whole workflow JSON (DAG, `run_if`, and repair-and-rerun). No cluster. No waiting around.

**install:** `pip install testbricks`

**fine print:** `Python 3.10+ · JDK on PATH for PySpark · notebooks, scripts and CI`

## Features

**SparkProxy**

> A SparkSession stand-in that speaks the same API. Catalog tables are `{base_path}/{schema}/{table}.csv`; file writes (`parquet` / `json` / `csv`) use native Spark under that folder. `format("delta").save` is parquet on disk — no cluster, no metastore.

**Drop-in dbutils**

> Widgets (including combobox, multiselect, getAll), fs (`ls` / `put` / `cp` / `mv` / `rm` / `mkdirs`), secrets, notebook (`run` / `exit`), `jobs.taskValues`, `library.restartPython`, and `data.summarize`. `%run`, `%sh`, and `%fs` magics work in notebooks the runner executes.

**Workflow runner**

> Parses a Databricks / Lakeflow workflow JSON, walks the task graph, and runs notebooks in order. Understands `run_if` and `depends_on` outcomes, `condition_task`, `for_each_task`, retries, taskValues, and repair-and-rerun (`only` / `from_task`).

**Zero cluster time**

> Iterate in seconds on your laptop. Debug with breakpoints, run it in CI, and keep your compute bill for the things that matter. (unchanged)

## How it works

1. **Swap in the mocks** — Use `SparkProxy` instead of a cluster `SparkSession`. Import `dbutils` from `testbricks.dbutils` (the runner injects it into notebooks).
2. **Drop your data in a folder** — CSV files at `{base_path}/{schema}/{table}.csv` stand in for Unity Catalog tables. Read, write, and inspect them with any tool you like.
3. **Run the whole workflow** — Hand the runner your workflow JSON. It walks the DAG, honors `run_if` / condition / for_each, and re-runs a subgraph with `only` or `from_task`.

## Code examples (`SNIPPETS` in `App.tsx`)

Rename the first tab from `SparkMock` to `SparkProxy`. Update the Prism comment that still says `SparkMock(...)`.

### SparkProxy

**blurb:** Point it at a folder. `schema.table` reads and writes land as `{schema}/{table}.csv` you can open anywhere.

```python
from testbricks import SparkProxy

spark = SparkProxy("./data")

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.customers")
df.write.mode("overwrite").saveAsTable("silver.customers_enriched")
```

### dbutils

**blurb:** Drop-in Databricks `dbutils`: widgets, fs, secrets, notebook, and `jobs.taskValues`. Notebooks the runner executes get it automatically.

```python
from testbricks.dbutils import configure, dbutils

configure("./data")  # same catalog root as SparkProxy

dbutils.widgets.text("filter_country", "USA")
country = dbutils.widgets.get("filter_country")

for info in dbutils.fs.ls("/"):
    print(info.name, info.size)
```

### LocalWorkflowRunner

**blurb:** Feed it an exported workflow JSON. It resolves the DAG and runs every notebook in order — including repair-and-rerun with `only` or `from_task`.

```python
from testbricks import SparkProxy, LocalWorkflowRunner

spark = SparkProxy("./data")
runner = LocalWorkflowRunner(
    source_dir="./notebooks",
    workflow_json_path="./workflow.json",
    base_path="./data",
)
runner.run_workflow(extra_globals={"spark": spark})
# runner.run_workflow(extra_globals={"spark": spark}, only=["build_summary"])
```
