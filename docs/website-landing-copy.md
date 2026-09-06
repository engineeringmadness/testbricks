# Landing page copy for [testbricks.netlify.app](https://testbricks.netlify.app/)

Drop-in files for `engineeringmadness/testbricks-website` (this library repo cannot push that repository):

- `docs/website/index.html` → website `index.html`
- `docs/website/App.tsx` → website `src/App.tsx`

The live site still uses the old `SparkMock` name in examples. Marketing copy stays high-level; the code tabs show the current API.

## Meta (`index.html`)

**title:** `testbricks — Run Databricks workflows locally`

**description:**

> testbricks runs Databricks workflows on your laptop with a Spark proxy, a drop-in dbutils, and a workflow runner.

**og:description:**

> Run Databricks workflows on your laptop with SparkProxy, a dbutils mock, and a JSON workflow runner.

## Hero

**h1:** Run Databricks Workflows E2E / **in your Local Environment** (unchanged)

**body:**

> testbricks is a Python library with genuinely useful mocks — a Spark proxy that reads and writes tables as CSV, a drop-in `dbutils` replacement, and a runner that executes a whole workflow JSON in dependency order. No cluster. No waiting around.

**install:** `pip install testbricks`

**fine print:** `Python 3.10+ · works in notebooks, scripts and CI`

## Features

**SparkProxy**

> A SparkSession stand-in that speaks the same API. Table reads and writes land as CSV files on disk — no cluster, no metastore, no waiting.

**Drop-in dbutils**

> Widgets, filesystem helpers, secrets, and more. Import it instead of the real thing and your notebook runs unchanged.

**Workflow runner**

> Parses a Databricks workflow JSON, builds the task graph, and runs notebooks in the right order on your machine.

**Zero cluster time**

> Iterate in seconds on your laptop. Debug with breakpoints, run it in CI, and keep your compute bill for the things that matter.

## How it works

1. **Swap in the mocks** — Replace the Spark session and dbutils object with the testbricks equivalents at the top of your notebook.
2. **Drop your data in a folder** — CSV files in a base directory stand in for your tables. Read, write and inspect them with any tool you like.
3. **Run the whole workflow** — Hand the runner your workflow JSON and it walks the dependency graph, notebook by notebook, right on your machine.

## Code examples (`SNIPPETS` in `App.tsx`)

First tab label: `SparkProxy` (not `SparkMock`).

### SparkProxy

**blurb:** Point it at a folder. Table reads and writes land as plain CSV files you can open anywhere.

```python
from testbricks import SparkProxy

spark = SparkProxy("./data")

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.customers")
df.write.mode("overwrite").saveAsTable("silver.customers_enriched")
```

### dbutils

**blurb:** A drop-in replacement for the Databricks dbutils object. Notebooks the runner executes get it automatically.

```python
from testbricks.dbutils import configure, dbutils

configure("./data")  # same catalog root as SparkProxy

dbutils.widgets.text("filter_country", "USA")
country = dbutils.widgets.get("filter_country")

for info in dbutils.fs.ls("/"):
    print(info.name, info.size)
```

### LocalWorkflowRunner

**blurb:** Feed it your exported workflow JSON. It resolves the task graph and runs every notebook in dependency order.

```python
from testbricks import SparkProxy, LocalWorkflowRunner

spark = SparkProxy("./data")
runner = LocalWorkflowRunner(
    source_dir="./notebooks",
    workflow_json_path="./workflow.json",
    base_path="./data",
)
runner.run_workflow(extra_globals={"spark": spark})
```
