# E2E Workflow Test Design (SparkProxy + LocalWorkflowRunner + dbutils)

**Date:** 2026-08-09  
**Status:** Draft for review  
**Scope:** Standalone end-to-end test combining `SparkProxy`, `LocalWorkflowRunner`, and `dbutils`; plus a backward-compatible enhancement to `run_workflow()` so the three components can integrate.

## Goal

Create a standalone E2E test that runs a realistic Databricks-style pipeline through the proxy: three notebooks arranged as a DAG (two parallel source notebooks → one dependent consumer notebook). The first two notebooks each call `spark.read.table(...)`, apply a transformation driven by a `dbutils.widgets.get()` parameter (set via environment variables before the test starts), and write an output table via `df.write.saveAsTable(...)`. The third notebook reads both output tables, joins and aggregates them, and writes the final table. Everything — sample data, notebooks, workflow JSON, and test code — lives in a new directory inside `tests/`.

## Requirements Summary

| Decision | Choice |
|---|---|
| Test location | `tests/e2e_workflow/` |
| Framework change | `run_workflow()` gains `extra_globals` (spark injection) and honors `base_parameters` (widget params) |
| Spark injection | Test passes `spark` into `run_workflow(extra_globals={"spark": spark})` |
| Widget parameters | Set as `os.environ` before the test; `run_workflow()` wraps each notebook in `argument_override_context(base_parameters.keys())` so `dbutils.widgets.text()` does not clobber them |
| Env var precedence | Test-set env vars win; JSON `base_parameters` act as defaults via `os.environ.setdefault` |
| NB3 transformation | `spark.sql()` with `USING(customer_id)` + unqualified columns (avoids `.`→`_` mangling) |
| Sample data domain | E-commerce (customers + orders) |
| Widget params | `filter_country=USA`, `min_amount=100` |

## Motivation / Integration Gaps

Two gaps prevent `SparkProxy` + `LocalWorkflowRunner` + `dbutils` from working together end-to-end today:

1. **No `spark` injection** — `run_workflow()` builds `execution_globals` internally with only `dbutils` and `__run_notebook__`. Notebooks have no `spark` object on which to call `.read.table()` / `.sql()` / `.write.saveAsTable()`.
2. **`base_parameters` ignored + widget defaults clobber env vars** — `dbutils.widgets.text(name, default)` unconditionally overwrites `os.environ[name]` with the default. Only `argument_override_context` (used today solely by `run_isolated`) prevents this, and `run_workflow()` neither reads `base_parameters` from the workflow JSON nor activates that context.

## Approaches Considered

| Approach | Trade-off |
|---|---|
| **A. Enhance `run_workflow()`** *(chosen)* | Add `extra_globals` param for `spark` injection; read `base_parameters` from the workflow JSON, seed env vars via `setdefault` (test-set values win), and wrap each notebook in `argument_override_context`. Most faithful to Databricks semantics; small backward-compatible change; cleanest integration. |
| B. Test-only workarounds | Each notebook constructs its own `SparkProxy` from an env var; pre-register widget names. Self-contained but `spark` isn't runner-managed and the pattern diverges from real Databricks usage. |
| C. Hybrid | Inject `spark` via a small change; handle widgets entirely on the test side. Less faithful — `base_parameters` (a real workflow concept) goes unused. |

**Decision: Approach A.** It makes the framework correctly honor `base_parameters` and `spark` injection — capabilities the project needs for any real notebook workflow — while staying backward-compatible.

## Architecture

Two deliverables:

```
Framework change (src/mock/local_workflow_runner.py)
    └─ run_workflow() gains: base_parameters parsing + extra_globals + argument_override_context

E2E test package (tests/e2e_workflow/)
    ├─ data/bronze/customers.csv        ← source table 1
    ├─ data/bronze/orders.csv           ← source table 2
    ├─ notebooks/enrich_customers.py    ← NB1: filter by widget + add column → silver
    ├─ notebooks/enrich_orders.py       ← NB2: filter by widget → silver
    ├─ notebooks/build_summary.py       ← NB3: SQL join + SUM → gold
    ├─ workflow.json                    ← 3-task DAG (2 parallel → 1 dependent)
    └─ test_e2e_workflow.py             ← the test
```

## Framework Change — `run_workflow()` (backward-compatible)

### `__init__` / `_parse_tasks()` — base-parameter storage

- Add `self._notebook_base_params: dict[str, dict[str, str]] = {}` in `__init__`.
- In `_parse_tasks()`, after extracting the notebook name, read `notebook_task.get("base_parameters", {})`. Validate it is a `dict` (raise `ValueError` if not). Store as `self._notebook_base_params[notebook_name] = base_parameters`.

### `run_workflow(self, extra_globals=None)`

- Build `execution_globals` exactly as today (`__name__`, `dbutils`, `__run_notebook__`).
- If `extra_globals` is provided, `execution_globals.update(extra_globals)` — this is how the test injects `spark`.
- In the per-notebook execution loop, before executing each notebook:
  - `base_params = self._notebook_base_params.get(notebook_name, {})`
  - For each `key, value` in `base_params`: `os.environ.setdefault(key, str(value))` — **test-set env vars take precedence** over JSON defaults; if the test didn't set a value, the JSON default applies.
  - Wrap `executor.exec_file(notebook_path, execution_globals, top_level=True)` in `argument_override_context(base_params.keys())` — this prevents `dbutils.widgets.text(name, default)` from overwriting the pre-set env value, so `dbutils.widgets.get(name)` returns the test's value.

### Backward compatibility

- `extra_globals` defaults to `None` (no change for existing callers).
- Tasks without `base_parameters` yield an empty dict → empty override context → identical behavior to today.
- The existing `tests/test_local_workflow_runner.py` suite must still pass unchanged.

## Sample Data

`data/bronze/customers.csv` — 5 customers across 3 countries:

| customer_id | name | country | signup_date |
|---|---|---|---|
| 1 | Alice | USA | 2023-01-15 |
| 2 | Bob | UK | 2023-02-20 |
| 3 | Charlie | USA | 2023-03-10 |
| 4 | Diana | Canada | 2023-04-05 |
| 5 | Eve | USA | 2023-05-22 |

`data/bronze/orders.csv` — 7 orders with varied amounts:

| order_id | customer_id | amount | order_date |
|---|---|---|---|
| 101 | 1 | 250.00 | 2023-06-01 |
| 102 | 2 | 50.00 | 2023-06-02 |
| 103 | 1 | 120.00 | 2023-06-03 |
| 104 | 3 | 75.00 | 2023-06-04 |
| 105 | 5 | 300.00 | 2023-06-05 |
| 106 | 2 | 90.00 | 2023-06-06 |
| 107 | 4 | 200.00 | 2023-06-07 |

## Workflow JSON

Mirrors `specs/workflow_sample.json` structure. The `notebook_path`'s last path segment becomes the notebook name the runner uses to find `{source_dir}/{name}.py`:

```json
{
  "name": "e2e-customer-orders-pipeline",
  "tasks": [
    {
      "task_key": "enrich_customers",
      "notebook_task": {
        "notebook_path": "/Workspace/e2e/notebooks/enrich_customers",
        "base_parameters": { "filter_country": "ALL" },
        "source": "WORKSPACE"
      }
    },
    {
      "task_key": "enrich_orders",
      "notebook_task": {
        "notebook_path": "/Workspace/e2e/notebooks/enrich_orders",
        "base_parameters": { "min_amount": "0" },
        "source": "WORKSPACE"
      }
    },
    {
      "task_key": "build_summary",
      "depends_on": [
        { "task_key": "enrich_customers" },
        { "task_key": "enrich_orders" }
      ],
      "notebook_task": {
        "notebook_path": "/Workspace/e2e/notebooks/build_summary",
        "source": "WORKSPACE"
      }
    }
  ]
}
```

## Notebooks

All three use `spark` and `dbutils` as injected globals (no imports of the mock framework).

### NB1 — `enrich_customers.py`

Reads `bronze.customers`, filters by widget param, adds an `is_active` column, writes `silver.customers_enriched`:

```python
from pyspark.sql.functions import lit

dbutils.widgets.text("filter_country", "ALL")
country = dbutils.widgets.get("filter_country")

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.customers")
if country != "ALL":
    df = df.filter(df.country == country)

df = df.withColumn("is_active", lit(True))
df.write.mode("overwrite").saveAsTable("silver.customers_enriched")
```

### NB2 — `enrich_orders.py`

Reads `bronze.orders`, filters by widget param, writes `silver.orders_enriched`:

```python
dbutils.widgets.text("min_amount", "0")
min_amount = float(dbutils.widgets.get("min_amount"))

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.orders")
df = df.filter(df.amount >= min_amount)

df.write.mode("overwrite").saveAsTable("silver.orders_enriched")
```

### NB3 — `build_summary.py`

Reads both silver output tables via `spark.sql`, joins + aggregates, writes `gold.customer_order_summary`:

```python
result = spark.sql("""
    SELECT customer_id,
           name,
           SUM(amount) AS total_amount
    FROM silver_customers_enriched
    JOIN silver_orders_enriched USING (customer_id)
    GROUP BY customer_id, name
    ORDER BY total_amount DESC
""")
result.write.mode("overwrite").saveAsTable("gold.customer_order_summary")
```

### Design rationale for NB3

- **Why `spark.sql()`:** `DataFrameWrapper.__getattr__` wraps `DataFrame` returns but not `GroupedData`. `df.groupBy(...)` returns a raw `GroupedData`; `.agg(...)` returns a raw `DataFrame` whose `.write` is Spark's native writer (not the mock `DataFrameWriter`), so `saveAsTable` would bypass the CSV-backed mock. `spark.sql()` sidesteps this entirely.
- **Why `USING(customer_id)` + unqualified columns:** `SparkProxy.sql()` does a blanket `query.replace('.', '_')`. A dotted reference like `c.customer_id` would be mangled to `c_customer_id` and break the query. Using `USING(customer_id)` makes the join column unambiguous, so all column references can be unqualified (dot-free) and survive the replacement intact.
- **Core command coverage:** the pipeline exercises all three commands from AGENTS.md — `spark.read.table` (NB1/NB2), `df.write.saveAsTable` (all three), and `spark.sql` (NB3).

## Test File — `test_e2e_workflow.py`

```python
WIDGET_PARAMS = {"filter_country": "USA", "min_amount": "100"}

@pytest.fixture
def e2e_env(tmp_path):
    # Copy data to tmp_path so output writes don't pollute the repo.
    data_dir = tmp_path / "data"
    shutil.copytree(DATA_DIR, str(data_dir))
    # Set widget parameters as env vars BEFORE the workflow starts.
    for key, value in WIDGET_PARAMS.items():
        os.environ[key] = value
    spark = SparkProxy(str(data_dir))
    yield spark, str(data_dir)
    # Tear down env vars to prevent cross-test leakage.
    for key in WIDGET_PARAMS:
        os.environ.pop(key, None)

def test_e2e_workflow_runs_full_pipeline(e2e_env):
    spark, data_dir = e2e_env
    runner = LocalWorkflowRunner(NOTEBOOKS_DIR, WORKFLOW_PATH, data_dir)
    runner.run_workflow(extra_globals={"spark": spark})

    # NB1: USA filter → 3 customers (Alice, Charlie, Eve)
    customers = spark.read.option("header","true").option("inferSchema","true") \
        .table("silver.customers_enriched")
    assert customers.count() == 3
    assert {r.name for r in customers.collect()} == {"Alice", "Charlie", "Eve"}

    # NB2: amount >= 100 → 4 orders (101, 103, 105, 107)
    orders = spark.read.option("header","true").option("inferSchema","true") \
        .table("silver.orders_enriched")
    assert orders.count() == 4
    assert {r.order_id for r in orders.collect()} == {101, 103, 105, 107}

    # NB3: inner join → Alice(370.0), Eve(300.0); Charlie dropped (no qualifying orders)
    summary = spark.read.option("header","true").option("inferSchema","true") \
        .table("gold.customer_order_summary")
    rows = summary.collect()
    assert len(rows) == 2
    assert rows[0].name == "Alice" and float(rows[0].total_amount) == 370.0
    assert rows[1].name == "Eve" and float(rows[1].total_amount) == 300.0
```

## Data Flow

```
                          env: filter_country=USA              env: min_amount=100
                                  │                                     │
bronze/customers.csv ─[NB1: filter country==USA + is_active]─▶ silver/customers_enriched.csv  (3 rows)
                                  │                                     │
                                  │   bronze/orders.csv ─[NB2: filter amount>=100]─▶ silver/orders_enriched.csv  (4 rows)
                                  │          │                                     │
                                  └──────────┴──────[NB3: JOIN USING(customer_id) + SUM(amount)]──────▶ gold/customer_order_summary.csv  (2 rows)
```

Stage-by-stage row counts (with test widget values `USA` / `100`):

| Stage | Table | Rows | Why |
|---|---|---|---|
| Input | `bronze.customers` | 5 | raw source |
| Input | `bronze.orders` | 7 | raw source |
| NB1 out | `silver.customers_enriched` | 3 | USA only (Alice, Charlie, Eve) |
| NB2 out | `silver.orders_enriched` | 4 | amount ≥ 100 (orders 101, 103, 105, 107) |
| NB3 out | `gold.customer_order_summary` | 2 | inner join: Alice=250+120=**370.0**, Eve=300=**300.0**; Charlie dropped (his only order 104=75 was filtered out); Diana dropped (not USA) |

These numbers are chosen so the widget-parameter effect is unambiguous: if the widget override mechanism failed, NB1 would return 5 rows and NB2 would return 7 rows (defaults `ALL` / `0`), making the failure immediately visible.

## Error Handling

- **Backward compatibility:** `extra_globals=None` default + empty `base_parameters` → no-op override context. The entire existing `tests/test_local_workflow_runner.py` suite must pass unchanged.
- **Env var isolation:** the fixture tears down `filter_country` and `min_amount` in teardown, preventing leakage into other tests (the `dbutils` widgets registry is a module singleton, but widget names here are unique to this test and `.text()` is idempotent on the registry set).
- **`base_parameters` validation:** a non-dict value raises `ValueError` at parse time (covered by a unit test).
- **Notebook execution order:** guaranteed by the runner's topological sort — NB3 always runs after NB1 and NB2, so the silver temp views it queries via `spark.sql()` already exist in the shared SparkSession.

## Testing Plan

1. **New E2E test** (`tests/e2e_workflow/test_e2e_workflow.py`) — asserts row counts and specific values at all three stages, proving: spark injection works, `spark.read.table` / `saveAsTable` / `spark.sql` all function end-to-end, widget parameters flow from env var → `dbutils.widgets.get()` → transformation logic, and the DAG dependency ordering is respected.
2. **New unit tests** (added to `tests/test_local_workflow_runner.py`) — `base_parameters` non-dict raises `ValueError`; `extra_globals` injects keys into the notebook namespace.
3. **Full suite regression** — `python -m pytest tests/ -v` confirms zero regressions from the `run_workflow()` change.

## Out of Scope

- `dbutils.notebook.run()` (isolated notebook invocation with its own `argument_override_context` — already covered by `run_isolated`)
- `spark.write.table` / other write APIs beyond `saveAsTable`
- `%run` / `__run_notebook__` notebook chaining inside the E2E notebooks
- Parallel workflow execution — the runner executes notebooks sequentially in topological order
