# Testbricks

Databricks notebooks are awsome way for interactive development. The inbuilt IDE is fantastic, stable, fully featured and has the right AI assistance levels. So what's the problem ?

Testing is the main problem, more specifically Unit testing.

Testbricks is my effort to decouple a typical Databricks Stack - 
- Delta lake and Unity Catalog etc for storing data
- Notebooks with Pyspark code to transform said data
- Databricks jobs to orchestrate a bunch of notebooks

## Quickstart
> pip install testbricks

## Key Modules
1. `SparkMock` - A Spark proxy that manipulates incoming Delta table reads and writes and redirects them to interactions with CSV files stored locally
2. `LocalWorkflowRunner` - A notebook orchestrator that takes the notebook .py files as defined in a Databricks Workflow JSON file and executes them as per the DAG definition. Adding support also for few magic commands such as `%run` and `%sh` which are commonly used in Databricks notebooks.
3. `dbutils` - A drop in replacement for dbutils. Currently supports `fs` module but future plans include support for `widgets` and `notebook` modules as well.
