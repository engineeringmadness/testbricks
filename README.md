# Testbricks

Databricks notebooks are awsome way for interactive development. The inbuilt IDE is fantastic, stable, fully featured and has the right AI assistance levels. So what's the problem ?

Testing is the main problem, more specifically Unit testing.

Testbricks is my effort to decouple a typical Databricks Stack - 
- Delta lake and Unity Catalog etc for storing data
- Notebooks with Pyspark code to transform said data
- Databricks jobs to orchestrate a bunch of notebooks

Allow unit testing to be done in CI environments like GitHub / GitLab using local spark and CSV files.

## Setup

> pip install pyspark pandas pytest

> cd tests

> pytest
