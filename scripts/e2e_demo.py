#!/usr/bin/env python3.14
"""End-to-end demo of SparkMock read/transform/write workflow."""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mock.spark_mock import SparkMock


def main():
    base = tempfile.mkdtemp(prefix="testbricks-demo-")
    try:
        # Seed demo data from the bundled F1 dataset
        src = os.path.join(os.path.dirname(__file__), "..", "tests", "data", "f1_data")
        dst = os.path.join(base, "f1_data")
        shutil.copytree(src, dst)

        spark = SparkMock(base)

        print("=== SparkMock end-to-end demo ===\n")

        # 1. Read a table (spark.read.table)
        drivers = spark.read.option("header", "true").table("f1_data.drivers")
        total = drivers.count()
        print(f"1. Read f1_data.drivers -> {total} rows")

        # 2. Query via SQL (spark.sql)
        uk_count = spark.sql(
            "SELECT COUNT(*) AS cnt FROM f1_data_drivers WHERE Country = 'United Kingdom'"
        ).collect()[0].cnt
        print(f"2. SQL query -> {uk_count} UK drivers")

        # 3. Transform and write (df.write.saveAsTable)
        uk = (
            drivers.filter("Country = 'United Kingdom'")
            .select("Abbreviation")
            .distinct()
        )
        uk.write.mode("overwrite").saveAsTable("f1_data.uk_drivers")

        result = spark.read.option("header", "true").table("f1_data.uk_drivers")
        abbreviations = sorted(row.Abbreviation for row in result.collect())
        print(f"3. Wrote f1_data.uk_drivers -> {abbreviations}")

        csv_path = os.path.join(base, "f1_data", "uk_drivers.csv")
        assert os.path.exists(csv_path), f"Expected CSV at {csv_path}"

        print("\nDemo completed successfully.")
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
