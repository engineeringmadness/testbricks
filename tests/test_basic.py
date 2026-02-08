from src.mock.spark_mock import SparkMock
import os

TEST_DIR = "tests/data"


def test_read_table():
    spark = SparkMock(TEST_DIR)
    df = spark.read.option("header", "true").table("f1_data.drivers")
    assert df.count() == 21, "DataFrame should have 21 rows"


def test_read_sql():
    spark = SparkMock(TEST_DIR)
    df = spark.sql("SELECT distinct Abbreviation FROM f1_data.drivers")
    assert df.count() == 21, "DataFrame should have 21 rows"


def test_write_table():
    spark = SparkMock(TEST_DIR)
    df = spark.parallelize([("Alice", 30), ("Bob", 25), ("Charlie", 35)]).toDF(["Name", "Age"])
    
    df.write.option("mode", "overwrite").saveAsTable("default.sample_data")
    df = spark.sql("SELECT * FROM default.sample_data")
    
    assert df.count() == 3, "DataFrame should have 21 rows"


def test_write_transformed_table():
    spark = SparkMock(TEST_DIR)
    
    df = spark.read.option("header", "true").table("f1_data.drivers")
    df2  = df.filter("Country = 'United Kingdom'")\
                .select("Abbreviation")\
                .distinct()
    df2.write.option("mode", "overwrite").saveAsTable("f1_data.uk_drivers")

    assert os.path.exists(os.path.join(TEST_DIR, "f1_data/uk_drivers.csv")), "CSV file should be created"