from src.mock.spark_mock import SparkMock

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
    df = spark.read.option("header", "true").table("f1_data.drivers")
    df.write.option("mode", "overwrite").saveAsTable("f1_data.drivers_copy")
    df = spark.read.option("header", "true").table("f1_data.drivers_copy")
    assert df.count() == 21, "DataFrame should have 21 rows"