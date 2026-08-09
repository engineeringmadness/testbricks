from pyspark.sql.functions import lit

dbutils.widgets.text("filter_country", "ALL")
country = dbutils.widgets.get("filter_country")

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.customers")
if country != "ALL":
    df = df.filter(df.country == country)

df = df.withColumn("is_active", lit(True))
df.write.mode("overwrite").saveAsTable("silver.customers_enriched")
