dbutils.widgets.text("min_amount", "0")
min_amount = float(dbutils.widgets.get("min_amount"))

df = spark.read.option("header", "true").option("inferSchema", "true").table("bronze.orders")
df = df.filter(df.amount >= min_amount)

df.write.mode("overwrite").saveAsTable("silver.orders_enriched")
