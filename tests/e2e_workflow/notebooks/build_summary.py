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
