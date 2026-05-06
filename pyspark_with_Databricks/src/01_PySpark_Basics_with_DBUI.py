# Databricks notebook source
# MAGIC %sql
# MAGIC select 'hello from databricks' as message

# COMMAND ----------

# this data set(samples.nyctaxi.trips) resides in catalog-->delta shares receieved
df_nyctaxi = spark.read.table("samples.nyctaxi.trips")
# display(df_nyctaxi)
df_nyctaxi.show(10)

# COMMAND ----------

# explore schema
df_nyctaxi.printSchema()
# count records
# df_nyctaxi.count() #21932


# COMMAND ----------

# DBTITLE 1,Cell 4
# basic sql analysis
spark.sql("""
Select
 pickup_zip,
  AVG(fare_amount) AS avg_fare
FROM samples.nyctaxi.trips
GROUP BY pickup_zip
ORDER BY pickup_zip
LIMIT 10
""").show()

# COMMAND ----------

#bronze level
df_nyctaxi_bronze=spark.read.table("samples.nyctaxi.trips")
# df_nyctaxi_bronze.show(5)
display(df_nyctaxi_bronze.limit(5))

# COMMAND ----------

#Silver layer
df_nyctaxi_silver=df_nyctaxi_bronze.filter(df_nyctaxi_bronze.fare_amount>100)
display(df_nyctaxi_silver.limit(5))

# COMMAND ----------

#Gold layer
df_nyctaxi_gold=df_nyctaxi_silver.groupBy("pickup_zip").count()
display(df_nyctaxi_gold)

# COMMAND ----------

