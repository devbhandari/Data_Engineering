# Databricks notebook source
# MAGIC %sql
# MAGIC select 'hello from databricks' as message

# COMMAND ----------

print("Hello from VS Code → Databricks 🚀")

print (2+3)# COMMAND ----------

# COMMAND ----------
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql("""
select 'hello' as msg
""")
df.show()
# COMMAND ----------

# day2
from pyspark.sql.functions import col
df_nyctaxi=spark.read.table("sample.nyctaxi.tripdata")
display(df_nyctaxi)
# df_nyctaxi_filter=df.nyctaxi.select(col("pickupz")
# COMMAND ----------
