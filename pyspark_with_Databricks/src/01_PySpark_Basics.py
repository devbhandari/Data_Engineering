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
