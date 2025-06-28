# Databricks notebook source
# MAGIC %md 
# MAGIC ## This is first class to learn pyspark
# MAGIC ##### 1. learn about basics of pyspark
# MAGIC ##### 2. Magic command to use different interpretor like python and scala in same notebook

# COMMAND ----------

# Run python command
print("Hi Python")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "hi sql"

# COMMAND ----------

# MAGIC %scala
# MAGIC println("hey scala")

# COMMAND ----------

#first simple program
sample_data=[(1,'ram'),(2,'shyam'),(3,'Mohan')]
df=spark.createDataFrame(data=sample_data,schema=['id','name'])
df.show()
df.printSchema()

# COMMAND ----------

#lets define our own schema instead pyspark infer it on the basis of data
from pyspark.sql.types import *
sample_data =[(1,'ram',15.20),(2,'Mohan',25.30),(3,'Mukesh',45.20)]
sample_schema= StructType([StructField(name='sid',dataType=IntegerType()),
                           StructField(name='sname',dataType=StringType()),
                           StructField(name='Salary',dataType=FloatType())
                            ])
df=spark.createDataFrame(data=sample_data,schema=sample_schema)
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV file

# COMMAND ----------

df=spark.read.csv(path='dbfs:/FileStore/tables/emp1-1.csv', header=True)
display(df)
df.printSchema()

# COMMAND ----------

#another way to read csv file using format ,option and load method
df=spark.read.format(source='csv').option(key='Header',value='True').load('dbfs:/FileStore/tables/emp1-1.csv')
display(df)
df.printSchema()

# COMMAND ----------

#read multiple csv files from a folder
df=spark.read.csv(path='dbfs:/FileStore/tables/',header=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Dataframe in to CSV using, write object
# MAGIC

# COMMAND ----------

# from pyspark.sql import *
# data
from pyspark.sql.types import *
sample_data =[(1,'ram',15.20),(2,'Mohan',25.30),(3,'Mukesh',45.20)]
df=spark.createDataFrame(data=sample_data,schema=['id','name','salary'])
display(df)

# COMMAND ----------

# help(df.write.csv)

# COMMAND ----------

df.write.csv(path='dbfs:/FileStore/tables/export',header=True)

# COMMAND ----------

display(spark.read.csv(path='dbfs:/FileStore/tables/export'),header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Jason File in Pyspark
# MAGIC #### use spark.read.json 
# MAGIC #### or
# MAGIC #### spark.read.format(source='json').option(key='Header',value='True').load('dbfs:/FileStore/tables/emp1-1.csv')
# MAGIC #### similarly we can use to read other files like parquet etc.

# COMMAND ----------

#use of StructType.add method to create schema
from pyspark.sql.types import *
sample_data =[(1,'ram',3000),(2,'shyam',4000),(3,'mohan',4000)]
sample_schema=StructType().add(field="id",data_type=IntegerType(),nullable=True)\
                        .add(field="name",data_type=StringType(),nullable=True)\
                        .add(field="Salary",data_type=IntegerType(),nullable=True) 
                            
df=spark.createDataFrame(data=sample_data,schema=sample_schema)
display(df)

# COMMAND ----------

# help(df.withColumn)
# add a new column with withColumn
from pyspark.sql.functions import col,lit
# df.show()
df1=df \
        .withColumn(colName="Bonus",col=(col("Salary")*.05).cast(dataType=IntegerType())) \
        .withColumn("Country",lit("india")) \
        .withColumn("Copied_Salary",col("Salary")) \
        .withColumn("Total_Salary",col=(col("Salary")+col("Bonus")).cast(dataType=IntegerType()))    
df1.show()
# df.printSchema()
df1.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## withColumnRenamed() : rename column in pyspark

# COMMAND ----------

df1.show()

# COMMAND ----------

df2=df1.withColumnRenamed("Copied_Salary","Dup_Salary_col")
df2.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC # use nested columns in the structure in pyspark

# COMMAND ----------

from pyspark.sql.types import *
sample_data =[(1,('ram','singh'),2000),(2,('Adam','Grant'),3000),(3,('Naval','Ravikant'),3000)]
structname= StructType([ \
                    StructField("firstname",StringType()), \
                    StructField("Lastname",StringType())
])
sample_schema= StructType([ \
                StructField("id",IntegerType()),\
                StructField("name",structname), \
                StructField("Salary",IntegerType())
])
df=spark.createDataFrame(data=sample_data,schema=sample_schema)
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC # use ArrayType Columns in Pyspark

# COMMAND ----------

from pyspark.sql.types import *
data =[(1,['C++','Java'],20),(2,['Python','SQL'],30)]
schema= StructType([ \
                    StructField("id",IntegerType()), \
                    StructField("Courses",ArrayType(StringType())),\
                    StructField("Fees",IntegerType())
])
df=spark.createDataFrame(data=data,schema=schema)
# df.show()
display(df)

# COMMAND ----------

#explode array function in to multiple columns
df1 = df \
        .withColumn("course_1",df.Courses[0]) \
        .withColumn("course_2",df.Courses[1])
df1.show()

# COMMAND ----------

# how to create a arary coulmn from diffferent multiple columns
from pyspark.sql.functions import col,array
data =[(1,2),(3,4)]
sch=['num1','num2']
df=spark.createDataFrame(data=data,schema=sch)
df.show()
df1=df.withColumn('arraycol',array(col('num1'),col('num2')))
df1.show()

# COMMAND ----------

# start from pyspark playlist from wafa studies (video 14)