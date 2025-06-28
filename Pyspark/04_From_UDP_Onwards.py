# Databricks notebook source
# 40: UDF in pyspark
#udf is similar to the function in any other language like (sql,java,C etc.)
# similar to sql function, you can write code and save it in database and then reuse this in code wherever is need

data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
sch=['id','firstname','lastname','salary','bonus']
df=spark.createDataFrame(data,sch)
display(df)
# df.printSchema()

# COMMAND ----------

#create function and register it
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import udf
@udf(returnType=IntegerType())
def total_sal(base_Sal,bonus):
    return base_Sal+bonus
@udf(returnType=StringType())
def fullname(fname,lname):
    return(fname + ' '+lname)

# COMMAND ----------

df.select('*',\
          fullname(df.firstname,df.lastname).alias('Emp_Full_Name'),\
          total_sal(df.salary,df.bonus).alias('total_emp_salary')
          ).show()

# COMMAND ----------

#now lets play the functions with tempview
# df.show()


# COMMAND ----------

data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
sch=['empid','firstname','lastname','salary','bonus']
df=spark.createDataFrame(data,sch)
df.createOrReplaceTempView('emps')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emps;

# COMMAND ----------

# Now register the udf to use in sql with tempview
def total_sal(base_Sal,bonus):
    return base_Sal+bonus
def fullname(fname,lname):
    return(fname + ' '+lname)
# register functions
spark.udf.register(name='Total_Salary_SQL',f=total_sal,returnType=IntegerType())
spark.udf.register(name='Full_Name_SQL',f=fullname,returnType=StringType())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use the above two funcitons in sql
# MAGIC SELECT empid,firstname,lastname,Full_Name_SQL(firstname,lastname) as FullName,
# MAGIC salary,bonus,Total_Salary_SQL(salary,bonus) as Total_Sal 
# MAGIC from emps;

# COMMAND ----------

# 41: Convert RDD to Dataframe
# RDD: Resilient Distributed Dataframe: similar to list in python and its immutable and does in memory processing. however dataframe is like a table
# using parallelize() function of sparkcontext you can create a RDD\
data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
rdd=spark.sparkContext.parallelize(data)
# print(type(rdd))
print(rdd.collect()) # we can get data using collect function


# COMMAND ----------

# convert rdd to df
df=rdd.toDF(['empid','firstname','lastname','salary','bonus'])
df.show()
# u can use spark.createDataframe(rdd,schema) this way to convert RDD to df

# COMMAND ----------

# 42: Map transformation in pyspark