# Databricks notebook source
#pivot is like any other simple pivot in excel,sql etc.

from pyspark.sql.functions import when,count,min,max,sum
data =[(1,'ram','M',1000,'IT'),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',3000,'IT'),(4,'Mohani','F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df=spark.createDataFrame(data,sch)
display(df)
df.groupBy(['dept_name','gender']).count().show()
df.groupBy('dept_name').pivot('gender').count().show() # pivot on all values of gender column
df.groupBy('dept_name').pivot('gender',['M']).count().show() # pivot on a specific value for example 'M' here

# COMMAND ----------

#32: Unpivot Dataframe
# rotating columns into rows, there is no direct unpivot function , which can be achieved using stack()
sch=['Dept','no_males','no_females']
data =[('IT','8','9'),('HR','10','20'),('Payrol','5','1')]
df=spark.createDataFrame(data,sch)
display(df)
from pyspark.sql.functions import expr
unpivot_df=df.select('Dept',expr("stack(2,'Male',no_males,'Female',no_females) as (gender,count)"))
unpivot_df.show()

# COMMAND ----------

# 33. Fill and fillna
# when you want to replace nulls/none values inside a dataframe
data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df=spark.createDataFrame(data,sch)
display(df)
# df.printSchema()
df_strng=df.fillna("No_Value").show()
display(df_strng)
# final_df=df_strng.fillna(0, subset=["salary"])
# display(final_df)
# df_filled = df.fillna({"salary": "No_Value"}).na.replace("salary", {None: "No_Value"})
df_filled = df_strng.fillna(0, subset=["salary"])
display(df_filled)
# df.na.fill("No_Value_Given").show()

# COMMAND ----------

# from pyspark.sql.functions import col

# # Replace both None and null values in the "salary" column with "No_Value"
# df_filled = df.fillna({"salary": "No_Value"}).na.replace("salary", {None: "No_Value"})

# df_filled.show()

data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df=spark.createDataFrame(data,sch)

# Replace both None and null values in the "salary" column with "No_Value"
df_filled = df.fillna(0, subset=["salary"])

# Show the DataFrame
df_filled.show()

# COMMAND ----------

data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df = spark.createDataFrame(data, sch)

# Define default values for each data type
default_values = {
    "name": "NO_VALUE",
    "gender": "NO_VALUE",
    "salary": 0,
    "Dept_name": "NO_VALUE"
}

# Replace None values with default values
for col_name, default_val in default_values.items():
    df = df.fillna(default_val, subset=[col_name])

# Show the DataFrame
df.show()


# COMMAND ----------

data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df = spark.createDataFrame(data, sch)

# Replace None values with default values for each column
df_filled = df.fillna("NO_VALUE", subset=["name", "gender", "Dept_name"]) \
              .fillna(0, subset=["salary"])

# Show the DataFrame
df_filled.show()


# COMMAND ----------

#34. sample :sampling data from large dataset using fractio and seed

df=spark.range(1,101)
display(df)
df.sample(fraction=0.1).show()
# df.sample(fraction=0.1,seed=10).show() # shows same random value each time you execute

# fraction=0.1: This parameter specifies the fraction of rows to be sampled. Here, 0.1 means 10%, indicating that we want to sample 10% of the DataFrame's rows.
# seed=10: This parameter sets the seed for reproducibility. When a seed is specified, the sampling process will generate the same random sample each time it's executed with the same seed value. This is useful for reproducibility purposes. In this case, seed=10 sets the seed value to 10.

# COMMAND ----------

#35. Collect : these are actions not transformation ..it returns an array/list of rows
#use collect for small dataset not for big datasets
data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df = spark.createDataFrame(data, sch)
display(df)
list_row=df.collect()
print(type(list_row))
print(list_row[0][1])


# COMMAND ----------

#36.Dataframe.transform functions :
# it is used to chain the custom transformations/user defined functions and this function returns the new dataframe after applying the specified transformations
#37: pyspark.sql.functions.transform() : this can be applied on a collumn which is an array type. it will perform transformation and then give you back a column with array type column

#38. CreateOrReplace TempView() : you can use sql queries like DB 
# Create a temporary view, these views are session scoped and cannot be shared between the sessions.

data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df = spark.createDataFrame(data, sch)
display(df)

df.createOrReplaceTempView("employees");
sql_df=spark.sql("""
                 SELECT *,
                 case
                 when e.gender ='M' then 'Male'
                 else 'Female'
                 end as Gender_Category
                 FROM employees e where salary is null
                 """
                 )
display(sql_df)

# once the temporary view is created you can utilize normal sql with magic command as below:


# COMMAND ----------

# MAGIC %sql
# MAGIC                  SELECT *,
# MAGIC                  case
# MAGIC                  when e.gender ='M' then 'Male'
# MAGIC                  else 'Female'
# MAGIC                  end as Gender_Category
# MAGIC                  FROM employees e  

# COMMAND ----------

#39. CreateOrReplaceGlobalTempView():
# it can be access across the session with in spark application
# to acces these we need append global_temp.<tablename>

data =[(1,'ram','M',1000,None),(2,'Shyam','M',2000,'HR'),(3,'Mohana','F',None,'IT'),(4,None,'F',5000,'HR')]
sch=['id','name','gender','salary','Dept_name']
df = spark.createDataFrame(data, sch)
display(df)

df.createOrReplaceGlobalTempView("global_employees");
sql_df=spark.sql("""
                 SELECT *,
                 case
                 when e.gender ='M' then 'Male'
                 else 'Female'
                 end as Gender_Category
                 FROM employees e where salary >2000
                 """
                 )
display(sql_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * from global_employees --The table or view `global_employees` cannot be found
# MAGIC SELECT * from global_temp.global_employees; --this will work so to acces global temp table we need append 'global_temp' in front of name
# MAGIC -- this table will be acces in another notebook session too
# MAGIC

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------

