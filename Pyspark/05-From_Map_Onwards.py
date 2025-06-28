# Databricks notebook source
# MAGIC %sql
# MAGIC select 4/2;

# COMMAND ----------

data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
sch=['id','firstname','lastname','salary','bonus']
df=spark.createDataFrame(data,sch)
display(df)

# COMMAND ----------

#42. map ()-- tranformation of RDD used to apply function(lambda)on every element of RDD and returns a new RDD
# Dataframe doesnt have map() transformation, so u need to generate RDD first

data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())
rdd1=rdd.map(lambda x : x + (x[1] + ' ' + x[2],))
print(rdd1.collect())

# COMMAND ----------

data =[(1,'Ram','Singh',2000,500),(2,'Shyam','Rawat',10000,700),(3,'Mohan','Kumar',3000,300)]
df= spark.createDataFrame(data,['id','fn','ln'])
# display(df)
rdd1=df.rdd.map(lambda x : x + (x[1] + ' ' + x[2],))
df1 = rdd1.toDF(['id','fn','ln','fullname'])
df1.show()
# df1.select('id','fn','ln','fullname').show()


# COMMAND ----------

#43. flatmap: flattens the list /array on top of RDD objects and returns a new RDD
# its not available with dartaframe so for that you can use explode in dataframe
data = [('Ram', 'Singh'), ('Shyam', 'Rawat'), ('Mohan', 'Kumar')]
rdd = spark.sparkContext.parallelize(data)

# Print the original RDD
for item in rdd.collect():
    print(item)

# Flatten the tuples and apply split on each string element
rdd1 = rdd.flatMap(lambda x: [elem for elem in x])

# Print the transformed RDD
for item1 in rdd1.collect():
    print(item1)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# 44 : partitionBy ()--partitioned large dataset in to smaller files based on one or multiple columns
data =[(1,'Ram','Singh','Male',2000,500),(2,'Shyamoli','Rawat','Female',10000,700),(3,'Mohani','Kumar','Female',3000,300),(4,'Mohan','Kumar','Male',3000,300)]
df=spark.createDataFrame(data,['id','fname','lname','Gender','salary','bonus'])
display(df)
df.write.parquet(path='/FileStore/team_employee/',mode='overwrite',partitionBy=['Gender'])

# COMMAND ----------

df_all=spark.read.parquet(path='/FileStore/team_employee/')
df_male=df_all.filter(df_all.Gender == 'Male')
# df_all.show()
df_male.show()

# COMMAND ----------

# from_json() converts json string in to maptype or structtype
# 45:from_json() function : take any json string and convert it in to maptype
# 46: from_json() function : take any json string and convert it in to Structtype
#47 : to_json : convert any column (maptype/structType) to json type
#48 json_tuple : used to extract element /query from json string column and create  as new columns
#49: get_json_object : extract JSON string based on path from json column
#50:date functions in pyspark
# current_date
# date_fromat
# to_date:
# 51: datediff,month_between,add_months,date_add(),year(),month()
# 52: timestamp function  :current_timestamp,to_timestamp,hour,minute,second
# 53:approx_count_distinct,avg(),collect_list,collect_Set,count_distinct,count
# 54 :row_number,rank,dense_rank