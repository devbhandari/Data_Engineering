# Databricks notebook source
# import pyspark.sql.types
# from pyspark.sql import SparkSession
data =[(1,'ram',['Maths','Hindi']),(2,'Shyam',['Science','Hindi']),(3,'Mohan',['English','Hindi'])]
sch=['id','name','courses']
df=spark.createDataFrame(data,sch)
display(df)
df.printSchema()

# COMMAND ----------

# use explode functions to create multiple rows for array columns

from pyspark.sql.functions import explode,col
df1= df.withColumn("skills",explode(col="courses"))
df1.show()

# COMMAND ----------

## Split function to split data on the basis of delimiter and return an array
from pyspark.sql.functions import split,col
data =[(1,'ram','Maths,Hindi'),(2,'Shyam','Science,Hindi'),(3,'Mohan','English,Hindi')]
sch=['id','name','courses']
tempdf=spark.createDataFrame(data=data,schema=sch)
display(tempdf)
splt_df=tempdf.withColumn("skills",split('courses',','))
display(splt_df)


# COMMAND ----------

# array_contains() : this function to check if an array column contains a specific value or not
# - returns null if array is null, false -- if doesnt contain that value else true 

# COMMAND ----------

from pyspark.sql.functions import array_contains,col
data =[(1,'ram',['Maths','Hindi']),(2,'Shyam',['Java','Hindi']),(3,'Mohan',['English','Hindi'])]
sch=['id','name','courses']
df=spark.createDataFrame(data,sch)
# display(df)
df1 = df.withColumn("HasJavaSkill",array_contains(col('courses'),'Java'))
display(df1)

#  notes be mindful that it is cases sensitive while comparing the values. it will give output false if we check against JAVA,JaVA

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

## Video 15. Map Function in Pyspark
# map function is like dictionary in python, where maptype will represent the values in key and value pair
from pyspark.sql.types import StructType,StructField,MapType,StringType,IntegerType
sample_data=[(1,'ram',{'country':'INDIA','state':'rajasthan'}),
             (2,'Scott',{'country':'USA','state':'newyork'})
             ]
# sch=['id','name','address']
sch = StructType([ \
        StructField('id',dataType=IntegerType()), \
        StructField('name',dataType=StringType()), \
        StructField('Address',MapType(keyType=StringType(),valueType=StringType()))

])
df=spark.createDataFrame(sample_data,sch)
display(df)
df.printSchema()

# COMMAND ----------

# handle null values in array (no array, array with missing element etc)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, array_remove

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Handle Null Values Example") \
    .getOrCreate()

# Sample DataFrame with null values in the 'courses' column
data = [
    (1, 'Alice', ['Maths', 'Hindi']),
    (2, 'Bob', None),
    (3, 'Charlie', ['Science', None]),
    (4, 'David', ['English', 'Hindi', 'Maths'])
]
schema = ["id", "name", "courses"]
df = spark.createDataFrame(data, schema)

# Handle null values in the 'courses' column
df_cleaned = df.withColumn("courses", when(col("courses").isNull(), [])
                                      .otherwise(col("courses")))

# Show the cleaned DataFrame
df_cleaned.show(truncate=False)


# COMMAND ----------

# to access the value of map in diffrent column
df1 = df \
        .withColumn('Country',df.Address['country']
            #  'State',df.Address['state']       
                    ) \
        .withColumn('State',df.Address.getItem('state'))
display(df1)

# COMMAND ----------

#map_keys and map_values functions provides the keys and values from Dictionary in pyspark.
# in above example map_keys will provide country and state however map_values will provide values like india,rajasthan,usa,newyork

# COMMAND ----------

# Video17: ROW class in pyspark
# In PySpark, Row is a class in the pyspark.sql module that represents a single row of data in a DataFrame. It is similar to a named tuple in Python, where fields can be accessed as attributes or as dictionary keys.
# Row objects are used internally by PySpark to store and manipulate data within DataFrames. You can also create Row objects explicitly to construct DataFrames from a list of rows.

from pyspark.sql import Row
row1=Row('ramesh','1000')
# access by index
print(row1[0] + ' ' + str(row1[1]))
#  access by param name
row2=Row(name='rajesh', salary='1000')
print(row2.name + ' ' + str(row2.salary))

# you can create dataframe using above row function
from pyspark.sql import Row
row1= Row(name='raj',sal=1000)
row2= Row(name='sartaj',sal=2000)
row3= Row(name='Faraz',sal=3000)

data = [row1,row2,row3]
df=spark.createDataFrame(data=data)
display(df)
df.printSchema()

# COMMAND ----------

# 18: columns in pyspark
# it represent a column in dataframe, you can perfrom various operations on column.
# to create a column class object the simplest way is to use lit function from pyspark.sql.functions import lit

# from pyspark.sql.functions import lit
# col1 = lit('abcd')
# print(col1)
# print(type(col1))

#different ways to fetch the column

data =[(1,'ram',['Maths','Hindi']),(2,'Shyam',['Science','Hindi']),(3,'Mohan',['English','Hindi'])]
sch=['id','name','courses']
df=spark.createDataFrame(data,sch)
# display(df)
# df.printSchema()

# 1st way to fetch column using column name
# df1=df.select(df.name).show()
# 2nd way to fetch column using index
# df2=df.select(df['name']).show()
# 3rd way to fetch column using col function
# df3=df.select(col('name')).show()

# COMMAND ----------

# 19: when and otherwise functions in pyspark
from pyspark.sql.functions import when
data =[(1,'ram','M',1000),(2,'Shyam','M',2000),(3,'Mohan','',3000),(3,'Mohan','',3000)]
sch=['id','name','sex','salary']
df=spark.createDataFrame(data,sch)
# display(df)
# df.printSchema()
# df1=df.select(df.id,df.name,\
#               when(df.sex =='M','Male').\
#               when (df.sex =='F','Female').\
#               otherwise('NotMentioned').alias('gender')
#               ).show()
#20 : alias ,asc,desc,cast,like
# alias
# df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.sex.alias('gender'),df.salary.alias('emp_salary')).show()
# asc
# df.sort(df.name.asc()).show()
# cast
# df1=df.select(df.id,df.name,df.sex,df.salary.cast('int'))
# df1.printSchema()

# like function
# df2=df.filter(df.name.like('M%'))
# display(df2)

#21: filter() and where() in pyspark

#filter
# display(df)
# df.filter(df.sex=='M').show()
# df.filter(df.salary > 2000).show()

#where :
# df.where(df.sex=='M').show()
# df.where((df.salary >= 2000) & (df.sex=='M')).show()

# 22. distinct() and dropduplicates()
# #  Distinct take all columns in consideration for removing duplicates, however dropduplicates take one or more columns in consideration
# df.show() # will show 4 rows
# df.distinct().show() # will show 3 rows
# df.dropDuplicates().show()# will show 3 rows
# df.dropDuplicates(['sex']).show()# will show 2 rows

#23. orderBy and sort() in pyspark
# df.show()
# df.sort(df.sex.asc(),df.salary.desc()).show()
# df.orderBy(df.sex.asc(),df.salary.desc()).show()

#24. Union vs UnionAll
# same as sql (to merge 2 or more than 2 dataframes with same schema or structure). to remove duplicates use distinct cz even union will not remove it unlike sql

data1=[(1,'ram','delhi',1000),(2,'shyam','jaipur',2000),(3,'Mohan','delhi',3000),(3,'Mohan','delhi',3000)]
df1=spark.createDataFrame(data1,['id','name','place','salary'])

data2=[(11,'abram','delhi',1000),(12,'Ghanshyam','jaipur',2000),(13,'Mohana','delhi',3000)]
df2=spark.createDataFrame(data2,['id','name','place','salary'])

# union_merged_df=df1.union(df2)
# # display(union_merged_df) #dups are here
# unionall_merged_df=df1.union(df2)
# # display(unionall_merged_df) #dups are here
# unionall_merged_df.distinct().show()
# union_merged_df.distinct().show()

# 25.Groupby in spark:
# same as sql group by
# df.show()
# df.groupBy('sex').count().show()
# df.groupBy('sex').sum('salary').show()


# COMMAND ----------

# 26. groupby and agg() fuynctions in pyspark.
# agg()-- to calculate more than one aggregate functions in one go


# df.printSchema()
# # 
# # df.groupBy('gender').count().alias('count_of_emp').show() # one agg function at a time

# df.groupBy('gender').agg(count('*').alias('count_emp_per_gender'),\
#                         min('salary').alias('min_sal_per_gender'),\
#                         max('salary').alias('max_Sal_pr_gender'),\
#                         sum('salary').alias('sum_Sal_pr_gender')).show()

#27: unionByname in pyspark
# merge two or more dataframe with different schema(columns) by passing allowmissingcolumns with value as True.

# data1=[(1,'ram','delhi',1000),(2,'shyam','jaipur',2000),(3,'Mohan','delhi',3000),(3,'Mohan','delhi',3000)]
# df1=spark.createDataFrame(data1,['id','name','place','salary'])

# data2=[(11,'abram',1000),(12,'Ghanshyam',2000),(13,'Mohana',3000)]
# df2=spark.createDataFrame(data2,['id','name','salary'])

# # df1.union(df2).show() #error : UNION can only be performed on inputs with the same number of columns, but the first input has 4 columns and the second input has 3 columns.;
# df1.unionByName(df2,allowMissingColumns=True).show()

#28: select function in pyspark:similar to select in sql
# you may select coulmns using index,columns from list and the nested columns from dataframe
# different ways to use the select in pyspark

# df.select('id','name').show()
# df.select(df.id,df.name).show()
# df.select(df['id'],df['name']).show()

# from pyspark.sql.functions import col
# df.select(col('id'),col('name')).show()

# df.select(['id','name','salary']).show()
# df.select('*').show()

#29 join() in pyspark : similar to sql joins(to join the data from two or more tables/dataframes)
# inner :
# left:
# right
# full:

# data1=[(1,'ram','delhi',1),(2,'shyam','jaipur',2),(3,'Mohan','delhi',1),(3,'Sohan','delhi',4)]
# emp_df=spark.createDataFrame(data1,['id','name','place','dept_id'])

# data2=[(1,'IT'),(2,'HR'),(3,'Finance')]
# dept_df=spark.createDataFrame(data2,['dept_id','name'])
# display(emp_df)
# display(dept_df)
# # inner join
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'inner').select('*').show()
# # left join
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'left').select('*').show()
# # right join
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'right').select('*').show()

# # full join
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'full').select('*').show()


# COMMAND ----------

# 30 : left semi,left anti, and self join
# leftsemi -- similar like inner join but only get rows from left side only dataframe
# leftanti -- is opposite to left semi it gets not matching rows from left side dataframe
# self join --join data with same dataframe 

# data1=[(1,'ram','delhi',1),(2,'shyam','jaipur',2),(3,'Mohan','delhi',2),(3,'Sohan','delhi',4)]
# emp_df=spark.createDataFrame(data1,['id','name','place','dept_id'])

# data2=[(1,'IT'),(2,'HR'),(3,'Finance')]
# dept_df=spark.createDataFrame(data2,['dept_id','name'])
# display(emp_df)
# display(dept_df)
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'inner').show() #like normal sql inner join columns from both the side of dfs for join criteria
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'left_semi').show() #same result as above but only left side columns
# emp_df.join(dept_df,emp_df.dept_id==dept_df.dept_id,'left_anti').show()# columns from left side only and row which is not matched from left df

# SELF JOIN
data=[(1,'ram',0),(2,'mukesh',1),(3,'Mohan',2)]
sch=['empid','name','mgr_id']
emp_df=spark.createDataFrame(data,sch)
# display(emp_df)
# empdf1=emp_df.alias('emp')
# mgrdf2=emp_df.alias('mgr')
# tt=empdf1.join(mgrdf2,empdf1.mgr_id==mgrdf2.empid,'inner').show()
# tt.show()
emp_alias = emp_df.alias('emp')
mgr_alias = emp_df.alias('mgr')
joined_df = emp_alias.join(mgr_alias, emp_alias["mgr_id"] == mgr_alias["empid"], 'inner')\
            .select(emp_alias["empid"].alias("emp_empid"),
                    emp_alias["name"].alias("emp_name"),
                    emp_alias["mgr_id"].alias("emp_mgr_id"),
                    mgr_alias["empid"].alias("mgr_empid"),
                    mgr_alias["name"].alias("mgr_name"),
                    mgr_alias["mgr_id"].alias("mgr_mgr_id")
            )
joined_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Sample data and schema
data = [(1, 'ram', 0), (2, 'mukesh', 1), (3, 'Mohan', 2)]
sch = ['empid', 'name', 'mgr_id']

# Create DataFrame
emp_df = spark.createDataFrame(data, sch)

# Alias the DataFrame
emp_alias = emp_df.alias('emp')
mgr_alias = emp_df.alias('mgr')

# Perform the join
joined_df = emp_alias.join(mgr_alias.withColumnRenamed("empid", "mgr_empid"), 
                           emp_alias["mgr_id"] == mgr_alias["mgr_empid"], 
                           'inner')

# Select the desired columns
joined_df = joined_df.select(
    emp_alias["empid"].alias("emp_empid"),
    emp_alias["name"].alias("emp_name"),
    emp_alias["mgr_id"].alias("emp_mgr_id"),
    mgr_alias["empid"].alias("mgr_empid"),  
    mgr_alias["name"].alias("mgr_name")
)

# Show the result
joined_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Sample data and schema
data = [(1, 'ram', 0), (2, 'mukesh', 1), (3, 'Mohan', 2)]
sch = ['empid', 'name', 'mgr_id']

# Create DataFrame
emp_df = spark.createDataFrame(data, sch)

# Alias the DataFrame
emp_alias = emp_df.alias('emp')
mgr_alias = emp_df.alias('mgr')

# Rename columns in one of the DataFrames to avoid ambiguity
mgr_alias = mgr_alias.withColumnRenamed("empid", "mgr_empid").withColumnRenamed("name", "mgr_name")

# Perform the join
joined_df = emp_alias.join(mgr_alias, emp_alias["mgr_id"] == mgr_alias["mgr_empid"], 'inner')

# Select the desired columns
joined_df = joined_df.select(
    emp_alias["empid"].alias("emp_empid"),
    emp_alias["name"].alias("emp_name"),
    emp_alias["mgr_id"].alias("emp_mgr_id"),
    mgr_alias["mgr_empid"],
    mgr_alias["mgr_name"]
)

# Show the result
joined_df.show()


# COMMAND ----------

