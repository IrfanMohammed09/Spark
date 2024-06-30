# Databricks notebook source
# MAGIC %md
# MAGIC Read the file
# MAGIC

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/uspopulation.csv

# COMMAND ----------

# MAGIC %md
# MAGIC Convert the file to Spark Dataframe

# COMMAND ----------

df=(spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("delimiter","|")
    .load("/FileStore/tables/uspopulation.csv"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Delta Table from Spark Dataframe

# COMMAND ----------

(df
 .write
 .format("delta")
 .option("mergeSchema",True)
 .mode("append")
 .saveAsTable("worldStocks"))

# COMMAND ----------

# MAGIC %md
# MAGIC Once We create delta table we get the power of using data frame as sql table. Delta tables are based on databricks lakehouse architecture, so they are optimized both from warehousing BI tasks and Machine Learning tasks.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from worldStocks

# COMMAND ----------

# MAGIC %md
# MAGIC Delta table store data in Parquet format

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/worldstocks/

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc formatted worldStocks 

# COMMAND ----------

spark.sql("insert into worldStocks values (234,'Hyderabad','IN',4234324,314234,'0.234')")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from worldStocks where city='Hyderabad'

# COMMAND ----------

data=[
    [239,'Hyderabad','TG',4234324,314234,'0.234','IN'],
    [236,'Mumbai','MH',4234324,314234,'0.234','IN'],
    [237,'Delhi','DL',4234324,314234,'0.234','IN'],
    [238,'Bangalore','KA',4234324,314234,'0.234','IN']
]
columns=("2019_rank",	"City",	"State_Code",	"2019_estimate",	"2010_Census",	"Change", "Country")
df2=spark.createDataFrame(data, columns)
display(df2)

# COMMAND ----------


from pyspark.sql.functions import col
df2=df2.withColumn("2019_rank", col("2019_rank").cast("integer"))
df2=df2.withColumn("2019_estimate", col("2019_estimate").cast("integer"))
df2=df2.withColumn("2010_Census", col("2010_Census").cast("integer"))

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Two delta tables, to show that delta table can also store unstructured data, or the schema of delta table is dynamic

# COMMAND ----------

(df2
 .write
 .format("delta")
 .option("mergeSchema",True)
 .mode("append")
 .saveAsTable("worldStocks"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from worldstocks

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/worldstocks/

# COMMAND ----------

# MAGIC %sql
# MAGIC update worldstocks
# MAGIC set country= 'United States' 
# MAGIC where country is null

# COMMAND ----------

# MAGIC %md
# MAGIC Time Travel -> Delta tables stores previous version of the table

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history worldstocks

# COMMAND ----------

# MAGIC %md
# MAGIC Revert back to previous versions

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from worldstocks
# MAGIC version as of 4
# MAGIC where country is null

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from worldstocks
# MAGIC version as of 3
# MAGIC where country is null