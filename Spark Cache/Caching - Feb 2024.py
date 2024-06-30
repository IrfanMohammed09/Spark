# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv

# COMMAND ----------

# MAGIC %md # Spark is a in-memory data processing framework, why do we need to cache?

# COMMAND ----------

# MAGIC %md # 1. Create a DF (Without Cache)

# COMMAND ----------

fire_df = (spark
           .read
           .format("csv")
           .option("header", "true")
           .option("inferSchema", "true") # spark will read first block of data to make guess about the columns
           .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"))

# COMMAND ----------

# MAGIC %md # 2. Try an action on fire_df (loads data from disk)

# COMMAND ----------

# groupBy, agg, select are transformations and write is an action
from pyspark.sql.functions import * 
(fire_df
 .groupBy("Zipcode of Incident")
 .agg(max("Delay").alias("MaxDelay"), min("Delay").alias("MinDelay"))
 .select("Zipcode of Incident", "MaxDelay", "MinDelay")
 .write
 .format("noop")
 .mode("overwrite")
 .save("/FileStore/temp"))

# COMMAND ----------

fire_df.show(5)

# COMMAND ----------

# MAGIC %md # 3. Try another action on fire_df (loads data again from the disk)

# COMMAND ----------

(fire_df
 .select("CallType")
 .where("CallType is not null")
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .write
 .format("noop")
 .mode("overwrite")
 .save("/FileStore/temp"))

# COMMAND ----------

# MAGIC %md # 4. DF Creation (With Cache)

# COMMAND ----------

fire_df = (spark
           .read
           .format("csv")
           .option("header", "true")
           .option("inferSchema", "true") # spark will read first block of data to make guess about the columns
           .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"))

fire_df.cache()

# COMMAND ----------

# MAGIC %md # 5. Try an action on fire_df (loads data from disk as this is the 1st action after caching and keeps in memory won't remove from memory as in previous operations where we didnot caching)

# COMMAND ----------

# groupBy, agg, select are transformations and write is an action
from pyspark.sql.functions import * 
(fire_df
 .groupBy("Zipcode of Incident")
 .agg(max("Delay").alias("MaxDelay"), min("Delay").alias("MinDelay"))
 .select("Zipcode of Incident", "MaxDelay", "MinDelay")
 .write
 .format("noop")
 .mode("overwrite")
 .save("/FileStore/temp"))

# COMMAND ----------

# MAGIC %md # 6. Try another action on fire_df (uses cached data)

# COMMAND ----------

(fire_df
 .select("CallType")
 .where("CallType is not null")
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .write
 .format("noop")
 .mode("overwrite")
 .save("/FileStore/temp"))

# COMMAND ----------

# MAGIC %md # Unpersist DF

# COMMAND ----------

fire_df.unpersist()

# COMMAND ----------

