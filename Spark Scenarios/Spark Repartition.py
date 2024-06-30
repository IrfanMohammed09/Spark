# Databricks notebook source
# MAGIC %fs head /FileStore/tables/uspopulation.csv

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

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df_new=df.withColumn("partitionID", spark_partition_id())

# COMMAND ----------

display(df_new)

# COMMAND ----------

df_agg=df_new.groupBy("partitionID").count()
display(df_agg)

# COMMAND ----------

repart_df=df.repartition(3)

# COMMAND ----------

repart_df.rdd.getNumPartitions()

# COMMAND ----------

repart_df_new=repart_df.withColumn("paritionId",spark_partition_id())
display(repart_df_new)

# COMMAND ----------

repart_df_agg=repart_df_new.groupBy("paritionId").count()
display(repart_df_agg)