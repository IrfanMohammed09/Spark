# Databricks notebook source
# MAGIC %fs head /FileStore/tables/Orders_csv/part_00000

# COMMAND ----------

type(spark)

# COMMAND ----------

Orders_df=(spark
           .read
           .format("csv")
           .load("/FileStore/tables/Orders_csv/part_00000")
           .toDF("order_id", "orderDate", "OrderCustomerId", "Status"))

# COMMAND ----------

Orders_df.show()

# COMMAND ----------

Orders_df.show(7)

# COMMAND ----------

Orders_df.show(truncate=False)

# COMMAND ----------

Orders_df.collect()

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/json/ford/ford.json

# COMMAND ----------

ford_df=(spark
         .read
         .format("json")
         .load("/FileStore/tables/json/ford/ford.json"))

# COMMAND ----------

ford_df.show(35)

# COMMAND ----------

ford_df1=(spark
         .read
         .format("json")
         .option("mode","DROPMALFORMED")
         .load("/FileStore/tables/json/ford/ford.json"))

# COMMAND ----------

ford_df1.show()

# COMMAND ----------

display(ford_df1)

# COMMAND ----------

ford_df2=(spark
         .read
         .format("json")
         .option("mode","FAILFAST")
         .load("/FileStore/tables/json/ford/ford.json"))

# COMMAND ----------

ford_df2=(spark
         .read
         .format("json")
         .option("badRecordsPath","/FileStore/tables/badRecords/")
         .load("/FileStore/tables/json/ford/ford.json"))

# COMMAND ----------

display(ford_df2)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/badRecords/20240612T020439/bad_records/

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/badRecords/20240612T020439/bad_records/part-00000-8a403c1c-bc7e-451e-bb22-274bea960d6a

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/json/employee.csv

# COMMAND ----------

employee_df=(spark
         .read
         .format("csv")
         .option("header","true")
         .load("/FileStore/tables/json/employee.json"))

# COMMAND ----------

employee_df.show(5)

# COMMAND ----------

