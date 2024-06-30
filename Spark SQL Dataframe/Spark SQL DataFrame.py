# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/json/department.csv

# COMMAND ----------

dept_df=(spark
         .read
         .format("csv")
         .option("header","true")
         .option("inferSchema","true")
         .load("/FileStore/tables/json/department.csv"))

# COMMAND ----------

display(dept_df)

# COMMAND ----------

emp_df=(spark
         .read
         .format("csv")
         .option("header","true")
         .option("inferSchema","true")
         .load("/FileStore/tables/json/employee.csv"))

# COMMAND ----------

display(emp_df)

# COMMAND ----------

dept_df.createOrReplaceTempView("department")

# COMMAND ----------

emp_df.createOrReplaceTempView("employee")

# COMMAND ----------

(spark.sql("SELECT * from department")).show()

# COMMAND ----------

display(spark.sql("SELECT * from employee"))

# COMMAND ----------

display(spark.sql("""
                  with employee_rank as (
                  select name,salary, dense_rank() over(order by salary desc) as Rank_Salary
                   from employee)
                   select * from employee_rank where Rank_Salary==2
                  """))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc
emp_rank_df=emp_df.withColumn("salaryRank", dense_rank().over(Window.orderBy(desc("Salary"))))
display(emp_rank_df.filter(emp_rank_df.salaryRank == 2))