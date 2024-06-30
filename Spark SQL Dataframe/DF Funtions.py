# Databricks notebook source
columns=["Employee_id","Employee Name", "Gender","Salary"]
emp_data=(
    (1,"Irfan", "M", 120000),
    (2,"Irfana", "F", 110000),
    (3,"Nikgil", "", 120000),
    (4,"XYZ", None , 80000)
)

# COMMAND ----------

emp_df=spark.createDataFrame(emp_data, columns)
display(emp_df)

# COMMAND ----------

from pyspark.sql.functions import when
emp_df=emp_df.withColumn("Full_Gender", when(emp_df.Gender == "M", "Male")
                                        .when(emp_df.Gender == "F", "Female")
                                        .when(emp_df.Gender.isNull(), "")
                                        .otherwise(emp_df.Gender))
display(emp_df)

# COMMAND ----------

from pyspark.sql.functions import expr
emp_df1=emp_df.withColumn("Sal_Greater_100000",expr("""case when Salary > 100000 Then "Yes"
                                                         Else "NO"
                                                         end"""))

# COMMAND ----------

display(emp_df1)

# COMMAND ----------

from pyspark.sql.functions import lit
emp_df1=emp_df1.withColumn("manager_id", lit(234))
display(emp_df1)

# COMMAND ----------

display(emp_df[emp_df.Employee_id == 3])

# COMMAND ----------

from pyspark.sql.functions import desc
emp_df=emp_df.orderBy(desc("Salary"))

# COMMAND ----------

display(emp_df.select(max("Salary").alias("max_sal")))

# COMMAND ----------

from pyspark.sql.functions import max, when
display(emp_df[emp_df.Salary!= emp_df.select(max("Salary").alias("max_sal")).max_sal])

# COMMAND ----------

columns=["Employee_id","Employee Name", "Gender","Salary", "department"]
emp_data=(
    (1,"Irfan", "M", 120000, "CSE"),
    (2,"Irfana", "F", 110000,"CSE"),
    (3,"Nikgil", "", 120000,"ECE"),
    (4,"XYZ", None , 180000,"ME"),
    (5,"ABC", None , 280000,"ME"),
    (9,"ABC2", None , 280000,"ME"),
    (6,"asd", None , 120000,"MS"),
    (7,"XYZasd", None , 380000,"MS"),
)
emp_df=spark.createDataFrame(emp_data,columns)

# COMMAND ----------

display(emp_df)

# COMMAND ----------

from pyspark.sql.functions import desc, rank, dense_rank
from pyspark.sql.window import Window
emp_df=emp_df.withColumn("Rank",dense_rank().over(Window.partitionBy("department").orderBy(desc("Salary"))))
display(emp_df)

# COMMAND ----------

display(emp_df.filter("Rank=2"))

# COMMAND ----------

from pyspark.sql.functions import desc, rank, dense_rank, row_number
from pyspark.sql.window import Window
emp_df=emp_df.withColumn("row_number",row_number().over(Window.partitionBy("department").orderBy(desc("Salary"))))
display(emp_df)

# COMMAND ----------

