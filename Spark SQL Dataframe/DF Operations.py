# Databricks notebook source
# MAGIC %fs head /FileStore/tables/Departments_csv/part_00000

# COMMAND ----------

dept_df=(spark
         .read
         .format("csv")
         .option("delimiter","|")
         .load("/FileStore/tables/Departments_csv/part_00000"))

# COMMAND ----------

dept_df.show(10)

# COMMAND ----------

dept_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
dept_df_schema=StructType([
    StructField("ID",IntegerType()),
    StructField("DepartmentType",StringType())
])

# COMMAND ----------

dept_df=(spark
         .read
         .format("csv")
         .option("delimiter","|")
         .schema("Id int, Type string")
         .load("/FileStore/tables/Departments_csv/part_00000"))

# COMMAND ----------

display(dept_df)

# COMMAND ----------

dept_df.printSchema()

# COMMAND ----------

department_df=(spark
               .read
               .format("CSV")
               .option("header","true")
               .option("inferSchema","true")
               .load("/FileStore/tables/Departments_csv/department.csv"))

# COMMAND ----------

display(department_df)

# COMMAND ----------

department_df.printSchema()

# COMMAND ----------

department_df=(spark
               .read
               .format("CSV")
               .option("header","true")
               
               .load("/FileStore/tables/Departments_csv/department.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType, DateType
department_df1_schema=StructType([
    StructField("Employee_ID",IntegerType()),
    StructField("DepartmentName",StringType()),
    StructField("Client",StringType()),
    StructField("OnboardedDate",DateType())
])

# COMMAND ----------

type(department_df1_schema)

# COMMAND ----------

department_df1_schema

# COMMAND ----------

department_df1=(spark
               .read
               .format("CSV")
               .option("header","true")
               .schema(department_df1_schema)
               .load("/FileStore/tables/Departments_csv/department.csv"))

# COMMAND ----------

display(department_df1)

# COMMAND ----------

department_df1.printSchema()

# COMMAND ----------

department_df2=(spark
               .read
               .format("CSV")
               .option("header","true")
               .schema("EmployeeId int, Department_name string, Cleint string, OnboardedDate date")
               .load("/FileStore/tables/Departments_csv/department.csv"))

# COMMAND ----------

display(department_df2)

# COMMAND ----------

department_df2.printSchema()

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/json/menu.json

# COMMAND ----------

menu_df=(spark
         .read
         .format("json")
         .option("multiline","true")
         .load("/FileStore/tables/json/menu.json"))

# COMMAND ----------

menu_df1=menu_df.select("menu.*")
display(menu_df1)

# COMMAND ----------

menu_df2=menu_df1.select("id","popup.*","value")
display(menu_df2)

# COMMAND ----------

from pyspark.sql.functions import explode
menu_df3=menu_df2.select("id",explode("menuitem").alias("menuitem"),"value")
display(menu_df3)

# COMMAND ----------

menu_df4=menu_df3.select("id","menuitem.*","value")
display(menu_df4)

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/json/students_nested_json_data.json

# COMMAND ----------

student_df=(spark
            .read
            .format("json")
            .option("multiline","true")
            .load("/FileStore/tables/json/students_nested_json_data.json"))
display(student_df)

# COMMAND ----------

from pyspark.sql.functions import explode 
student_df=student_df.select("name",explode("Education").alias("Education"))
display(student_df)

# COMMAND ----------

student_df=student_df.select("name","Education.*")
display(student_df)

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/wordCount/sample.txt

# COMMAND ----------

sample_df=(spark
           .read
           .format("text")
           .load("/FileStore/tables/wordCount/sample.txt"))
display(sample_df)

# COMMAND ----------

from pyspark.sql.functions import split, explode, desc
sample_df=sample_df.select(split("value"," ").alias("Word"))
sample_df=sample_df.select(explode("Word").alias("Word"))
count_df=sample_df.groupBy("Word").count().orderBy(desc("count"))
display(count_df)

# COMMAND ----------

