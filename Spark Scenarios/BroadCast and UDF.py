# Databricks notebook source
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
# MAGIC User Defined Functions

# COMMAND ----------

lookup ={
    "NY":"New York",
    "CA":"California",
    "IL":"Illinois",
    "TX":"Texas",
    "AZ":"Arizona",
    "CO":"Colorado",
    "OH":"Ohio"
}
@udf
def map_state_code(state_code):
    if(state_code in lookup.keys()):
        return lookup[state_code]
    else:
        return state_code

# COMMAND ----------

df=df.withColumn("State_Udf", map_state_code("State_Code"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Broadcast Variable

# COMMAND ----------

broadcastVariable=sc.broadcast(lookup)

# COMMAND ----------

type(broadcastVariable)

# COMMAND ----------

@udf
def map_state_code_bv(State_code):
    return broadcastVariable.value[State_code]

# COMMAND ----------

df=df.withColumn("State_Bvar", map_state_code_bv("State_Code"))
display(df)

# COMMAND ----------

broadcastVariable.value["NY"]

# COMMAND ----------

# By default, broad cast variables are cached on the worker machine
# You can also unpersist them by calling .unpersist( )
broadcastVariable.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC If I call destroy() then all worker machines will destroy the copy their broad cast variable & not to use it for any task execution

# COMMAND ----------

broadcastVariable.destroy()

# COMMAND ----------

broadcastVariable.value["NY"]