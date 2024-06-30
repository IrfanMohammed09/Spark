# Databricks notebook source
sc

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/Orders/part_00000

# COMMAND ----------

Orders_rdd=sc.textFile("/FileStore/tables/Orders/part_00000")

# COMMAND ----------

Orders_rdd.take(2)

# COMMAND ----------

type(Orders_rdd)

# COMMAND ----------

len_order_rdd=Orders_rdd.map(lambda x: (x,len(x)))

# COMMAND ----------

len_order_rdd.take(10)

# COMMAND ----------

Orders_rdd.count(), len_order_rdd.count()

# COMMAND ----------

type(len_order_rdd)

# COMMAND ----------

filter_rdd_len=Orders_rdd.filter(lambda x: len(x)>40)

# COMMAND ----------

type(filter_rdd_len)

# COMMAND ----------

filter_rdd_len.take(5)

# COMMAND ----------

filter_rdd_status=Orders_rdd.filter(lambda x: x.split(",")[3]=="CLOSED")

# COMMAND ----------

filter_rdd_status.take(10)

# COMMAND ----------

filter_rdd_status.count()

# COMMAND ----------

int_rdd=sc.parallelize([1,2,3,4,5,5,5,6,7,8,8,3,11])
int_rdd.collect()

# COMMAND ----------

unique_int_rdd=int_rdd.distinct()

# COMMAND ----------

unique_int_rdd.collect()

# COMMAND ----------

sum_int_rdd=int_rdd.reduce(lambda x,y: x+y)

# COMMAND ----------

max_int_rdd=int_rdd.reduce(lambda x,y: max(x,y))

# COMMAND ----------

max_int_rdd, sum_int_rdd

# COMMAND ----------

s="My name is irfan , irfan is a grat boy. irfan is smart and irfan is handsome"

# COMMAND ----------

c1=s.split(' ')
c1_rdd=sc.parallelize(c1)

# COMMAND ----------

c_irfan_rdd=c1_rdd.filter(lambda x:x=="irfan")

# COMMAND ----------

c_irfan_rdd.count()

# COMMAND ----------

c_irfan_rdd.collect()

# COMMAND ----------

count_closed=Orders_rdd.map(lambda x:tuple(x.split(','))).map(lambda x:x.count("CLOSED")).reduce(lambda x,y:x+y)
count_closed


# COMMAND ----------

word_rdd=Orders_rdd.flatMap(lambda x:x.split(","))
word_count_rdd=word_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)

# COMMAND ----------

word_count_rdd.take(20)

# COMMAND ----------

word_count_rdd.saveAsTextFile("/FileStore/tables/Orders/word_count")

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/Orders/word_count/

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/Orders/word_count/part-00000

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/Orders/word_count/part-00001

# COMMAND ----------

word_rdd.getNumPartitions()

# COMMAND ----------

