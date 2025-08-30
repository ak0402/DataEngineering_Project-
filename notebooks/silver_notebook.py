# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Reading 

# COMMAND ----------

df = spark.read.format("parquet")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@p1datalake.dfs.core.windows.net/rawdata")

# COMMAND ----------

df.display(10);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = df.withColumn('model_category', split(col('Model_ID'), '-')[0])
df.display()

# COMMAND ----------

df = df.withColumn('RevPerUnit', col('Revenue')/col('Units_Sold'))
df.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # AD_HOC

# COMMAND ----------

df.display(10)

# COMMAND ----------

display(df.groupBy("Year","BranchName").agg(sum("Units_Sold")\
  .alias("Total_Units"))\
  .sort("Year","Total_Units", ascending=[True, False]))
  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Writting 

# COMMAND ----------

df.write.format("parquet")\
  .mode("overwrite")\
  .option('path','abfss://silver@p1datalake.dfs.core.windows.net/carsales')\
  .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from parquet.`abfss://silver@p1datalake.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from parquet.`abfss://silver@p1datalake.dfs.core.windows.net/carsales`

# COMMAND ----------

