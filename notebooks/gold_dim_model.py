# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Creating Dimension Model

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Fetching Relative Columns

# COMMAND ----------


df_src = spark.sql('''
                    Select distinct(Model_ID) as Model_ID , Model_category 
                    from parquet.`abfss://silver@p1datalake.dfs.core.windows.net/carsales`
                    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Dim_model Sink - Intial and Incremental

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_model'):

    df_sink = spark.sql('''
                    Select 1 as dim_model_key , Model_ID , Model_category
                    from cars_catalog.gold.dim_model
                    ''')

else :

    df_sink = spark.sql('''
                    Select 1 as dim_model_key , Model_ID , Model_category
                    from parquet.`abfss://silver@p1datalake.dfs.core.windows.net/carsales`
                    where 1=0
                    ''')
    
df_sink.display()    


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###Filtering new and old records

# COMMAND ----------

df_filter = df_src.join(df_sink,df_src['Model_ID'] == df_sink['Model_ID'],'left')\
    .select(df_src['Model_ID'],df_src['Model_category'],df_sink['dim_model_key'])
        

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC **Df_FIlter_Old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())
#df_filter_old.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC **Df_FIlter_New**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_ID'],df_src['Model_category'])

df_filter_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Create Surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ***Fetch the max Surrogate key from Existing table***

# COMMAND ----------

tables_df = spark.sql("show tables in cars_catalog.gold")
tables_df.display()

# COMMAND ----------


# if ('incremental_flag' == 0):
#     max_value = 0

# else :
#     max_value_df = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
#     max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# Check if the table exists before querying
# tables_df = spark.sql(
#     "SHOW TABLES IN cars_catalog.gold"
# )
# table_exists = tables_df.filter(
#     tables_df.tableName == "dim_model"
# ).count() > 0

# if 'incremental_flag' == 0:
#     max_value = 0
# elif table_exists:
#     max_value_df = spark.sql(
#         "SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model"
#     )
#     max_value = max_value_df.collect()[0][0]
# else:
#     raise Exception("Table cars_catalog.gold.dim_model does not exist.")

# COMMAND ----------

if (incremental_flag == '0'): 
    max_value = 1
else:
    if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
        max_value_df = spark.sql("SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model")
        max_value = max_value_df.collect()[0][0]



# COMMAND ----------

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    max_value_df = spark.sql("SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create Surrogate key column and ADD the max surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display() 

# COMMAND ----------

# MAGIC %md 
# MAGIC  **Create final DF - df_filter_olf + df_filter_new**

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)
df_final.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Slowly Changing Dimension (SCD) TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


# # Remove duplicates from df_final based on dim_model_key
# df_final_deduped = df_final.dropDuplicates(["dim_model_key"])


# # Incremental RUN 
# if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
#     delta_table = DeltaTable.forPath(spark, "abfss://gold@p1datalake.dfs.core.windows.net/dim_model")
#     # update when the value exists
#     # insert when new value 
#     delta_table.alias("target").merge(df_final.alias("source"), "target.dim_model_key = source.dim_model_key")\
#         .whenMatchedUpdateAll()\
#         .whenNotMatchedInsertAll()\
#         .execute()

# # Initial RUN 
# else: # no table exists
#     df_final.write.format("delta")\
#         .mode("overwrite")\
#         .option("path", "abfss://gold@p1datalake.dfs.core.windows.net/dim_model")\
#         .saveAsTable("cars_catalog.gold.dim_model")


# COMMAND ----------

# Remove duplicates from df_final based on dim_model_key
df_final_deduped = df_final.dropDuplicates(["dim_model_key"])

# Incremental RUN 
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark,"abfss://gold@p1datalake.dfs.core.windows.net/dim_model"
    )
    delta_table.alias("target").merge(df_final_deduped.alias("source"),"target.dim_model_key = source.dim_model_key"
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()
# Initial RUN 
else:
    df_final_deduped.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@p1datalake.dfs.core.windows.net/dim_model")\
        .saveAsTable("cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from cars_catalog.gold.dim_model

# COMMAND ----------

