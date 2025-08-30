# Databricks notebook source
# MAGIC %md 
# MAGIC # **Create Catalog**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog Cars_catalog;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## **Create Schema**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create schema Cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create schema Cars_catalog.gold;

# COMMAND ----------

