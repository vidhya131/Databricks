# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS F1_DEV

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG f1_dev

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze
# MAGIC managed location "abfss://bronze@dbxucextdl.dfs.core.windows.net/"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver
# MAGIC managed location "abfss://silver@dbxucextdl.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold
# MAGIC managed location "abfss://gold@dbxucextdl.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

display(spark.sql("show schemas"))