# Databricks notebook source
# MAGIC %sql select * from demo_catalog.demo_schema.circuits

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo_catalog;
# MAGIC select * from circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog()

# COMMAND ----------

display(spark.sql("show tables"))

# COMMAND ----------

df = spark.table("demo_catalog.demo_schema.circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing exernal locations

# COMMAND ----------

dbutils.fs.ls("abfss://demo@dbxucextdl.dfs.core.windows.net/")