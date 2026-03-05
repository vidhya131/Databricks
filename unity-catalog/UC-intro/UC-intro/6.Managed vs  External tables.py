# Databricks notebook source
# MAGIC %md
# MAGIC ## Below is demonstration of creating managed table
# MAGIC  abfss://metastore@dbricksucdl1.dfs.core.windows.net/df97f0a4-c857-44be-adbc-002764917eec/tables/f378bf7d-8887-4bbc-ab61-4102beff7b0c
# MAGIC
# MAGIC  above location is unity catalogs default location for manged tables - tables gets stored in default location - below table is tsored in default location

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog demo_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create Schema managed_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists demo_catalog.managed_schema.managed_table;
# MAGIC CREATE TABLE demo_catalog.managed_schema.managed_table
# MAGIC AS
# MAGIC SELECT * FROM f1_dev.silver.results

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists demo_catalog.managed_schema.managed_table;
# MAGIC CREATE TABLE demo_catalog.managed_schema.managed_table
# MAGIC AS
# MAGIC SELECT * FROM f1_dev.silver.results

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo_catalog.managed_schema;
# MAGIC desc table demo_catalog.managed_schema.managed_table;
# MAGIC     
# MAGIC -- drop table if exists demo_catalog.managed_schema.managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema external_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists demo_catalog.external_schema.external_table;
# MAGIC CREATE TABLE demo_catalog.external_schema.external_table
# MAGIC LOCATION "abfss://demo@dbxucextdl.dfs.core.windows.net/external_tables/"
# MAGIC AS
# MAGIC SELECT * FROM f1_dev.silver.results;
# MAGIC     
# MAGIC show tables in demo_catalog.external_schema;
# MAGIC desc table demo_catalog.external_schema.external_table;
# MAGIC