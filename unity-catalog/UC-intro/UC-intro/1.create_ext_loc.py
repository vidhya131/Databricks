# Databricks notebook source
# MAGIC %md
# MAGIC ## Create the external locations required for this project
# MAGIC 1. Bronze
# MAGIC 2. Silver
# MAGIC 3. Gold
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION dbxucextdl_bronze
# MAGIC URL 'abfss://bronze@dbxucextdl.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbx-ext-strg-creds`);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION dbxucextdl_silver
# MAGIC URL 'abfss://silver@dbxucextdl.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbx-ext-strg-creds`);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION dbxucextdl_gold
# MAGIC URL 'abfss://gold@dbxucextdl.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbx-ext-strg-creds`);