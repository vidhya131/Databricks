# Databricks notebook source
metastore_name = spark.sql("SELECT current_metastore()").collect()[0][0]
display(spark.createDataFrame([(metastore_name,)], ["current_metastore"]))

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE METASTORE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_metastore();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create storage credentials. --- not working
# MAGIC CREATE STORAGE CREDENTIAL my_azure_cred
# MAGIC WITH AZURE_MANAGED_IDENTITY (
# MAGIC   ACCESS_CONNECTOR_ID = '/subscriptions/bae635cb-7bf7-4be0-830e-4617399b5403/resourceGroups/dbx-ast-prep/providers/Microsoft.Databricks/accessConnectors/dbx-ast-prep-connector'
# MAGIC )
# MAGIC COMMENT 'Storage credential using Databricks access connector';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION gizmobox_ext_loc
# MAGIC URL 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `dbx-ast-creds`)
# MAGIC COMMENT 'external location gizmobiox container';

# COMMAND ----------

# MAGIC %sql
# MAGIC show external locations;
# MAGIC

# COMMAND ----------

dbutils.fs.mv(
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/operational_data",
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing",
  True
)

# COMMAND ----------

dbutils.fs.mv(
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/external_data",
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing",
  True
)

# COMMAND ----------

dbutils.fs.mv(
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/external_data",
  "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing",
  True
)

# COMMAND ----------

dbutils.fs.mv(
    "abfss://gizmobox@dbxastdl.dfs.core.windows.net/external_data/payments/",
    "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing/external_data/payments/",
    True
)

# COMMAND ----------

dbutils.fs.mv(
    "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing/orders/",
    "abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing/operational_data/orders/",
    True
)