# Databricks notebook source
# MAGIC %md
# MAGIC ## Access ADLS storage account files using access keys - full access
# MAGIC  1. set the spark config
# MAGIC  2. list files from demo container
# MAGIC  3. read the circuit.csv from demo container(bucket)
# MAGIC

# COMMAND ----------

secret_value = dbutils.secrets.get(scope="formula1-scope", key="formula1dl-access-key")

# COMMAND ----------

# Set the Spark config with the ADLS access key
spark.conf.set(
    "fs.azure.account.key.dlforformula1.dfs.core.windows.net",
    secret_value
)

# COMMAND ----------

# abfss is the protocol used to access Azure Data Lake Storage Gen2 (ADLS Gen2) using secure (SSL) endpoints.
# Example usage to list files in a container:
dbutils.fs.ls("abfss://demo@dlforformula1.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlforformula1.dfs.core.windows.net/"))

# COMMAND ----------

spark.read.csv("abfss://demo@dlforformula1.dfs.core.windows.net/").show()


# COMMAND ----------

display(spark.read.csv("abfss://demo@dlforformula1.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Drawbacks of using access keys:**
# MAGIC - Access keys provide full access to the storage account, increasing security risk if leaked.
# MAGIC - Keys must be rotated manually, which can be operationally complex.
# MAGIC - No granular access control; all users with the key have the same permissions.
# MAGIC - Difficult to audit or track usage by individual users or applications.
# MAGIC - If a key is compromised, all resources in the storage account are at risk.