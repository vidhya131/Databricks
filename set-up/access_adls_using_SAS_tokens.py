# Databricks notebook source
# MAGIC %md
# MAGIC ## Access ADLS storage account files using SAS Tokens 
# MAGIC  1. set the spark config
# MAGIC  2. list files from demo container
# MAGIC  3. read the circuit.csv from demo container(bucket)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Benefits of using SAS tokens:** 
# MAGIC - Provides granular, time-limited access to specific resources.
# MAGIC - Reduces security risk by not exposing account keys.
# MAGIC - Supports fine-grained permissions (read, write, list, etc.).
# MAGIC - Easy to revoke or rotate without impacting other access.
# MAGIC - Enables secure sharing with external users or applications.

# COMMAND ----------

# Set the Spark config with the ADLS SAS token
spark.conf.set(
    "fs.azure.account.auth.type.dlforformula1.dfs.core.windows.net",
    "SAS"
)
spark.conf.set(
    "fs.azure.sas.token.provider.type.dlforformula1.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)
spark.conf.set(
    "fs.azure.sas.fixed.token.dlforformula1.dfs.core.windows.net",
    "sp=rl&st=2026-02-17T17:32:19Z&se=2026-02-18T01:47:19Z&spr=https&sv=2024-11-04&sr=c&sig=r0FjZyA7dBpxTd70HjD7WAnIPzUxQlNzX%2BJizNJBK8g%3D"
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
# MAGIC **Drawbacks of using SAS tokens:**
# MAGIC - SAS tokens can be leaked if not handled securely, leading to unauthorized access.
# MAGIC - Tokens are often embedded in code or shared, increasing exposure risk.
# MAGIC - Revoking a SAS token before expiry can be operationally challenging.
# MAGIC - Limited auditing and tracking of individual usage.
# MAGIC - Managing and rotating multiple tokens can become complex.
# MAGIC
# MAGIC **A better way:**  
# MAGIC Use managed identities or service/storage credentials with fine-grained access control (such as Azure AD or Unity Catalog storage credentials) for improved security, easier management, and better auditing.