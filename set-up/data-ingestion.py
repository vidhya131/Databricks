# Databricks notebook source
client_secret = "<client_secret>"
client_id = "<client_id>"
tenant_id = "<tenant_id>"

cli_token = "<cli_token>"

# COMMAND ----------

storage_account = "dlforformula1"
container = "bronze"

account_fqdn = f"{storage_account}.dfs.core.windows.net"

# (Optional but helpful) remove any key-based config that may be set on cluster
for k in [
    f"fs.azure.account.key.{account_fqdn}",
    f"fs.azure.account.keyprovider.{account_fqdn}",
    "fs.azure.account.key"
]:
    try:
        spark.conf.unset(k)
    except:
        pass

# Set OAuth configs (IMPORTANT: include the account FQDN in the key)
spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_fqdn}",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}",
               dbutils.secrets.get(scope="adls-scope", key="client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Now try listing
path = f"abfss://{container}@{account_fqdn}/"
display(dbutils.fs.ls(path))

# COMMAND ----------

df = spark.range(0, 1000000)
df.groupBy().count().show()

# COMMAND ----------

df = spark.range(0, 1000000)
df.groupBy("id").count().show()

# COMMAND ----------

df = spark.range(0, 1000000)
df = df.withColumn("key", df.id % 10)
from pyspark.sql.functions import monotonically_increasing_id
df  = df.withColumn("col1", monotonically_increasing_id())

# df = df.withColumn("col2", monotonically_increasing_id()
df.groupBy("col1", "key").count().show()