# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list("formula1-scope"))


# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="formula1dl-access-key")

# COMMAND ----------

display("ji")