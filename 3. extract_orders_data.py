# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading the complex json as text files
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %md
# MAGIC ### above data is not parsed well when we read as a json file - so we go for text file

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gizmobox;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`/Volumes/gizmobox/landing/operational_data/orders/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bronze.v_orders
# MAGIC as
# MAGIC select * from text.`/Volumes/gizmobox/landing/operational_data/orders/`
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.v_orders;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/gizmobox/landing/operational_data/memberships/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/`

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bronze.v_memberships
# MAGIC as
# MAGIC select * from binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.v_memberships;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract data from addresses

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files('/Volumes/gizmobox/landing/operational_data/addresses/', format => 'csv', delimiter => '\t', header => true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bronze.v_addresses
# MAGIC as
# MAGIC select * from read_files('/Volumes/gizmobox/landing/operational_data/addresses/', format => 'csv', delimiter => '\t', header => true);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.v_addresses;