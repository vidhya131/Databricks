# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract the data from volumes created in the unity catalog - operational data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Query single JSON file

# COMMAND ----------

# MAGIC %fs ls /Volumes/gizmobox/landing/operational_data/customers/

# COMMAND ----------

# MAGIC %sql
# MAGIC select cpunt(*) from json.`/Volumes/gizmobox/landing/operational_data/customers/customers_2024_10.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Query multiple json files

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.`/Volumes/gizmobox/landing/operational_data/customers/customers_*.json`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Creating views

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gizmobox;
# MAGIC -- use schema landing;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view bronze.v_customers
# MAGIC as
# MAGIC select *, 
# MAGIC        _metadata.file_path as file_path 
# MAGIC        from json.`/Volumes/gizmobox/landing/operational_data/customers/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.v_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Temperary views - this wont be aialbale in the unity catlog and no longer exists once the spark session exits

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773343343271.png](./image_1773343343271.png "image_1773343343271.png")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view tv_customers
# MAGIC as
# MAGIC select *, 
# MAGIC        _metadata.file_path as file_path 
# MAGIC        from json.`/Volumes/gizmobox/landing/operational_data/customers/`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Global temporary views - stays alive until the spark application ends i.e cluster terminates

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temporary view gtv_customers
# MAGIC as
# MAGIC select *, 
# MAGIC        _metadata.file_path as file_path 
# MAGIC        from json.`/Volumes/gizmobox/landing/operational_data/customers/`;