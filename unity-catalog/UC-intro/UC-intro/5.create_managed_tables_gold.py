# Databricks notebook source
# MAGIC %md
# MAGIC ### Create gold tables in golder layer
# MAGIC -- Join drivers and results to identify the no of wins per driver
# MAGIC
# MAGIC #### note: gold tables are managed tables - they are managed by dbx
# MAGIC
# MAGIC

# COMMAND ----------

## get the schema of two tables
# driver
df = spark.table('f1_dev.silver.drivers')
df.printSchema()
# print(type(df))
# results
df = spark.table('f1_dev.silver.results')
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_dev.gold.drivers_win;
# MAGIC create table f1_dev.gold.drivers_win
# MAGIC as
# MAGIC select d.forename, count(1) as no_of_wins 
# MAGIC from f1_dev.silver.drivers d join f1_dev.silver.results r on (d.driver_id = r.driver_id)
# MAGIC where r.position_number = 1
# MAGIC group by d.forename;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_dev.gold.drivers_win order by no_of_wins desc