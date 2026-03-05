# Databricks notebook source
# MAGIC %md
# MAGIC ### Create managed tables in SILVER LAYER from bronze layer
# MAGIC 1. drivers
# MAGIC 2. results

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_dev.silver.drivers
# MAGIC as 
# MAGIC select 
# MAGIC   driverId as driver_id,
# MAGIC   driverRef as driver_ref,
# MAGIC   name.forename as forename,
# MAGIC   name.surname as surname,
# MAGIC   dob as date_of_birth,
# MAGIC   nationality as nationality,
# MAGIC   url as url
# MAGIC from f1_dev.bronze.drivers;
# MAGIC
# MAGIC select * from f1_dev.silver.drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_dev.silver.results;
# MAGIC create table if not exists f1_dev.silver.results
# MAGIC as 
# MAGIC select
# MAGIC   driverId as driver_id,
# MAGIC   constructorId as constructor_id,
# MAGIC   position as position_number,
# MAGIC   positionText as position_text,
# MAGIC   positionOrder as position_order,
# MAGIC   fastestLap as fastest_lap,
# MAGIC   rank as fastest_lap_rank,
# MAGIC   fastestLapTime as fastest_lap_time,
# MAGIC   fastestLapSpeed as fastest_lap_speed
# MAGIC from f1_dev.bronze.results;
# MAGIC
# MAGIC select * from f1_dev.silver.results;