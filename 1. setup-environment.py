# Databricks notebook source
# MAGIC %md
# MAGIC 1.  Create Schemas
# MAGIC    - landing(raw)
# MAGIC    - Bronze
# MAGIC    - Silver
# MAGIC    - Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC Select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop catalog gizmobox cascade;
# MAGIC
# MAGIC create catalog gizmobox
# MAGIC  managed location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gizmobox

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists landing
# MAGIC managed location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing';
# MAGIC create schema if not exists bronze
# MAGIC managed location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/bronze';
# MAGIC create schema if not exists silver
# MAGIC managed location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/silver';        
# MAGIC create schema if not exists gold
# MAGIC managed location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/gold';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create volume for operational data under landing 
# MAGIC ##### What is volume in databricks?
# MAGIC  - In Databricks, Volumes are a feature of Unity Catalog that let you store and manage files (non-table data) in a governed location inside the lakehouse.
# MAGIC  - Think of Volumes as folders inside the Databricks catalog where you can store files like:
# MAGIC
# MAGIC     CSV
# MAGIC
# MAGIC     JSON
# MAGIC
# MAGIC     Images
# MAGIC
# MAGIC     ML models
# MAGIC
# MAGIC     Python scripts
# MAGIC
# MAGIC     Unstructured files
# MAGIC
# MAGIC     Unlike tables, volumes store raw files, not structured table data.
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC In **Databricks**, **Volumes** are a feature of **Unity Catalog** that let you **store and manage files (non-table data)** in a governed location inside the lakehouse.
# MAGIC
# MAGIC Think of **Volumes as folders inside the Databricks catalog** where you can store files like:
# MAGIC
# MAGIC * CSV
# MAGIC * JSON
# MAGIC * Images
# MAGIC * ML models
# MAGIC * Python scripts
# MAGIC * Unstructured files
# MAGIC
# MAGIC Unlike tables, **volumes store raw files**, not structured table data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1. Why Volumes Exist
# MAGIC
# MAGIC Before volumes, people used **Databricks File System (DBFS)** to store files.
# MAGIC
# MAGIC Problems with DBFS:
# MAGIC
# MAGIC * No proper governance
# MAGIC * Hard to control permissions
# MAGIC * Not fully integrated with Unity Catalog security
# MAGIC
# MAGIC **Volumes solve this** by bringing **file storage under Unity Catalog governance**.
# MAGIC
# MAGIC So you get:
# MAGIC
# MAGIC * **Access control**
# MAGIC * **Auditing**
# MAGIC * **Centralized data governance**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2. Simple Mental Model
# MAGIC
# MAGIC | Thing  | Used For                       |
# MAGIC | ------ | ------------------------------ |
# MAGIC | Table  | Structured data (rows/columns) |
# MAGIC | Volume | Files / unstructured data      |
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```
# MAGIC Catalog
# MAGIC    └── Schema
# MAGIC          ├── Tables
# MAGIC          │      └── sales_table
# MAGIC          │
# MAGIC          └── Volumes
# MAGIC                 └── raw_files
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3. Example: Creating a Volume
# MAGIC
# MAGIC ```sql
# MAGIC CREATE VOLUME my_volume;
# MAGIC ```
# MAGIC
# MAGIC Now it creates a location like:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/catalog/schema/my_volume/
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 4. Example: Uploading Files
# MAGIC
# MAGIC You can upload a file like:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/main/default/my_volume/customers.csv
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 5. Reading File from Volume (PySpark)
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.csv("/Volumes/main/default/my_volume/customers.csv")
# MAGIC
# MAGIC display(df)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 6. Writing Files to Volume
# MAGIC
# MAGIC ```python
# MAGIC df.write.mode("overwrite").csv("/Volumes/main/default/my_volume/output/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 7. Real Industry Use Cases (Very Important for Data Engineers)
# MAGIC
# MAGIC ### 1️⃣ ML Models Storage
# MAGIC
# MAGIC Data scientists store trained models.
# MAGIC
# MAGIC ```
# MAGIC /Volumes/ml/models/model_v1.pkl
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Raw Data Landing Zone
# MAGIC
# MAGIC Example pipeline:
# MAGIC
# MAGIC ```
# MAGIC Source → Volume → Bronze Table
# MAGIC ```
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/raw/iot/device_logs.json
# MAGIC ```
# MAGIC
# MAGIC Then ingestion job reads:
# MAGIC
# MAGIC ```python
# MAGIC spark.read.json("/Volumes/raw/iot/device_logs.json")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ File Sharing Between Jobs
# MAGIC
# MAGIC Job A writes file
# MAGIC
# MAGIC ```
# MAGIC /Volumes/data/temp/output.parquet
# MAGIC ```
# MAGIC
# MAGIC Job B reads it.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 8. Volume vs Table
# MAGIC
# MAGIC | Feature         | Table     | Volume       |
# MAGIC | --------------- | --------- | ------------ |
# MAGIC | Structured data | ✅         | ❌            |
# MAGIC | SQL queries     | ✅         | ❌            |
# MAGIC | Raw files       | ❌         | ✅            |
# MAGIC | Governance      | ✅         | ✅            |
# MAGIC | Use case        | Analytics | File storage |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 9. Volume vs DBFS
# MAGIC
# MAGIC | Feature         | DBFS | Volume |
# MAGIC | --------------- | ---- | ------ |
# MAGIC | Governance      | ❌    | ✅      |
# MAGIC | Unity Catalog   | ❌    | ✅      |
# MAGIC | Recommended now | ❌    | ✅      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 10. Simple Real Pipeline Example
# MAGIC
# MAGIC Suppose an **IoT pipeline**:
# MAGIC
# MAGIC Step 1: Device sends JSON files
# MAGIC
# MAGIC ```
# MAGIC /Volumes/iot/raw/device_data.json
# MAGIC ```
# MAGIC
# MAGIC Step 2: Spark reads them
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.json("/Volumes/iot/raw/device_data.json")
# MAGIC ```
# MAGIC
# MAGIC Step 3: Save to Bronze table
# MAGIC
# MAGIC ```python
# MAGIC df.write.saveAsTable("iot.bronze_device_data")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ✅ **Key idea**
# MAGIC
# MAGIC **Volume = governed file storage inside Unity Catalog**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 💡 Since you're learning **Databricks data engineering**, the **most important concept interviewers ask next is:**
# MAGIC
# MAGIC **Managed Volume vs External Volume**
# MAGIC
# MAGIC They ask this almost as often as **managed vs external tables**.
# MAGIC
# MAGIC If you want, I can explain that with **architecture diagrams and real company examples.**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gizmobox;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop volume if exists landing.operational_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop volume if exists landing.operational_data;
# MAGIC -- create volume landing.operational_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC create external volume landing.operational_data
# MAGIC  location 'abfss://gizmobox@dbxastdl.dfs.core.windows.net/landing/operational_data';

# COMMAND ----------

# MAGIC %md
# MAGIC ### We now have this structure

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773340835977.png](./image_1773340835977.png "image_1773340835977.png")

# COMMAND ----------



# COMMAND ----------

