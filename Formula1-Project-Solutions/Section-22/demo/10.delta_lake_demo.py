# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Below is a managed database but tables created inside might not always be managed tables but the tables created underthis database will be always stored in the location 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 Your Statement
# MAGIC
# MAGIC ```sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dl/demo';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 Is This Managed or External?
# MAGIC
# MAGIC 👉 This is **creating a database (schema)**, not a table.
# MAGIC
# MAGIC Because you **specified a LOCATION**, this database becomes an:
# MAGIC
# MAGIC > ✅ **External Database**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔎 Why?
# MAGIC
# MAGIC In Databricks (Hive Metastore context):
# MAGIC
# MAGIC * If you create a database **without LOCATION** → Managed database
# MAGIC * If you create a database **with LOCATION** → External database
# MAGIC
# MAGIC You explicitly told Databricks:
# MAGIC
# MAGIC ```sql
# MAGIC LOCATION '/mnt/formula1dl/demo'
# MAGIC ```
# MAGIC
# MAGIC So Databricks will store all tables created inside this database under that path.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📦 What Happens Next?
# MAGIC
# MAGIC If you create a table:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_demo.races (id INT, name STRING);
# MAGIC ```
# MAGIC
# MAGIC That table will be stored at:
# MAGIC
# MAGIC ```text
# MAGIC /mnt/formula1dl/demo/races
# MAGIC ```
# MAGIC
# MAGIC Even though you didn’t specify table LOCATION.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # ⚠️ Important Clarification
# MAGIC
# MAGIC This does **NOT automatically make every table external**.
# MAGIC
# MAGIC There are two levels:
# MAGIC
# MAGIC | Level    | Managed vs External Depends On    |
# MAGIC | -------- | --------------------------------- |
# MAGIC | Database | LOCATION clause                   |
# MAGIC | Table    | Whether table has LOCATION clause |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 What About the Table Inside?
# MAGIC
# MAGIC If you create:
# MAGIC
# MAGIC ### 1️⃣ Without LOCATION
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_demo.races (id INT);
# MAGIC ```
# MAGIC
# MAGIC 👉 Managed table
# MAGIC 👉 Stored under `/mnt/formula1dl/demo/races`
# MAGIC 👉 Drop table = data deleted
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ With LOCATION
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_demo.races
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/some_other_path/races';
# MAGIC ```
# MAGIC
# MAGIC 👉 External table
# MAGIC 👉 Drop table = data remains
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏁 Final Answer
# MAGIC
# MAGIC Your statement:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE DATABASE ... LOCATION ...
# MAGIC ```
# MAGIC
# MAGIC ✔ Creates an **external database**
# MAGIC ❗ But tables inside can still be managed or external depending on how they are created.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can next explain:
# MAGIC
# MAGIC * 🔥 Difference between external database vs external table
# MAGIC * 🧠 What happens in Unity Catalog instead of /mnt
# MAGIC * ⚡ Real interview trick question around this concept
# MAGIC

# COMMAND ----------

storage_account = "dlforformula1"

client_id = "16e41178-4311-45a7-b2ec-ff3999a7593b"
tenant_id = "0bf495b2-f9e4-4786-82ea-d1ff2c6f9353"
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

# COMMAND ----------

location = "abfss://demo@dlforformula1.dfs.core.windows.net/delta_lake_demo"

# COMMAND ----------

# MAGIC %md
# MAGIC Short answer: **Yes — in a Unity Catalog–enabled workspace, a default catalog is created and attached to that workspace.** But it depends on how the workspace is configured.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏛️ How it works in practice
# MAGIC
# MAGIC ## ✅ If the workspace is **Unity Catalog–enabled**
# MAGIC
# MAGIC When a workspace is attached to a UC metastore:
# MAGIC
# MAGIC * A **default catalog** is available (often named like your workspace, e.g., `dbx_course_ws`, or sometimes `main`)
# MAGIC * You can check with:
# MAGIC
# MAGIC ```sql
# MAGIC SHOW CATALOGS;
# MAGIC ```
# MAGIC
# MAGIC You’ll typically see something like:
# MAGIC
# MAGIC ```text
# MAGIC main
# MAGIC dbx_course_ws
# MAGIC hive_metastore
# MAGIC ```
# MAGIC
# MAGIC ### 🔹 What those mean
# MAGIC
# MAGIC | Catalog          | Purpose                                         |
# MAGIC | ---------------- | ----------------------------------------------- |
# MAGIC | `main`           | Default UC catalog (created with the metastore) |
# MAGIC | `dbx_course_ws`  | Workspace-specific default catalog              |
# MAGIC | `hive_metastore` | Legacy metastore for backward compatibility     |
# MAGIC
# MAGIC So yes — once UC is enabled, **a default catalog is available for that workspace**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ If the workspace is NOT UC-enabled
# MAGIC
# MAGIC Then there is **no catalog layer**.
# MAGIC
# MAGIC You only have:
# MAGIC
# MAGIC ```text
# MAGIC hive_metastore
# MAGIC ```
# MAGIC
# MAGIC Hierarchy becomes:
# MAGIC
# MAGIC ```text
# MAGIC database.table
# MAGIC ```
# MAGIC
# MAGIC No `catalog.schema.table`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 Important Concept
# MAGIC
# MAGIC In Unity Catalog:
# MAGIC
# MAGIC ```text
# MAGIC Metastore (Account Level)
# MAGIC         ↓
# MAGIC Catalog
# MAGIC         ↓
# MAGIC Schema
# MAGIC         ↓
# MAGIC Table
# MAGIC ```
# MAGIC
# MAGIC When a workspace is attached to a metastore:
# MAGIC
# MAGIC * It gets access to one or more catalogs
# MAGIC * Usually at least one default catalog is available
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔍 How to Confirm in Your Workspace
# MAGIC
# MAGIC Run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT current_catalog();
# MAGIC SHOW CATALOGS;
# MAGIC ```
# MAGIC
# MAGIC If you see:
# MAGIC
# MAGIC * Something other than `hive_metastore`
# MAGIC   → You are in UC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 Industry Reality
# MAGIC
# MAGIC In enterprise setups:
# MAGIC
# MAGIC * Admin creates metastore
# MAGIC * Admin attaches workspaces
# MAGIC * Admin creates standard catalogs (e.g., `main`, `dev`, `prod`, `finance`, etc.)
# MAGIC * Workspace users get assigned to specific catalogs
# MAGIC
# MAGIC So catalogs are often **centrally managed**, not automatically created by random users.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏁 Final Answer
# MAGIC
# MAGIC | Question                           | Answer                             |
# MAGIC | ---------------------------------- | ---------------------------------- |
# MAGIC | Is a catalog created by default?   | ✅ Yes, if UC is enabled            |
# MAGIC | Is it created per workspace?       | Usually yes (or at least assigned) |
# MAGIC | Does Hive Metastore have catalogs? | ❌ No                               |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want next, I can explain:
# MAGIC
# MAGIC * What exactly is a metastore in Unity Catalog
# MAGIC * How catalogs are shared across multiple workspaces
# MAGIC * Why enterprises create multiple catalogs (dev/prod separation)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC MANAGED LOCATION 'abfss://demo@dlforformula1.dfs.core.windows.net/delta_lake_demo/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STORAGE CREDENTIAL f1_sp_cred
# MAGIC WITH AZURE_SERVICE_PRINCIPAL (
# MAGIC   CLIENT_ID     = '<app-id>',
# MAGIC   TENANT_ID     = '<tenant-id>',
# MAGIC   CLIENT_SECRET = '<secret>'
# MAGIC );

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed"). # managed table

# COMMAND ----------

# MAGIC %md
# MAGIC Great question 👌 — this is a **very common confusion** in Databricks.
# MAGIC
# MAGIC Let’s break it down clearly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 1️⃣ Option 1
# MAGIC
# MAGIC ```python
# MAGIC results_df.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("f1_demo.results_managed")
# MAGIC ```
# MAGIC
# MAGIC ## ✅ What This Does
# MAGIC
# MAGIC * Creates (or overwrites) a **table in the metastore**
# MAGIC * Registers it inside:
# MAGIC
# MAGIC   ```
# MAGIC   f1_demo.results_managed
# MAGIC   ```
# MAGIC * Metadata stored in:
# MAGIC
# MAGIC   * Hive Metastore OR
# MAGIC   * Unity Catalog (depending on your setup)
# MAGIC
# MAGIC ### ✔ Result:
# MAGIC
# MAGIC You can query it using:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM f1_demo.results_managed;
# MAGIC ```
# MAGIC
# MAGIC 👉 This is typically a **managed table** (unless special config exists).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Where Is Data Stored?
# MAGIC
# MAGIC If your database was created with:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE DATABASE f1_demo LOCATION '/mnt/formula1dl/demo';
# MAGIC ```
# MAGIC
# MAGIC Then data will be stored under:
# MAGIC
# MAGIC ```
# MAGIC /mnt/formula1dl/demo/results_managed
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 2️⃣ Option 2
# MAGIC
# MAGIC ```python
# MAGIC results_df.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save("f1_demo.results_managed")
# MAGIC ```
# MAGIC
# MAGIC ## ❌ This Does NOT Create a Table
# MAGIC
# MAGIC `.save()` expects a **file path**, not a table name.
# MAGIC
# MAGIC So this line treats:
# MAGIC
# MAGIC ```
# MAGIC "f1_demo.results_managed"
# MAGIC ```
# MAGIC
# MAGIC as a folder name.
# MAGIC
# MAGIC It will try to create a directory literally named:
# MAGIC
# MAGIC ```
# MAGIC f1_demo.results_managed
# MAGIC ```
# MAGIC
# MAGIC in your current working storage location.
# MAGIC
# MAGIC No table is registered.
# MAGIC
# MAGIC You cannot query it via SQL unless you manually create a table on top of it.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # ⚖️ Key Difference
# MAGIC
# MAGIC | Feature                      | saveAsTable() | save()         |
# MAGIC | ---------------------------- | ------------- | -------------- |
# MAGIC | Registers table in metastore | ✅ Yes         | ❌ No           |
# MAGIC | Accepts table name           | ✅ Yes         | ❌ No           |
# MAGIC | Accepts file path            | ❌ No          | ✅ Yes          |
# MAGIC | Query using SQL              | ✅ Yes         | ❌ Not directly |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 Correct Usage Examples
# MAGIC
# MAGIC ### ✔ Create Managed Table
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").saveAsTable("f1_demo.results")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✔ Write to File Path (External Data)
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").save("/mnt/formula1dl/demo/results")
# MAGIC ```
# MAGIC
# MAGIC Then manually register:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_demo.results
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl/demo/results';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 Industry Pattern
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").save("/mnt/bronze/raw_data")
# MAGIC ```
# MAGIC
# MAGIC Then register external table.
# MAGIC
# MAGIC ### Silver / Gold Layer
# MAGIC
# MAGIC ```python
# MAGIC df.write.format("delta").saveAsTable("silver.cleaned_data")
# MAGIC ```
# MAGIC
# MAGIC Managed tables preferred.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏁 Final Answer
# MAGIC
# MAGIC ### This:
# MAGIC
# MAGIC ```python
# MAGIC saveAsTable("f1_demo.results_managed")
# MAGIC ```
# MAGIC
# MAGIC ✔ Creates a metastore table
# MAGIC ✔ Usually managed
# MAGIC
# MAGIC ### This:
# MAGIC
# MAGIC ```python
# MAGIC save("f1_demo.results_managed")
# MAGIC ```
# MAGIC
# MAGIC ❌ Writes to a folder
# MAGIC ❌ Does NOT create a table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can next explain:
# MAGIC
# MAGIC * 🔥 What happens internally in metastore when saveAsTable runs
# MAGIC * 🧠 How to check if a table is managed or external
# MAGIC * ⚡ What happens during overwrite mode in Delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dl/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl/demo/results_managed")

deltaTable.update("position <= 10", { "points": "21 - position" } ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl/demo/results_managed")

deltaTable.delete("points = 0") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md Day1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2021-06-23T15:40:33.000+0000';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2021-06-23T15:40:33.000+0000').load("/mnt/formula1dl/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2021-06-23T15:40:33.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2021-06-23T15:40:33.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM  f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dl/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dl/demo/drivers_convert_to_delta_new`

# COMMAND ----------


