# Databricks notebook source
# MAGIC %md
# MAGIC ## Create External tables in BRONZE LAYER
# MAGIC 1. driver.json
# MAGIC 2. results.json - "abfss://bronze@dbxucextdl.dfs.core.windows.net/"

# COMMAND ----------

df = spark.read.json("abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers.json")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_dev.bronze.drivers
# MAGIC (
# MAGIC   driverId long,
# MAGIC   driverRef string,
# MAGIC   number long,
# MAGIC   code string,
# MAGIC   name struct<forename:string, surname:string>,
# MAGIC   dob date,
# MAGIC   nationality string,
# MAGIC   url string
# MAGIC )
# MAGIC using json
# MAGIC OPTIONS(path "abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers.json")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Your statement is creating a **table over an existing file**, not storing data itself. Let’s walk through exactly what happens.
# MAGIC
# MAGIC ```sql
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_dev.bronze.drivers
# MAGIC (
# MAGIC   driverId LONG,
# MAGIC   driverRef STRING,
# MAGIC   number LONG,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename:STRING, surname:STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (
# MAGIC   path "abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers.json"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1️⃣ What happens when this runs
# MAGIC
# MAGIC Databricks will:
# MAGIC
# MAGIC 1. **Create a table definition in Unity Catalog**
# MAGIC 2. **Point the table to the JSON file**
# MAGIC 3. **Use the schema you defined**
# MAGIC 4. **Read data directly from that file**
# MAGIC
# MAGIC Important:
# MAGIC It **does NOT move or copy the data**.
# MAGIC
# MAGIC The JSON file stays exactly where it is.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2️⃣ Is this Managed or External?
# MAGIC
# MAGIC This becomes an **EXTERNAL TABLE**.
# MAGIC
# MAGIC Reason:
# MAGIC
# MAGIC You explicitly provided a **data path**.
# MAGIC
# MAGIC Whenever a table references an existing storage path (via `path` or `LOCATION`), it is treated as **external**.
# MAGIC
# MAGIC | Case                       | Table Type |
# MAGIC | -------------------------- | ---------- |
# MAGIC | No location specified      | Managed    |
# MAGIC | LOCATION or path specified | External   |
# MAGIC
# MAGIC So this table will show:
# MAGIC
# MAGIC ```
# MAGIC Type: EXTERNAL
# MAGIC ```
# MAGIC
# MAGIC in Catalog Explorer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3️⃣ Where is the data physically stored?
# MAGIC
# MAGIC Your data remains in the same ADLS location:
# MAGIC
# MAGIC ```
# MAGIC abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers.json
# MAGIC ```
# MAGIC
# MAGIC Databricks only stores **metadata** in Unity Catalog.
# MAGIC
# MAGIC ### Physical storage
# MAGIC
# MAGIC ```
# MAGIC ADLS
# MAGIC  └── bronze container
# MAGIC       └── drivers.json
# MAGIC ```
# MAGIC
# MAGIC ### Metadata storage
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog
# MAGIC  └── f1_dev
# MAGIC       └── bronze
# MAGIC            └── drivers (table definition)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 4️⃣ What the table actually becomes
# MAGIC
# MAGIC Conceptually this is like creating a **SQL view over files**.
# MAGIC
# MAGIC When you run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM f1_dev.bronze.drivers
# MAGIC ```
# MAGIC
# MAGIC Databricks will:
# MAGIC
# MAGIC 1. Read the JSON file
# MAGIC 2. Apply the schema
# MAGIC 3. Return the rows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 5️⃣ What happens if you drop the table?
# MAGIC
# MAGIC If you run:
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE f1_dev.bronze.drivers;
# MAGIC ```
# MAGIC
# MAGIC Only the **table metadata is deleted**.
# MAGIC
# MAGIC The file remains:
# MAGIC
# MAGIC ```
# MAGIC drivers.json still exists in ADLS
# MAGIC ```
# MAGIC
# MAGIC This is a key property of **external tables**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 6️⃣ Visual Architecture
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog
# MAGIC    │
# MAGIC    │ metadata only
# MAGIC    ▼
# MAGIC Table: f1_dev.bronze.drivers
# MAGIC    │
# MAGIC    │ reads data from
# MAGIC    ▼
# MAGIC ADLS Storage
# MAGIC    bronze/drivers.json
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 7️⃣ Small industry tip
# MAGIC
# MAGIC In modern **Databricks lakehouse pipelines**, bronze tables are usually stored as **Delta**, not JSON.
# MAGIC
# MAGIC Typical pipeline:
# MAGIC
# MAGIC ```
# MAGIC Raw JSON files
# MAGIC         ↓
# MAGIC Bronze Delta table
# MAGIC         ↓
# MAGIC Silver Delta tables
# MAGIC         ↓
# MAGIC Gold aggregated tables
# MAGIC ```
# MAGIC
# MAGIC Reason: JSON is **slow for analytics**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ✅ **Final answer**
# MAGIC
# MAGIC | Question                         | Answer                                                        |
# MAGIC | -------------------------------- | ------------------------------------------------------------- |
# MAGIC | Managed or External              | **External table**                                            |
# MAGIC | Data location                    | `abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers.json` |
# MAGIC | Does Databricks copy the data?   | ❌ No                                                          |
# MAGIC | What is stored in Unity Catalog? | Only **table metadata + schema**                              |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you'd like, I can also show **one subtle but very important mistake in this statement that many data engineers make in Bronze ingestion pipelines.**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Yes — **you can query it like a normal SQL table**, but there are **important differences** compared to a **Delta table**.
# MAGIC
# MAGIC Your table:
# MAGIC
# MAGIC ```sql
# MAGIC f1_dev.bronze.drivers
# MAGIC USING JSON
# MAGIC ```
# MAGIC
# MAGIC can be queried exactly the same way for **SELECT operations**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1️⃣ Queries that work normally
# MAGIC
# MAGIC ### Select all
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM f1_dev.bronze.drivers;
# MAGIC ```
# MAGIC
# MAGIC ### Filtering
# MAGIC
# MAGIC ```sql
# MAGIC SELECT driverId, nationality
# MAGIC FROM f1_dev.bronze.drivers
# MAGIC WHERE nationality = 'British';
# MAGIC ```
# MAGIC
# MAGIC ### Aggregations
# MAGIC
# MAGIC ```sql
# MAGIC SELECT nationality, COUNT(*)
# MAGIC FROM f1_dev.bronze.drivers
# MAGIC GROUP BY nationality;
# MAGIC ```
# MAGIC
# MAGIC ### Joins
# MAGIC
# MAGIC ```sql
# MAGIC SELECT d.driverId, r.raceId
# MAGIC FROM f1_dev.bronze.drivers d
# MAGIC JOIN f1_dev.silver.results r
# MAGIC ON d.driverId = r.driver_id;
# MAGIC ```
# MAGIC
# MAGIC So from a **query perspective**, it behaves like a normal SQL table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2️⃣ What you **cannot do** (compared to Delta)
# MAGIC
# MAGIC Because JSON is just a **file format**, these operations are **not supported**.
# MAGIC
# MAGIC ### ❌ Update
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE f1_dev.bronze.drivers
# MAGIC SET nationality = 'USA'
# MAGIC WHERE driverId = 1;
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Delete
# MAGIC
# MAGIC ```sql
# MAGIC DELETE FROM f1_dev.bronze.drivers
# MAGIC WHERE driverId = 1;
# MAGIC ```
# MAGIC
# MAGIC ### ❌ Merge
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO f1_dev.bronze.drivers ...
# MAGIC ```
# MAGIC
# MAGIC These require **Delta tables**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3️⃣ Performance difference
# MAGIC
# MAGIC | Feature        | JSON Table | Delta Table |
# MAGIC | -------------- | ---------- | ----------- |
# MAGIC | Query speed    | Slow       | Fast        |
# MAGIC | Compression    | No         | Yes         |
# MAGIC | Column pruning | No         | Yes         |
# MAGIC | Updates        | No         | Yes         |
# MAGIC | Deletes        | No         | Yes         |
# MAGIC | Time travel    | No         | Yes         |
# MAGIC
# MAGIC JSON must be **parsed every time you query it**, which makes it slower.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 4️⃣ How Bronze pipelines usually work
# MAGIC
# MAGIC Most production pipelines do this:
# MAGIC
# MAGIC ### Step 1 — Raw files (JSON)
# MAGIC
# MAGIC ```text
# MAGIC ADLS
# MAGIC bronze/drivers.json
# MAGIC ```
# MAGIC
# MAGIC ### Step 2 — Convert to Delta
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE bronze.drivers_delta
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM bronze.drivers;
# MAGIC ```
# MAGIC
# MAGIC ### Step 3 — Work with Delta tables
# MAGIC
# MAGIC Now you can run:
# MAGIC
# MAGIC ```
# MAGIC UPDATE
# MAGIC DELETE
# MAGIC MERGE
# MAGIC OPTIMIZE
# MAGIC VACUUM
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 5️⃣ Real-world pattern
# MAGIC
# MAGIC Typical lakehouse pipeline:
# MAGIC
# MAGIC ```
# MAGIC Raw JSON
# MAGIC    ↓
# MAGIC Bronze Delta table
# MAGIC    ↓
# MAGIC Silver cleaned Delta table
# MAGIC    ↓
# MAGIC Gold analytics tables
# MAGIC ```
# MAGIC
# MAGIC JSON tables are usually **temporary ingestion tables**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 6️⃣ One small note about your table
# MAGIC
# MAGIC Your statement:
# MAGIC
# MAGIC ```sql
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://bronze@.../drivers.json")
# MAGIC ```
# MAGIC
# MAGIC means the table reads **directly from the file**.
# MAGIC
# MAGIC If the file changes in storage, the table **automatically reflects the new data**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ✅ **Final answer**
# MAGIC
# MAGIC Yes — you can query it like a normal SQL table for:
# MAGIC
# MAGIC * `SELECT`
# MAGIC * `JOIN`
# MAGIC * `GROUP BY`
# MAGIC * `FILTER`
# MAGIC
# MAGIC But you **cannot modify the data** unless the table is **Delta format**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you'd like, I can also explain a **very important Unity Catalog concept**:
# MAGIC
# MAGIC Why most companies **never create tables using `USING JSON` or `USING PARQUET` directly** in production pipelines. It’s a subtle but very common design rule in lakehouse architectures.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Good question. Let’s compare the **three cases** clearly.
# MAGIC
# MAGIC You currently have:
# MAGIC
# MAGIC ```sql
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://.../drivers.json")
# MAGIC ```
# MAGIC
# MAGIC Now imagine the same thing but with **Parquet files** or **Delta tables**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 1️⃣ If the files were **Parquet**
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_dev.bronze.drivers
# MAGIC USING PARQUET
# MAGIC OPTIONS (
# MAGIC   path "abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers/"
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ## What happens
# MAGIC
# MAGIC Databricks:
# MAGIC
# MAGIC 1. Creates a **table metadata entry**
# MAGIC 2. Points to **existing parquet files**
# MAGIC 3. Reads them directly
# MAGIC
# MAGIC ### Storage
# MAGIC
# MAGIC ```
# MAGIC ADLS
# MAGIC └── drivers/
# MAGIC       part-0001.parquet
# MAGIC       part-0002.parquet
# MAGIC ```
# MAGIC
# MAGIC No files are moved.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Capabilities of Parquet table
# MAGIC
# MAGIC You can run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM f1_dev.bronze.drivers
# MAGIC ```
# MAGIC
# MAGIC You can also:
# MAGIC
# MAGIC * filter
# MAGIC * join
# MAGIC * aggregate
# MAGIC
# MAGIC But **you cannot modify data**.
# MAGIC
# MAGIC These fail:
# MAGIC
# MAGIC ```sql
# MAGIC UPDATE table
# MAGIC DELETE FROM table
# MAGIC MERGE INTO table
# MAGIC ```
# MAGIC
# MAGIC Because Parquet is **just files**, not a table system.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 2️⃣ If the files were **Delta**
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE f1_dev.bronze.drivers
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@dbxucextdl.dfs.core.windows.net/drivers/"
# MAGIC ```
# MAGIC
# MAGIC Now the folder must contain a **Delta table**.
# MAGIC
# MAGIC ### Delta folder structure
# MAGIC
# MAGIC ```
# MAGIC drivers/
# MAGIC    part-0000.parquet
# MAGIC    part-0001.parquet
# MAGIC    _delta_log/
# MAGIC        000000.json
# MAGIC        000001.json
# MAGIC ```
# MAGIC
# MAGIC That `_delta_log` folder is the key difference.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 3️⃣ Why Delta is special
# MAGIC
# MAGIC Delta = **Parquet + transaction log**
# MAGIC
# MAGIC ```
# MAGIC Delta Table
# MAGIC    ├── parquet files
# MAGIC    └── _delta_log
# MAGIC ```
# MAGIC
# MAGIC The log stores:
# MAGIC
# MAGIC * schema
# MAGIC * versions
# MAGIC * transactions
# MAGIC * updates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 4️⃣ Capabilities comparison
# MAGIC
# MAGIC | Feature           | JSON | Parquet  | Delta         |
# MAGIC | ----------------- | ---- | -------- | ------------- |
# MAGIC | File format       | Text | Columnar | Parquet + log |
# MAGIC | Query support     | Yes  | Yes      | Yes           |
# MAGIC | Updates           | ❌    | ❌        | ✅             |
# MAGIC | Deletes           | ❌    | ❌        | ✅             |
# MAGIC | Merge             | ❌    | ❌        | ✅             |
# MAGIC | ACID transactions | ❌    | ❌        | ✅             |
# MAGIC | Time travel       | ❌    | ❌        | ✅             |
# MAGIC | Schema evolution  | ❌    | ❌        | ✅             |
# MAGIC | Performance       | Slow | Fast     | Fastest       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 5️⃣ Example behavior
# MAGIC
# MAGIC ## JSON table
# MAGIC
# MAGIC ```
# MAGIC drivers.json
# MAGIC ```
# MAGIC
# MAGIC Every query must **parse the JSON text**.
# MAGIC
# MAGIC Slow.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Parquet table
# MAGIC
# MAGIC ```
# MAGIC drivers/
# MAGIC    part1.parquet
# MAGIC    part2.parquet
# MAGIC ```
# MAGIC
# MAGIC Columnar → faster.
# MAGIC
# MAGIC But still **immutable files**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Delta table
# MAGIC
# MAGIC ```
# MAGIC drivers/
# MAGIC    part1.parquet
# MAGIC    part2.parquet
# MAGIC    _delta_log/
# MAGIC ```
# MAGIC
# MAGIC Now Databricks knows:
# MAGIC
# MAGIC * which files belong to each version
# MAGIC * which rows were deleted
# MAGIC * which updates occurred
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 6️⃣ Example operations
# MAGIC
# MAGIC ### Only Delta supports this
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO silver.drivers s
# MAGIC USING bronze.drivers b
# MAGIC ON s.driverId = b.driverId
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC Parquet / JSON cannot do this.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 7️⃣ Real production architecture
# MAGIC
# MAGIC Almost all **Databricks pipelines convert everything to Delta**.
# MAGIC
# MAGIC ```
# MAGIC Raw files
# MAGIC (JSON / CSV / API)
# MAGIC        ↓
# MAGIC Bronze Delta tables
# MAGIC        ↓
# MAGIC Silver Delta tables
# MAGIC        ↓
# MAGIC Gold Delta tables
# MAGIC ```
# MAGIC
# MAGIC Even if raw data is JSON, engineers usually **convert it immediately**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 8️⃣ Key concept
# MAGIC
# MAGIC Think of it like this:
# MAGIC
# MAGIC ```
# MAGIC JSON    = text file
# MAGIC Parquet = optimized file
# MAGIC Delta   = database table built on parquet
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ✅ **Short answer**
# MAGIC
# MAGIC If your files were **Parquet instead of JSON**:
# MAGIC
# MAGIC * Query performance would be faster
# MAGIC * But the table would still behave like **read-only files**
# MAGIC
# MAGIC Delta tables are different because they support:
# MAGIC
# MAGIC * **ACID transactions**
# MAGIC * **updates/deletes**
# MAGIC * **time travel**
# MAGIC * **schema enforcement**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can also explain something that **confuses many Databricks engineers**:
# MAGIC
# MAGIC 👉 Why a **Delta table is still physically stored as Parquet files** even though we say “Delta format”.
# MAGIC

# COMMAND ----------

df = spark.read.json("abfss://bronze@dbxucextdl.dfs.core.windows.net/results.json")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_dev.bronze.results;
# MAGIC create table if not exists f1_dev.bronze.results
# MAGIC (
# MAGIC   resultId long,
# MAGIC   raceId long,
# MAGIC   driverId long,
# MAGIC   constructorId long,
# MAGIC   number long,
# MAGIC   grid long,
# MAGIC   position long,
# MAGIC   positionText string,
# MAGIC   positionOrder long,
# MAGIC   points double,
# MAGIC   laps long,
# MAGIC   time string,
# MAGIC   milliseconds long,
# MAGIC   fastestLap long,
# MAGIC   rank long,
# MAGIC   fastestLapTime string,
# MAGIC   fastestLapSpeed double,
# MAGIC   statusId long
# MAGIC )
# MAGIC using json
# MAGIC OPTIONS(path "abfss://bronze@dbxucextdl.dfs.core.windows.net/results.json")
# MAGIC