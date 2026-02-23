# Databricks notebook source
secret_value = "<secret>"
secret_scope_url = https://adb-7405618875300049.9.azuredatabricks.net/#secrets/createScope

# COMMAND ----------

# MAGIC %md
# MAGIC Perfect 👍 I’ll give you **clear end-to-end steps** to access **ADLS Gen2 from Azure Databricks using Unity Catalog**, and I’ll explain **why each step is needed**.
# MAGIC
# MAGIC We’ll connect:
# MAGIC
# MAGIC ```text
# MAGIC ADLS Storage Account: dlforformula1
# MAGIC Container: bronze
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # ✅ BIG PICTURE (What We Are Building)
# MAGIC
# MAGIC ```
# MAGIC Databricks
# MAGIC    ↓
# MAGIC Unity Catalog
# MAGIC    ↓
# MAGIC Storage Credential (Service Principal)
# MAGIC    ↓
# MAGIC ADLS Gen2 (bronze container)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 1 — Create a Service Principal (Identity)
# MAGIC
# MAGIC Go to:
# MAGIC
# MAGIC 👉 Microsoft Entra ID
# MAGIC → App registrations
# MAGIC → New registration
# MAGIC
# MAGIC Create:
# MAGIC
# MAGIC * Client ID
# MAGIC * Tenant ID
# MAGIC * Client Secret
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC Databricks needs a **secure identity** to talk to ADLS.
# MAGIC
# MAGIC Instead of:
# MAGIC
# MAGIC * ❌ Using storage account keys
# MAGIC * ❌ Hardcoding passwords
# MAGIC
# MAGIC We create:
# MAGIC
# MAGIC * ✅ A Service Principal (secure application identity)
# MAGIC
# MAGIC Think of this as:
# MAGIC
# MAGIC > Creating a “user account” for Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 2 — Give Storage Permission (RBAC)
# MAGIC
# MAGIC Go to:
# MAGIC
# MAGIC 👉 Azure Storage
# MAGIC → Storage account `dlforformula1`
# MAGIC → Access Control (IAM)
# MAGIC → Add role assignment
# MAGIC
# MAGIC Assign role:
# MAGIC
# MAGIC ```
# MAGIC Storage Blob Data Contributor
# MAGIC ```
# MAGIC
# MAGIC Assign it to:
# MAGIC
# MAGIC ```
# MAGIC Your Service Principal
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC RBAC gives **high-level permission**.
# MAGIC
# MAGIC It tells Azure:
# MAGIC
# MAGIC > “This identity is allowed to access this storage account.”
# MAGIC
# MAGIC Without this:
# MAGIC
# MAGIC * Databricks cannot access storage at all.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 3 — Set ACL (Only If HNS Enabled)
# MAGIC
# MAGIC Check in:
# MAGIC
# MAGIC Storage account → Configuration
# MAGIC If:
# MAGIC
# MAGIC ```
# MAGIC Hierarchical namespace = Enabled
# MAGIC ```
# MAGIC
# MAGIC Then go to:
# MAGIC
# MAGIC Containers → bronze → Access Control (ACL)
# MAGIC
# MAGIC Add your Service Principal with:
# MAGIC
# MAGIC * Read (r)
# MAGIC * Execute (x)
# MAGIC * Write (w) — if writing data
# MAGIC
# MAGIC Apply to subfolders and files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC When HNS is enabled (ADLS Gen2):
# MAGIC
# MAGIC Azure enforces file-system permissions.
# MAGIC
# MAGIC Even if IAM is correct,
# MAGIC without ACL you get:
# MAGIC
# MAGIC ```
# MAGIC Permission denied
# MAGIC Authorization failed
# MAGIC ```
# MAGIC
# MAGIC Think:
# MAGIC
# MAGIC * IAM = Allowed into building
# MAGIC * ACL = Allowed into specific rooms
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 4 — Store Secret in Databricks
# MAGIC
# MAGIC Go to:
# MAGIC
# MAGIC 👉 Azure Databricks
# MAGIC
# MAGIC Create a secret scope and store the client secret.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC We never expose secrets in code.
# MAGIC
# MAGIC Unity Catalog will securely read the secret from the secret scope.
# MAGIC
# MAGIC Security best practice:
# MAGIC
# MAGIC > No plain-text credentials in notebooks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 5 — Create Storage Credential (Unity Catalog)
# MAGIC
# MAGIC In Databricks SQL:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE STORAGE CREDENTIAL sc_dlforformula1
# MAGIC WITH AZURE_SERVICE_PRINCIPAL (
# MAGIC   CLIENT_ID = '<client-id>',
# MAGIC   TENANT_ID = '<tenant-id>',
# MAGIC   CLIENT_SECRET = SECRET('scope-name', 'secret-key')
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC Storage Credential tells Unity Catalog:
# MAGIC
# MAGIC > “Here is how to authenticate to ADLS.”
# MAGIC
# MAGIC Without this:
# MAGIC Unity Catalog cannot connect to Azure Storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 6 — Create External Location
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION el_bronze
# MAGIC URL 'abfss://bronze@dlforformula1.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL sc_dlforformula1);
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC External Location registers the storage path inside Unity Catalog.
# MAGIC
# MAGIC It allows:
# MAGIC
# MAGIC * Governance
# MAGIC * Permission control
# MAGIC * Auditing
# MAGIC
# MAGIC Without this:
# MAGIC Users cannot access the path through Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 7 — Grant Access to Users
# MAGIC
# MAGIC ```sql
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION el_bronze TO `account users`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC Unity Catalog enforces access control.
# MAGIC
# MAGIC Even if storage is configured,
# MAGIC users must be granted permission explicitly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 8 — Test Access in Notebook
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.option("header", True).csv(
# MAGIC     "abfss://bronze@dlforformula1.dfs.core.windows.net/races.csv"
# MAGIC )
# MAGIC display(df)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 WHY this step is needed
# MAGIC
# MAGIC This confirms:
# MAGIC
# MAGIC * Identity works
# MAGIC * RBAC works
# MAGIC * ACL works
# MAGIC * Unity Catalog is correctly configured
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔥 Final Summary — Why Each Layer Exists
# MAGIC
# MAGIC | Step               | Purpose                       |
# MAGIC | ------------------ | ----------------------------- |
# MAGIC | Service Principal  | Secure identity               |
# MAGIC | IAM Role           | High-level storage access     |
# MAGIC | ACL                | Folder/file-level access      |
# MAGIC | Secret Scope       | Secure secret storage         |
# MAGIC | Storage Credential | Authentication config in UC   |
# MAGIC | External Location  | Governed storage registration |
# MAGIC | Grant              | User-level access control     |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🎯 One-Line Explanation
# MAGIC
# MAGIC To connect Databricks to ADLS securely using Unity Catalog, you must configure:
# MAGIC
# MAGIC ```
# MAGIC Identity + Permission + File Access + Governance
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can next explain:
# MAGIC
# MAGIC * Difference between External Location vs External Table
# MAGIC * Or common connection errors and how to fix them
# MAGIC * Or how companies structure bronze/silver/gold in Unity Catalog
# MAGIC
# MAGIC What would you like next?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # above didn't work
# MAGIC have to check another approch for connect azure storage to databrix