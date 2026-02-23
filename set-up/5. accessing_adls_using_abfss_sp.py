# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔹 Connecting **ADLS to Azure Databricks using Mount (DBFS Mounting)**
# MAGIC
# MAGIC > ⚠️ Note: Mounting is the **older method**. In modern setups, **Unity Catalog + External Locations** is recommended.
# MAGIC > But if your workspace allows mounts, here are the **clear step-by-step instructions**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # ✅ What You Need Before Starting
# MAGIC
# MAGIC 1. ✔️ ADLS Gen2 storage account
# MAGIC 2. ✔️ A container inside ADLS
# MAGIC 3. ✔️ Service Principal (App Registration)
# MAGIC 4. ✔️ Storage role assigned to Service Principal
# MAGIC 5. ✔️ Client ID, Client Secret, Tenant ID
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 1 — Create Service Principal (App Registration)
# MAGIC
# MAGIC Go to:
# MAGIC
# MAGIC 👉 **Azure Portal → Azure Active Directory → App registrations → New registration**
# MAGIC
# MAGIC After creation, note:
# MAGIC
# MAGIC * **Application (client) ID**
# MAGIC * **Directory (tenant) ID**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 2 — Create Client Secret
# MAGIC
# MAGIC Inside the App:
# MAGIC
# MAGIC 👉 Certificates & Secrets → New Client Secret
# MAGIC
# MAGIC Copy the **secret value immediately** (you won’t see it again).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 3 — Assign Storage Role
# MAGIC
# MAGIC Go to:
# MAGIC
# MAGIC 👉 Storage Account → Access Control (IAM) → Add Role Assignment
# MAGIC
# MAGIC Assign role:
# MAGIC
# MAGIC **Storage Blob Data Contributor**
# MAGIC
# MAGIC To:
# MAGIC
# MAGIC 👉 Your Service Principal
# MAGIC
# MAGIC ### 🔎 Why?
# MAGIC
# MAGIC This allows Databricks (via SP) to read/write ADLS data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 4 — Store Secret in Databricks Secret Scope
# MAGIC
# MAGIC In Databricks:
# MAGIC
# MAGIC 👉 User Settings → Developer → Access Tokens → Generate Token
# MAGIC
# MAGIC Then use CLI or API to create secret scope:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope --scope adls-scope
# MAGIC ```
# MAGIC
# MAGIC Add secret:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets put --scope adls-scope --key client-secret
# MAGIC ```
# MAGIC
# MAGIC Paste your client secret.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 5 — Mount ADLS in Databricks Notebook
# MAGIC
# MAGIC Now go to a Notebook and run:
# MAGIC
# MAGIC ```python
# MAGIC configs = {
# MAGIC   "fs.azure.account.auth.type": "OAuth",
# MAGIC   "fs.azure.account.oauth.provider.type": 
# MAGIC     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC   "fs.azure.account.oauth2.client.id": "<CLIENT-ID>",
# MAGIC   "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adls-scope", key="client-secret"),
# MAGIC   "fs.azure.account.oauth2.client.endpoint": 
# MAGIC     "https://login.microsoftonline.com/<TENANT-ID>/oauth2/token"
# MAGIC }
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/adls",
# MAGIC   extra_configs = configs)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 STEP 6 — Verify Mount
# MAGIC
# MAGIC ```python
# MAGIC dbutils.fs.ls("/mnt/adls")
# MAGIC ```
# MAGIC
# MAGIC If successful, you’ll see files inside container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 How It Works (Architecture View)
# MAGIC
# MAGIC ![Image](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/media/azure-databricks-modern-analytics-architecture.svg)
# MAGIC
# MAGIC ![Image](https://kodekloud.com/kk-media/image/upload/v1752866905/notes-assets/images/AZ-305-Microsoft-Azure-Solutions-Architect-Expert-Design-for-Azure-Data-Lake-Storage/adls-data-flow-diagram.jpg)
# MAGIC
# MAGIC ![Image](https://docs.itrontotal.com/AzureActiveDirectoryIntegration/Content/Content/Images/sp-flow_577x493.png)
# MAGIC
# MAGIC ![Image](https://media.licdn.com/dms/image/v2/D5612AQENdkel2RsMdA/article-cover_image-shrink_720_1280/article-cover_image-shrink_720_1280/0/1722164338455?e=2147483647\&t=I3a51Uiu1ph_FD2MIEprSVAItbDqh5bcFbmnzX61ido\&v=beta)
# MAGIC
# MAGIC ### Flow:
# MAGIC
# MAGIC Databricks → Service Principal → Azure AD → ADLS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 Where Data Appears
# MAGIC
# MAGIC Mounted path:
# MAGIC
# MAGIC ```
# MAGIC /mnt/adls
# MAGIC ```
# MAGIC
# MAGIC But actual data is in:
# MAGIC
# MAGIC ```
# MAGIC abfss://container@storageaccount.dfs.core.windows.net/
# MAGIC ```
# MAGIC
# MAGIC Mount just creates a shortcut.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 To Unmount (If Needed)
# MAGIC
# MAGIC ```python
# MAGIC dbutils.fs.unmount("/mnt/adls")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🚨 Important Notes (Very Important)
# MAGIC
# MAGIC | Mounting               | Unity Catalog              |
# MAGIC | ---------------------- | -------------------------- |
# MAGIC | Old method             | Recommended                |
# MAGIC | Cluster scoped         | Workspace level governance |
# MAGIC | No fine-grained access | Table-level security       |
# MAGIC | Uses DBFS              | Uses External Location     |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔥 When Should You Use Mount?
# MAGIC
# MAGIC ✔️ Legacy projects
# MAGIC ✔️ Non-Unity Catalog workspace
# MAGIC ✔️ Quick PoC
# MAGIC ❌ Not recommended for enterprise governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can next give you:
# MAGIC
# MAGIC * ✅ Same process using **Access Keys (simpler but less secure)**
# MAGIC * ✅ Same process using **Unity Catalog (recommended modern way)**
# MAGIC * ✅ Full comparison: Mount vs Unity Catalog vs Direct ABFSS access**
# MAGIC
# MAGIC Tell me what you prefer 👌
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list("adls-scope")

# COMMAND ----------

client_secret = "<secret>"
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

# MAGIC %md
# MAGIC ## 🔷 What is **ABFSS**?
# MAGIC
# MAGIC **ABFSS** stands for:
# MAGIC
# MAGIC > **Azure Blob File System Secure**
# MAGIC
# MAGIC It is the **secure protocol** used to access **Azure Data Lake Storage Gen2 (ADLS Gen2)** from tools like **Azure Databricks**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 Full Form Breakdown
# MAGIC
# MAGIC | Part  | Meaning                           |
# MAGIC | ----- | --------------------------------- |
# MAGIC | ABFS  | Azure Blob File System            |
# MAGIC | ABFSS | Azure Blob File System **Secure** |
# MAGIC | S     | Secure (uses HTTPS + OAuth)       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 What Does an ABFSS Path Look Like?
# MAGIC
# MAGIC ```text
# MAGIC abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
# MAGIC ```
# MAGIC
# MAGIC ### Example (your case):
# MAGIC
# MAGIC ```text
# MAGIC abfss://bronze@dlforformula1.dfs.core.windows.net/
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 Why Do We Use ABFSS?
# MAGIC
# MAGIC Because ADLS Gen2 supports:
# MAGIC
# MAGIC * ✔️ OAuth authentication
# MAGIC * ✔️ Azure AD integration
# MAGIC * ✔️ ACLs (like file system permissions)
# MAGIC * ✔️ Hierarchical namespace
# MAGIC
# MAGIC ABFSS enables all of that securely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 ABFS vs ABFSS
# MAGIC
# MAGIC | Protocol   | Secure?              | When Used   |
# MAGIC | ---------- | -------------------- | ----------- |
# MAGIC | `abfs://`  | ❌ Not secure         | Rarely used |
# MAGIC | `abfss://` | ✅ Secure (HTTPS)     | Recommended |
# MAGIC | `wasbs://` | ❌ Older Blob Storage | Legacy      |
# MAGIC
# MAGIC Always use:
# MAGIC
# MAGIC ```text
# MAGIC abfss://
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 How It Works (Simple Flow)
# MAGIC
# MAGIC ![Image](https://learn-attachment.microsoft.com/api/attachments/fc2d080b-f8ed-485c-9d14-9647f81b207f?platform=QnA)
# MAGIC
# MAGIC ![Image](https://learn.microsoft.com/en-us/entra/architecture/media/authentication-patterns/oauth.png)
# MAGIC
# MAGIC ![Image](https://azure.github.io/Storage/docs/analytics/azure-storage-data-lake-gen2-billing-faq/images/adls_hns.png)
# MAGIC
# MAGIC ![Image](https://azure.github.io/Storage/docs/analytics/hitchhikers-guide-to-the-datalake/images/data_lake_zones.png)
# MAGIC
# MAGIC **Flow:**
# MAGIC
# MAGIC Databricks
# MAGIC → ABFS Driver
# MAGIC → Azure AD (OAuth)
# MAGIC → ADLS Gen2
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 Where ABFSS Is Used
# MAGIC
# MAGIC You’ll see it in:
# MAGIC
# MAGIC * Spark read/write paths
# MAGIC * Unity Catalog external locations
# MAGIC * Direct file access
# MAGIC * Delta Lake storage paths
# MAGIC
# MAGIC Example:
# MAGIC
# MAGIC ```python
# MAGIC spark.read.parquet("abfss://container@account.dfs.core.windows.net/data/")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔹 Important Concept
# MAGIC
# MAGIC ABFSS is:
# MAGIC
# MAGIC * ❌ Not a storage service
# MAGIC * ❌ Not a database
# MAGIC * ✅ A secure access protocol to ADLS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 In One Line
# MAGIC
# MAGIC > **ABFSS is the secure file system protocol used to access ADLS Gen2 from Spark/Databricks.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want next, I can explain:
# MAGIC
# MAGIC * 🔥 How ABFSS differs from S3A in AWS
# MAGIC * 🔥 How it relates to Unity Catalog
# MAGIC * 🔥 How it works internally inside Spark
# MAGIC
# MAGIC Just tell me 👌
# MAGIC

# COMMAND ----------

