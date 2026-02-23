# Databricks notebook source
# MAGIC %md
# MAGIC ## Access ADLS storage account files using Service Principal
# MAGIC  1. Set the Spark config with service principal credentials (client ID, tenant ID, client secret).
# MAGIC  2. List files from the demo container.
# MAGIC  3. Read the circuit.csv from the demo container (bucket).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Steps to create a Service Principal in Azure
# MAGIC
# MAGIC 1. **Register an application in Microsoft Entra ID (Azure AD):**
# MAGIC    - Go to the Azure portal.
# MAGIC    - Navigate to **Microsoft Entra ID** > **App registrations** > **New registration**.
# MAGIC    - Enter a name and click **Register**.
# MAGIC
# MAGIC 2. **Create a client secret:**
# MAGIC    - In the registered app, go to **Certificates & secrets**.
# MAGIC    - Click **New client secret** and add a description and expiry.
# MAGIC    - Copy and save the secret value securely.
# MAGIC
# MAGIC 3. **Note the Application (client) ID and Directory (tenant) ID:**
# MAGIC    - In the app's **Overview** page, copy the **Application (client) ID** and **Directory (tenant) ID**.
# MAGIC
# MAGIC 4. **Assign the service principal access to resources:**
# MAGIC    - Grant the required permissions (e.g., Storage Blob Data Contributor) to the service principal on the target resource.
# MAGIC
# MAGIC 5. **(Optional) Add the service principal to Azure Databricks:**
# MAGIC    - In Databricks workspace, go to **Settings** > **Identity and access** > **Service principals** > **Manage** > **Add service principal**.
# MAGIC    - Paste the Application (client) ID and assign a display name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 What is **Storage Blob Data Contributor** in Azure?
# MAGIC
# MAGIC **Storage Blob Data Contributor** is a built-in **RBAC role** in Azure that allows a user, service principal, or managed identity to **read, write, and delete blob data** in Azure Storage.
# MAGIC
# MAGIC It is specifically used for **data-level access to blobs** (not storage account management).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Applies To
# MAGIC
# MAGIC It is used with:
# MAGIC
# MAGIC * Azure Blob Storage
# MAGIC * Azure Data Lake Storage Gen2
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ What Permissions Does It Give?
# MAGIC
# MAGIC With **Storage Blob Data Contributor**, you can:
# MAGIC
# MAGIC * 📥 Read blobs
# MAGIC * 📤 Upload blobs
# MAGIC * ✏️ Modify blobs
# MAGIC * ❌ Delete blobs
# MAGIC * 📂 Manage containers (create/delete)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ❌ What It Cannot Do
# MAGIC
# MAGIC It **cannot**:
# MAGIC
# MAGIC * Delete the storage account
# MAGIC * Change networking settings
# MAGIC * Rotate access keys
# MAGIC * Modify IAM (RBAC)
# MAGIC
# MAGIC Because it is **data-level access only**, not management-level.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Important Concept
# MAGIC
# MAGIC Azure has **two types of access**:
# MAGIC
# MAGIC | Access Type      | Example Role                  |
# MAGIC | ---------------- | ----------------------------- |
# MAGIC | Management Plane | Contributor                   |
# MAGIC | Data Plane       | Storage Blob Data Contributor |
# MAGIC
# MAGIC This role works at the **data plane level**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔎 Real Databricks Example
# MAGIC
# MAGIC If:
# MAGIC
# MAGIC * Databricks needs to read/write to ADLS
# MAGIC * You use a Service Principal
# MAGIC
# MAGIC You assign:
# MAGIC
# MAGIC > **Storage Blob Data Contributor** on the storage account or container
# MAGIC
# MAGIC Now Databricks can access data securely.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Comparison With Similar Roles
# MAGIC
# MAGIC | Role                          | Access Level                |
# MAGIC | ----------------------------- | --------------------------- |
# MAGIC | Storage Blob Data Reader      | Read-only                   |
# MAGIC | Storage Blob Data Contributor | Read + Write + Delete       |
# MAGIC | Owner                         | Full control including RBAC |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏆 Interview-Ready Definition
# MAGIC
# MAGIC > Storage Blob Data Contributor is an Azure built-in RBAC role that provides read, write, and delete permissions to blob data in Azure Storage but does not allow management of the storage account itself.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you'd like, I can also explain:
# MAGIC
# MAGIC * Difference between Contributor vs Storage Blob Data Contributor
# MAGIC * Why access keys are less secure
# MAGIC * How this works with Unity Catalog
# MAGIC
# MAGIC Just tell me 🚀
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔐 What Are Other Roles Similar to **Storage Blob Data Contributor**?
# MAGIC
# MAGIC If you're asking about **data-level RBAC roles for Azure Storage**, here are the main ones related to:
# MAGIC
# MAGIC * Azure Blob Storage
# MAGIC * Azure Data Lake Storage Gen2
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 📦 Blob Data Roles (Data Plane Roles)
# MAGIC
# MAGIC These roles control **access to the actual data**, not the storage account settings.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1️⃣ Storage Blob Data Reader
# MAGIC
# MAGIC **Access Level:** Read-only
# MAGIC
# MAGIC ### ✅ Can:
# MAGIC
# MAGIC * Read blobs
# MAGIC * List containers
# MAGIC
# MAGIC ### ❌ Cannot:
# MAGIC
# MAGIC * Upload
# MAGIC * Modify
# MAGIC * Delete
# MAGIC
# MAGIC 🔹 Used when applications only need **read access**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2️⃣ Storage Blob Data Contributor
# MAGIC
# MAGIC **Access Level:** Read + Write + Delete
# MAGIC
# MAGIC ### ✅ Can:
# MAGIC
# MAGIC * Upload blobs
# MAGIC * Modify blobs
# MAGIC * Delete blobs
# MAGIC * Create containers
# MAGIC
# MAGIC ### ❌ Cannot:
# MAGIC
# MAGIC * Manage RBAC
# MAGIC * Delete storage account
# MAGIC
# MAGIC 🔹 Most commonly used for Databricks access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3️⃣ Storage Blob Data Owner
# MAGIC
# MAGIC **Access Level:** Full data control
# MAGIC
# MAGIC ### ✅ Can:
# MAGIC
# MAGIC * Everything Contributor can
# MAGIC * Manage access (assign RBAC on blobs)
# MAGIC
# MAGIC 🔹 Rarely given — high privilege
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🗂️ If Using ADLS Gen2 (Hierarchical Namespace)
# MAGIC
# MAGIC There are also roles for filesystem-level access.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4️⃣ Storage Queue Data Contributor
# MAGIC
# MAGIC For Azure Storage Queues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5️⃣ Storage Table Data Contributor
# MAGIC
# MAGIC For Azure Table Storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🧠 Important: Data Plane vs Management Plane
# MAGIC
# MAGIC | Type             | Example Role                  |
# MAGIC | ---------------- | ----------------------------- |
# MAGIC | Data Plane       | Storage Blob Data Contributor |
# MAGIC | Management Plane | Contributor / Owner           |
# MAGIC
# MAGIC Management plane roles control:
# MAGIC
# MAGIC * Networking
# MAGIC * Access keys
# MAGIC * Storage account deletion
# MAGIC
# MAGIC Data plane roles control:
# MAGIC
# MAGIC * Actual file/blob access
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔎 Real Databricks Scenario
# MAGIC
# MAGIC If:
# MAGIC
# MAGIC * Databricks reads data → Storage Blob Data Reader
# MAGIC * Databricks writes bronze/silver/gold → Storage Blob Data Contributor
# MAGIC * Admin team controls access → Storage Blob Data Owner
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏆 Interview-Ready Summary
# MAGIC
# MAGIC > The similar roles to Storage Blob Data Contributor are Storage Blob Data Reader (read-only) and Storage Blob Data Owner (full data control including permission management).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you want, I can also explain:
# MAGIC
# MAGIC * Why we combine RBAC + ACL in ADLS Gen2
# MAGIC * Difference between Blob roles and file system ACLs
# MAGIC * AWS equivalent roles for S3
# MAGIC
# MAGIC Tell me what you want next 🚀
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Yes — It Is Similar to an AWS Policy (But Not Exactly the Same)
# MAGIC
# MAGIC When you talk about:
# MAGIC
# MAGIC > **Storage Blob Data Contributor**
# MAGIC
# MAGIC It is conceptually similar to an **AWS IAM Policy**, but there’s an architectural difference.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔄 Azure vs AWS Mapping
# MAGIC
# MAGIC | Azure                         | AWS                                 |
# MAGIC | ----------------------------- | ----------------------------------- |
# MAGIC | Storage Blob Data Contributor | IAM Policy (e.g., S3 access policy) |
# MAGIC | Service Principal             | IAM Role                            |
# MAGIC | RBAC Role Assignment          | Policy Attachment                   |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 How It Works in Azure
# MAGIC
# MAGIC In Azure:
# MAGIC
# MAGIC 1. You create a **Service Principal**
# MAGIC 2. You assign a **Role** (like Storage Blob Data Contributor)
# MAGIC 3. That role defines permissions (read/write/delete blob data)
# MAGIC
# MAGIC So the **role contains the permissions**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 How It Works in AWS
# MAGIC
# MAGIC In AWS:
# MAGIC
# MAGIC 1. You create an **IAM Role**
# MAGIC 2. You attach an **IAM Policy**
# MAGIC 3. The policy defines permissions (s3:GetObject, s3:PutObject)
# MAGIC
# MAGIC So in AWS, the **policy contains the permissions**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔑 Key Architectural Difference
# MAGIC
# MAGIC ### Azure:
# MAGIC
# MAGIC Role = Permission bundle (predefined)
# MAGIC
# MAGIC ### AWS:
# MAGIC
# MAGIC Policy = Permission document (JSON)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🔎 Example Comparison
# MAGIC
# MAGIC ### Azure:
# MAGIC
# MAGIC Assign:
# MAGIC
# MAGIC ```
# MAGIC Storage Blob Data Contributor
# MAGIC ```
# MAGIC
# MAGIC ### AWS:
# MAGIC
# MAGIC Attach policy like:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "Effect": "Allow",
# MAGIC   "Action": [
# MAGIC     "s3:GetObject",
# MAGIC     "s3:PutObject"
# MAGIC   ],
# MAGIC   "Resource": "arn:aws:s3:::my-bucket/*"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏗️ Real-World Equivalent
# MAGIC
# MAGIC If Databricks needs storage access:
# MAGIC
# MAGIC * Azure → Assign Storage Blob Data Contributor to Service Principal
# MAGIC * AWS → Attach S3 access policy to IAM Role
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🏆 Interview-Ready Answer
# MAGIC
# MAGIC > Storage Blob Data Contributor in Azure is conceptually similar to an AWS IAM Policy because both define what actions are allowed on storage resources, but in Azure permissions are bundled inside roles, while in AWS they are defined inside policies.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC If you'd like, I can also explain:
# MAGIC
# MAGIC * Why Azure uses RBAC roles instead of JSON policies
# MAGIC * Which model is more flexible
# MAGIC * How cross-account access compares
# MAGIC
# MAGIC Just tell me 🚀
# MAGIC

# COMMAND ----------

tenant_id = "<tenant_id>"
client_id = "<client_id>"
client_secret = "<client_secret>"

# Set the Spark config with the ADLS Service Principal credentials
spark.conf.set(
    "fs.azure.account.auth.type.dlforformula1.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    "fs.azure.account.oauth.provider.type.dlforformula1.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    "fs.azure.account.oauth2.client.id.dlforformula1.dfs.core.windows.net",
    client_id
)
spark.conf.set(
    "fs.azure.account.oauth2.client.secret.dlforformula1.dfs.core.windows.net",
    client_secret
)
spark.conf.set(
    "fs.azure.account.oauth2.client.endpoint.dlforformula1.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"
)

# COMMAND ----------

# abfss is the protocol used to access Azure Data Lake Storage Gen2 (ADLS Gen2) using secure (SSL) endpoints.
# Example usage to list files in a container:
dbutils.fs.ls("abfss://demo@dlforformula1.dfs.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlforformula1.dfs.core.windows.net/"))

# COMMAND ----------

spark.read.csv("abfss://demo@dlforformula1.dfs.core.windows.net/").show()


# COMMAND ----------

display(spark.read.csv("abfss://demo@dlforformula1.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Drawbacks of Service Principal connection:
# MAGIC  - Requires manual management and rotation of client secrets, increasing operational overhead.
# MAGIC  - Secrets can be accidentally exposed in code, notebooks, or logs, leading to security risks.
# MAGIC   - Difficult to audit and track usage at a granular level.
# MAGIC  - Limited support for network-restricted storage accounts.
# MAGIC  - Not as seamless for multi-workspace or cross-region scenarios.
# MAGIC
# MAGIC # A better way:
# MAGIC Use managed identities (via Azure Databricks Access Connector) or Unity Catalog storage credentials.
# MAGIC Benefits:
# MAGIC - No secrets to manage or rotate.
# MAGIC - Improved security: credentials are managed by Azure and never exposed.
# MAGIC - Easier auditing and access control.
# MAGIC - Supports network-restricted storage accounts.