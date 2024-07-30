# Databricks notebook source
# DBTITLE 1,Credentials to connect ADLS@datalake #python
App_ID = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06AppID")
Secret = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06Blob")
Tenant_ID = "a8a4aed7-f228-48e2-bdd7-c2c82758d462"
Storage_Account_Name = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06")
Container_Name = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "containerNameDataLake")

spark.conf.set("fs.azure.account.auth.type." + Storage_Account_Name + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + Storage_Account_Name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + Storage_Account_Name + ".dfs.core.windows.net", App_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret." + Storage_Account_Name + ".dfs.core.windows.net", Secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + Storage_Account_Name + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + Tenant_ID + "/oauth2/token")

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
File_System_Datalake = "abfss://" + Container_Name + "@" + Storage_Account_Name + ".dfs.core.windows.net/"
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
print("File_System_Datalake=" + File_System_Datalake)

configs = {
"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": App_ID,
"fs.azure.account.oauth2.client.secret": Secret,
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + Tenant_ID + "/oauth2/token"} 
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
try:
  dbutils.fs.mount(
  source = File_System_Datalake,
    mount_point = "/mnt/adls_th",
    extra_configs = configs
  )
except:
  pass

mount_point = '/mnt/adls_th'
print(mount_point)

# COMMAND ----------

# DBTITLE 1,Credentials to connect ADLS@datalake #scala
# MAGIC %scala
# MAGIC
# MAGIC val appid = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06AppID")
# MAGIC val tenantID = "a8a4aed7-f228-48e2-bdd7-c2c82758d462"
# MAGIC val Storage_Account_Name = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06")
# MAGIC val Container_Name = "datalake"
# MAGIC val secret = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "dlsgdpeas06Blob")
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id", appid)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret", secret)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/a8a4aed7-f228-48e2-bdd7-c2c82758d462/oauth2/token")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# MAGIC spark.conf.set("fs.azure.account.auth.type." + Storage_Account_Name + ".dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type." + Storage_Account_Name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id." + Storage_Account_Name + ".dfs.core.windows.net", appid)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret." + Storage_Account_Name + ".dfs.core.windows.net", secret)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint." + Storage_Account_Name + ".dfs.core.windows.net", "https://login.microsoftonline.com/a8a4aed7-f228-48e2-bdd7-c2c82758d462/oauth2/token")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# MAGIC dbutils.fs.ls("abfss://"+Storage_Account_Name + "@" + Storage_Account_Name+ ".dfs.core.windows.net/")
# MAGIC spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
# MAGIC spark.conf.set("spark.databricks.sqldw.writeSemantics", "polybase")
# MAGIC val path="abfss://"+Container_Name+"@"+Storage_Account_Name+".dfs.core.windows.net/"
# MAGIC var File_System_Datalake = "abfss://" + Container_Name + "@" + Storage_Account_Name + ".dfs.core.windows.net/"

# COMMAND ----------

# DBTITLE 1,MLOPS
# MAGIC %scala
# MAGIC val mlopsContainerName = "mlops"
# MAGIC var File_System_MLOPS = "abfss://" + mlopsContainerName + "@" + Storage_Account_Name + ".dfs.core.windows.net/"

# COMMAND ----------

