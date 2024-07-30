# Databricks notebook source
# DBTITLE 1,Scala Credentials to SQL SERVER
# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.SaveMode
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import java.sql.DriverManager
# MAGIC import java.util.Properties
# MAGIC 
# MAGIC 
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC 
# MAGIC // TH AI UW SQL DB connection details
# MAGIC val jdbc_hostname = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--server")
# MAGIC val jdbc_database = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sqd-gdp-eas-th-01--db")
# MAGIC val jdbc_username_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--uTH-DEV-AIUW")
# MAGIC val jdbc_password_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--TH-DEV-AIUW")
# MAGIC val jdbc_port = 1433
# MAGIC val jdbc_url_sqls = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
# MAGIC 
# MAGIC val User_Credentials = new Properties()
# MAGIC User_Credentials.put("user", jdbc_username_sqls)
# MAGIC User_Credentials.put("password", jdbc_password_sqls)
# MAGIC 
# MAGIC val Driver_Class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC User_Credentials.setProperty("driver", Driver_Class)
# MAGIC 
# MAGIC val Connection_String = DriverManager.getConnection(jdbc_url_sqls, jdbc_username_sqls, jdbc_password_sqls)
# MAGIC val Statement = Connection_String.createStatement()

# COMMAND ----------

# DBTITLE 1,Python Credentials to SQL SERVER
# TH AI UW SQL DB connection details
jdbc_hostname_th = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--server")
jdbc_database_th = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sqd-gdp-eas-th-01--db")
jdbc_username_sqls_th = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--uTH-DEV-AIUW")
jdbc_password_sqls_th = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--TH-DEV-AIUW")
jdbc_port_th = 1433
jdbc_url_sqls_th = f"jdbc:sqlserver://{jdbc_hostname_th}:{jdbc_port_th};database={jdbc_database_th};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"

print("jdbc_hostname_th        : " , jdbc_hostname_th)
print("jdbc_database_th        : " , jdbc_database_th)
print("jdbc_username_sqls_th   : " , jdbc_username_sqls_th)
print("jdbc_password_sqls_th   : " , jdbc_password_sqls_th)
print("jdbc_port_th            : " , jdbc_port_th)
print("jdbc_url_sqls_th        : " , jdbc_url_sqls_th)
############# TH

# connection properties
connection_properties = {
  "user" : jdbc_username_sqls_th,
  "password" : jdbc_password_sqls_th,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
