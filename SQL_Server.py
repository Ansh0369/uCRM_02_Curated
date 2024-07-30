# Databricks notebook source
# DBTITLE 1,Credentials to connect SQL Server
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
# MAGIC // Application Zone DB
# MAGIC val jdbc_hostname = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03IP")
# MAGIC val jdbc_database = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--dbIDQ-DATA")
# MAGIC val jdbc_username_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--uTH-IDQETL-01")
# MAGIC val jdbc_password_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--TH-IDQETL-01")
# MAGIC val jdbc_port = 48000
# MAGIC val jdbc_url_sqls = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database};encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=60;"
# MAGIC 
# MAGIC // IDQ Data Domain
# MAGIC val jdbc_database_CLAIM = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--dbDOM-CLAIM")
# MAGIC val jdbc_database_POLICY = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--dbDOM-POLICY")
# MAGIC val jdbc_database_CUST = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--dbDOM-CUSTOMER")
# MAGIC val jdbc_database_PH_EMAIL = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--dbCUST-PHNO-EMAIL-TBL")
# MAGIC val jdbc_un_IDQDataLoad_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--uTH-DataLoad-01")
# MAGIC val jdbc_pwd_IDQDataLoad_sqls = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "weasinfsql03--TH-DataLoad-01")
# MAGIC val jdbc_url_CLAIM = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database_CLAIM}"
# MAGIC val jdbc_url_POLICY = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database_POLICY}"
# MAGIC val jdbc_url_CUST = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database_CUST}"
# MAGIC val jdbc_url_PH_EMAIL = s"jdbc:sqlserver://${jdbc_hostname}:${jdbc_port};database=${jdbc_database_PH_EMAIL}"
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
