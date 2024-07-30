# Databricks notebook source
jdbcHostname = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--server")
jdbcDbname = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--sqd-gdp-eas-th-02")
user = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--uTH-UCRMETL-01")
pw = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "sql-gdp-eas-th-01--TH-UCRMETL-01")
jdbcPort = 1433
schema = "UCRM"


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDbname, user, pw)

driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
con = driver_manager.getConnection(jdbcUrl)
