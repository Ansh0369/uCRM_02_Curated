// Databricks notebook source
// DBTITLE 1,Import Dependencies
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.sql.DriverManager
import java.util.Properties

// COMMAND ----------

// DBTITLE 1,Initialize Parameters
// MAGIC %scala
// MAGIC dbutils.widgets.text("Batch_ID", "Parameterized")
// MAGIC var Batch_ID = dbutils.widgets.get("Batch_ID")
// MAGIC 
// MAGIC dbutils.widgets.text("Error_Message", "Parameterized")
// MAGIC var Error_Message = dbutils.widgets.get("Error_Message")
// MAGIC 
// MAGIC //Example value : LA
// MAGIC dbutils.widgets.text("Data_Source", "Parameterized")
// MAGIC var Data_Source = dbutils.widgets.get("Data_Source")
// MAGIC 
// MAGIC //Example value: TH_Inital_LA
// MAGIC dbutils.widgets.text("PipelineName", "Parameterized")
// MAGIC var Pipeline_Name = dbutils.widgets.get("PipelineName")
// MAGIC 
// MAGIC //Example value: PipelineTriggerID
// MAGIC dbutils.widgets.text("PipelineTriggerID", "Parameterized")
// MAGIC var Pipeline_Trigger_ID = dbutils.widgets.get("PipelineTriggerID")
// MAGIC 
// MAGIC //Example value: MY_DEV_ETL_01
// MAGIC dbutils.widgets.text("PipelineTriggerName", "Parameterized")
// MAGIC var Pipeline_Trigger_Name = dbutils.widgets.get("PipelineTriggerName")
// MAGIC 
// MAGIC //Example value: MY_DEV_ETL_01
// MAGIC dbutils.widgets.text("PipelineTriggerType", "Parameterized")
// MAGIC var Pipeline_Trigger_Type = dbutils.widgets.get("PipelineTriggerType")

// COMMAND ----------

// DBTITLE 1,Run Credentials Notebook
// MAGIC %run /THAILAND/CommonZone/Credentials/AIUW_ADLS

// COMMAND ----------

// DBTITLE 1,Run Credentials SQL Server
// MAGIC %run /THAILAND/CommonZone/Credentials/AIUW_Credentials_Sql_Server

// COMMAND ----------

// DBTITLE 1,Get Current DateTime
val Current_DateTime = java.time.LocalDateTime.now.toString
print(Current_DateTime)

// COMMAND ----------

// DBTITLE 1,Load Data into DWH Table
// MAGIC %scala
// MAGIC var Load_Data_Query = "exec AIUW.uspPipeAudit '" + Batch_ID + "','" + Pipeline_Name + "','" + Pipeline_Trigger_ID + "','" + Pipeline_Trigger_Name + "','" + Pipeline_Trigger_Type + "','" + Error_Message + "','NA','" + Data_Source + "','" + Current_DateTime + "'"
// MAGIC print(Load_Data_Query)
// MAGIC Statement.execute(Load_Data_Query)
