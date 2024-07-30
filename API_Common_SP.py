# Databricks notebook source
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_RN_ID','')

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_Parameters

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_SQLConnections

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

dataTimestampString = returnDict.get('DATA_DT')

storedProcedureName = adbParam.get('sp_name')

# COMMAND ----------

statement = """EXEC {0}.{1}""".format(targetSqlSchema,storedProcedureName)          

# COMMAND ----------

result = call_sp_v2(statement)

# COMMAND ----------

returnString = return_log(result[0].get('Source'),result[0].get('Target'),result[0].get('Status'),'')

# COMMAND ----------

dbutils.notebook.exit(returnString)
