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

targetTable = returnDict.get('TGT_TBL_NAME')

storedProcedureName = adbParam.get('sp_name')

# COMMAND ----------

statement = """ALTER INDEX ALL ON {0}.{1} REBUILD""".format(targetSqlSchema,targetTable)          

# COMMAND ----------

try:
  call_sp_norow(statement)
  logStatus = 'SUCCESS'
except Exception as e:
  raise Exception('PROCESS RUN FAILED')

# COMMAND ----------

returnString = return_log('','',logStatus,'')

# COMMAND ----------

dbutils.notebook.exit(returnString)
