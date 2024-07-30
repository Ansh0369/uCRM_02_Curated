# Databricks notebook source
dbutils.widgets.text('P_PRCS_NM','')
dbutils.widgets.text('P_PRCS_OBJ','')
dbutils.widgets.text('P_PRCS_RN_ID','')
dbutils.widgets.text('P_SRC_ROW_CNT','')

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_Parameters

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))
sourceRowCount = int(dbutils.widgets.get('P_SRC_ROW_CNT'))

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

interfaceName = returnDict.get('TGT_TBL_NAME')

# COMMAND ----------

df = spark.sql("""
select sum(processed_count) as processed_count, sum(failed_count) as failed_count ,sum(processed_count) - sum(failed_count) as success_count
from {0}.salesforce_archive_record_count_stg
where interface_name = '{1}' and prcs_nm = '{2}' and prcs_rn_id = {3}""".format(configSchema,interfaceName,processName,processRunId)).cache()
result = df.collect()
successCount = result[0][2]

# COMMAND ----------

fileList = dbutils.fs.ls(tmpFilePath + interfaceName)
for file in fileList:
  dbutils.fs.rm(file[0],recurse = False)
  print(file[0] + 'removed')

# COMMAND ----------

returnString = return_log(sourceRowCount,successCount,'SUCCESS','')

# COMMAND ----------

dbutils.notebook.exit(returnString)
