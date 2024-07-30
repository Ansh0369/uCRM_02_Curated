# Databricks notebook source
dbutils.widgets.text('P_FAILED_REC_STR','')
dbutils.widgets.text('P_PRCS_RN_ID','')
dbutils.widgets.text('P_PRCS_NM','')
dbutils.widgets.text('P_PRCS_OBJ','')
dbutils.widgets.text('P_FILE_NAME','')
dbutils.widgets.text('P_PRORCESSED_REC_CNT','')
dbutils.widgets.text('P_FAILED_REC_CNT','')

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_Parameters

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))
recordString = dbutils.widgets.get('P_FAILED_REC_STR')
fileName = dbutils.widgets.get('P_FILE_NAME')
processedRecordCount = int(dbutils.widgets.get('P_PRORCESSED_REC_CNT'))
failedRecordCount = int(dbutils.widgets.get('P_FAILED_REC_CNT'))

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

targetObject = returnDict.get("TGT_TBL_NAME")
dataTimestampString = returnDict.get('DATA_DT')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataDatetime.date()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Log file count

# COMMAND ----------

spark.sql("""INSERT INTO {0}.salesforce_archive_record_count_stg
SELECT 
'{1}' AS INTERFACE_NAME
,'{2}' AS FILENAME
,{3} AS PROCESSED_COUNT
,{4} AS FAILED_COUNT
,'{5}' AS DATA_DTTM
,'{6}' AS DATA_DT
,{7} AS PRCS_RN_ID
,'{8}' AS PRCS_NM
""".format(configSchema,targetObject,fileName,processedRecordCount,failedRecordCount,dataDatetime,dataDate,processRunId,processName))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Log Failed Records

# COMMAND ----------

recordString

# COMMAND ----------

if failedRecordCount > 0:
  recordIO = StringIO(recordString)
  df = pd.read_csv(recordIO)
  sparkDf = spark.createDataFrame(df)
  sparkDf.cache()
  sparkDf2 = sparkDf.withColumn("DATA_DTTM",lit(dataDatetime)) \
            .withColumn("DATA_DT",lit(dataDate)) \
            .withColumn("PRCS_NM",lit(processName)) \
            .withColumn("PRCS_RN_ID",lit(processRunId).cast('int')) \
            .withColumn("INTERFACE_NAME",lit(targetObject)) \
            .withColumn("FILENAME",lit(fileName)) \
            .withColumn("TS",lit(datetime.now())).cache()
  sparkDf2.write.mode('append').saveAsTable('{0}.salesforce_archive_failed_api_log'.format(configSchema))

# COMMAND ----------

dbutils.notebook.exit('')
