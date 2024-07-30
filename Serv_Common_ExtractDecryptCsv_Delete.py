# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Prepare input

# COMMAND ----------

# DBTITLE 1,Display input parameters from ADF
# dbutils.widgets.text('P_PROCESS_NAME','')
# dbutils.widgets.text('P_PROCESS_OBJECT','')
# dbutils.widgets.text('P_PROCESS_RUN_ID','')

# COMMAND ----------

# DBTITLE 1,Define functions and import libraries needed
# MAGIC %run ../99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_Parameters

# COMMAND ----------

# DBTITLE 1,Retrieve parameters from ADF
objectString = dbutils.widgets.get('P_PROCESS_OBJECT')
processName = dbutils.widgets.get('P_PROCESS_NAME')
processRunId = int(dbutils.widgets.get('P_PROCESS_RUN_ID'))

# COMMAND ----------

# DBTITLE 1,Convert ADF parameter to dictionary type
returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

# DBTITLE 1,Retrieve converted parameters
sourceTableName = returnDict.get('SRC_TBL_NAME')
dataTimestampString = returnDict.get('DATA_DT')
targetObject = returnDict.get('TGT_TBL_NAME')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDatetimeString = dataDatetime.strftime("%Y%m%d%H%M%S")
tableSearchName = get_table_search_name(sourceTableName,'_')
fileName = targetObject + '_Delete_' + dataDatetimeString
filePathFull = tmpFilePath+targetObject+'/'+fileName

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(datamartSchema)
print(sourceTableName)
print(targetObject)
print(configSchema)
print(tmpFilePath)
print(filePathFull)
print(tableSearchName)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Prepare statement

# COMMAND ----------

# DBTITLE 1,Create SQL statement for target table metadata
sqlString = """select 
meta.COL_NM
,inf.API_FLD_NM
,meta.PRIMARY_KEY
,meta.ENCRYPT_COL
from {0}.table_metadata meta
left JOIN {0}.interface_mapping inf
ON meta.COL_NM = inf.FLD_NM
AND inf.INF_NM = '{1}'
where tbl_nm = '{2}'
ORDER BY inf.COL_ODR ASC""".format(configSchema,targetObject,tableSearchName)

# COMMAND ----------

# DBTITLE 1,Run above SQL statement and convert into array to loop through
configDf = spark.sql(sqlString)
configDf.cache()

# COMMAND ----------

pkDfString = configDf.filter(configDf.PRIMARY_KEY == True).collect()
configDf.unpersist()

# COMMAND ----------

# DBTITLE 1,Create part of SQL statement and decrypt records
if pkDfString[0][3]:
  selectStatement = 'AES_DECRYPT(' + pkDfString[0][0] + ')'
else:
  selectStatement = pkDfString[0][0]

# COMMAND ----------

# DBTITLE 1,Create dataframe with decrypted data
decryptedDf = spark.sql("""
SELECT '{1}' Name,'{1}' as sObject_Type__c,d as Bulk__c
FROM (
SELECT grp,CONCAT_WS(';', COLLECT_LIST(EXTERNAL_ID)) as d
FROM (
SELECT EXTERNAL_ID, (rn div {7}) + 1 as grp
FROM (
SELECT {0} as EXTERNAL_ID,row_number() over(order by {0} asc) as rn
FROM {2}.{3}
WHERE DATA_DTTM = '{4}'
AND ACTN = 'delete'
AND {0} NOT IN
(
SELECT DISTINCT {0}
FROM {2}.{3}
WHERE DATA_DTTM >= TIMESTAMP'{4}'
AND ACTN = 'insert')
)
)
GROUP BY grp
)""".format(selectStatement,targetObject,datamartSchema,sourceTableName,dataDatetime,processName,processRunId,maximumRecordPerRowDelete))

# COMMAND ----------

decryptedDf.cache()

# COMMAND ----------

rowCount = decryptedDf.count()

# COMMAND ----------

numPartition = max(math.ceil(rowCount/maximumRowPerFileDelete),1)

# COMMAND ----------

decryptedDf.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Write files

# COMMAND ----------

# DBTITLE 1,Write file from dataframe to ADLS in csv format
decryptedDf.repartition(numPartition).write.format("csv").mode("overwrite").option("header",'true').option('sep','|').option('encoding','UTF-8').save(filePathFull)
files = dbutils.fs.ls(filePathFull)
csvFile = [x.path for x in files if x.path.endswith('.csv')]
fileCount = 1
for fi in csvFile:
  dbutils.fs.mv(fi,filePathFull.rstrip('/')+ '_' + str(fileCount).zfill(3) + '.csv')
  fileCount += 1
dbutils.fs.rm(filePathFull,recurse = True)

# COMMAND ----------

returnString = return_log(rowCount,'','SUCCESS','')

# COMMAND ----------

dbutils.notebook.exit(returnString)
