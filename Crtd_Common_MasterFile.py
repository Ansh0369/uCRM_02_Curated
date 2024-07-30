# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Prepare input

# COMMAND ----------

# DBTITLE 1,Display input parameters from ADF
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_RN_ID','')
# dbutils.widgets.text('P_SRC_ROW_CNT','')

# COMMAND ----------

# DBTITLE 1,Define functions and import libraries needed
# MAGIC %run ../99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_Parameters

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# DBTITLE 1,Retrieve parameters from ADF
objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))
sourceRowCount = int(dbutils.widgets.get('P_SRC_ROW_CNT'))

# COMMAND ----------

# DBTITLE 1,Convert ADF parameter to dictionary type
returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

tableName = returnDict.get('TGT_TBL_NAME')
dataTimestampString = returnDict.get('DATA_DT')

baseFilePath = adbParam.get('dest_path')
baseFileName = adbParam.get('source_file_name')
baseFilePathAbs = baseFilePath.replace('datalake','dbfs:/mnt/adls_th')
sourceFilePath = baseFilePathAbs.replace('/<Steps>/','/Process/')
dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataDatetime.date()
fileName = baseFileName.replace('<yyyyMMdd>',dataDatetime.strftime('%Y%m%d'))

destPathSuccess = baseFilePathAbs.replace('/<Steps>/','/Done/')
destPathFailed = baseFilePathAbs.replace('/<Steps>/','/Error/')
logPath = baseFilePathAbs.replace('/<Steps>/','/Log/').replace('dbfs:','/dbfs')
logFileName = baseFileName.replace('<yyyyMMdd>',dataDatetime.strftime('%Y%m%d')).split('.')[0] + '_'  + str(processRunId) + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.txt'
  
logHeader = 'Status,Record,Message'

# COMMAND ----------

print(tableName)
print(dataTimestampString)
print(curatedSchema)
print(sourceFilePath)
print(dataDatetime)
print(dataDate)
print(fileName)
print(destPathSuccess)
print(destPathFailed)
print(logPath)
print(logFileName)

# COMMAND ----------

df = spark.read.option('delimiter',',').option('header','True').option('quote','\"').option('escape','\"').csv(sourceFilePath+fileName)
df.cache()
df.createOrReplaceTempView('master_stg')

# COMMAND ----------

queryString = """
select 
meta.TBL_NM
,meta.COL_NM
,meta.DAT_TYPE
,meta.PRIMARY_KEY
,meta.NULLABLE_COL
,meta.ENCRYPT_COL
,meta.COL_ODR
,rules.RULE_QUERY
from {0}.table_metadata meta
left join {0}.table_transformation transform
on meta.TBL_NM = transform.TBL_NM
and meta.COL_NM = transform.COL_NM
left join {0}.transformation_rules_master rules
on transform.RULE_NM = rules.RULE_NM
where meta.TBL_NM = '{1}'
and meta.DAT_LYR = 'Curated'
order by COL_ODR asc
""".format(configSchema,tableName)

# COMMAND ----------

configDf = spark.sql(queryString)
configDf.cache()
configString = configDf.collect()
configDf.unpersist()

# COMMAND ----------

selectStatement = ""
cnt = 0
for rows in configString:
  columnName = rows[1]
  tmpString = columnName
  changeCount = 0
  if rows[5]: 
    tmpString = "AES_DECRYPT({0})".format(tmpString)
    changeCount +=1
  if rows[7]: 
    tmpString = rows[7].format(tmpString)
    changeCount +=1
  if rows[5]: 
    tmpString = "AES_ENCRYPT({0})".format(tmpString)
    changeCount +=1
  if rows[2].upper() != "STRING": 
    tmpString = "CAST({0} AS {1})".format(tmpString,rows[2])
    changeCount +=1
  if cnt > 0:
    tmpString = "," + tmpString
  if changeCount > 0:
    tmpString = tmpString + ' ' + columnName
  selectStatement = selectStatement + tmpString
  cnt = cnt + 1

# COMMAND ----------

df2 = spark.sql("""
SELECT {0}
FROM master_stg
""".format(selectStatement))
df2.cache()

# COMMAND ----------

df3 = df2.withColumn("DATA_DTTM",lit(dataDatetime)) \
                     .withColumn("DATA_DT",lit(dataDate)) \
                     .withColumn("PRCS_NM",lit(processName)) \
                     .withColumn("PRCS_RN_ID",lit(processRunId))
df3.cache()

# COMMAND ----------

df3.count()

# COMMAND ----------

runStatus = 'FAILED'
if df3.count() == sourceRowCount:
  df3.write.insertInto("{0}.{1}".format(curatedSchema,tableName),overwrite = True)
  spark.sql("""optimize {0}.{1}""".format(curatedSchema,tableName))
  spark.sql("""VACUUM {0}.{1} RETAIN {2} HOURS""".format(curatedSchema,tableName,numHourRetention))
  runStatus = 'SUCCESS'
  errorMessage = ''
elif df3.count() != sourceRowCount:
  errorMessage = 'Error on processing records, expected {} row(s) but only have {} row(s)'.format(sourceRowCount,df3.count())
  sourceRowCount = 0
else:
  errorMessage = 'Unexpected Error'

# COMMAND ----------

df.unpersist()
df2.unpersist()
df3.unpersist()

# COMMAND ----------

targetCount = spark.read.table('{0}.{1}'.format(curatedSchema,tableName)).count()

# COMMAND ----------

if runStatus:
  dbutils.fs.mv(sourceFilePath+fileName,destPathSuccess+fileName)
  logBody = '{},{},{}'.format(runStatus,targetCount,errorMessage)
else:
  dbutils.fs.mv(sourceFilePath+fileName,destPathFailed+fileName)
  logBody = '{},{},{}'.format(runStatus,0,errorMessage)

# COMMAND ----------

with open(logPath + logFileName,'w') as f:
  f.write(logHeader)
  f.write('\n')
  f.write(logBody)

# COMMAND ----------

returnString = return_log(sourceRowCount,targetCount,'SUCCESS','')

# COMMAND ----------

dbutils.notebook.exit(returnString)
