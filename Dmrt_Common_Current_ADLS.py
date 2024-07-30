# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Prepare input

# COMMAND ----------

# DBTITLE 1,Display input parameters from ADF
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_RN_ID','')

# COMMAND ----------

# DBTITLE 1,Define functions and import libraries needed
# MAGIC %run ../99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_Parameters

# COMMAND ----------

# DBTITLE 1,Retrieve parameters from ADF
objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))

# COMMAND ----------

# DBTITLE 1,Convert ADF parameter to dictionary type
returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

# DBTITLE 1,Retrieve converted parameters
sourceTableName = returnDict.get('SRC_TBL_NAME')
tableName = returnDict.get('TGT_TBL_NAME')
dataTimestampString = returnDict.get('DATA_DT')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataTimestampString[0:10]

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(sourceTableName)
print(datamartSchema)
print(tableName)
print(dataTimestampString)
print(configSchema)
print(dataDatetime)
print(dataDate)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Prepare merge statement

# COMMAND ----------

# DBTITLE 1,Create SQL statement for target table metadata
sqlString = "select * from {0}.table_metadata where TBL_NM = '{1}' order by col_odr asc".format(configSchema,tableName)

# COMMAND ----------

# DBTITLE 1,Execute above statement and save to dataframe
configDf = spark.sql(sqlString)
configDf.cache()

# COMMAND ----------

# DBTITLE 1,Convert dataframe into array
configString = configDf.collect()

# COMMAND ----------

# DBTITLE 1,Create part of SQL statement to merge records
insertColumnString = ""
cnt = 0
for rows in configString:
  columnName = rows[2]
  tmpString = columnName
  if cnt > 0:
    tmpString = "," + tmpString
  insertColumnString = insertColumnString + tmpString
  cnt = cnt + 1
insertColumnString = insertColumnString + ",DATA_DTTM,DATA_DT,PRCS_NM,PRCS_RN_ID"

# COMMAND ----------

insertValueString = ""
cnt = 0
for rows in configString:
  columnName = rows[2]
  tmpString = "updates." + columnName
  if cnt > 0:
    tmpString = "," + tmpString
  insertValueString = insertValueString + tmpString
  cnt = cnt + 1
insertValueString = insertValueString + ",'{0}','{1}','{2}',{3}".format(dataDatetime,dataDate,processName,processRunId)

# COMMAND ----------

pkDfConfig = configDf.filter(configDf.PRIMARY_KEY == True).collect()
configDf.unpersist()

# COMMAND ----------

pkConcat = ""
cnt = 0

for rows in pkDfConfig:
  columnName = rows[2]
  tmpString = "CAST(" + columnName + " AS STRING)"
  if cnt > 0:
    tmpString = "||" + tmpString
  pkConcat = pkConcat + tmpString
  cnt += 1

# COMMAND ----------

pkMergeConcat = ""
cnt = 0

for rows in pkDfConfig:
  columnName = rows[2]
  tmpString = "CAST(original_rec." + columnName + " AS STRING)"
  if cnt > 0:
    tmpString = "||" + tmpString
  pkMergeConcat = pkMergeConcat + tmpString
  cnt += 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Execute merge

# COMMAND ----------

# DBTITLE 1,Run merge command to merge data between datamart history table and datamart current table, applying SCD type 4 in process
runDf = spark.sql("""merge into {datamartSchema}.{tableName} original_rec
USING(
select null as mergeKey,a.*
from {datamartSchema}.{sourceTableName} a
where actn = 'insert'
and DATA_DTTM = '{dataDatetime}'
union all
select md5({pkConcat}) as mergeKey,b.*
from {datamartSchema}.{sourceTableName} b
where actn = 'delete'
and DATA_DTTM = '{dataDatetime}'
) updates
ON md5({pkMergeConcat}) = updates.mergeKey
WHEN MATCHED
THEN DELETE
WHEN NOT MATCHED
THEN INSERT({insertColumnString}) VALUES({insertValueString})
""".format(datamartSchema = datamartSchema,tableName = tableName,sourceTableName = sourceTableName,pkConcat = pkConcat,pkMergeConcat = pkMergeConcat,dataDatetime = dataDatetime,processName = processName,processRunId = processRunId,insertColumnString = insertColumnString,insertValueString = insertValueString))

# COMMAND ----------

spark.sql("""optimize {0}.{1}""".format(datamartSchema,tableName))
spark.sql("""VACUUM {0}.{1} RETAIN {2} HOURS""".format(datamartSchema,tableName,numHourRetention))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Monitoring and logging

# COMMAND ----------

# DBTITLE 1,Display run result for debugging
display(runDf)

# COMMAND ----------

targetCount = spark.read.table('{0}.{1}'.format(datamartSchema,tableName)).count()

# COMMAND ----------

# DBTITLE 1,Prepare logging result
returnString = return_log('',targetCount,'SUCCESS','')

# COMMAND ----------

# DBTITLE 1,Exit notebook and return result for logging
dbutils.notebook.exit(returnString)
