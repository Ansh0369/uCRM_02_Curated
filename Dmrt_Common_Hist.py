# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Prepare input

# COMMAND ----------

# DBTITLE 1,Display input parameters from ADF
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_NM','')
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
tableSearchName = get_table_search_name(tableName,'_')

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(consolidatedSchema)
print(sourceTableName)
print(datamartSchema)
print(tableName)
print(dataTimestampString)
print(configSchema)
print(dataDatetime)
print(dataDate)
print(tableSearchName)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Prepare merge statement

# COMMAND ----------

# DBTITLE 1,Create SQL statement for target table metadata
sqlString = "select * from {0}.table_metadata where TBL_NM = '{1}' order by col_odr asc".format(configSchema,tableSearchName)

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
insertColumnString = insertColumnString + ",START_DTTM,ACTV_FLG,DATA_DTTM,DATA_DT,PRCS_NM,PRCS_RN_ID"

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
insertValueString = insertValueString + ",'{0}',{1},'{2}','{3}','{4}',{5}".format(dataDatetime,0,dataDatetime,dataDate,processName,processRunId)

# COMMAND ----------

subSelectStatement1 = ""
cnt = 0

for rows in configString:
  columnName = rows[2]
  tmpString = "coalesce(new.{colName},old.{colName}) as {colName}".format(colName = columnName)
  changeCount = 0
  if cnt > 0:
    tmpString = "," + tmpString
  subSelectStatement1 = subSelectStatement1 + tmpString
  cnt = cnt + 1

# COMMAND ----------

subSelectStatement2 = ""
cnt = 0

for rows in configString:
  columnName = rows[2]
  tmpString = tmpString = "new." + columnName
  changeCount = 0
  if cnt > 0:
    tmpString = "," + tmpString
  subSelectStatement2 = subSelectStatement2 + tmpString
  cnt = cnt + 1

# COMMAND ----------

pkDfConfig = configDf.filter(configDf.PRIMARY_KEY == True).collect()
valDfConfig = configDf.filter(configDf.PRIMARY_KEY == False).collect()
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

valConcat = ""
cnt = 0

for rows in valDfConfig:
  columnName = rows[2]
  tmpString = "COALESCE(CAST(" + columnName + " AS STRING),'')"
  if cnt > 0:
    tmpString = "||" + tmpString
  valConcat = valConcat + tmpString
  cnt += 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Execute merge

# COMMAND ----------

# DBTITLE 1,Run merge command to merge data between latest run result and current record on existing datamart history table, applying SCD type 4 in process
runDf = spark.sql("""merge into {datamartSchema}.{tableName} original_rec
USING(
select coalesce(new.pkhash,old.pkhash) as mergeKey,{subSelectStatement1}
from 
(
select md5({pkConcat}) pkhash,md5({valConcat}) valhash, a.*
from {consolidatedSchema}.{sourceTableName} a
) new
full outer join
(
select md5({pkConcat}) pkhash,md5({valConcat}) valhash, a.*
from {datamartSchema}.{tableName} a
where ACTV_FLG = 0
) old
on new.pkhash = old.pkhash
where old.valhash <> coalesce(new.valhash,'')
union all
select null as mergeKey,{subSelectStatement2}
from 
(
select md5({pkConcat}) pkhash,md5({valConcat}) valhash, a.*
from {consolidatedSchema}.{sourceTableName} a
) new
left join
(
select md5({pkConcat}) pkhash,md5({valConcat}) valhash, a.*
from {datamartSchema}.{tableName} a
where ACTV_FLG = 0
) old
on new.pkhash = old.pkhash
where new.valhash <> coalesce(old.valhash,'')
) updates
ON md5({pkMergeConcat}) = updates.mergeKey 
WHEN MATCHED and original_rec.ACTV_FLG = 0
THEN UPDATE SET original_rec.END_DTTM = '{dataDatetime}', original_rec.ACTV_FLG = 1, original_rec.UPDT_DATA_DTTM = '{dataDatetime}',original_rec.UPDT_PRCS_NM = '{processName}',original_rec.UPDT_PRCS_RN_ID = {processRunId} 
WHEN NOT MATCHED 
THEN INSERT({insertColumnString}) VALUES({insertValueString})
""".format(datamartSchema = datamartSchema,tableName = tableName,subSelectStatement1 = subSelectStatement1,subSelectStatement2 = subSelectStatement2, consolidatedSchema = consolidatedSchema,sourceTableName = sourceTableName,pkConcat = pkConcat,valConcat = valConcat,pkMergeConcat = pkMergeConcat,dataDatetime = dataDatetime,processName = processName,processRunId = processRunId,insertColumnString = insertColumnString,insertValueString = insertValueString))

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

sourceCount = spark.read.table('{0}.{1}'.format(consolidatedSchema,sourceTableName)).count()
targetCount = spark.read.table('{0}.{1}'.format(datamartSchema,tableName)).filter('ACTV_FLG == 0').count()

# COMMAND ----------

# DBTITLE 1,Prepare logging result
returnString = return_log(sourceCount,targetCount,'SUCCESS','')

# COMMAND ----------

# DBTITLE 1,Exit notebook and return result for logging
dbutils.notebook.exit(returnString)
