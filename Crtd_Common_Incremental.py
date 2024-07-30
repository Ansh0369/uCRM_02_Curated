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

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# DBTITLE 1,Retrieve parameters from ADF
objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))

# COMMAND ----------

# DBTITLE 1,Convert ADF parameter to dictionary type
returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

# DBTITLE 1,Retrieve configurations
sourceSystem = returnDict.get('SYSTEM_NAME')
sourceTableName = returnDict.get('SRC_TBL_NAME')
tableName = returnDict.get('TGT_TBL_NAME')
dataTimestampString = returnDict.get('DATA_DT')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataDatetime.date()

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(sourceSystem)
print(structuredSchema)
print(sourceTableName)
print(curatedSchema)
print(tableName)
print(dataTimestampString)
print(configSchema)
print(dataDatetime)
print(dataDate)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Prepare latest record view from Structured zone 

# COMMAND ----------

# DBTITLE 1,Create SQL statement for selecting target table metadata and transformation rules applied
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

# DBTITLE 1,Run above SQL statement and convert into array to loop through
configDf = spark.sql(queryString)
configDf.cache()
configString = configDf.collect()
pkDfConfig = configDf.filter(configDf.PRIMARY_KEY == True).collect()
configDf.unpersist()

# COMMAND ----------

pkConcatCurated = ""
cnt = 0

for rows in pkDfConfig:
  columnName = rows[1]
  tmpString = "CAST(crtd." + columnName + " AS STRING)"
  if cnt > 0:
    tmpString = "||" + tmpString
  pkConcatCurated = pkConcatCurated + tmpString
  cnt += 1

pkConcatCurated

# COMMAND ----------

pkConcatStg = ""
cnt = 0

for rows in pkDfConfig:
  columnName = rows[1]
  tmpString = "CAST(stg." + columnName + " AS STRING)"
  if cnt > 0:
    tmpString = "||" + tmpString
  pkConcatStg = pkConcatStg + tmpString
  cnt += 1

pkConcatStg

# COMMAND ----------

# DBTITLE 1,Create table select statement from metadata
selectStatement = ""
cnt = 0
for rows in configString:
  columnName = rows[1]
  tmpString = columnName
  changeCount = 0
  if rows[5] and rows[7]: 
    tmpString = "AES_DECRYPT({0})".format(tmpString)
    changeCount +=1
  if rows[7]: 
    tmpString = rows[7].format(tmpString)
    changeCount +=1
  if rows[5] and rows[7]: 
    tmpString = "AES_ENCRYPT(COALESCE({0},''))".format(tmpString)
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

# DBTITLE 1,Create SQL statement to identify and latest changes made to record from CDC logic
fullSqlString = """
select {0}
from {1}.{2}
""".format(selectStatement,structuredSchema,sourceTableName)

# COMMAND ----------

fullSqlString

# COMMAND ----------

resultDf = spark.sql(fullSqlString)
resultDf2 = resultDf.withColumn("START_DTTM",lit(dataDatetime)) \
          .withColumn("ACTV_FLG",lit(0)) \
          .withColumn("DATA_DT",lit(dataDate)) \
          .withColumn("DATA_DTTM",lit(dataDatetime)) \
          .withColumn("PRCS_NM",lit(processName)) \
          .withColumn("PRCS_RN_ID",lit(processRunId)) \
          .withColumn("END_DTTM",lit(None)) \
          .withColumn("UPDT_DATA_DTTM",lit(None)) \
          .withColumn("UPDT_PRCS_NM",lit(None)) \
          .withColumn("UPDT_PRCS_RN_ID",lit(None)) \
          .withColumn("__op",lit(None)) \
          .withColumn("__ts_ms",lit(None)) \
          .withColumn("__source_table",lit(None)) \
          .withColumn("__deleted",lit(None))
if "VALIDFLAG" in resultDf2.columns:
  resultDf3 = resultDf2.withColumn('REC_VLD_FLG',f.when(f.col('VALIDFLAG') != 2,0).otherwise(1))
else:
  resultDf3 = resultDf2.withColumn('REC_VLD_FLG',lit(0))

# COMMAND ----------

resultDf3.cache()
resultDf3.createOrReplaceTempView('{0}_crtd_stg'.format(tableName))

# COMMAND ----------

runDf = spark.sql("""
merge into {0}.{1} crtd using
(
select {2} as mergeKey,stg.*
from {1}_crtd_stg stg
join {0}.{1} crtd
on {3} = {2}
union all 
select null as mergeKey,stg.*
from {1}_crtd_stg stg
) updates
ON updates.mergeKey = {2}
WHEN MATCHED
THEN
DELETE
WHEN NOT MATCHED 
THEN
INSERT *
""".format(curatedSchema,tableName,pkConcatCurated,pkConcatStg))

# COMMAND ----------

resultDf3.unpersist()

# COMMAND ----------

spark.sql("""optimize {0}.{1}""".format(curatedSchema,tableName))
spark.sql("""VACUUM {0}.{1} RETAIN {2} HOURS""".format(curatedSchema,tableName,numHourRetention))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Monitoring and logging

# COMMAND ----------

display(runDf)

# COMMAND ----------

# DBTITLE 1,Get source and target table record counts
sourceCount = spark.read.table('{0}.{1}'.format(structuredSchema,tableName)).count()
targetCount = spark.read.table('{0}.{1}'.format(curatedSchema,tableName)).count()
print(sourceCount)
print(targetCount)

# COMMAND ----------

# DBTITLE 1,Prepare logging result
returnString = return_log(sourceCount,targetCount,'SUCCESS','')

# COMMAND ----------

# DBTITLE 1,Exit notebook and return result for logging
dbutils.notebook.exit(returnString)
