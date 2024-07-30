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
dataDate = dataDatetime.date()
tableSearchName = get_table_search_name(sourceTableName,'_')

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(datamartSchema)
print(sourceTableName)
print(tableName)
print(dataTimestampString)
print(configSchema)
print(dataDatetime)
print(dataDate)
print(tableSearchName)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Prepare statement

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
configDf.unpersist()

# COMMAND ----------

# DBTITLE 1,Create part of SQL statement
selectStatementString = ""
cnt = 0
for rows in configString:
  columnName = rows[2]
  tmpString = columnName
  if cnt > 0:
    tmpString = "," + tmpString
  selectStatementString = selectStatementString + tmpString
  cnt = cnt + 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Execute statement

# COMMAND ----------

# DBTITLE 1,Run select to save data changes needed into dataframe
df= spark.sql(
"""
select {selectStatementString},'insert' as actn
from {datamartSchema}.{sourceTableName}
where start_dttm = TIMESTAMP'{dataDatetime}'
union
select {selectStatementString},'delete' as actn
from {datamartSchema}.{sourceTableName}
where end_dttm = TIMESTAMP'{dataDatetime}'
""".format(selectStatementString = selectStatementString,datamartSchema = datamartSchema,sourceTableName = sourceTableName,dataDatetime=dataDatetime))
df.cache()

# COMMAND ----------

# DBTITLE 1,Add control column to dataframe
dfMod = df.withColumn("DATA_DTTM",lit(dataDatetime)) \
                     .withColumn("DATA_DT",lit(dataDate)) \
                     .withColumn("PRCS_NM",lit(processName)) \
                     .withColumn("PRCS_RN_ID",lit(processRunId))
dfMod.cache()

# COMMAND ----------

# DBTITLE 1,Display result for debugging
# display(dfMod)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Write data

# COMMAND ----------

# DBTITLE 1,Overwrite data from dataframe to target staging table in ADLS
dfMod.write \
  .format("delta") \
  .mode("append") \
  .saveAsTable("""{0}.{1}""".format(datamartSchema,tableName))

# COMMAND ----------

df.unpersist()
dfMod.unpersist()

# COMMAND ----------

spark.sql("""optimize {0}.{1}""".format(datamartSchema,tableName))
spark.sql("""VACUUM {0}.{1} RETAIN {2} HOURS""".format(datamartSchema,tableName,numHourRetention))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Monitoring and logging

# COMMAND ----------

# DBTITLE 1,Prepare statement to check run result
targetCount = spark.read.table('{0}.{1}'.format(datamartSchema,tableName)).count()

# COMMAND ----------

# DBTITLE 1,Prepare logging result
returnString = return_log('',targetCount,'SUCCESS','')

# COMMAND ----------

# DBTITLE 1,Exit notebook and return result for logging
dbutils.notebook.exit(returnString)
