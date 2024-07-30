# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Prepare input

# COMMAND ----------

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
tableName = returnDict.get('TGT_TBL_NAME')
dataTimestampString = returnDict.get('DATA_DT')

sqlFilePath = adbParam.get('sql_file_path')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataDatetime.date()

# COMMAND ----------

# DBTITLE 1,Print parameters for debugging
print(tableName)
print(dataTimestampString)
print(curatedSchema)
print(consolidatedSchema)
print(configSchema)
print(sqlFilePath)
print(dataDatetime)
print(dataDate)

# COMMAND ----------

# DBTITLE 1,Get SQL statement from file
selectStatement = get_sql_cnsl(sqlFilePath)

# COMMAND ----------

selectStatement

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Execute statement

# COMMAND ----------

# DBTITLE 1,Execute retrieve statement and store into dataframe
df = spark.sql(selectStatement.format(TH_UCRM_CRTD=curatedSchema,TH_UCRM_CNSL=consolidatedSchema,TH_UCRM_DMRT=datamartSchema,th_ucrm_cnfg = configSchema))
df.cache()

# COMMAND ----------

# DBTITLE 1,Add control column to dataframe
dfControl = df.withColumn("DATA_DTTM",lit(dataDatetime)) \
            .withColumn("DATA_DT",lit(dataDate)) \
            .withColumn("PRCS_NM",lit(processName)) \
            .withColumn("PRCS_RN_ID",lit(processRunId).cast('int'))
dfControl.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Write result

# COMMAND ----------

spark.sql('truncate table {0}.{1}'.format(consolidatedSchema,tableName))

# COMMAND ----------

# DBTITLE 1,Overwrite result into target delta table
dfControl.write.insertInto("{0}.{1}".format(consolidatedSchema,tableName))

# COMMAND ----------

spark.sql("""optimize {0}.{1}""".format(consolidatedSchema,tableName))
spark.sql("""VACUUM {0}.{1} RETAIN {2} HOURS""".format(consolidatedSchema,tableName,numHourRetention))

# COMMAND ----------

df.unpersist()
dfControl.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Monitoring and logging

# COMMAND ----------

# DBTITLE 1,Display run result for debugging purpose
targetCount = spark.read.table('{0}.{1}'.format(consolidatedSchema,tableName)).count()

# COMMAND ----------

# DBTITLE 1,Prepare logging result
returnString = return_log('',targetCount,'SUCCESS','')

# COMMAND ----------

# DBTITLE 1,Exit notebook and return result for logging
dbutils.notebook.exit(returnString)
