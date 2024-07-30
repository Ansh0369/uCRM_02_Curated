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
dataTimestampString = returnDict.get('DATA_DT')
targetObject = returnDict.get('TGT_TBL_NAME')

orderCol = adbParam.get('ordered_column')

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDatetimeString = dataDatetime.strftime("%Y%m%d%H%M%S")
tableSearchName = get_table_search_name(sourceTableName,'_')
fileName = targetObject + '_Upsert_' + dataDatetimeString
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
,inf.REL_FLD_FLG
,inf.DAT_TYPE
,inf.COL_LEN
,inf.COL_ODR
,recid.QUERY
from {0}.table_metadata meta
JOIN {0}.interface_mapping inf
ON meta.COL_NM = inf.FLD_NM
AND inf.INF_NM = '{1}'
LEFT JOIN {0}.interface_recordtypeid recid
on inf.api_fld_nm = recid.api_fld_nm
and recid.inf_nm = '{1}'
where tbl_nm = '{2}'
ORDER BY inf.COL_ODR ASC""".format(configSchema,targetObject,tableSearchName)

# COMMAND ----------

# DBTITLE 1,Run above SQL statement and convert into array to loop through
configDf = spark.sql(sqlString)
configDf.cache()

# COMMAND ----------

configString = configDf.collect()
configDf.unpersist()

# COMMAND ----------

# DBTITLE 1,Create part of SQL statement and decrypt records
selectStatement = ""
cnt = 0
for rows in configString:
  columnName = rows[0]
  columnAPIName = '`' + rows[1] + '`'
  tmpString = columnName
  changeCount = 0
  if rows[3]:
    tmpString = "AES_DECRYPT({0})".format(tmpString)
  if rows[8] != None:
    tmpString = rows[8].format(tmpString)
  elif rows[5].upper() == 'VARCHAR':
    tmpString = 'Left({0},{1})'.format(tmpString,rows[6])
  elif rows[5].upper() == 'DECIMAL':
    tmpString = 'CAST({0} AS DECIMAL({1}))'.format(tmpString,rows[6])
  elif rows[5].upper() == 'DATE':
    #tmpString = """DATE_FORMAT({0},'yyyy-MM-dd')""".format(tmpString)   
    #Add Logic to handle out of bound date from Salesforce (Outside 1700-01-01 and 4000-12-31)
    tmpString = """CASE WHEN EXTRACT(YEAR FROM DATE_FORMAT({0},'yyyy-MM-dd')) < 1700 THEN '1700-12-01' WHEN EXTRACT(YEAR FROM DATE_FORMAT({0},'yyyy-MM-dd')) > 4000 THEN '4000-12-01' ELSE DATE_FORMAT({0},'yyyy-MM-dd') END""".format(tmpString)
  elif rows[5].upper() == 'DATETIME':
    #tmpString = """date_format({0},'yyyy-MM-dd') || "T" || date_format({0},'HH:mm:ss') || '+07:00'""".format(tmpString)
    #Add Logic to handle out of bound date from Salesforce (Outside 1700-01-01 and 4000-12-31)
    tmpString = """CASE WHEN EXTRACT(YEAR FROM DATE_FORMAT({0},'yyyy-MM-dd')) < 1700 THEN '1700-12-01T00:00:00+07:00' WHEN EXTRACT(YEAR FROM DATE_FORMAT({0},'yyyy-MM-dd')) > 4000 THEN '4000-12-01T00:00:00+07:00' ELSE date_format({0},'yyyy-MM-dd') || "T" || date_format({0},'HH:mm:ss') || '+07:00' END""".format(tmpString)
  if rows[4] and rows[1].find('.') == -1:
    tmpString = """CASE WHEN {0} IS NOT NULL THEN NULL ELSE '#N/A' END""".format(tmpString)
  elif rows[4] == False:
     tmpString = """COALESCE({0},'#N/A')""".format(tmpString)
  if rows[3]:
    tmpString = """CASE WHEN {0} = 'N/A' THEN '#N/A' ELSE {0} END""".format(tmpString)
  if cnt > 0:
    tmpString = "," + tmpString
  tmpString = tmpString + ' ' + columnAPIName
  selectStatement = selectStatement + tmpString
  cnt = cnt + 1

# COMMAND ----------

selectStatement

# COMMAND ----------

# DBTITLE 1,Create dataframe with decrypted data
decryptedDf = spark.sql("""SELECT DISTINCT {0} FROM {1}.{2} WHERE DATA_DTTM = '{3}' and ACTN = 'insert'""".format(selectStatement,datamartSchema,sourceTableName,dataDatetime))
decryptedDf.cache()

# COMMAND ----------

"""SELECT {0} FROM {1}.{2} WHERE DATA_DTTM = '{3}' and ACTN = 'insert'""".format(selectStatement,datamartSchema,sourceTableName,dataDatetime)

# COMMAND ----------

rowCount = decryptedDf.count()

# COMMAND ----------

numPartition = max(math.ceil(rowCount/maximumRowPerFileUpsert),1)

# COMMAND ----------

decryptedDf.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Write files

# COMMAND ----------

# DBTITLE 1,Write file from dataframe to ADLS in csv format
if orderCol == '' or orderCol == None:
  decryptedDf.repartition(numPartition).write.format("csv").mode("overwrite").option("header",'true').option('nullValue','').option('emptyValue','').option('sep','|').option('encoding','UTF-8').save(filePathFull)
else:
    decryptedDf.repartition(numPartition).sortWithinPartitions('`{0}`'.format(orderCol)).write.format("csv").mode("overwrite").option("header",'true').option('nullValue','').option('emptyValue','').option('sep','|').option('encoding','UTF-8').save(filePathFull)
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
