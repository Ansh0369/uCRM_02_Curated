# Databricks notebook source
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_RN_ID','')

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_SQLConnections

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_Parameters

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_UDF

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

sourceSqlSchema = returnDict.get('SRC_DB_NAME')
sourceTableName = returnDict.get('SRC_TBL_NAME')
targetSqlSchema = returnDict.get('TGT_DB_NAME')
targetTableName = returnDict.get('TGT_TBL_NAME')
partitionColumn = adbParam.get('pk_column')
dataTimestampString = returnDict.get('DATA_DT')


dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDate = dataTimestampString[0:10]

# COMMAND ----------

maxDate = call_sp_v2("""select coalesce(max(System_ChangeDate),0) Max_SystemChangeDate FROM {0}.{1}""".format(targetSqlSchema,targetTableName))

# COMMAND ----------

latestChangeDate = maxDate[0].get('Max_SystemChangeDate')
latestChangeDate = int(latestChangeDate)
if latestChangeDate == 0:
  initialFlag = True
else:
  initialFlag = False
print(initialFlag)

# COMMAND ----------

if partitionColumn == 'Rownum':
  if initialFlag:
    resultList = call_sp_v2("""select count(*) as cnt_val from {1}.{2}""".format(partitionColumn,sourceSqlSchema,sourceTableName))
  else:
    resultList = call_sp_v2("""select count(*) as cnt_val from {1}.{2} where System_ChangeDate >= {3}""".format(partitionColumn,sourceSqlSchema,sourceTableName,latestChangeDate))
else:
  if initialFlag:
    resultList = call_sp_v2("""select count(*) as cnt_val,min({0}) as min_val,max({0}) as max_val from {1}.{2}""".format(partitionColumn,sourceSqlSchema,sourceTableName))
  else:
    resultList = call_sp_v2("""select count(*) as cnt_val,min({0}) as min_val,max({0}) as max_val from {1}.{2} where System_ChangeDate >= {3}""".format(partitionColumn,sourceSqlSchema,sourceTableName,latestChangeDate))

# COMMAND ----------

sourceCount = resultList[0].get('cnt_val')
minPartitionValue = resultList[0].get('min_val')
maxPartitionValue = resultList[0].get('max_val')
print(sourceCount)
print(minPartitionValue)
print(maxPartitionValue)

# COMMAND ----------

numPartitionCount = min(math.ceil(int(sourceCount)/partitionSize),maxPartitionCount)

# COMMAND ----------

if partitionColumn == 'Rownum':
  if initialFlag:
    sourceReader = spark.read\
    .format('jdbc')\
    .option('url',jdbcUrl)\
    .option('dbtable','(select *,row_number() over(order by System_ChangeDate asc) as Rownum from {0}.{1}) a'.format(sourceSqlSchema,sourceTableName))\
    .option('isolationLevel','READ_UNCOMMITTED')\
    .option('partitionColumn','Rownum'.format(partitionColumn))\
    .option('lowerBound',1)\
    .option('upperBound',sourceCount)\
    .option('numPartitions',numPartitionCount)
  else:
    sourceReader = spark.read\
    .format('jdbc')\
    .option('url',jdbcUrl)\
    .option('dbtable','(select *,row_number() over(order by System_ChangeDate asc) as Rownum from {0}.{1} where System_ChangeDate >= {2}) a'.format(sourceSqlSchema,sourceTableName,latestChangeDate))\
    .option('isolationLevel','READ_UNCOMMITTED')\
    .option('partitionColumn','Rownum'.format(partitionColumn))\
    .option('lowerBound',1)\
    .option('upperBound',sourceCount)\
    .option('numPartitions',numPartitionCount)
else:
  if initialFlag:
    sourceReader = spark.read\
    .format('jdbc')\
    .option('url',jdbcUrl)\
    .option('dbtable','{0}.{1}'.format(sourceSqlSchema,sourceTableName))\
    .option('isolationLevel','READ_UNCOMMITTED')\
    .option('partitionColumn','{0}'.format(partitionColumn))\
    .option('lowerBound',minPartitionValue)\
    .option('upperBound',maxPartitionValue)\
    .option('numPartitions',numPartitionCount)
  else:
    sourceReader = spark.read\
    .format('jdbc')\
    .option('url',jdbcUrl)\
    .option('dbtable','(select * from {0}.{1} where System_ChangeDate >= {2}) a'.format(sourceSqlSchema,sourceTableName,latestChangeDate))\
    .option('isolationLevel','READ_UNCOMMITTED')\
    .option('partitionColumn','{0}'.format(partitionColumn))\
    .option('lowerBound',minPartitionValue)\
    .option('upperBound',maxPartitionValue)\
    .option('numPartitions',numPartitionCount)

# COMMAND ----------

sourceDf = sourceReader.load().cache()
sourceDf.createOrReplaceTempView('test_load')

# COMMAND ----------

sqlString = "select * from {0}.table_metadata_sql where TBL_NM = '{1}' and DAT_LYR = 'Staging_SQL' order by col_odr asc".format(configSchema,targetTableName)

# COMMAND ----------

configString = spark.sql(sqlString).collect()

# COMMAND ----------

selectStatementStringStaging = ""
cnt = 0
for rows in configString:
  columnName = rows[2]
  if rows[8] == '':
    tmpString = columnName
  else: 
    tmpString = rows[8]
  changeCount = 0
  if rows[8] != ''and rows[9] != '' and rows[6] == True and rows[7] == True:
    tmpString = """AES_ENCRYPT(SUBSTRING(AES_DECRYPT({0}),{1}))""".format(tmpString,rows[9])
    changeCount +=1
  elif rows[8] != ''and rows[9] == '' and rows[6] == False and rows[7] == True:
    tmpString = """AES_ENCRYPT({0})""".format(tmpString)
    changeCount +=1
  if changeCount > 0:
    tmpString = """{0} AS {1}""".format(tmpString,columnName)
  if cnt > 0:
    tmpString = "," + tmpString
  selectStatementStringStaging = selectStatementStringStaging + tmpString
  cnt = cnt + 1

selectStatementStringStaging = selectStatementStringStaging + ",'{0}' AS DATA_DTTM,'{1}' AS DATA_DT,'{2}' AS PRCS_NM,{3} AS PRCS_RN_ID".format(dataDatetime,dataDate,processName,processRunId)

# COMMAND ----------

selectStatementStringStaging

# COMMAND ----------

writeDf = spark.sql("""select {0} from test_load""".format(selectStatementStringStaging))
writeDf.cache()

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_SQLConnections

# COMMAND ----------

if initialFlag:
  writeDf.write.mode('overwrite').saveAsTable('{0}.{1}'.format(configSchema,targetTableName))
  recCount = spark.read.table('{0}.{1}'.format(configSchema,targetTableName)).count()
  batchNum = math.ceil(recCount/partitionSize)+1
  for num in range(1,batchNum):
    driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager
    con = driver_manager.getConnection(jdbcUrl)
    tmpdf = spark.sql('select * from (select *,row_number  () over(order by id_key asc) as rn from {0}.{1}) where rn between ({2}*({3}-1) + 1) and ({2}*({3}))'.format(configSchema,targetTableName,partitionSize,num)).drop('rn')
    tmpdf.write \
      .format("jdbc") \
      .option("url", jdbcUrl)  \
      .option("dbTable", '{0}.{1}'.format(targetSqlSchema,targetTableName)) \
      .mode("append") \
      .save()
  spark.sql('drop table {0}.{1}'.format(configSchema,targetTableName))
else:
  writeDf.write \
    .format("jdbc") \
    .option("url", jdbcUrl)  \
    .option("dbTable", '{0}.{1}_STG'.format(targetSqlSchema,targetTableName)) \
    .option("truncate", True) \
    .mode ("overwrite") \
    .save()

# COMMAND ----------

selectStatementString = ""
cnt = 0
for rows in configString:
  columnName = rows[2]
  tmpString = columnName
  if cnt > 0:
    tmpString = "," + tmpString
  selectStatementString = selectStatementString + tmpString
  cnt = cnt + 1

selectStatementString = selectStatementString + ",'{0}','{1}','{2}',{3}".format(dataDatetime,dataDate,processName,processRunId)

# COMMAND ----------

selectStatementString

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_SQLConnections

# COMMAND ----------

if initialFlag == False:
  call_sp_norow("""DELETE from {0}.{1} WHERE {2} IN (SELECT {2} FROM {0}.{1}_STG)""".format(targetSqlSchema,targetTableName,partitionColumn))

# COMMAND ----------

# MAGIC %run ../99_Configuration/UCRM_SQLConnections

# COMMAND ----------

if initialFlag == False:
  call_sp_norow("""INSERT INTO {0}.{1} SELECT {2} FROM {0}.{1}_STG""".format(targetSqlSchema,targetTableName,selectStatementString))

# COMMAND ----------

result = call_sp_v2("""SELECT count(*) as cnt FROM {0}.{1} WHERE System_ChangeDate >= {2}""".format(targetSqlSchema,targetTableName,latestChangeDate))

# COMMAND ----------

targetCount = result[0].get('cnt')

# COMMAND ----------

sourceDf.unpersist()
writeDf.unpersist()

# COMMAND ----------

returnString = return_log(sourceCount,targetCount,'SUCCESS','')

# COMMAND ----------

dbutils.notebook.exit(returnString)
