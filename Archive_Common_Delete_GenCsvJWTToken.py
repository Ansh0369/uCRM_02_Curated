# Databricks notebook source
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_PRCS_RN_ID','')

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_Parameters

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

jwtToken = get_jwt_token()

# COMMAND ----------

dataTimestampString = returnDict.get('DATA_DT')

targetObject = returnDict.get('TGT_TBL_NAME')
sourcePath = adbParam.get('source_path')
retentionPeriod = int(adbParam.get('retention_period'))

dataDatetime =  datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDatetimeString = dataDatetime.strftime("%Y%m%d%H%M%S")
dataDate = dataDatetime.date()
fileName = targetObject + '_Archive_Delete_' + dataDatetimeString
filePathFull = tmpFilePath+targetObject+'/'+fileName
searchString = targetObject + '_Encrypted_' + dataDatetimeString + '*'

key = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-01-achv")

print(dataDatetimeString)
print(filePathFull)

# COMMAND ----------

filteredDate = dataDatetime - relativedelta(months=retentionPeriod)
filteredDateString = filteredDate.strftime("%Y-%m-%d %H:%M:%S")
filteredDateString


# COMMAND ----------

fileList = dbutils.fs.ls(sourcePath)
fileList.sort(reverse = True)
fileList
for file in fileList:
  if fnmatch.fnmatch(file[1], searchString):
    print(file[1])
    filePath = file[0].replace('dbfs:/','/dbfs/')
    break
filePath

# COMMAND ----------

f = Fernet(key)
with open(filePath, 'rb') as encryptedFile:
    encryptedBytes = encryptedFile.read()

decryptedBytes = f.decrypt(encryptedBytes)
decryptedString = str(decryptedBytes,'utf-8')
decryptedData = StringIO(decryptedString)
df = pd.read_csv(decryptedData)

# COMMAND ----------

filteredDf = df[df['CreatedDate'] <= filteredDateString]
idDf = filteredDf[['Id']]
sparkIdDf = spark.createDataFrame(idDf)
sparkIdDf2 = sparkIdDf.select('Id').where('length(Id) == 18')
rowCount = sparkIdDf2.count()

# COMMAND ----------

numPartition = max(math.ceil(rowCount/maximumRowPerFileArchiveDelete),1)
print(rowCount)
print(numPartition)
print(maximumRowPerFileArchiveDelete)

# COMMAND ----------

sparkIdDf2.repartition(numPartition).write.format("csv").mode("overwrite").option("header",'true').option('nullValue','').option('emptyValue','').option('sep','|').option('encoding','UTF-8').save(filePathFull)
files = dbutils.fs.ls(filePathFull)
csvFile = [x.path for x in files if x.path.endswith('.csv')]
fileCount = 0
for fi in csvFile:
  dbutils.fs.mv(fi,filePathFull.rstrip('/')+ '_' + str(fileCount).zfill(3) + '.csv')
  fileCount += 1
dbutils.fs.rm(filePathFull,recurse = True)

# COMMAND ----------

returnString = return_log(rowCount,'','SUCCESS','')
returnString["FILE_CNT"]=str(fileCount)
returnString["JWT_TOKEN"]=jwtToken

# COMMAND ----------

dbutils.notebook.exit(returnString)
