# Databricks notebook source
# dbutils.widgets.text('P_PRCS_NM','')
# dbutils.widgets.text('P_PRCS_RN_ID','')
# dbutils.widgets.text('P_PRCS_OBJ','')
# dbutils.widgets.text('P_SRC_ROW_CNT','')

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_UDF

# COMMAND ----------

# MAGIC %run ../UCRM/99_Configuration/UCRM_Parameters

# COMMAND ----------

objectString = dbutils.widgets.get('P_PRCS_OBJ')
processName = dbutils.widgets.get('P_PRCS_NM')
processRunId = int(dbutils.widgets.get('P_PRCS_RN_ID'))
sourceRowCount = int(dbutils.widgets.get('P_SRC_ROW_CNT'))

# COMMAND ----------

returnDict,adbParam = extract_adf_object(objectString)

# COMMAND ----------

sourceObjectName = returnDict.get('SRC_TBL_NAME')
sourceFilePath = tmpFilePath + sourceObjectName + '/'
targetFilePath = adbParam.get('dest_path').replace('datalake/','/dbfs/mnt/adls_th/')
sourceFileName = sourceObjectName + '.csv'
dataTimestampString = returnDict.get('DATA_DT')

time = datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDatetime = time.strftime("%Y%m%d%H%M%S")

targetFileName = sourceObjectName + '_Encrypted_' + dataDatetime + '_' + str(processRunId).zfill(5) + '.csv'

# COMMAND ----------

key = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-01-achv")

# COMMAND ----------

fullSourceFilePath = sourceFilePath + sourceFileName
fullTargetFilePath = targetFilePath + targetFileName
df = spark.read.csv(fullSourceFilePath,header = True,multiLine = True,sep = ",")
targetRowCount = df.select('Id').where('length(Id) == 18').count()
print(sourceRowCount)
print(targetRowCount)

# COMMAND ----------

f = Fernet(key)
time = datetime.strptime(dataTimestampString,'%Y-%m-%d %H:%M:%S')
dataDatetime = time.strftime("%Y%m%d%H%M%S")

if int(targetRowCount) == int(sourceRowCount):
  with open(fullSourceFilePath.replace('dbfs:/','/dbfs/'), 'rb') as original_file:
      original = original_file.read()
  encrypted = f.encrypt(original)
  
  with open (fullTargetFilePath, 'wb') as encrypted_file:     
      encrypted_file.write(encrypted)
  print("File encrypted.")    
  returnString = return_log(sourceRowCount,targetRowCount,'SUCCESS','')
else:
  print("Error, Data not equal.")
  returnString = return_log(sourceRowCount,0,'FAILED','')
if os.path.isfile(fullSourceFilePath.replace('dbfs:/','/dbfs/')):
  os.remove(fullSourceFilePath.replace('dbfs:/','/dbfs/'))
  print("Source file has been deleted.")
else:
  print("Source file does not exist.")

# COMMAND ----------

dbutils.notebook.exit(returnString)
