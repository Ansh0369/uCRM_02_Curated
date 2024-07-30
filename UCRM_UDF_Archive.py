# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Libraries

# COMMAND ----------

# DBTITLE 1,Import libraries
import csv
import os
import base64
import hashlib
import uuid
import datetime
import ast
import math
import json
import jwt
import pandas as pd
import numpy as np
import fnmatch
import time
from ast import literal_eval
from datetime import datetime
from dateutil.relativedelta import relativedelta
from cryptography.fernet import Fernet
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType,LongType,DateType,FloatType
from pyspark.sql.functions import lit,col
from pyspark.sql import functions as f
from pyspark.sql import Row
from io import StringIO

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #UDF

# COMMAND ----------

def get_sql_cnsl(filePath):
  sqlStatement = ''
  with open('{0}'.format(filePath),'r') as sqlFile:
    for lines in sqlFile:
      if lines.find('--') == -1:
        sqlStatement = sqlStatement + lines
      elif lines.find('--') > 0:
        sqlStatement = sqlStatement + lines[0:lines.index('--')]
  return(sqlStatement)

# COMMAND ----------

def call_sp_norow(sqlStatement):
  exec_statement = con.prepareCall(sqlStatement)
  returnObj = exec_statement.execute()

# COMMAND ----------

def call_sp_singlerow(sqlStatement):
  exec_statement = con.prepareCall(sqlStatement)
  returnObj = exec_statement.executeQuery()
  objMetadata = returnObj.getMetaData()
  
  dictOutput = {}
  
  while returnObj.next():
    col = 1
    while col <= objMetadata.getColumnCount():
      dictOutput[objMetadata.getColumnName(col)] = returnObj.getString(col)
      col += 1
  return(dictOutput)

# COMMAND ----------

def call_sp_multirow(sqlStatement):
  exec_statement = con.prepareCall(statement)
  returnObj = exec_statement.executeQuery()
  objMetadata = returnObj.getMetaData()
  
  dictOutput = {}
  row = 0
  while returnObj.next():
    
    col = 1
    dictOutput['I'+str(row)] = {}
    while col <= objMetadata.getColumnCount():
      dictOutput['I'+str(row)][objMetadata.getColumnName(col)] = returnObj.getString(col)
      col += 1
    row += 1
  return(dictOutput)

# COMMAND ----------

def call_sp_v2(sqlStatement):
  exec_statement = con.prepareCall(sqlStatement)
  returnObj = exec_statement.executeQuery()
  objMetadata = returnObj.getMetaData()

  returnArray = []
  row = 0
  while returnObj.next():
    dctTmp = {}
    col = 1
    while col <= objMetadata.getColumnCount():
      dctTmp[objMetadata.getColumnName(col)] = returnObj.getString(col)
      col += 1
    returnArray.append(dctTmp)
    row += 1
  return(returnArray)

# COMMAND ----------

def extract_adf_object(objectString):
  objectString=objectString.replace(':null',':None')
  returnDict = ast.literal_eval(objectString)
  paramString = returnDict.get("ADB_PARM")
  if paramString:
    returnDict2 = ast.literal_eval(paramString)
    return returnDict,returnDict2
  else:
    return returnDict

# COMMAND ----------

def read_schema_structured(schemaArg):
  sch = StructType()
  for rows in schemaArg:
    sch.add(rows[2],StringType(),rows[5])
  return sch

# COMMAND ----------

def read_schema_curated(schemaArg):
  dTypes = {
    "STRING":StringType(),
    "INTEGER":IntegerType(),
    "TIMESTAMP":TimestampType(),
    "DATE":DateType(),
    "DOUBLE":DoubleType(),
    "LONG":LongType(),
    "FLOAT":FloatType()
  }
  
  sch = StructType()
  for rows in schemaArg:
    sch.add(rows[2],dTypes[rows[4].upper()],rows[6])
  return sch

# COMMAND ----------

def get_table_search_name(inputString,searchString):
  n = inputString.count(searchString)
  if inputString[-3:] == 'SQL':
    n -= 1
  start = inputString.find(searchString)
  while start >=0 and n >1:
    start = inputString.find(searchString,start+len(searchString))
    n -= 1
  return inputString[0:start]

# COMMAND ----------

def return_log(sourceRecordCount,targetRecordCount,logStatus,logRemark):
  returnDict = {
    'SRC_REC_CNT':'{}'.format(sourceRecordCount),
    'TGT_REC_CNT':'{}'.format(targetRecordCount),
    'STATUS':'{}'.format(logStatus),
    'REMARK':'{}'.format(logRemark)
  }
  return returnDict

# COMMAND ----------

def generate_uuid_from_string(inputString):
  
  hashString = str(inputString) + '209157'
  
  md5Hash = hashlib.md5()
  
  md5Hash.update(hashString.encode("utf-8"))
  
  md5HexString = md5Hash.hexdigest()
  
  return str(uuid.UUID(md5HexString)).replace('-','')

a = spark.udf.register("gen_uuid_from_string", generate_uuid_from_string)

# COMMAND ----------

def run_notebook_retry(notebook, timeout, args = {}, maxRetries = 1):
  numRetries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if numRetries >= maxRetries:
        return "{'STATUS':'FAILED'}"
      else:
        print('Retrying error', e)
        numRetries += 1

# COMMAND ----------

def get_jwt_token():
  username = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-sit-01-user")
  consumerkey = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-sit-01-consumerkey")
  url = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-sit-01-url")
  privatekey = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "adf-ucrm-th-sf-sit-01-privatekey")
  
  privateKeyDecoded = bytes(privatekey,'utf-8').decode('unicode_escape')
  
  payload ={
    'iss':consumerkey,
    'sub':username,
    'aud':url,
    'exp':int(time.time())+3600
  }
  
  encoded = jwt.encode(payload,privateKeyDecoded,algorithm = 'RS256',headers = {'typ':None})
  
  return(encoded)

# COMMAND ----------

print('Import function successfully!')
