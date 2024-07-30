# Databricks notebook source
# DBTITLE 1,Other Environment
 configSchema = 'th_ucrm_cnfg'
structuredSchema = 'th_ucrm_strc'
curatedSchema = 'th_ucrm_crtd'
consolidatedSchema = 'th_ucrm_cnsl'
datamartSchema = 'th_ucrm_dmrt'

sourceSystemList = ['LA','IL','GA','LSP']
baseConfigPath = 'dbfs:/mnt/adls_th/TH/FWD/UCRM'
numHourRetention = 360
 
sourceSqlSchema = 'dbo'
maxPartitionCount = 6
partitionSize = 1000000
targetSqlSchema = 'UCRM'

tmpFilePath = 'dbfs:/mnt/adls_th/TH/FWD/UCRM/Working/TempCSVFiles/'
maximumRowPerFileUpsert = 100000
maximumRowPerFileDelete = 50
maximumRecordPerRowDelete = 200
tmpFilePath = 'dbfs:/mnt/adls_th/TH/FWD/UCRM/Working/TempCSVFiles/'
