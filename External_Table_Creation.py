# Databricks notebook source
# DBTITLE 1,Create Stage Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS TH_STAGE;

# COMMAND ----------

# DBTITLE 1,Create Table CLNTPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_CLNTPF;
# MAGIC CREATE TABLE STG_GA_CLNTPF (
# MAGIC `ID_Key` string,
# MAGIC `CLNTPFX` string,
# MAGIC `CLNTCOY` string,
# MAGIC `CLNTNUM` string,
# MAGIC `TRANID` string,
# MAGIC `VALIDFLAG` string,
# MAGIC `CLTTYPE` string,
# MAGIC `SECUITYNO` string,
# MAGIC `PAYROLLNO` string,
# MAGIC `SURNAME` string,
# MAGIC `GIVNAME` string,
# MAGIC `SALUT` string,
# MAGIC `INITIALS` string,
# MAGIC `CLTSEX` string,
# MAGIC `CLTADDR01` string,
# MAGIC `CLTADDR02` string,
# MAGIC `CLTADDR03` string,
# MAGIC `CLTADDR04` string,
# MAGIC `CLTADDR05` string,
# MAGIC `CLTPCODE` string,
# MAGIC `CTRYCODE` string,
# MAGIC `MAILING` string,
# MAGIC `DIRMAIL` string,
# MAGIC `ADDRTYPE` string,
# MAGIC `CLTPHONE01` string,
# MAGIC `CLTPHONE02` string,
# MAGIC `VIP` string,
# MAGIC `OCCPCODE` string,
# MAGIC `SERVBRH` string,
# MAGIC `STATCODE` string,
# MAGIC `CLTDOB` string,
# MAGIC `SOE` string,
# MAGIC `DOCNO` string,
# MAGIC `CLTDOD` string,
# MAGIC `CLTSTAT` string,
# MAGIC `CLTMCHG` string,
# MAGIC `MIDDL01` string,
# MAGIC `MIDDL02` string,
# MAGIC `MARRYD` string,
# MAGIC `TLXNO` string,
# MAGIC `FAXNO` string,
# MAGIC `TGRAM` string,
# MAGIC `BIRTHP` string,
# MAGIC `SALUTL` string,
# MAGIC `ROLEFLAG01` string,
# MAGIC `ROLEFLAG02` string,
# MAGIC `ROLEFLAG03` string,
# MAGIC `ROLEFLAG04` string,
# MAGIC `ROLEFLAG05` string,
# MAGIC `ROLEFLAG06` string,
# MAGIC `ROLEFLAG07` string,
# MAGIC `ROLEFLAG08` string,
# MAGIC `ROLEFLAG09` string,
# MAGIC `ROLEFLAG10` string,
# MAGIC `ROLEFLAG11` string,
# MAGIC `ROLEFLAG12` string,
# MAGIC `ROLEFLAG13` string,
# MAGIC `ROLEFLAG14` string,
# MAGIC `ROLEFLAG15` string,
# MAGIC `ROLEFLAG16` string,
# MAGIC `ROLEFLAG17` string,
# MAGIC `ROLEFLAG18` string,
# MAGIC `ROLEFLAG19` string,
# MAGIC `ROLEFLAG20` string,
# MAGIC `ROLEFLAG21` string,
# MAGIC `ROLEFLAG22` string,
# MAGIC `ROLEFLAG23` string,
# MAGIC `ROLEFLAG24` string,
# MAGIC `ROLEFLAG25` string,
# MAGIC `ROLEFLAG26` string,
# MAGIC `ROLEFLAG27` string,
# MAGIC `ROLEFLAG28` string,
# MAGIC `ROLEFLAG29` string,
# MAGIC `ROLEFLAG30` string,
# MAGIC `ROLEFLAG31` string,
# MAGIC `ROLEFLAG32` string,
# MAGIC `ROLEFLAG33` string,
# MAGIC `ROLEFLAG34` string,
# MAGIC `ROLEFLAG35` string,
# MAGIC `CHDRSTCDA` string,
# MAGIC `CHDRSTCDB` string,
# MAGIC `CHDRSTCDC` string,
# MAGIC `CHDRSTCDD` string,
# MAGIC `CHDRSTCDE` string,
# MAGIC `PROCFLAG` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `SNDXCDE` string,
# MAGIC `NATLTY` string,
# MAGIC `FOR_ATT_OF` string,
# MAGIC `CLTIND` string,
# MAGIC `STATE` string,
# MAGIC `LANGUAGE` string,
# MAGIC `CAPITAL` string,
# MAGIC `CTRYORIG` string,
# MAGIC `ECACT` string,
# MAGIC `ETHORIG` string,
# MAGIC `START_DATE` string,
# MAGIC `STAFFNO` string,
# MAGIC `LSURNAME` string,
# MAGIC `LGIVNAME` string,
# MAGIC `TAXFLAG` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `CLTDOB_DFM` string,
# MAGIC `CLTDOD_DFM` string,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `START_DATE_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CLNTPFX` string,
# MAGIC `BI_CLNTCOY` string,
# MAGIC `BI_CLNTNUM` string,
# MAGIC `BI_VALIDFLAG` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_CLNTPF"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_CLNTPF;

# COMMAND ----------

# DBTITLE 1,Create Table CLEXPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_CLEXPF;
# MAGIC CREATE TABLE STG_GA_CLEXPF (
# MAGIC `ID_Key` string,
# MAGIC `CLNTPFX` string,
# MAGIC `CLNTCOY` string,
# MAGIC `CLNTNUM` string,
# MAGIC `RDIDTELNO` string,
# MAGIC `RMBLPHONE` string,
# MAGIC `RPAGER` string,
# MAGIC `FAXNO` string,
# MAGIC `RINTERNET` string,
# MAGIC `RTAXIDNUM` string,
# MAGIC `RSTAFLAG` string,
# MAGIC `SPLINDIC` string,
# MAGIC `ZSPECIND` string,
# MAGIC `OLDIDNO` string,
# MAGIC `VALIDFLAG` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CLNTPFX` string,
# MAGIC `BI_CLNTCOY` string,
# MAGIC `BI_CLNTNUM` string,
# MAGIC `BI_VALIDFLAG` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_CLEXPF"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_CLEXPF;

# COMMAND ----------

# DBTITLE 1,Create Table DESCPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_DESCPF;
# MAGIC CREATE TABLE STG_GA_DESCPF (
# MAGIC `ID_Key` string,
# MAGIC `DESCPFX` string,
# MAGIC `DESCCOY` string,
# MAGIC `DESCTABL` string,
# MAGIC `DESCITEM` string,
# MAGIC `ITEMSEQ` string,
# MAGIC `LANGUAGE` string,
# MAGIC `TRANID` string,
# MAGIC `SHORTDESC` string,
# MAGIC `LONGDESC` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_DESCPFX` string,
# MAGIC `BI_DESCCOY` string,
# MAGIC `BI_DESCTABL` string,
# MAGIC `BI_DESCITEM` string,
# MAGIC `BI_ITEMSEQ` string,
# MAGIC `BI_LANGUAGE` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud_en.dbo.GA_DESCPF"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_DESCPF;

# COMMAND ----------

# DBTITLE 1,Create Table CLRRPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_CLRRPF;
# MAGIC CREATE TABLE STG_GA_CLRRPF (
# MAGIC `ID_Key` string,
# MAGIC `CLNTPFX` string,
# MAGIC `CLNTCOY` string,
# MAGIC `CLNTNUM` string,
# MAGIC `CLRRROLE` string,
# MAGIC `FOREPFX` string,
# MAGIC `FORECOY` string,
# MAGIC `FORENUM` string,
# MAGIC `USED_TO_BE` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CLNTPFX` string,
# MAGIC `BI_CLNTCOY` string,
# MAGIC `BI_CLNTNUM` string,
# MAGIC `BI_CLRRROLE` string,
# MAGIC `BI_FOREPFX` string,
# MAGIC `BI_FORECOY` string,
# MAGIC `BI_FORENUM` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_CLRRPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_CLRRPF;

# COMMAND ----------

# DBTITLE 1,Create Table GCLHPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GCLHPF;
# MAGIC CREATE TABLE STG_GA_GCLHPF (
# MAGIC `ID_Key` string,
# MAGIC `CLMCOY` string,
# MAGIC `CLAMNUM` string,
# MAGIC `GCOCCNO` string,
# MAGIC `CHDRNUM` string,
# MAGIC `MBRNO` string,
# MAGIC `DPNTNO` string,
# MAGIC `CLNTCOY` string,
# MAGIC `CLNTNUM` string,
# MAGIC `GCSTS` string,
# MAGIC `CLAIMCUR` string,
# MAGIC `CRATE` string,
# MAGIC `PRODTYP` string,
# MAGIC `GRSKCLS` string,
# MAGIC `DTEVISIT` string,
# MAGIC `DTEDCHRG` string,
# MAGIC `GCDIAGCD` string,
# MAGIC `PLANNO` string,
# MAGIC `PREAUTNO` string,
# MAGIC `PROVORG` string,
# MAGIC `AREACDE` string,
# MAGIC `REFERRER` string,
# MAGIC `CLAMTYPE` string,
# MAGIC `TIMEHH` string,
# MAGIC `TIMEMM` string,
# MAGIC `CLIENT_CLAIM_REF` string,
# MAGIC `GCDTHCLM` string,
# MAGIC `APAIDAMT` string,
# MAGIC `REQNTYPE` string,
# MAGIC `CRDTCARD` string,
# MAGIC `WHOPAID` string,
# MAGIC `DTEKNOWN` string,
# MAGIC `GCFRPDTE` string,
# MAGIC `RECVD_DATE` string,
# MAGIC `MCFROM` string,
# MAGIC `MCTO` string,
# MAGIC `GDEDUCT` string,
# MAGIC `COPAY` string,
# MAGIC `MBRTYPE` string,
# MAGIC `PROVNET` string,
# MAGIC `AAD` string,
# MAGIC `THIRDRCVY` string,
# MAGIC `THIRDPARTY` string,
# MAGIC `TLMBRSHR` string,
# MAGIC `TLHMOSHR` string,
# MAGIC `DATEAUTH` string,
# MAGIC `GCAUTHBY` string,
# MAGIC `GCOPRSCD` string,
# MAGIC `REVLINK` string,
# MAGIC `REVERSAL_IND` string,
# MAGIC `TPRCVPND` string,
# MAGIC `PENDFROM` string,
# MAGIC `MMPROD` string,
# MAGIC `HMOSHRMM` string,
# MAGIC `TAKEUP` string,
# MAGIC `DATACONV` string,
# MAGIC `CLRATE` string,
# MAGIC `REFNO` string,
# MAGIC `UPDATE_IND` string,
# MAGIC `PREMRCVY` string,
# MAGIC `DATEAUTH1` string,
# MAGIC `DATEAUTH2` string,
# MAGIC `GCAUTHBY1` string,
# MAGIC `GCAUTHBY2` string,
# MAGIC `CASHLESS` string,
# MAGIC `TPAREFNO` string,
# MAGIC `INWARDNO` string,
# MAGIC `ICD101L` string,
# MAGIC `ICD102L` string,
# MAGIC `ICD103L` string,
# MAGIC `REGCLM` string,
# MAGIC `GCCAUSCD` string,
# MAGIC `PAYENME` string,
# MAGIC `PAY_OPTION` string,
# MAGIC `CHQNUM` string,
# MAGIC `BANKDESC` string,
# MAGIC `BANKACCKEY` string,
# MAGIC `PAYAMT` string,
# MAGIC `PAYDTE` string,
# MAGIC `INVNO` string,
# MAGIC `DINVOICEDT` string,
# MAGIC `ZDIAGCDE01` string,
# MAGIC `ZDIAGCDE02` string,
# MAGIC `ZDIAGCDE03` string,
# MAGIC `TLCFAMT` string,
# MAGIC `ZCLMTYP` string,
# MAGIC `DTEREG` string,
# MAGIC `ZFAXCLAIM` string,
# MAGIC `CLAIMCAT` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `DTEVISIT_DFM` string,
# MAGIC `DTEDCHRG_DFM` string,
# MAGIC `DTEKNOWN_DFM` string,
# MAGIC `GCFRPDTE_DFM` string,
# MAGIC `RECVD_DATE_DFM` string,
# MAGIC `MCFROM_DFM` string,
# MAGIC `MCTO_DFM` string,
# MAGIC `DATEAUTH_DFM` string,
# MAGIC `DATEAUTH1_DFM` string,
# MAGIC `DATEAUTH2_DFM` string,
# MAGIC `PAYDTE_DFM` string,
# MAGIC `DINVOICEDT_DFM` string,
# MAGIC `DTEREG_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CLMCOY` string,
# MAGIC `BI_CLAMNUM` string,
# MAGIC `BI_GCOCCNO` string,
# MAGIC `BI_DATIME` string,
# MAGIC `ZMBRPAID` string,
# MAGIC `WHOMPAY` string,
# MAGIC `ZACCDDT` string,
# MAGIC `ZADVPAY` string,
# MAGIC `ZACCDDT_DFM` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GCLHPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GCLHPF;

# COMMAND ----------

# DBTITLE 1,Create Table CLMPPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_CLMPPF;
# MAGIC CREATE TABLE STG_GA_CLMPPF (
# MAGIC `ID_Key` string,
# MAGIC `CLMCOY` string,
# MAGIC `CLAMNUM` string,
# MAGIC `GCOCCNO` string,
# MAGIC `PAYMTYP` string,
# MAGIC `GCPYSEQ` string,
# MAGIC `PAYCOY` string,
# MAGIC `PAYCLT` string,
# MAGIC `REQNBCDE` string,
# MAGIC `REQNTYPE` string,
# MAGIC `TRANTYP` string,
# MAGIC `REQNNO` string,
# MAGIC `PAYAMT` string,
# MAGIC `TRANDATE` string,
# MAGIC `POSTYEAR` string,
# MAGIC `POSTMONTH` string,
# MAGIC `RECEIPT01` string,
# MAGIC `RECEIPT02` string,
# MAGIC `RECEIPT03` string,
# MAGIC `INDC` string,
# MAGIC `TAKEUP` string,
# MAGIC `BATCREQ` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `TRANDATE_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CLMCOY` string,
# MAGIC `BI_CLAMNUM` string,
# MAGIC `BI_GCOCCNO` string,
# MAGIC `BI_PAYMTYP` string,
# MAGIC `BI_GCPYSEQ` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_CLMPPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_CLMPPF;

# COMMAND ----------

# DBTITLE 1,Create Table GCPYPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GCPYPF;
# MAGIC CREATE TABLE STG_GA_GCPYPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CLAMNUM` string,
# MAGIC `GCOCCNO` string,
# MAGIC `PAYMTYP` string,
# MAGIC `INSTLNO` string,
# MAGIC `GCPYSEQ` string,
# MAGIC `PAYCOY` string,
# MAGIC `PAYCLT` string,
# MAGIC `GCTXNAMT` string,
# MAGIC `REQNTYPE` string,
# MAGIC `REQNBCDE` string,
# MAGIC `CHEQNO` string,
# MAGIC `OCHEQNO` string,
# MAGIC `BRANCH` string,
# MAGIC `REQNNO` string,
# MAGIC `PAY_TYPE` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CLAMNUM` string,
# MAGIC `BI_GCOCCNO` string,
# MAGIC `BI_PAYMTYP` string,
# MAGIC `BI_INSTLNO` string,
# MAGIC `BI_GCPYSEQ` string,
# MAGIC `BI_DATIME` string,
# MAGIC `BATCREQ` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GCPYPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GCPYPF;

# COMMAND ----------

# DBTITLE 1,Create Table GCMHPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GCMHPF;
# MAGIC CREATE TABLE STG_GA_GCMHPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CLAMNUM` string,
# MAGIC `GCOCCNO` string,
# MAGIC `GCCLMSEQ` string,
# MAGIC `CHDRNUM` string,
# MAGIC `PRODTYP` string,
# MAGIC `GCCLMTNO` string,
# MAGIC `GCDPNTNO` string,
# MAGIC `CLNTPFX` string,
# MAGIC `CLNTCOY` string,
# MAGIC `CLNTNUM` string,
# MAGIC `GCSTS` string,
# MAGIC `DTECLAM` string,
# MAGIC `CLAIMCUR` string,
# MAGIC `CRATE` string,
# MAGIC `PLANNO` string,
# MAGIC `FMLYCDE` string,
# MAGIC `GCRMK` string,
# MAGIC `BNKCHARGE` string,
# MAGIC `SPECTRM` string,
# MAGIC `GCDBLIND` string,
# MAGIC `GCFRPDTE` string,
# MAGIC `GCLRPDTE` string,
# MAGIC `GCDTHCLM` string,
# MAGIC `GCCAUSCD` string,
# MAGIC `GCOPPVCD` string,
# MAGIC `GCOPBNCD` string,
# MAGIC `GCOPIAMT` string,
# MAGIC `GCOPBAMT` string,
# MAGIC `GCOPBNPY` string,
# MAGIC `GCOPDGCD` string,
# MAGIC `GCOPPTCD` string,
# MAGIC `REQNBCDE` string,
# MAGIC `REQNTYPE` string,
# MAGIC `GCAUTHBY` string,
# MAGIC `DATEAUTH` string,
# MAGIC `GCOPRSCD` string,
# MAGIC `GCGRSPY` string,
# MAGIC `GCNETPY` string,
# MAGIC `GCCOINA` string,
# MAGIC `DEDUCTPC` string,
# MAGIC `GCADVPY` string,
# MAGIC `GCTRDRPY` string,
# MAGIC `MMFLG` string,
# MAGIC `PACCAMT` string,
# MAGIC `OVRLMT` string,
# MAGIC `DTEPYMTADV` string,
# MAGIC `DTEPYMTOTH` string,
# MAGIC `DTEATT` string,
# MAGIC `DTETRM` string,
# MAGIC `GCLOCCNO` string,
# MAGIC `REVLINK` string,
# MAGIC `REVERSAL_IND` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `APAIDAMT` string,
# MAGIC `INTPAIDAMT` string,
# MAGIC `GCSETLMD` string,
# MAGIC `GCSECSTS` string,
# MAGIC `CLIENT_CLAIM_REF` string,
# MAGIC `RECVD_DATE` string,
# MAGIC `CLNT_PROC` string,
# MAGIC `CLAIM_REF` string,
# MAGIC `FEE_CURR` string,
# MAGIC `FEE_TYPE` string,
# MAGIC `CALC_FEE` string,
# MAGIC `OVRD_FEE` string,
# MAGIC `GRSKCLS` string,
# MAGIC `BENCDE` string,
# MAGIC `CLAIMCOND` string,
# MAGIC `OUTLOAN` string,
# MAGIC `DTEREG` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `DTECLAM_DFM` string,
# MAGIC `GCFRPDTE_DFM` string,
# MAGIC `GCLRPDTE_DFM` string,
# MAGIC `DATEAUTH_DFM` string,
# MAGIC `DTEPYMTADV_DFM` string,
# MAGIC `DTEPYMTOTH_DFM` string,
# MAGIC `DTEATT_DFM` string,
# MAGIC `DTETRM_DFM` string,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `RECVD_DATE_DFM` string,
# MAGIC `DTEREG_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CLAMNUM` string,
# MAGIC `BI_GCOCCNO` string,
# MAGIC `BI_GCCLMSEQ` string,
# MAGIC `BI_DATIME` string,
# MAGIC `ZREMINST` string,
# MAGIC `ZBILLNUM` string,
# MAGIC `GDIAGCD` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GCMHPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GCMHPF;

# COMMAND ----------

# DBTITLE 1,Create Table GAPHPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GAPHPF;
# MAGIC CREATE TABLE STG_GA_GAPHPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `HEADCNTIND` string,
# MAGIC `MBRNO` string,
# MAGIC `DPNTNO` string,
# MAGIC `PRODTYP` string,
# MAGIC `PLANNO` string,
# MAGIC `EFFDATE` string,
# MAGIC `DTETRM` string,
# MAGIC `TRANNO` string,
# MAGIC `SUBSCOY` string,
# MAGIC `SUBSNUM` string,
# MAGIC `APREM` string,
# MAGIC `AEXTPRM` string,
# MAGIC `AEXTPRMR` string,
# MAGIC `HSUMINSU` string,
# MAGIC `PRMAMTRT` string,
# MAGIC `INDICX` string,
# MAGIC `UPDTYPE` string,
# MAGIC `RESNCD` string,
# MAGIC `SRCDATA` string,
# MAGIC `BATCTRCD` string,
# MAGIC `VALIDFLAG` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `EFFDATE_DFM` string,
# MAGIC `DTETRM_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `RRN` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GAPHPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GAPHPF;

# COMMAND ----------

# DBTITLE 1,Create Table CHDRPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_CHDRPF;
# MAGIC CREATE TABLE STG_GA_CHDRPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRPFX` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `RECODE` string,
# MAGIC `SERVUNIT` string,
# MAGIC `CNTTYPE` string,
# MAGIC `TRANNO` string,
# MAGIC `TRANID` string,
# MAGIC `VALIDFLAG` string,
# MAGIC `CURRFROM` string,
# MAGIC `CURRTO` string,
# MAGIC `PROCTRANCD` string,
# MAGIC `PROCFLAG` string,
# MAGIC `PROCID` string,
# MAGIC `STATCODE` string,
# MAGIC `STATREASN` string,
# MAGIC `STATDATE` string,
# MAGIC `STATTRAN` string,
# MAGIC `TRANLUSED` string,
# MAGIC `OCCDATE` string,
# MAGIC `CCDATE` string,
# MAGIC `CRDATE` string,
# MAGIC `ANNAMT01` string,
# MAGIC `ANNAMT02` string,
# MAGIC `ANNAMT03` string,
# MAGIC `ANNAMT04` string,
# MAGIC `ANNAMT05` string,
# MAGIC `ANNAMT06` string,
# MAGIC `RNLTYPE` string,
# MAGIC `RNLNOTS` string,
# MAGIC `RNLNOTTO` string,
# MAGIC `RNLATTN` string,
# MAGIC `RNLDURN` string,
# MAGIC `REPTYPE` string,
# MAGIC `REPNUM` string,
# MAGIC `COWNPFX` string,
# MAGIC `COWNCOY` string,
# MAGIC `COWNNUM` string,
# MAGIC `JOWNNUM` string,
# MAGIC `PAYRPFX` string,
# MAGIC `PAYRCOY` string,
# MAGIC `PAYRNUM` string,
# MAGIC `DESPPFX` string,
# MAGIC `DESPCOY` string,
# MAGIC `DESPNUM` string,
# MAGIC `ASGNPFX` string,
# MAGIC `ASGNCOY` string,
# MAGIC `ASGNNUM` string,
# MAGIC `CNTBRANCH` string,
# MAGIC `AGNTPFX` string,
# MAGIC `AGNTCOY` string,
# MAGIC `AGNTNUM` string,
# MAGIC `CNTCURR` string,
# MAGIC `ACCTCCY` string,
# MAGIC `CRATE` string,
# MAGIC `PAYPLAN` string,
# MAGIC `ACCTMETH` string,
# MAGIC `BILLFREQ` string,
# MAGIC `BILLCHNL` string,
# MAGIC `COLLCHNL` string,
# MAGIC `BILLDAY` string,
# MAGIC `BILLMONTH` string,
# MAGIC `BILLCD` string,
# MAGIC `BTDATE` string,
# MAGIC `PTDATE` string,
# MAGIC `PAYFLAG` string,
# MAGIC `SINSTFROM` string,
# MAGIC `SINSTTO` string,
# MAGIC `SINSTAMT01` string,
# MAGIC `SINSTAMT02` string,
# MAGIC `SINSTAMT03` string,
# MAGIC `SINSTAMT04` string,
# MAGIC `SINSTAMT05` string,
# MAGIC `SINSTAMT06` string,
# MAGIC `INSTFROM` string,
# MAGIC `INSTTO` string,
# MAGIC `INSTBCHNL` string,
# MAGIC `INSTCCHNL` string,
# MAGIC `INSTFREQ` string,
# MAGIC `INSTTOT01` string,
# MAGIC `INSTTOT02` string,
# MAGIC `INSTTOT03` string,
# MAGIC `INSTTOT04` string,
# MAGIC `INSTTOT05` string,
# MAGIC `INSTTOT06` string,
# MAGIC `INSTPAST01` string,
# MAGIC `INSTPAST02` string,
# MAGIC `INSTPAST03` string,
# MAGIC `INSTPAST04` string,
# MAGIC `INSTPAST05` string,
# MAGIC `INSTPAST06` string,
# MAGIC `INSTJCTL` string,
# MAGIC `NOFOUTINST` string,
# MAGIC `OUTSTAMT` string,
# MAGIC `BILLDATE01` string,
# MAGIC `BILLDATE02` string,
# MAGIC `BILLDATE03` string,
# MAGIC `BILLDATE04` string,
# MAGIC `BILLAMT01` string,
# MAGIC `BILLAMT02` string,
# MAGIC `BILLAMT03` string,
# MAGIC `BILLAMT04` string,
# MAGIC `FACTHOUS` string,
# MAGIC `BANKKEY` string,
# MAGIC `BANKACCKEY` string,
# MAGIC `DISCODE01` string,
# MAGIC `DISCODE02` string,
# MAGIC `DISCODE03` string,
# MAGIC `DISCODE04` string,
# MAGIC `GRUPKEY` string,
# MAGIC `MEMBSEL` string,
# MAGIC `APLSUPR` string,
# MAGIC `APLSPFROM` string,
# MAGIC `APLSPTO` string,
# MAGIC `BILLSUPR` string,
# MAGIC `BILLSPFROM` string,
# MAGIC `BILLSPTO` string,
# MAGIC `COMMSUPR` string,
# MAGIC `COMMSPFROM` string,
# MAGIC `COMMSPTO` string,
# MAGIC `LAPSSUPR` string,
# MAGIC `LAPSSPFROM` string,
# MAGIC `LAPSSPTO` string,
# MAGIC `MAILSUPR` string,
# MAGIC `MAILSPFROM` string,
# MAGIC `MAILSPTO` string,
# MAGIC `NOTSSUPR` string,
# MAGIC `NOTSSPFROM` string,
# MAGIC `NOTSSPTO` string,
# MAGIC `RNWLSUPR` string,
# MAGIC `RNWLSPFROM` string,
# MAGIC `RNWLSPTO` string,
# MAGIC `CAMPAIGN` string,
# MAGIC `SRCEBUS` string,
# MAGIC `NOFRISKS` string,
# MAGIC `JACKET` string,
# MAGIC `INSTSTAMT01` string,
# MAGIC `INSTSTAMT02` string,
# MAGIC `INSTSTAMT03` string,
# MAGIC `INSTSTAMT04` string,
# MAGIC `INSTSTAMT05` string,
# MAGIC `INSTSTAMT06` string,
# MAGIC `PSTATCODE` string,
# MAGIC `PSTATREASN` string,
# MAGIC `PSTATTRAN` string,
# MAGIC `PSTATDATE` string,
# MAGIC `PDIND` string,
# MAGIC `REGISTER` string,
# MAGIC `CHDRSTCDA` string,
# MAGIC `CHDRSTCDB` string,
# MAGIC `CHDRSTCDC` string,
# MAGIC `CHDRSTCDD` string,
# MAGIC `CHDRSTCDE` string,
# MAGIC `MPLPFX` string,
# MAGIC `MPLCOY` string,
# MAGIC `MPLNUM` string,
# MAGIC `POAPFX` string,
# MAGIC `POACOY` string,
# MAGIC `POANUM` string,
# MAGIC `FINPFX` string,
# MAGIC `FINCOY` string,
# MAGIC `FINNUM` string,
# MAGIC `WVFDAT` string,
# MAGIC `WVTDAT` string,
# MAGIC `WVFIND` string,
# MAGIC `CLUPFX` string,
# MAGIC `CLUCOY` string,
# MAGIC `CLUNUM` string,
# MAGIC `POLPLN` string,
# MAGIC `CHGFLAG` string,
# MAGIC `LAPRIND` string,
# MAGIC `SPECIND` string,
# MAGIC `DUEFLG` string,
# MAGIC `BFCHARGE` string,
# MAGIC `DISHNRCNT` string,
# MAGIC `PDTYPE` string,
# MAGIC `DISHNRDTE` string,
# MAGIC `STMPDTYAMT` string,
# MAGIC `STMPDTYDTE` string,
# MAGIC `POLINC` string,
# MAGIC `POLSUM` string,
# MAGIC `NXTSFX` string,
# MAGIC `AVLISU` string,
# MAGIC `STATEMENT_DATE` string,
# MAGIC `BILLCURR` string,
# MAGIC `FREE_SWITCHES_USED` string,
# MAGIC `FREE_SWITCHES_LEFT` string,
# MAGIC `LAST_SWITCH_DATE` string,
# MAGIC `MANDREF` string,
# MAGIC `CNTISS` string,
# MAGIC `CNTRCV` string,
# MAGIC `COPPN` string,
# MAGIC `COTYPE` string,
# MAGIC `COVERNT` string,
# MAGIC `DOCNUM` string,
# MAGIC `DTECAN` string,
# MAGIC `QUOTENO` string,
# MAGIC `RNLSTS` string,
# MAGIC `SUSTRCDE` string,
# MAGIC `BANKCODE` string,
# MAGIC `PNDATE` string,
# MAGIC `SUBSFLG` string,
# MAGIC `HRSKIND` string,
# MAGIC `SLRYPFLG` string,
# MAGIC `TAKOVRFLG` string,
# MAGIC `GPRNLTYP` string,
# MAGIC `GPRMNTHS` string,
# MAGIC `COYSRVAC` string,
# MAGIC `MRKSRVAC` string,
# MAGIC `POLSCHPFLG` string,
# MAGIC `ADJDATE` string,
# MAGIC `PTDATEAB` string,
# MAGIC `LMBRNO` string,
# MAGIC `LHEADNO` string,
# MAGIC `EFFDCLDT` string,
# MAGIC `PNTRCDE` string,
# MAGIC `TAXFLAG` string,
# MAGIC `AGEDEF` string,
# MAGIC `TERMAGE` string,
# MAGIC `PERSONCOV` string,
# MAGIC `ENROLLTYP` string,
# MAGIC `SPLITSUBS` string,
# MAGIC `DTLSIND` string,
# MAGIC `ZRENNO` string,
# MAGIC `ZENDNO` string,
# MAGIC `ZRESNPD` string,
# MAGIC `ZREPOLNO` string,
# MAGIC `ZCOMTYP` string,
# MAGIC `ZRINUM` string,
# MAGIC `ZSCHPRT` string,
# MAGIC `ZPAYMODE` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOBNAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `JOB_NAME` string,
# MAGIC `CURRFROM_DFM` string,
# MAGIC `CURRTO_DFM` string,
# MAGIC `STATDATE_DFM` string,
# MAGIC `OCCDATE_DFM` string,
# MAGIC `CCDATE_DFM` string,
# MAGIC `CRDATE_DFM` string,
# MAGIC `BILLCD_DFM` string,
# MAGIC `BTDATE_DFM` string,
# MAGIC `PTDATE_DFM` string,
# MAGIC `SINSTFROM_DFM` string,
# MAGIC `SINSTTO_DFM` string,
# MAGIC `INSTFROM_DFM` string,
# MAGIC `INSTTO_DFM` string,
# MAGIC `BILLDATE01_DFM` string,
# MAGIC `BILLDATE02_DFM` string,
# MAGIC `BILLDATE03_DFM` string,
# MAGIC `BILLDATE04_DFM` string,
# MAGIC `APLSPFROM_DFM` string,
# MAGIC `APLSPTO_DFM` string,
# MAGIC `BILLSPFROM_DFM` string,
# MAGIC `BILLSPTO_DFM` string,
# MAGIC `COMMSPFROM_DFM` string,
# MAGIC `COMMSPTO_DFM` string,
# MAGIC `LAPSSPFROM_DFM` string,
# MAGIC `LAPSSPTO_DFM` string,
# MAGIC `MAILSPFROM_DFM` string,
# MAGIC `MAILSPTO_DFM` string,
# MAGIC `NOTSSPFROM_DFM` string,
# MAGIC `NOTSSPTO_DFM` string,
# MAGIC `RNWLSPFROM_DFM` string,
# MAGIC `RNWLSPTO_DFM` string,
# MAGIC `PSTATDATE_DFM` string,
# MAGIC `WVFDAT_DFM` string,
# MAGIC `WVTDAT_DFM` string,
# MAGIC `DISHNRDTE_DFM` string,
# MAGIC `STMPDTYDTE_DFM` string,
# MAGIC `STATEMENT_DATE_DFM` string,
# MAGIC `LAST_SWITCH_DATE_DFM` string,
# MAGIC `CNTISS_DFM` string,
# MAGIC `CNTRCV_DFM` string,
# MAGIC `DTECAN_DFM` string,
# MAGIC `PNDATE_DFM` string,
# MAGIC `ADJDATE_DFM` string,
# MAGIC `PTDATEAB_DFM` string,
# MAGIC `EFFDCLDT_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRPFX` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CHDRNUM` string,
# MAGIC `BI_TRANNO` string,
# MAGIC `BI_VALIDFLAG` string,
# MAGIC `BI_CURRFROM` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_CHDRPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_CHDRPF;

# COMMAND ----------

# DBTITLE 1,Create Table GCHIPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GCHIPF;
# MAGIC CREATE TABLE STG_GA_GCHIPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `EFFDATE` string,
# MAGIC `CCDATE` string,
# MAGIC `CRDATE` string,
# MAGIC `PRVBILFLG` string,
# MAGIC `BILLFREQ` string,
# MAGIC `GADJFREQ` string,
# MAGIC `PAYRPFX` string,
# MAGIC `PAYRCOY` string,
# MAGIC `PAYRNUM` string,
# MAGIC `AGNTPFX` string,
# MAGIC `AGNTCOY` string,
# MAGIC `AGNTNUM` string,
# MAGIC `CNTBRANCH` string,
# MAGIC `CHDRSTCDA` string,
# MAGIC `CHDRSTCDB` string,
# MAGIC `CHDRSTCDC` string,
# MAGIC `CHDRSTCDD` string,
# MAGIC `CHDRSTCDE` string,
# MAGIC `BTDATENR` string,
# MAGIC `NRISDATE` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `TRANNO` string,
# MAGIC `CRATE` string,
# MAGIC `TERNMPRM` string,
# MAGIC `SURGSCHMV` string,
# MAGIC `AREACDEMV` string,
# MAGIC `MEDPRVDR` string,
# MAGIC `SPSMBR` string,
# MAGIC `CHILDMBR` string,
# MAGIC `SPSMED` string,
# MAGIC `CHILDMED` string,
# MAGIC `BANKCODE` string,
# MAGIC `BILLCHNL` string,
# MAGIC `MANDREF` string,
# MAGIC `RIMTHVCD` string,
# MAGIC `PRMRVWDT` string,
# MAGIC `APPLTYP` string,
# MAGIC `RIIND` string,
# MAGIC `POLBREAK` string,
# MAGIC `CFTYPE` string,
# MAGIC `LMTDRL` string,
# MAGIC `CFLIMIT` string,
# MAGIC `NOFCLAIM` string,
# MAGIC `TPA` string,
# MAGIC `WKLADRT` string,
# MAGIC `WKLCMRT` string,
# MAGIC `NOFMBR` string,
# MAGIC `CREDTERM` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `EFFDATE_DFM` string,
# MAGIC `CCDATE_DFM` string,
# MAGIC `CRDATE_DFM` string,
# MAGIC `BTDATENR_DFM` string,
# MAGIC `NRISDATE_DFM` string,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `PRMRVWDT_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CHDRNUM` string,
# MAGIC `BI_EFFDATE` string,
# MAGIC `BI_TRANNO` string,
# MAGIC `BI_DATIME` string,
# MAGIC `POLYEAR` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GCHIPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GCHIPF;

# COMMAND ----------

# DBTITLE 1,Create Table GXHIPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GXHIPF;
# MAGIC CREATE TABLE STG_GA_GXHIPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `MBRNO` string,
# MAGIC `PRODTYP` string,
# MAGIC `PLANNO` string,
# MAGIC `EFFDATE` string,
# MAGIC `FMLYCDE` string,
# MAGIC `DTEATT` string,
# MAGIC `DTETRM` string,
# MAGIC `REASONTRM` string,
# MAGIC `XCESSSI` string,
# MAGIC `APRVDATE` string,
# MAGIC `ACCPTDTE` string,
# MAGIC `SPECTRM` string,
# MAGIC `EXTRPRM` string,
# MAGIC `SUMINSU` string,
# MAGIC `DECFLG` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `TRANNO` string,
# MAGIC `HEADNO` string,
# MAGIC `DPNTNO` string,
# MAGIC `EMLOAD` string,
# MAGIC `OALOAD` string,
# MAGIC `BILLACTN` string,
# MAGIC `IMPAIRCD01` string,
# MAGIC `IMPAIRCD02` string,
# MAGIC `IMPAIRCD03` string,
# MAGIC `RIEMLOAD` string,
# MAGIC `RIOALOAD` string,
# MAGIC `USERSI` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `RIPROCDT` string,
# MAGIC `STDPRMLOAD` string,
# MAGIC `DTECLAM` string,
# MAGIC `EFFDATE_DFM` string,
# MAGIC `DTEATT_DFM` string,
# MAGIC `DTETRM_DFM` string,
# MAGIC `APRVDATE_DFM` string,
# MAGIC `ACCPTDTE_DFM` string,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `RIPROCDT_DFM` string,
# MAGIC `DTECLAM_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CHDRNUM` string,
# MAGIC `BI_MBRNO` string,
# MAGIC `BI_PRODTYP` string,
# MAGIC `BI_PLANNO` string,
# MAGIC `BI_EFFDATE` string,
# MAGIC `BI_TRANNO` string,
# MAGIC `BI_DPNTNO` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GXHIPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GXHIPF;

# COMMAND ----------

# DBTITLE 1,Create Table GMHDPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GMHDPF;
# MAGIC CREATE TABLE STG_GA_GMHDPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `MBRNO` string,
# MAGIC `DPNTNO` string,
# MAGIC `DTETRM` string,
# MAGIC `REASONTRM` string,
# MAGIC `CLNTPFX` string,
# MAGIC `FSUCO` string,
# MAGIC `CLNTNUM` string,
# MAGIC `HEADCNT` string,
# MAGIC `DTEATT` string,
# MAGIC `MEDEVD` string,
# MAGIC `RELN` string,
# MAGIC `FAUWDT` string,
# MAGIC `LDPNTNO` string,
# MAGIC `TERMID` string,
# MAGIC `USER` string,
# MAGIC `TRANSACTION_DATE` string,
# MAGIC `TRANSACTION_TIME` string,
# MAGIC `TRANNO` string,
# MAGIC `PNDATE` string,
# MAGIC `CLIENT` string,
# MAGIC `TERMRSNCD` string,
# MAGIC `REFIND` string,
# MAGIC `EMPNO` string,
# MAGIC `SMOKEIND` string,
# MAGIC `OCCPCLAS` string,
# MAGIC `ETHORG` string,
# MAGIC `GHEIGHT` string,
# MAGIC `GWEIGHT` string,
# MAGIC `PAYMENT_METHOD` string,
# MAGIC `DEFCLMPYE` string,
# MAGIC `PRVPOLDT` string,
# MAGIC `DEPT` string,
# MAGIC `NEWOLDCL` string,
# MAGIC `AGE` string,
# MAGIC `ORDOB` string,
# MAGIC `STATCODE` string,
# MAGIC `BANKACCKEY` string,
# MAGIC `APPLICNO` string,
# MAGIC `CERTNO` string,
# MAGIC `MEDCMPDT` string,
# MAGIC `INFORCE` string,
# MAGIC `WEIGHTUNIT` string,
# MAGIC `HEIGHTUNIT` string,
# MAGIC `MBRTYPC` string,
# MAGIC `SIFACT` string,
# MAGIC `RDYPROC` string,
# MAGIC `INSUFFMN` string,
# MAGIC `DPNTTYPE` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `DTETRM_DFM` string,
# MAGIC `DTEATT_DFM` string,
# MAGIC `FAUWDT_DFM` string,
# MAGIC `TRANSACTION_DATE_DFM` string,
# MAGIC `PNDATE_DFM` string,
# MAGIC `PRVPOLDT_DFM` string,
# MAGIC `MEDCMPDT_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `BI_CHDRCOY` string,
# MAGIC `BI_CHDRNUM` string,
# MAGIC `BI_MBRNO` string,
# MAGIC `BI_DPNTNO` string,
# MAGIC `BI_TRANNO` string,
# MAGIC `BI_DATIME` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GMHDPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GMHDPF;

# COMMAND ----------

# DBTITLE 1,Create Table GCHEPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_GCHEPF;
# MAGIC CREATE TABLE STG_GA_GCHEPF (
# MAGIC `ID_Key` string,
# MAGIC `CHDRCOY` string,
# MAGIC `CHDRNUM` string,
# MAGIC `EFFDATE` string,
# MAGIC `WAIVEWP` string,
# MAGIC `SUBSHRF` string,
# MAGIC `TPANUM` string,
# MAGIC `BILLOPT` string,
# MAGIC `TPAFLAG` string,
# MAGIC `NEWELM` string,
# MAGIC `ZPARTNER` string,
# MAGIC `ZAGTCHNL` string,
# MAGIC `PRTCARD` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `EFFDATE_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `RRN` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_GCHEPF/"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_GCHEPF;

# COMMAND ----------

# DBTITLE 1,Create Table ITEMPF
# MAGIC %sql
# MAGIC USE TH_STAGE;
# MAGIC DROP TABLE IF EXISTS STG_GA_ITEMPF;
# MAGIC CREATE TABLE STG_GA_ITEMPF (
# MAGIC `ID_Key` string,
# MAGIC `ITEMPFX` string,
# MAGIC `ITEMCOY` string,
# MAGIC `ITEMTABL` string,
# MAGIC `ITEMITEM` string,
# MAGIC `ITEMSEQ` string,
# MAGIC `TRANID` string,
# MAGIC `TABLEPROG` string,
# MAGIC `VALIDFLAG` string,
# MAGIC `ITMFRM` string,
# MAGIC `ITMTO` string,
# MAGIC `GENAREA` string,
# MAGIC `USER_PROFILE` string,
# MAGIC `JOB_NAME` string,
# MAGIC `DATIME` bigint,
# MAGIC `ITMFRM_DFM` string,
# MAGIC `ITMTO_DFM` string,
# MAGIC `System_CreateByUser` string,
# MAGIC `System_CreateDate` string,
# MAGIC `System_ChangeByUser` string,
# MAGIC `System_ChangeDate` string,
# MAGIC `RRN` string,
# MAGIC `__op` string,
# MAGIC `__source_table` string,
# MAGIC `__ts_ms` bigint,
# MAGIC `__deleted` string
# MAGIC ) USING org.apache.spark.sql.json
# MAGIC   OPTIONS (
# MAGIC     path "/mnt/adls_th/TH/FWD/IN/GA/RAW/FINAL/th_stg_cloud.dbo.GA_ITEMPF"
# MAGIC );
# MAGIC 
# MAGIC REFRESH TABLE TH_STAGE.STG_GA_ITEMPF;
