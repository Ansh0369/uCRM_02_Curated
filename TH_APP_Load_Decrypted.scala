// Databricks notebook source
// DBTITLE 1,Import Dependencies
import org.apache.spark.sql.types._
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.CallableStatement;

// COMMAND ----------

// DBTITLE 1,Initialize Parameters
//Example value : dwh.uspCustomerInitLoad
// dbutils.widgets.text("SP_Name", "Parameterized")
// var SP_Name = dbutils.widgets.get("SP_Name")

//Example value : 123456789
dbutils.widgets.text("Batch_ID", "Parameterized")
var Batch_ID = dbutils.widgets.get("Batch_ID")

//Example value : 123456789
//dbutils.widgets.text("Business_Date", "Parameterized")
//var Business_Date = dbutils.widgets.get("Business_Date")

// COMMAND ----------

//var BusinessDate = LocalDate.parse(Business_Date,DateTimeFormatter.ofPattern("yyyyMMdd")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

// COMMAND ----------

// DBTITLE 1,Run Credentials ADLS
// MAGIC %run /THAILAND/CommonZone/Credentials/ADLS

// COMMAND ----------

// DBTITLE 1,Run Credentials SQL Server
// MAGIC %run /THAILAND/CommonZone/Credentials/SQL_Server

// COMMAND ----------

// DBTITLE 1,Connection details 
val User_Credentials = new Properties()
val Driver_Class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
User_Credentials.put("user", jdbc_username_sqls)
User_Credentials.put("password", jdbc_password_sqls)
User_Credentials.setProperty("driver", Driver_Class)
val Connection_String = DriverManager.getConnection(jdbc_url_sqls, jdbc_username_sqls, jdbc_password_sqls)
val Statement = Connection_String.createStatement()

val User_Credentials_dd = new Properties()
User_Credentials_dd.put("user", jdbc_un_IDQDataLoad_sqls)
User_Credentials_dd.put("password", jdbc_pwd_IDQDataLoad_sqls)
User_Credentials_dd.setProperty("driver", Driver_Class)


// COMMAND ----------

val claimselectStatement = "(select CONTRACT_NUMBER,CLAIM_ID,OCCURRENCE_NUMBER,LIFE,COVERAGE,RIDER,CLAIM_STATUS_CODE,CLAIM_STATUS,CLAIM_TYPE,CLAIM_REQUEST_AMOUNT,CLAIM_COVERAGE_AMOUNT,CLAIM_AMOUNT_PAID,CLAIM_REQUEST_DATE,CLAIM_APPROVAL_DATE,VALIDFLAG,IS_ACTIVE,DATA_SOURCE,EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,UPDATED_DATETIME from dwh.Claim where IS_ACTIVE=1 and ValidFlag=1)a"

val claim_table = spark
  .read
  .format("jdbc")
  .option("url", jdbc_url_sqls)
  .option("user", jdbc_username_sqls)
  .option("password", jdbc_password_sqls)
  .option("dbtable", claimselectStatement)
  .load()

claim_table.createOrReplaceTempView("Claim")

val customerselectStatement = "(select CUSTOMER_ID, CUSTOMER_ID_TYPE_CODE,CUSTOMER_ID_TYPE,SALUTATION_CODE,SALUTATION,GIVEN_NAME,FAMILY_NAME,GENDER_CODE,GENDER,MARITAL_STATUS_CODE,MARITAL_STATUS,DATE_OF_BIRTH,EMAIL_ADDRESS,MOBILE_PHONE_NUMBER,NATIONALITY_CODE,NATIONALITY,OCCUPATION_CLASS,OCCUPATION_NAME,RESIDENTIAL_ADDRESS1,RESIDENTIAL_ADDRESS2,RESIDENTIAL_ADDRESS3,RESIDENTIAL_ADDRESS4,RESIDENTIAL_ADDRESS5,RESIDENTIAL_ADDRESS,COUNTRY_NAME,PASSPORT_NUMBER,ID_CARD_NO,CUSTOMER_SATISFICATION_SCORE,CUSTOMER_TIER,VALIDFLAG,IS_ACTIVE,COMPANY_CODE,DATA_SOURCE,EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,UPDATED_DATETIME from dwh.Customer where IS_ACTIVE=1 and ValidFlag=1)a"

val customer_table = spark
  .read
  .format("jdbc")
  .option("url", jdbc_url_sqls)
  .option("user", jdbc_username_sqls)
  .option("password", jdbc_password_sqls)
  .option("dbtable", customerselectStatement)
  .load()

customer_table.createOrReplaceTempView("Customer")

val customerroleselectStatement = "(select CLIENT_ID,POLICY_ID,CLIENT_ROLE_CODE,CLIENT_ROLE,BENEFICIARY_NAME,INSURED_NAME,OWNER_NAME,PAYER_NAME,VALIDFLAG,IS_ACTIVE,DATA_SOURCE,EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,UPDATED_DATETIME from dwh.CustomerRole where IS_ACTIVE=1 and ValidFlag=1)a"

val customerrole_table = spark
  .read
  .format("jdbc")
  .option("url", jdbc_url_sqls)
  .option("user", jdbc_username_sqls)
  .option("password", jdbc_password_sqls)
  .option("dbtable", customerroleselectStatement)
  .load()

customerrole_table.createOrReplaceTempView("CustomerRole")

val policyselectStatement = "(select CONTRACT_NUMBER,POLICY_NUMBER,LIFE,COVERAGE,RIDER,POLICY_STATUS,COVERAGE_RIDER,POLICY_STATUS_CODE,PLAN_CODE,PLAN_NAME,INSURED_CLIENT_ID,PAYER_ID,ISSUE_AGE,FOLLOW_UP_CODE,REASON_CODE,BILLING_CHANNEL,BILLING_CHANNEL_CODE,BILLING_FREQUENCY,BILLING_FREQUENCY_CODE,POLICY_ISSUE_DATE,POLICY_STATUS_DATE,PAID_TO_DATE,PROPOSAL_DATE,PROPOSAL_RECEIVED_DATE,EFFECTIVE_DATE,EXPIRY_DATE,FIRST_ISSUE_DATE,PAY_UP_DATE,RECEIPT_DATE,TRANSACTION_DATE,REQUESTED_DATE,RECEIVED_DATE,POLICY_ACKNOWLEDGEMENT_DATE,BILLED_TO_DATE,ICP_OPTION_PAYMENT_DATE,PREMIUM_STATUS,ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,ANNUALISED_FIRST_YEAR_PREMIUM,ANNUALISED_PREMIUM,CASH_SURRENDER,COLLECTED_PREMIUM,COVERAGE_BASE,MODAL_PREMIUM,ANNUAL_PREMIUM,REGULAR_INVESTMENT_PREMIUM,DISCOUNT_PREMIUM,SINGLE_PREMIUM,TOP_UP_PREMIUM,REGULAR_PREMIUM,SUM_ASSURED,INITIAL_SUM_ASSURED,EXTRA_LOADING,PREMIUM_TERM,POLICY_TERM,PAYMENT_DUE_TERM,TYPE_OF_UNDERWRITING,SALES_CHANNEL,POLICY_NOTE,EXCLUSION,PAYMENT_DUE_YEAR,TAX_DEDUCTION,TAX_CONSENT,PAYMENT_TYPE,RECIEPT_NO,PREMIUM_PAID_AMOUNT,ICP_AMOUNT,ICP_OPTION,NFO_OPTION,DIVIDEND_OPTION,INTEREST,ACCUM_DIVIDEND,LOAN_QUOTATION,APPLICATION_NUMBER,VALIDFLAG,IS_ACTIVE,COMPANY_CODE,DATA_SOURCE,EFFECTIVE_START_DATE,EFFECTIVE_END_DATE,UPDATED_DATETIME from dwh.Policy where IS_ACTIVE=1 and ValidFlag=1)a"

val policy_table = spark
  .read
  .format("jdbc")
  .option("url", jdbc_url_sqls)
  .option("user", jdbc_username_sqls)
  .option("password", jdbc_password_sqls)
  .option("dbtable", policyselectStatement)
  .load()

policy_table.createOrReplaceTempView("Policy")


// COMMAND ----------

// DBTITLE 1,Create Temp View for Claim
// MAGIC %sql CREATE
// MAGIC OR REPLACE TEMP VIEW Claim_DQ AS
// MAGIC SELECT
// MAGIC   Claim.DATA_SOURCE as DataSource,
// MAGIC   CLAIM_AMOUNT_PAID as Claim_Paid_Amount,
// MAGIC   CLAIM_APPROVAL_DATE,
// MAGIC   CLAIM_COVERAGE_AMOUNT,
// MAGIC   CLAIM_ID as Claim_Number,
// MAGIC   AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   CLAIM_REQUEST_AMOUNT,
// MAGIC   CLAIM_REQUEST_DATE,
// MAGIC   CLAIM_STATUS_CODE,
// MAGIC   CLAIM_TYPE as CLAIM_TYPE_CODE,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER, -- AES_DECRYPT(ROLE_1.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_1.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_1CUST.CUSTOMER_ID_TYPE_CODE,
// MAGIC   AES_DECRYPT(ROLE_1CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Claim_ID,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_START_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Claim
// MAGIC         WHERE
// MAGIC           Claim.Data_Source in ('IL', 'LA')
// MAGIC           and Claim.CLAIM_ID is not null
// MAGIC       ) TMP_CLAIM
// MAGIC     WHERE
// MAGIC       TMP_CLAIM.CURRENTFLAG = 1
// MAGIC   ) CLAIM
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC         where
// MAGIC           IS_ACTIVE = 1
// MAGIC           and ValidFlag = 1
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   /* Customer Role Insured, Data Source IL,LA start */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'LF'
// MAGIC           AND IS_ACTIVE = 1
// MAGIC           and ValidFlag = 1
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_1 ON SUBSTR(AES_DECRYPT(ROLE_1.POLICY_ID), 1, 8) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND SUBSTR(AES_DECRYPT(ROLE_1.POLICY_ID), 9, 2) = '01'
// MAGIC   AND ROLE_1.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC         WHERE
// MAGIC           IS_ACTIVE = 1
// MAGIC           and ValidFlag = 1
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_1CUST ON ROLE_1.CLIENT_ID = ROLE_1CUST.CUSTOMER_ID
// MAGIC   AND ROLE_1CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_1CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Insured, Data Source IL,LA ENDS */
// MAGIC union all
// MAGIC SELECT
// MAGIC   Claim.DATA_SOURCE as DataSource,
// MAGIC   CLAIM_AMOUNT_PAID as Claim_Paid_Amount,
// MAGIC   -- renamed term to as agreed in BG doc
// MAGIC   CLAIM_APPROVAL_DATE,
// MAGIC   CLAIM_COVERAGE_AMOUNT,
// MAGIC   CLAIM_ID as Claim_Number,
// MAGIC   -- renamed term to as suggested by FWD TH-Surin,
// MAGIC   AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   -- added as suggested by FWD TH - Surin
// MAGIC   CLAIM_REQUEST_AMOUNT,
// MAGIC   CLAIM_REQUEST_DATE,
// MAGIC   CLAIM_STATUS_CODE,
// MAGIC   CLAIM_TYPE as CLAIM_TYPE_CODE,
// MAGIC   -- renamed term as agreed in the BG doc
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER, -- AES_DECRYPT(ROLE_2.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_2.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_2CUST.CUSTOMER_ID_TYPE_CODE,
// MAGIC   AES_DECRYPT(ROLE_2CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Claim_ID,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_START_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Claim
// MAGIC         WHERE
// MAGIC           Claim.Data_Source in ('IL', 'LA')
// MAGIC           and Claim.CLAIM_ID is not null
// MAGIC       ) TMP_CLAIM
// MAGIC     WHERE
// MAGIC       TMP_CLAIM.CURRENTFLAG = 1
// MAGIC   ) CLAIM
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   /* Customer Role Payer, Data Source IL,LA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'PY'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_2 ON AES_DECRYPT(ROLE_2.POLICY_ID) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER) || 1
// MAGIC   AND AES_DECRYPT(ROLE_2.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER) || 1
// MAGIC   AND ROLE_2.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_2CUST ON ROLE_2.CLIENT_ID = ROLE_2CUST.CUSTOMER_ID
// MAGIC   AND ROLE_2CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   AND AES_DECRYPT(ROLE_2CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Payer, Data Source IL,LA ENDS */
// MAGIC union all
// MAGIC SELECT
// MAGIC   Claim.DATA_SOURCE as DataSource,
// MAGIC   CLAIM_AMOUNT_PAID as Claim_Paid_Amount,
// MAGIC   -- renamed term to as agreed in BG doc
// MAGIC   CLAIM_APPROVAL_DATE,
// MAGIC   CLAIM_COVERAGE_AMOUNT,
// MAGIC   CLAIM_ID as Claim_Number,
// MAGIC   -- renamed term to as suggested by FWD TH-Surin,
// MAGIC   AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   -- added as suggested by FWD TH - Surin
// MAGIC   CLAIM_REQUEST_AMOUNT,
// MAGIC   CLAIM_REQUEST_DATE,
// MAGIC   CLAIM_STATUS_CODE,
// MAGIC   CLAIM_TYPE as CLAIM_TYPE_CODE,
// MAGIC   -- renamed term as agreed in the BG doc
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER, -- AES_DECRYPT(ROLE_3.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_3.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_3CUST.CUSTOMER_ID_TYPE_CODE,
// MAGIC   AES_DECRYPT(ROLE_3CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Claim_ID,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_START_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Claim
// MAGIC         WHERE
// MAGIC           Claim.Data_Source in ('IL', 'LA')
// MAGIC           and Claim.CLAIM_ID is not null
// MAGIC       ) TMP_CLAIM
// MAGIC     WHERE
// MAGIC       TMP_CLAIM.CURRENTFLAG = 1
// MAGIC   ) CLAIM
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'OW'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_3 ON AES_DECRYPT(ROLE_3.POLICY_ID) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   AND SUBSTR(AES_DECRYPT(ROLE_3.POLICY_ID), 1, 8) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND ROLE_3.CLIENT_ROLE_CODE = 'OW'
// MAGIC   AND ROLE_3.DATA_SOURCE in ('IL', 'LA')
// MAGIC   /* Customer Role Owner, Data Source IL,LA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_3CUST ON ROLE_3.CLIENT_ID = ROLE_3CUST.CUSTOMER_ID
// MAGIC   AND ROLE_3CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_3CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Owner, Data Source IL,LA ends */
// MAGIC union all
// MAGIC SELECT
// MAGIC   Claim.DATA_SOURCE as DataSource,
// MAGIC   CLAIM_AMOUNT_PAID as Claim_Paid_Amount,
// MAGIC   -- renamed term to as agreed in BG doc
// MAGIC   CLAIM_APPROVAL_DATE,
// MAGIC   CLAIM_COVERAGE_AMOUNT,
// MAGIC   CLAIM_ID as Claim_Number,
// MAGIC   -- renamed term to as suggested by FWD TH-Surin,
// MAGIC   AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   -- added as suggested by FWD TH - Surin
// MAGIC   CLAIM_REQUEST_AMOUNT,
// MAGIC   CLAIM_REQUEST_DATE,
// MAGIC   CLAIM_STATUS_CODE,
// MAGIC   CLAIM_TYPE as CLAIM_TYPE_CODE,
// MAGIC   -- renamed term as agreed in the BG doc
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER, -- AES_DECRYPT(ROLE_4.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_4.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_4CUST.CUSTOMER_ID_TYPE_CODE,
// MAGIC   AES_DECRYPT(ROLE_4CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Claim_ID,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_START_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Claim
// MAGIC         WHERE
// MAGIC           Claim.Data_Source in ('IL', 'LA')
// MAGIC           and Claim.CLAIM_ID is not null
// MAGIC       ) TMP_CLAIM
// MAGIC     WHERE
// MAGIC       TMP_CLAIM.CURRENTFLAG = 1
// MAGIC   ) CLAIM
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'BN'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_4 ON AES_DECRYPT(ROLE_4.POLICY_ID) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and AES_DECRYPT(ROLE_4.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND ROLE_4.DATA_SOURCE in ('IL', 'LA')
// MAGIC   AND ROLE_4.CLIENT_ROLE_CODE = 'BN'
// MAGIC   /* Customer Role Beneficiary, Data Source IL,LA start */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_4CUST ON ROLE_4.CLIENT_ID = ROLE_4CUST.CUSTOMER_ID
// MAGIC   AND ROLE_4CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_4CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Beneficiary, Data Source IL,LA ends */
// MAGIC union all
// MAGIC SELECT
// MAGIC   Claim.DATA_SOURCE as DataSource,
// MAGIC   CLAIM_AMOUNT_PAID as Claim_Paid_Amount,
// MAGIC   -- renamed term to as agreed in BG doc
// MAGIC   CLAIM_APPROVAL_DATE,
// MAGIC   CLAIM_COVERAGE_AMOUNT,
// MAGIC   CLAIM_ID as Claim_Number,
// MAGIC   -- renamed term to as suggested by FWD TH-Surin,
// MAGIC   CLAIM.CONTRACT_NUMBER as CLAIM_POLICY_NUMBER,
// MAGIC   -- added as suggested by FWD TH - Surin
// MAGIC   CLAIM_REQUEST_AMOUNT,
// MAGIC   CLAIM_REQUEST_DATE,
// MAGIC   CLAIM_STATUS_CODE,
// MAGIC   CLAIM_TYPE as CLAIM_TYPE_CODE,
// MAGIC   -- renamed term as agreed in the BG doc
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER, -- AES_DECRYPT(ROLE_5.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_5.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_5CUST.CUSTOMER_ID_TYPE_CODE,
// MAGIC   AES_DECRYPT(ROLE_5CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Claim_ID,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_START_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Claim
// MAGIC         WHERE
// MAGIC           Claim.Data_Source = 'GA'
// MAGIC           and Claim.CLAIM_ID is not null
// MAGIC       ) TMP_CLAIM
// MAGIC     WHERE
// MAGIC       TMP_CLAIM.CURRENTFLAG = 1
// MAGIC   ) CLAIM
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code IN ('LF', 'OW', 'PY', 'BN')
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_5 ON AES_DECRYPT(ROLE_5.POLICY_ID) = AES_DECRYPT(CLAIM.CONTRACT_NUMBER)
// MAGIC   AND ROLE_5.DATA_SOURCE in ('GA')
// MAGIC   and AES_DECRYPT(ROLE_5.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   /* All Customer Roles, Data Source GA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_5CUST ON ROLE_5.CLIENT_ID = ROLE_5CUST.CUSTOMER_ID
// MAGIC   AND ROLE_5CUST.DATA_SOURCE in ('GA')
// MAGIC   and AES_DECRYPT(ROLE_5CUST.Customer_ID) is not null
// MAGIC   /* Customer Roles, Data Source GA ends */

// COMMAND ----------

// DBTITLE 1,Create Temp View for Customer
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW Customer_DQ AS 
// MAGIC    SELECT
// MAGIC     AES_DECRYPT(TMP_CUSTOMER.CUSTOMER_ID) as CustomerID,
// MAGIC   /*Joining key*/
// MAGIC   TMP_CUSTOMER.Customer_ID_Type_Code,
// MAGIC   --ROLE_1.client_role_code as PartyRole,
// MAGIC   TMP_CUSTOMER.DATA_SOURCE as DataSource,
// MAGIC   --ROLE_1.Client_Role as CustomerType,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.DATE_OF_BIRTH) as Birth_Date,
// MAGIC   --changed name as present in BG doc
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.EMAIL_ADDRESS) as Client_Email_Address,
// MAGIC   --changed name as present in BG doc
// MAGIC   TMP_CUSTOMER.CUSTOMER_SATISFICATION_SCORE,
// MAGIC   TMP_CUSTOMER.CUSTOMER_TIER,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.GIVEN_NAME) as First_Name,
// MAGIC   --changed name as present in BG doc
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.GENDER_CODE) as GENDER_CODE,
// MAGIC   TMP_CUSTOMER.GENDER,
// MAGIC   -- added to query
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.FAMILY_NAME) as Insured_Last_Name,
// MAGIC   --changed name as present in BG doc
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.MARITAL_STATUS_CODE) as MARITAL_STATUS_CODE,
// MAGIC   TMP_CUSTOMER.MARITAL_STATUS,
// MAGIC   -- added to query
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.MOBILE_PHONE_NUMBER) AS MOBILE_PHONE_NUMBER,
// MAGIC   TMP_CUSTOMER.SALUTATION_CODE,
// MAGIC   TMP_CUSTOMER.SALUTATION,
// MAGIC   -- added to query
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.NATIONALITY_CODE) AS NATIONALITY_CODE,
// MAGIC   TMP_CUSTOMER.NATIONALITY,
// MAGIC   -- added to query
// MAGIC   TMP_CUSTOMER.OCCUPATION_NAME,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.PASSPORT_NUMBER) AS PASSPORT_NUMBER,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.RESIDENTIAL_ADDRESS1) AS RESIDENTIAL_ADDRESS1,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.RESIDENTIAL_ADDRESS2) AS RESIDENTIAL_ADDRESS2,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.RESIDENTIAL_ADDRESS3) AS RESIDENTIAL_ADDRESS3,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.RESIDENTIAL_ADDRESS4) AS RESIDENTIAL_ADDRESS4,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.RESIDENTIAL_ADDRESS5) AS RESIDENTIAL_ADDRESS5,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.OCCUPATION_CLASS) AS OCCUPATION_CLASS,
// MAGIC   AES_DECRYPT(TMP_CUSTOMER.ID_CARD_NO) AS ID_CARD_NO -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   -- AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_1.POLICY_ID) as POLICY_ID,
// MAGIC   -- AES_DECRYPT(ROLE_1.CLIENT_ID) as CLIENT_ID    
// MAGIC     
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1

// COMMAND ----------

// DBTITLE 1,Create Temp View for Policy
// MAGIC %sql CREATE
// MAGIC OR REPLACE TEMP VIEW Policy_DQ AS
// MAGIC SELECT
// MAGIC   POLICY.DATA_SOURCE as DataSource,
// MAGIC   POLICY.Policy_Status as PolicyStatus,
// MAGIC   ANNUALISED_PREMIUM as ANNUALIZED_PREMIUM,  -- Spelling as agreed in the BG doc
// MAGIC   BILLED_TO_DATE,
// MAGIC   BILLING_CHANNEL,
// MAGIC   BILLING_FREQUENCY,
// MAGIC   COLLECTED_PREMIUM,
// MAGIC   COVERAGE_BASE as BASE_SUM_ASSURE,  -- Added based on DQ comments column BG for CDE= 'Base Sum Assure"
// MAGIC   POLICY_ACKNOWLEDGEMENT_DATE,
// MAGIC   EFFECTIVE_DATE,
// MAGIC   EXPIRY_DATE,
// MAGIC   FIRST_ISSUE_DATE,
// MAGIC   AES_DECRYPT(ISSUE_AGE) as ISSUE_AGE,
// MAGIC   MODAL_PREMIUM,
// MAGIC   PAID_TO_DATE,
// MAGIC   PLAN_CODE,
// MAGIC   POLICY_ISSUE_DATE,
// MAGIC   AES_DECRYPT(POLICY_NUMBER) AS POLICY_NUMBER,  /*Joining key*/
// MAGIC   POLICY_STATUS_CODE,
// MAGIC   PREMIUM_STATUS,
// MAGIC   SALES_CHANNEL,
// MAGIC   SINGLE_PREMIUM,
// MAGIC   SUM_ASSURED,
// MAGIC   REGULAR_PREMIUM,
// MAGIC   INITIAL_SUM_ASSURED,
// MAGIC   COVERAGE_RIDER as RIDER_SUM_ASSURE,  -- Updated as per BG doc
// MAGIC   ANNUAL_PREMIUM,
// MAGIC   RECEIPT_DATE,
// MAGIC   ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ANNUALISED_FIRST_YEAR_PREMIUM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   CASH_SURRENDER,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PLAN_NAME,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_STATUS_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_RECEIVED_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXTRA_LOADING as EXTRA_PREMIUM,  -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TOP_UP_PREMIUM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_TERM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_TERM as POLICY_COVERAGE_TERM,  -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TYPE_OF_UNDERWRITING,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAY_UP_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REGULAR_INVESTMENT_PREMIUM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DISCOUNT_PREMIUM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_NOTE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXCLUSION,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_TERM,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_YEAR,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_DEDUCTION,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_CONSENT,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_TYPE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECIEPT_NO,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_PAID_AMOUNT,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TRANSACTION_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION_PAYMENT_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_AMOUNT,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DIVIDEND_OPTION,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   INTEREST as DIVIDEND_INTEREST,  -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   ACCUM_DIVIDEND,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   FOLLOW_UP_CODE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REASON_CODE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REQUESTED_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECEIVED_DATE,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   LOAN_QUOTATION,  -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_1.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_1.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_1CUST.Customer_ID_Type_Code,
// MAGIC   AES_DECRYPT(ROLE_1CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY
// MAGIC   /* Customer Role Insured, Data Source IL,LA start */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'LF'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_1 ON SUBSTR(AES_DECRYPT(ROLE_1.POLICY_ID), 1, 8) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND SUBSTR(AES_DECRYPT(ROLE_1.POLICY_ID), 9, 2) = '01'
// MAGIC   AND ROLE_1.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_1CUST ON ROLE_1.CLIENT_ID = ROLE_1CUST.CUSTOMER_ID
// MAGIC   AND ROLE_1CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN Claim ON AES_DECRYPT(POLICY.CONTRACT_NUMBER) = AES_DECRYPT(Claim.CONTRACT_NUMBER)
// MAGIC   and POLICY.LIFE = Claim.LIFE
// MAGIC   and POLICY.COVERAGE = Claim.COVERAGE
// MAGIC   and POLICY.RIDER = Claim.RIDER
// MAGIC   and Claim.Data_Source in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_1CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Insured, Data Source IL,LA ENDS */
// MAGIC union all
// MAGIC SELECT
// MAGIC   POLICY.DATA_SOURCE as DataSource,
// MAGIC   POLICY.Policy_Status as PolicyStatus,
// MAGIC   ANNUALISED_PREMIUM as ANNUALIZED_PREMIUM,
// MAGIC   -- Spelling as agreed in the BG doc
// MAGIC   BILLED_TO_DATE,
// MAGIC   BILLING_CHANNEL,
// MAGIC   BILLING_FREQUENCY,
// MAGIC   COLLECTED_PREMIUM,
// MAGIC   COVERAGE_BASE as BASE_SUM_ASSURE,
// MAGIC   -- Added based on DQ comments column BG for CDE= 'Base Sum Assure"
// MAGIC   POLICY_ACKNOWLEDGEMENT_DATE,
// MAGIC   EFFECTIVE_DATE,
// MAGIC   EXPIRY_DATE,
// MAGIC   FIRST_ISSUE_DATE,
// MAGIC   AES_DECRYPT(ISSUE_AGE) as ISSUE_AGE,
// MAGIC   MODAL_PREMIUM,
// MAGIC   PAID_TO_DATE,
// MAGIC   PLAN_CODE,
// MAGIC   POLICY_ISSUE_DATE,
// MAGIC   AES_DECRYPT(POLICY_NUMBER) AS POLICY_NUMBER,
// MAGIC   /*Joining key*/
// MAGIC   POLICY_STATUS_CODE,
// MAGIC   PREMIUM_STATUS,
// MAGIC   SALES_CHANNEL,
// MAGIC   SINGLE_PREMIUM,
// MAGIC   SUM_ASSURED,
// MAGIC   REGULAR_PREMIUM,
// MAGIC   INITIAL_SUM_ASSURED,
// MAGIC   COVERAGE_RIDER as RIDER_SUM_ASSURE,
// MAGIC   -- Updated as per BG doc
// MAGIC   ANNUAL_PREMIUM,
// MAGIC   RECEIPT_DATE,
// MAGIC   ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ANNUALISED_FIRST_YEAR_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   CASH_SURRENDER,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PLAN_NAME,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_STATUS_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXTRA_LOADING as EXTRA_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TOP_UP_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_TERM as POLICY_COVERAGE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TYPE_OF_UNDERWRITING,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAY_UP_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REGULAR_INVESTMENT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DISCOUNT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_NOTE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXCLUSION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_YEAR,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_DEDUCTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_CONSENT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_TYPE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECIEPT_NO,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_PAID_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TRANSACTION_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION_PAYMENT_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DIVIDEND_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   INTEREST as DIVIDEND_INTEREST,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   ACCUM_DIVIDEND,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   FOLLOW_UP_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REASON_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REQUESTED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   LOAN_QUOTATION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_2.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_2.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_2CUST.Customer_ID_Type_Code,
// MAGIC   AES_DECRYPT(ROLE_2CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY
// MAGIC   /* Customer Role Payer, Data Source IL,LA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'PY'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_2 ON AES_DECRYPT(ROLE_2.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER) || 1
// MAGIC   AND ROLE_2.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_2CUST ON ROLE_2.CLIENT_ID = ROLE_2CUST.CUSTOMER_ID
// MAGIC   AND ROLE_2CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN Claim ON AES_DECRYPT(ROLE_2.POLICY_ID) = AES_DECRYPT(Claim.CONTRACT_NUMBER) || 1
// MAGIC   and Claim.Data_Source in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_2CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Payer, Data Source IL,LA ENDS */
// MAGIC union all
// MAGIC SELECT
// MAGIC   POLICY.DATA_SOURCE as DataSource,
// MAGIC   POLICY.Policy_Status as PolicyStatus,
// MAGIC   ANNUALISED_PREMIUM as ANNUALIZED_PREMIUM,
// MAGIC   -- Spelling as agreed in the BG doc
// MAGIC   BILLED_TO_DATE,
// MAGIC   BILLING_CHANNEL,
// MAGIC   BILLING_FREQUENCY,
// MAGIC   COLLECTED_PREMIUM,
// MAGIC   COVERAGE_BASE as BASE_SUM_ASSURE,
// MAGIC   -- Added based on DQ comments column BG for CDE= 'Base Sum Assure"
// MAGIC   POLICY_ACKNOWLEDGEMENT_DATE,
// MAGIC   EFFECTIVE_DATE,
// MAGIC   EXPIRY_DATE,
// MAGIC   FIRST_ISSUE_DATE,
// MAGIC   AES_DECRYPT(ISSUE_AGE) as ISSUE_AGE,
// MAGIC   MODAL_PREMIUM,
// MAGIC   PAID_TO_DATE,
// MAGIC   PLAN_CODE,
// MAGIC   POLICY_ISSUE_DATE,
// MAGIC   AES_DECRYPT(POLICY_NUMBER) AS POLICY_NUMBER,
// MAGIC   /*Joining key*/
// MAGIC   POLICY_STATUS_CODE,
// MAGIC   PREMIUM_STATUS,
// MAGIC   SALES_CHANNEL,
// MAGIC   SINGLE_PREMIUM,
// MAGIC   SUM_ASSURED,
// MAGIC   REGULAR_PREMIUM,
// MAGIC   INITIAL_SUM_ASSURED,
// MAGIC   COVERAGE_RIDER as RIDER_SUM_ASSURE,
// MAGIC   -- Updated as per BG doc
// MAGIC   ANNUAL_PREMIUM,
// MAGIC   RECEIPT_DATE,
// MAGIC   ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ANNUALISED_FIRST_YEAR_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   CASH_SURRENDER,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PLAN_NAME,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_STATUS_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXTRA_LOADING as EXTRA_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TOP_UP_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_TERM as POLICY_COVERAGE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TYPE_OF_UNDERWRITING,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAY_UP_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REGULAR_INVESTMENT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DISCOUNT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_NOTE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXCLUSION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_YEAR,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_DEDUCTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_CONSENT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_TYPE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECIEPT_NO,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_PAID_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TRANSACTION_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION_PAYMENT_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DIVIDEND_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   INTEREST as DIVIDEND_INTEREST,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   ACCUM_DIVIDEND,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   FOLLOW_UP_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REASON_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REQUESTED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   LOAN_QUOTATION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_3.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_3.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_3CUST.Customer_ID_Type_Code,
// MAGIC   AES_DECRYPT(ROLE_3CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY
// MAGIC   /* Customer Role Owner, Data Source IL,LA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'OW'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_3 ON SUBSTR(AES_DECRYPT(ROLE_3.POLICY_ID), 1, 8) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND ROLE_3.CLIENT_ROLE_CODE = 'OW'
// MAGIC   AND ROLE_3.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_3CUST ON ROLE_3.CLIENT_ID = ROLE_3CUST.CUSTOMER_ID
// MAGIC   AND ROLE_3CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN Claim ON AES_DECRYPT(ROLE_3.POLICY_ID) = AES_DECRYPT(Claim.CONTRACT_NUMBER)
// MAGIC   and Claim.Data_Source in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_3CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Owner, Data Source IL,LA ends */
// MAGIC union all
// MAGIC SELECT
// MAGIC   POLICY.DATA_SOURCE as DataSource,
// MAGIC   POLICY.Policy_Status as PolicyStatus,
// MAGIC   ANNUALISED_PREMIUM as ANNUALIZED_PREMIUM,
// MAGIC   -- Spelling as agreed in the BG doc
// MAGIC   BILLED_TO_DATE,
// MAGIC   BILLING_CHANNEL,
// MAGIC   BILLING_FREQUENCY,
// MAGIC   COLLECTED_PREMIUM,
// MAGIC   COVERAGE_BASE as BASE_SUM_ASSURE,
// MAGIC   -- Added based on DQ comments column BG for CDE= 'Base Sum Assure"
// MAGIC   POLICY_ACKNOWLEDGEMENT_DATE,
// MAGIC   EFFECTIVE_DATE,
// MAGIC   EXPIRY_DATE,
// MAGIC   FIRST_ISSUE_DATE,
// MAGIC   AES_DECRYPT(ISSUE_AGE) as ISSUE_AGE,
// MAGIC   MODAL_PREMIUM,
// MAGIC   PAID_TO_DATE,
// MAGIC   PLAN_CODE,
// MAGIC   POLICY_ISSUE_DATE,
// MAGIC   AES_DECRYPT(POLICY_NUMBER) AS POLICY_NUMBER,
// MAGIC   /*Joining key*/
// MAGIC   POLICY_STATUS_CODE,
// MAGIC   PREMIUM_STATUS,
// MAGIC   SALES_CHANNEL,
// MAGIC   SINGLE_PREMIUM,
// MAGIC   SUM_ASSURED,
// MAGIC   REGULAR_PREMIUM,
// MAGIC   INITIAL_SUM_ASSURED,
// MAGIC   COVERAGE_RIDER as RIDER_SUM_ASSURE,
// MAGIC   -- Updated as per BG doc
// MAGIC   ANNUAL_PREMIUM,
// MAGIC   RECEIPT_DATE,
// MAGIC   ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ANNUALISED_FIRST_YEAR_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   CASH_SURRENDER,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PLAN_NAME,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_STATUS_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXTRA_LOADING as EXTRA_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TOP_UP_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_TERM as POLICY_COVERAGE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TYPE_OF_UNDERWRITING,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAY_UP_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REGULAR_INVESTMENT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DISCOUNT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_NOTE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXCLUSION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_YEAR,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_DEDUCTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_CONSENT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_TYPE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECIEPT_NO,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_PAID_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TRANSACTION_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION_PAYMENT_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DIVIDEND_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   INTEREST as DIVIDEND_INTEREST,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   ACCUM_DIVIDEND,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   FOLLOW_UP_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REASON_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REQUESTED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   LOAN_QUOTATION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_4.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_4.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_4CUST.Customer_ID_Type_Code,
// MAGIC   AES_DECRYPT(ROLE_4CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC   ) POLICY
// MAGIC   /* Customer Role Beneficiary, Data Source IL,LA start */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code = 'BN'
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_4 ON AES_DECRYPT(ROLE_4.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND ROLE_4.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_4CUST ON ROLE_4.CLIENT_ID = ROLE_4CUST.CUSTOMER_ID
// MAGIC   AND ROLE_4CUST.DATA_SOURCE in ('IL', 'LA')
// MAGIC   LEFT JOIN Claim AS Claim ON AES_DECRYPT(ROLE_4.POLICY_ID) = AES_DECRYPT(Claim.CONTRACT_NUMBER)
// MAGIC   and Claim.Data_Source in ('IL', 'LA')
// MAGIC   and AES_DECRYPT(ROLE_4CUST.Customer_ID) is not null
// MAGIC   /* Customer Role Beneficiary, Data Source IL,LA ends */
// MAGIC union all
// MAGIC SELECT
// MAGIC   POLICY.DATA_SOURCE as DataSource,
// MAGIC   POLICY.Policy_Status as PolicyStatus,
// MAGIC   ANNUALISED_PREMIUM as ANNUALIZED_PREMIUM,
// MAGIC   -- Spelling as agreed in the BG doc
// MAGIC   BILLED_TO_DATE,
// MAGIC   BILLING_CHANNEL,
// MAGIC   BILLING_FREQUENCY,
// MAGIC   COLLECTED_PREMIUM,
// MAGIC   COVERAGE_BASE as BASE_SUM_ASSURE,
// MAGIC   -- Added based on DQ comments column BG for CDE= 'Base Sum Assure"
// MAGIC   POLICY_ACKNOWLEDGEMENT_DATE,
// MAGIC   EFFECTIVE_DATE,
// MAGIC   EXPIRY_DATE,
// MAGIC   FIRST_ISSUE_DATE,
// MAGIC   AES_DECRYPT(ISSUE_AGE) as ISSUE_AGE,
// MAGIC   MODAL_PREMIUM,
// MAGIC   PAID_TO_DATE,
// MAGIC   PLAN_CODE,
// MAGIC   POLICY_ISSUE_DATE,
// MAGIC   AES_DECRYPT(POLICY_NUMBER) AS POLICY_NUMBER,
// MAGIC   /*Joining key*/
// MAGIC   POLICY_STATUS_CODE,
// MAGIC   PREMIUM_STATUS,
// MAGIC   SALES_CHANNEL,
// MAGIC   SINGLE_PREMIUM,
// MAGIC   SUM_ASSURED,
// MAGIC   REGULAR_PREMIUM,
// MAGIC   INITIAL_SUM_ASSURED,
// MAGIC   COVERAGE_RIDER as RIDER_SUM_ASSURE,
// MAGIC   -- Updated as per BG doc
// MAGIC   ANNUAL_PREMIUM,
// MAGIC   RECEIPT_DATE,
// MAGIC   ANNUAL_PREMIUM_EQUIVALENT_EXTERNAL,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ANNUALISED_FIRST_YEAR_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   CASH_SURRENDER,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PLAN_NAME,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_STATUS_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PROPOSAL_RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXTRA_LOADING as EXTRA_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TOP_UP_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_TERM as POLICY_COVERAGE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   TYPE_OF_UNDERWRITING,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAY_UP_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REGULAR_INVESTMENT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DISCOUNT_PREMIUM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   POLICY_NOTE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   EXCLUSION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_TERM,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_DUE_YEAR,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_DEDUCTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TAX_CONSENT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PAYMENT_TYPE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECIEPT_NO,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   PREMIUM_PAID_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   TRANSACTION_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_OPTION_PAYMENT_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   ICP_AMOUNT,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   DIVIDEND_OPTION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   INTEREST as DIVIDEND_INTEREST,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.Also changed its name as agreed in BG.
// MAGIC   ACCUM_DIVIDEND,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   FOLLOW_UP_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REASON_CODE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   REQUESTED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   RECEIVED_DATE,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   LOAN_QUOTATION,
// MAGIC   -- Added one of the 90 CDEs, as agreed in BG doc.
// MAGIC   -- AES_DECRYPT(CLAIM.CONTRACT_NUMBER) as CLAIM_POLICY_NUMBER,
// MAGIC   AES_DECRYPT(POLICY.CONTRACT_NUMBER) as CONTRACT_NUMBER,
// MAGIC   -- AES_DECRYPT(ROLE_5.POLICY_ID) as POLICY_ID,
// MAGIC   AES_DECRYPT(ROLE_5.CLIENT_ID) as CLIENT_ID,
// MAGIC   ROLE_5CUST.Customer_ID_Type_Code,
// MAGIC   AES_DECRYPT(ROLE_5CUST.CUSTOMER_ID) as CUSTOMER_ID
// MAGIC FROM
// MAGIC   (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY Policy_Number,
// MAGIC             Data_Source
// MAGIC             ORDER BY
// MAGIC               EFFECTIVE_DATE DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Policy
// MAGIC       ) TMP_POLICY
// MAGIC     WHERE
// MAGIC       TMP_POLICY.CURRENTFLAG = 1
// MAGIC       and AES_DECRYPT(TMP_POLICY.POLICY_NUMBER) is not null
// MAGIC   and AES_DECRYPT(TMP_POLICY.CONTRACT_NUMBER) is not null
// MAGIC   ) POLICY
// MAGIC   /* All Customer Roles, Data Source GA starts */
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC         WHERE
// MAGIC           Client_Role_Code IN ('LF', 'OW', 'PY', 'BN')
// MAGIC       ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1
// MAGIC   ) ROLE_5 ON AES_DECRYPT(ROLE_5.POLICY_ID) = AES_DECRYPT(POLICY.CONTRACT_NUMBER)
// MAGIC   AND ROLE_5.DATA_SOURCE in ('GA')
// MAGIC   LEFT JOIN (
// MAGIC     SELECT
// MAGIC       *
// MAGIC     FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY CUSTOMER_ID,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           Customer
// MAGIC       ) TMP_CUSTOMER
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER.CURRENTFLAG = 1
// MAGIC   ) ROLE_5CUST ON ROLE_5.CLIENT_ID = ROLE_5CUST.CUSTOMER_ID
// MAGIC   AND ROLE_5CUST.DATA_SOURCE in ('GA')
// MAGIC   LEFT JOIN Claim AS Claim ON AES_DECRYPT(ROLE_5.POLICY_ID) = AES_DECRYPT(Claim.CONTRACT_NUMBER)
// MAGIC   and Claim.Data_Source = 'GA'
// MAGIC   and AES_DECRYPT(ROLE_5CUST.Customer_ID) is not null
// MAGIC   /* Customer Roles, Data Source GA ends */

// COMMAND ----------

// DBTITLE 1,Create Temp View for CustomerRole
// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW CustomerRole_DQ AS 
// MAGIC SELECT 
// MAGIC AES_DECRYPT(CLIENT_ID) as CLIENT_ID,
// MAGIC AES_DECRYPT(POLICY_ID) as POLICY_ID,
// MAGIC CLIENT_ROLE_CODE,
// MAGIC CLIENT_ROLE,
// MAGIC AES_DECRYPT(BENEFICIARY_NAME) as BENEFICIARY_NAME,
// MAGIC AES_DECRYPT(INSURED_NAME) as INSURED_NAME,
// MAGIC AES_DECRYPT(OWNER_NAME) as OWNER_NAME,
// MAGIC AES_DECRYPT(PAYER_NAME) as PAYER_NAME,
// MAGIC VALIDFLAG,
// MAGIC IS_ACTIVE,
// MAGIC DATA_SOURCE,
// MAGIC EFFECTIVE_START_DATE,
// MAGIC EFFECTIVE_END_DATE,
// MAGIC UPDATED_DATETIME 
// MAGIC FROM
// MAGIC       (
// MAGIC         SELECT
// MAGIC           *,
// MAGIC           ROW_NUMBER() OVER (
// MAGIC             PARTITION BY POLICY_ID,
// MAGIC             CLIENT_ID,
// MAGIC             CLIENT_ROLE_CODE,
// MAGIC             DATA_SOURCE
// MAGIC             ORDER BY
// MAGIC               UPDATED_DATETIME DESC
// MAGIC           ) AS CURRENTFLAG
// MAGIC         FROM
// MAGIC           CustomerRole
// MAGIC          ) TMP_CUSTOMER_ROLE
// MAGIC     WHERE
// MAGIC       TMP_CUSTOMER_ROLE.CURRENTFLAG = 1

// COMMAND ----------

// DBTITLE 1,Selecting all the data from Claim_DQ, Customer_DQ, Policy_DQ tables
val Claim_DQ =spark.sql("select * from Claim_DQ")
val Customer_DQ =spark.sql("select * from Customer_DQ")
val Policy_DQ =spark.sql("select * from Policy_DQ")
val CustomerRole_DQ =spark.sql("select * from CustomerRole_DQ")

// COMMAND ----------

// DBTITLE 1,Writing the CustomerRole_DQ table into Domain Database
CustomerRole_DQ.write
  .format("jdbc")
  .option("url", jdbc_url_CUST) 
  .option("user", jdbc_un_CustDataLoad_sqls)
  .option("password", jdbc_pwd_CustDataLoad_sqls)
  .option("useAzureMSI", "true") 
  .option("dbTable", "stg.CustomerRole_DQ") 
  .mode ("overwrite")
  .save()

// COMMAND ----------

// DBTITLE 1,Writing the Customer_DQ into Domain Database
Customer_DQ.write
  .format("jdbc")
  .option("url", jdbc_url_CUST) 
  .option("user", jdbc_un_CustDataLoad_sqls)
  .option("password", jdbc_pwd_CustDataLoad_sqls)
  .option("useAzureMSI", "true") 
  .option("dbTable", "stg.Customer_DQ") 
  .mode ("overwrite")
  .save()

// COMMAND ----------

// DBTITLE 1,Writing the Claim_DQ table into Domain Database
Claim_DQ.write
  .format("jdbc")
  .option("url", jdbc_url_CLAIM) 
  .option("user", jdbc_un_ClaimDataLoad_sqls)
  .option("password", jdbc_pwd_ClaimDataLoad_sqls)
  .option("useAzureMSI", "true") 
  .option("dbTable", "stg.Claim_DQ") 
  .mode ("overwrite")
  .save()

// COMMAND ----------

// DBTITLE 1,Write Policy DQ into Domain Database
Policy_DQ.write
  .format("jdbc")
  .option("url", jdbc_url_POLICY) 
  .option("user", jdbc_un_PolicyDataLoad_sqls)
  .option("password", jdbc_pwd_PolicyDataLoad_sqls)
  .option("useAzureMSI", "true") 
  .option("dbTable", "stg.Policy_DQ") 
  .mode ("overwrite")
  .save()
