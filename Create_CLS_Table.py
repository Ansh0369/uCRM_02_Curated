# Databricks notebook source
# MAGIC %md ## Version

# COMMAND ----------

"""
in this notebook, including 1 ODS and 4 dwd table create scripts.
----------------------------------------------------
v1.0    Herbert Xie    2022-10-10
v1.1    Herbert Xie    2022-10-14    remove 3 ods and 2 dwd table create scripts
v1.2    Herbert Xie    2022-10-17    add __md5 column in 4 dwd table
v1.3    Herbert Xie    2022-10-20    add new version column and column "__timestamp" to ods_th.ldp_source_lead_dim
v1.4    Herbert Xie    2022-10-24    modify ods_th.ldp_source_lead_dim, save only activityLogList column
v1.5    Herbert Xie    2022-10-25    modify dwd_th.ldp_lead_curr_dim, set pii column data type to string
v1.6    Herbert Xie    2022-11-09    modify dwd table name and folder path
v1.7    Herbert Xie    2022-12-05    add ddl of dwd tables: tb_cmpn_lead_customer_rel and tb_ai_recommend_crosssell; update ddl of dwd_th.tb_cmpn_lead_curr_dim
v1.8    Herbert Xie    2022-12-07    update tb_ai_recommend_crosssell: columns top1 to top5 as product_code
v1.9    Herbert Xie    2022-12-08    update ldp_lead_curr_dim lead_score from int to string
v1.10    Herbert Xie    2023-03-01    add ods_th.dh_lead_common_ingest
v1.11    Herbert Xie    2023-04-06    add ods_th.cube_lead_activity
v1.12    Zhirong LI    2023-04-20    add plu tables
v1.13    Herbert Xie    2023-04-26    add preferred_contact_time, annual_income_currency, monthly_income_currency to curr_dim ddl
v1.14    Zhirong LI    2023-08-04    add ods_th.underwriting new table
v1.15    Herbert Xie    2023-12-20    add agent_number to ods_th.underwriting
"""

# COMMAND ----------

# MAGIC %md # ODS

# COMMAND ----------

# MAGIC %md ## ods_th.ldp_source_lead_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v1.3 add new version column and column "__timestamp" to ods_th.ldp_source_lead_dim
# MAGIC -- v1.4 modify ods_th.ldp_source_lead_dim, save only activityLogList column
# MAGIC DROP TABLE IF EXISTS ods_th.ldp_source_lead_dim;
# MAGIC CREATE TABLE ods_th.ldp_source_lead_dim(
# MAGIC clientType string,
# MAGIC leadStatusCode string,
# MAGIC `name` STRUCT<`en`: STRING>,
# MAGIC preferredLang string,
# MAGIC age string,
# MAGIC productInterestedTypeCode string,
# MAGIC `productInterested` STRUCT<`en_th`: STRING, `th`: STRING>,
# MAGIC mobileNumber string,
# MAGIC phoneMobile string,
# MAGIC emailAddress string,
# MAGIC createdTime long,
# MAGIC updatedTime long,
# MAGIC sourceTypeCode string,
# MAGIC id integer,
# MAGIC acknowledged boolean,
# MAGIC status string,
# MAGIC agentCode string,
# MAGIC birthDate string,
# MAGIC contactedFlag boolean,
# MAGIC appointmentFlag boolean,
# MAGIC illustrationFlag boolean,
# MAGIC submittedFlag boolean,
# MAGIC activityLogList array<STRUCT<
# MAGIC     leadActivityLogId: integer,
# MAGIC     agencyId: integer,
# MAGIC     leadId: integer,
# MAGIC     activityType: string,
# MAGIC     activityDate: long,
# MAGIC     feedback: string,
# MAGIC     createdDate: long,
# MAGIC     deleted: string,
# MAGIC     updatedDate: long,
# MAGIC     rreFbId: integer,
# MAGIC     reasonCode: string,
# MAGIC     reasonDesc: string,
# MAGIC     feedbackDetails: string>>,
# MAGIC __timestamp timestamp
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/ldp_source_lead_dim'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true) ;

# COMMAND ----------

# MAGIC %md ## ods_th.dh_lead_common_ingest

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v1.10 add ods_th.dh_lead_common_ingest
# MAGIC DROP TABLE IF EXISTS ods_th.dh_lead_common_ingest;
# MAGIC CREATE TABLE ods_th.dh_lead_common_ingest(
# MAGIC source_lead_id  string,
# MAGIC source_system  string,
# MAGIC lead_type  string,
# MAGIC lead_salutation  string,
# MAGIC lead_name_lang  string,
# MAGIC lead_full_name  string,
# MAGIC lead_first_name  string,
# MAGIC lead_middle_name  string,
# MAGIC lead_last_name  string,
# MAGIC lead_name_suffix  string,
# MAGIC lead_extension_name  string,
# MAGIC title  string,
# MAGIC lead_birth_date  string,
# MAGIC lead_gender_code  string,
# MAGIC address_country  string,
# MAGIC address_province  string,
# MAGIC address_city  string,
# MAGIC address_postal_code  string,
# MAGIC address_line_1  string,
# MAGIC address_line_2  string,
# MAGIC address_line_3  string,
# MAGIC address_line_4  string,
# MAGIC address_line_5  string,
# MAGIC lead_email_address  string,
# MAGIC phone_info  array<STRUCT<
# MAGIC     phone_type: string,
# MAGIC     phone_country_code: string,
# MAGIC     phone_area_code: string,
# MAGIC     phone_num: string>>,
# MAGIC government_id_num  string,
# MAGIC passport_num  string,
# MAGIC preferred_contact_date  string,
# MAGIC company_name  string,
# MAGIC occupation_code  string,
# MAGIC occupation  string,
# MAGIC occupation_group_code  string,
# MAGIC occupation_group  string,
# MAGIC occupation_industry_code  string,
# MAGIC occupation_industry  string,
# MAGIC occupation_class_code  string,
# MAGIC occupation_class  string,
# MAGIC marital  boolean,
# MAGIC is_smoke  boolean,
# MAGIC annual_income_range  string,
# MAGIC monthly_income_range  string,
# MAGIC number_of_kids  int,
# MAGIC campaign_code  string,
# MAGIC servicing_bank_lead_id  string,
# MAGIC servicing_bank_branch_code  string,
# MAGIC preferred_bank_branch_code  string,
# MAGIC servicing_bank_agent_id  string,
# MAGIC lead_valid_status  string,
# MAGIC inquire_reason  string,
# MAGIC tsr  string,
# MAGIC lead_score  string,
# MAGIC channel_id  string,
# MAGIC agent_code  string,
# MAGIC customer_id  string,
# MAGIC lead_status  string,
# MAGIC status_date  string,
# MAGIC product_recommended_type_code  string,
# MAGIC product_recommended  string,
# MAGIC operation  string,
# MAGIC operation_time  string,
# MAGIC __timestamp timestamp
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/dh_lead_common_ingest/20230301'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true) ;

# COMMAND ----------

# MAGIC %md ## ods_th.cube_lead_activity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v1.11 add ods_th.cube_lead_activity
# MAGIC DROP TABLE IF EXISTS ods_th.cube_lead_activity;
# MAGIC CREATE TABLE ods_th.cube_lead_activity(
# MAGIC lead_id  string,
# MAGIC lead_action  string,
# MAGIC action_datetime  string,
# MAGIC lost_reason  string,
# MAGIC __timestamp timestamp
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/cube_lead_activity/20230406'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true) ;

# COMMAND ----------

# MAGIC %md ## ods_th.plu_ai_result

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.plu_ai_result;
# MAGIC CREATE TABLE ods_th.plu_ai_result(
# MAGIC       `id`                            String,
# MAGIC       transaction_id                  String,
# MAGIC       session_id                      String,
# MAGIC       user_request                    String,
# MAGIC       gender_code                     String,
# MAGIC       age                             String,
# MAGIC       birth_date                      String,
# MAGIC       smoking_status                  String,
# MAGIC       marital_status                  String,
# MAGIC       occupation                      String,
# MAGIC       monthly_income_range            String,
# MAGIC       monthly_income_currency         String,
# MAGIC       number_of_kids                  String,
# MAGIC       ai_request                      String,
# MAGIC       ai_response                     String,
# MAGIC       plu_response                    String,
# MAGIC       ai_transaction_id               String,
# MAGIC       create_time                     timestamp,
# MAGIC       update_time                     timestamp,
# MAGIC       create_datetime                 timestamp,
# MAGIC       update_datetime                 timestamp,
# MAGIC       __timestamp                     timestamp,
# MAGIC       operation                       STRING
# MAGIC
# MAGIC
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/plu_ai_result'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ods_th.plu_lead_contact_info

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.plu_lead_contact_info;
# MAGIC CREATE TABLE ods_th.plu_lead_contact_info(
# MAGIC     `id`                          String,
# MAGIC     transaction_id                String,
# MAGIC     session_id                    String,
# MAGIC     site_token                    String,
# MAGIC     request_to_contact            int,
# MAGIC     first_name                    String,
# MAGIC     last_name                     String,
# MAGIC     phone_num                     String,
# MAGIC     booked_time                   timestamp,
# MAGIC     booked_hour_code              int,
# MAGIC     create_time                   timestamp,
# MAGIC     update_time                   timestamp,
# MAGIC     create_datetime               timestamp,
# MAGIC     update_datetime               timestamp,
# MAGIC     __timestamp                   timestamp,
# MAGIC     operation                     STRING
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/plu_lead_contact_info'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter table ods_th.plu_lead_contact_info add column booked_hour_code int  -- 20230630 add
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ods_th.plu_lead_product_info

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.plu_lead_product_info;
# MAGIC CREATE TABLE ods_th.plu_lead_product_info(
# MAGIC     `id`                          string,
# MAGIC     transaction_id                string,
# MAGIC     session_id                    string,
# MAGIC     site_token                    string,
# MAGIC     selected_product_category     string,
# MAGIC     selected_product_code         string,
# MAGIC     selected_plan_code            string,
# MAGIC     selected_premium              DECIMAL(24,4),
# MAGIC     selected_premium_currency     CHAR(3),
# MAGIC     selected_promo_code           string,
# MAGIC     selected_reward_code          string,
# MAGIC     create_time                   timestamp,
# MAGIC     channel                       string,
# MAGIC     create_datetime               timestamp,
# MAGIC     update_datetime               timestamp,
# MAGIC     __timestamp                   timestamp,
# MAGIC     operation                     STRING
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/plu_lead_product_info'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## ods_th.plu_protection_score

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.plu_protection_score;
# MAGIC CREATE TABLE ods_th.plu_protection_score(
# MAGIC     `id`                          string,
# MAGIC     transaction_id                string,
# MAGIC     session_id                    string,
# MAGIC     site_token                    string,
# MAGIC     current_total_score           TINYINT,
# MAGIC     average_total_score           TINYINT,
# MAGIC     create_time                   timestamp,
# MAGIC     create_datetime               timestamp,
# MAGIC     update_datetime               timestamp,
# MAGIC     __timestamp                   timestamp,
# MAGIC     operation                     STRING
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/plu_protection_score'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## ods_th.plu_protection_category_score

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.plu_protection_category_score;
# MAGIC CREATE TABLE ods_th.plu_protection_category_score(
# MAGIC       `id`                          string,
# MAGIC       score_id                      string,
# MAGIC       session_id                    string,
# MAGIC       site_token                    string,
# MAGIC       product_category_code         string,
# MAGIC       average_score                 TINYINT,
# MAGIC       current_score                 TINYINT,
# MAGIC       coverage_benchmark_amount     DECIMAL(24,4),
# MAGIC       coverage_benchmark_currency   CHAR(3),
# MAGIC       coverage_average_amount       DECIMAL(24,4),
# MAGIC       coverage_average_currency     CHAR(3),
# MAGIC       coverage_selected_amount      DECIMAL(24,4),
# MAGIC       coverage_selected_currency    CHAR(3),
# MAGIC       coverage_gap                  DECIMAL(24,4),
# MAGIC       score_impact                  TINYINT,
# MAGIC       create_time                   timestamp,
# MAGIC       create_datetime               timestamp,
# MAGIC       update_datetime               timestamp,
# MAGIC       __timestamp                   timestamp,
# MAGIC       operation                     STRING
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/plu_protection_category_score'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md ## ods_th.underwriting

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v1.15 add agent_number to ods_th.underwriting
# MAGIC ALTER TABLE ods_th.underwriting_pa add COLUMN agent_number STRING;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ods_th.underwriting_pa;
# MAGIC CREATE TABLE ods_th.underwriting_pa(
# MAGIC   lead_id string,
# MAGIC   customer_id string,
# MAGIC   Pre_approval_flag string,
# MAGIC   SI_limit  DOUBLE,
# MAGIC   UW_type string,
# MAGIC   expiration_date 	date,
# MAGIC   agent_number string,
# MAGIC   etl_valid_start_time timestamp,
# MAGIC   etl_valid_end_time timestamp,
# MAGIC   etl_is_current_flag string,
# MAGIC   etl_create_time timestamp,
# MAGIC   etl_create_by string,
# MAGIC   etl_update_time timestamp,
# MAGIC   etl_update_by string,
# MAGIC   etl_source_system_record_time timestamp,
# MAGIC   etl_source_bu string,
# MAGIC   etl_source_system string,
# MAGIC   etl_source_table string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/ODS/uw_underwriting_pa'
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md # DWD

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_cmpn_lead_agent_rel

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_cmpn_lead_agent_rel;
# MAGIC CREATE TABLE dwd_th.tb_cmpn_lead_agent_rel(
# MAGIC agent_rel_id string,
# MAGIC lead_id string,
# MAGIC source_lead_id string,
# MAGIC channel_id string,
# MAGIC agent_code string,
# MAGIC assign_date date,
# MAGIC agent_status string,
# MAGIC start_date timestamp,
# MAGIC end_date timestamp,
# MAGIC update_date timestamp,
# MAGIC is_current string,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_cmpn_lead_agent_rel' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_cmpn_lead_status_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_cmpn_lead_status_dim;
# MAGIC CREATE TABLE dwd_th.tb_cmpn_lead_status_dim(
# MAGIC lead_status_id string,
# MAGIC lead_id string,
# MAGIC source_lead_id string,
# MAGIC lead_status string,
# MAGIC status_date date,
# MAGIC start_date timestamp,
# MAGIC end_date timestamp,
# MAGIC update_date timestamp,
# MAGIC is_current string,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_cmpn_lead_status_dim' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_txn_lead_product_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_txn_lead_product_fact;
# MAGIC CREATE TABLE dwd_th.tb_txn_lead_product_fact(
# MAGIC product_fact_id string,
# MAGIC lead_id string,
# MAGIC source_lead_id string,
# MAGIC product_recommended_type_code string,
# MAGIC product_recommended string,
# MAGIC first_prod_name string,
# MAGIC second_prod_name string,
# MAGIC third_prod_name string,
# MAGIC bought_agency_product string,
# MAGIC purchase_date date,
# MAGIC product_purchased string,
# MAGIC ape string,
# MAGIC ps_cluster string,
# MAGIC plan_code string,
# MAGIC plan_category string,
# MAGIC product_category_coverage_benchmark string,
# MAGIC product_category_coverage_medium string,
# MAGIC first_coverage_level_prod_name string,
# MAGIC second_coverage_level_prod_name string,
# MAGIC third_coverage_level_prod_name string,
# MAGIC update_date timestamp,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_txn_lead_product_fact' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_cmpn_lead_curr_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC --v1.5 modify dwd_th.ldp_lead_curr_dim, set pii column data type to string
# MAGIC --v1.7 update ddl of dwd_th.tb_cmpn_lead_curr_dim
# MAGIC --v1.13 add preferred_contact_time, annual_income_currency, monthly_income_currency to curr_dim ddl
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_cmpn_lead_curr_dim;
# MAGIC CREATE TABLE dwd_th.tb_cmpn_lead_curr_dim(
# MAGIC lead_id string,
# MAGIC source_lead_id string,
# MAGIC source_system string,
# MAGIC lead_type string,
# MAGIC lead_salutation string,
# MAGIC lead_name_lang string,
# MAGIC lead_full_name string,
# MAGIC lead_first_name string,
# MAGIC lead_middle_name string,
# MAGIC lead_last_name string,
# MAGIC lead_name_suffix string,
# MAGIC lead_extension_name string,
# MAGIC title string,
# MAGIC lead_birth_date string,
# MAGIC lead_gender_code string,
# MAGIC address_country string,
# MAGIC address_province string,
# MAGIC address_city string,
# MAGIC address_postal_code string,
# MAGIC address_line_1 string,
# MAGIC address_line_2 string,
# MAGIC address_line_3 string,
# MAGIC address_line_4 string,
# MAGIC address_line_5 string,
# MAGIC address_reserve_column1 string,
# MAGIC address_reserve_column2 string,
# MAGIC lead_email_address string,
# MAGIC mobile_phone_country_code string,
# MAGIC mobile_phone_area_code string,
# MAGIC mobile_phone_num string,
# MAGIC home_phone_country_code string,
# MAGIC home_phone_area_code string,
# MAGIC home_phone_num string,
# MAGIC office_phone_country_code string,
# MAGIC office_phone_area_code string,
# MAGIC office_phone_num string,
# MAGIC phone_country_code4 string,
# MAGIC phone_area_code4 string,
# MAGIC phone_num4 string,
# MAGIC phone_reserve_column1 string,
# MAGIC phone_reserve_column2 string,
# MAGIC government_id_num string,
# MAGIC passport_num string,
# MAGIC preferred_contact_date date,
# MAGIC preferred_contact_time timestamp,
# MAGIC company_name string,
# MAGIC occupation_code string,
# MAGIC occupation string,
# MAGIC occupation_group_code string,
# MAGIC occupation_group string,
# MAGIC occupation_industry_code string,
# MAGIC occupation_industry string,
# MAGIC occupation_class_code string,
# MAGIC occupation_class string,
# MAGIC marital string,
# MAGIC is_smoke string,
# MAGIC annual_income_range string,
# MAGIC annual_income_currency string,
# MAGIC monthly_income_range string,
# MAGIC monthly_income_currency string,
# MAGIC number_of_kids int,
# MAGIC campaign_code string,
# MAGIC servicing_bank_lead_id string,
# MAGIC servicing_bank_branch_code string,
# MAGIC preferred_bank_branch_code string,
# MAGIC servicing_bank_agent_id string,
# MAGIC lead_valid_status string,
# MAGIC inquire_reason string,
# MAGIC tsr string,
# MAGIC lead_score string,
# MAGIC reserve_column1 string,
# MAGIC reserve_column2 string,
# MAGIC reserve_column3 string,
# MAGIC reserve_column4 string,
# MAGIC reserve_column5 string,
# MAGIC reserve_column6 string,
# MAGIC reserve_column7 string,
# MAGIC reserve_column8 string,
# MAGIC reserve_column9 string,
# MAGIC reserve_column10 string,
# MAGIC etl_valid_start_time timestamp,
# MAGIC etl_valid_end_time timestamp,
# MAGIC etl_is_current_flag string,
# MAGIC etl_create_time timestamp,
# MAGIC etl_create_by string,
# MAGIC etl_update_time timestamp,
# MAGIC etl_update_by string,
# MAGIC etl_source_system_record_time timestamp,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC etl_source_table string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_cmpn_lead_curr_dim' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter table dwd_th.tb_cmpn_lead_curr_dim add columns (preferred_contact_time timestamp); 
# MAGIC -- alter table dwd_th.tb_cmpn_lead_curr_dim add columns (annual_income_currency string); 
# MAGIC -- alter table dwd_th.tb_cmpn_lead_curr_dim add columns (monthly_income_currency string); 

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_cmpn_lead_customer_rel

# COMMAND ----------

# MAGIC %sql
# MAGIC --v1.7 add ddl of dwd tables: tb_cmpn_lead_customer_rel and tb_ai_recommend_crosssell
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_cmpn_lead_customer_rel;
# MAGIC CREATE TABLE dwd_th.tb_cmpn_lead_customer_rel(
# MAGIC lead_id string,
# MAGIC source_lead_id string,
# MAGIC customer_id string,
# MAGIC etl_valid_start_time timestamp,
# MAGIC etl_valid_end_time timestamp,
# MAGIC etl_is_current_flag string,
# MAGIC etl_create_time timestamp,
# MAGIC etl_create_by string,
# MAGIC etl_update_time timestamp,
# MAGIC etl_update_by string,
# MAGIC etl_source_system_record_time timestamp,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC etl_source_table string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_cmpn_lead_customer_rel' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %md ## dwd_th.tb_ai_recommend_crosssell

# COMMAND ----------

# MAGIC %sql
# MAGIC --v1.7 add ddl of dwd tables: tb_cmpn_lead_customer_rel and tb_ai_recommend_crosssell
# MAGIC DROP TABLE IF EXISTS dwd_th.tb_ai_recommend_crosssell;
# MAGIC CREATE TABLE dwd_th.tb_ai_recommend_crosssell(
# MAGIC lead_id string,
# MAGIC order integer,
# MAGIC product_code string,
# MAGIC score double,
# MAGIC rank string,
# MAGIC application_id string,
# MAGIC model_id string,
# MAGIC version_id string,
# MAGIC etl_valid_start_time timestamp,
# MAGIC etl_valid_end_time timestamp,
# MAGIC etl_is_current_flag string,
# MAGIC etl_create_time timestamp,
# MAGIC etl_create_by string,
# MAGIC etl_update_time timestamp,
# MAGIC etl_update_by string,
# MAGIC etl_source_system_record_time timestamp,
# MAGIC etl_source_bu string,
# MAGIC etl_source_system string,
# MAGIC etl_source_table string,
# MAGIC __md5 string
# MAGIC )USING DELTA 
# MAGIC LOCATION '/mnt/adls_th/TH/FWD/IN/CLS/DeltaTable/DWD/tb_ai_recommend_crosssell' 
# MAGIC TBLPROPERTIES (DELTA.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true, delta.enableChangeDataFeed = true) ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ALTER TABLE ods_th.plu_ai_result add column update_time timestamp

# COMMAND ----------

