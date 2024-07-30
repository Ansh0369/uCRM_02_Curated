// Databricks notebook source
// MAGIC %md ### Version

// COMMAND ----------

"""
in this notebook, including some common function, like update function
----------------------------------------------------
v1.0    Zhirong Li    2023-05-07
v1.1    Herbert Xie    2023-09-26    add tenacity.retry to handle ConcurrentAppendException error
"""

// COMMAND ----------

// MAGIC %md
// MAGIC ### reading the csv data to delta function

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def getDataframeFromCsv(filePath: String ,deltaTable: String )  {
  
  val jdbcDF = spark.read
                    .format("csv")
                    .option("delimiter", ",")
                    .option("header", "true")           
                    .option("inferSchema", "true")              
                    .load(filePath)
                    .withColumn("etl_create_time", lit(from_unixtime(unix_timestamp() + 28800, "yyyy-MM-dd HH:mm:ss").cast(TimestampType)))
  
  jdbcDF.write.format("delta").mode("overwrite").option("mergeSchema",true).saveAsTable(deltaTable)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## sink to ods table

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def sink2Ods(tmpTable: String ,deltaTable: String )  {
  
  var max_update_time = spark.sql(f"select max(etl_update_time) from ${deltaTable} where etl_update_time is not null").collect()(0)(0)
  
  max_update_time = if (max_update_time == null) "2023-01-01 00:00:00" else max_update_time.toString
  println(max_update_time)
  
  val tmpTB = spark.read.format("delta").table(tmpTable).where(col("last_mdf_time") > max_update_time).drop("last_mdf_time")
                    .withColumn("etl_create_time", lit(from_unixtime(unix_timestamp() + 28800, "yyyy-MM-dd HH:mm:ss").cast(TimestampType)))
                    .withColumn("etl_valid_start_time", lit(from_unixtime(unix_timestamp() + 28800, "yyyy-MM-dd HH:mm:ss").cast(TimestampType)))
                    .withColumn("etl_valid_end_time", lit("2999-12-31 23:59:59").cast(TimestampType))
                    .withColumn("etl_is_current_flag", lit("1").cast(StringType))
                    .withColumn("etl_create_by", lit("ODS").cast(StringType))
                    .withColumn("etl_update_time", lit(from_unixtime(unix_timestamp() + 28800, "yyyy-MM-dd HH:mm:ss").cast(TimestampType)))
                    .withColumn("etl_update_by", lit("ODS").cast(StringType))
                    .withColumn("etl_source_system_record_time", lit(from_unixtime(unix_timestamp() + 28800, "yyyy-MM-dd HH:mm:ss").cast(TimestampType)))
                    .withColumn("etl_source_bu", lit("TH").cast(StringType))
                    .withColumn("etl_source_system", lit("AI").cast(StringType))
                    .withColumn("etl_source_table", lit("ods_th.source_ai_recommend_crosssell_tmp").cast(StringType))
                    .withColumnRenamed("customer_id", "CustomerID")
                    .withColumnRenamed("application_id", "applicationID")
                    .withColumnRenamed("model_id", "modelID")
                    .withColumnRenamed("version_id", "VersionID")
                   .withColumn("VersionID", col("VersionID").cast(DoubleType)) 
  
  println("count-->" + tmpTB.count.toString)
  tmpTB.write.format("delta").mode("append").option("mergeSchema",true).saveAsTable(deltaTable)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## merge to fact table

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC from pyspark.sql import Window
// MAGIC from delta.tables import DeltaTable
// MAGIC from pyspark.sql.functions import md5, concat_ws
// MAGIC
// MAGIC # v1.1 add tenacity.retry to handle ConcurrentAppendException error
// MAGIC from tenacity import retry, stop_after_attempt, retry_if_exception_type
// MAGIC import delta
// MAGIC
// MAGIC @retry(reraise=True, retry=retry_if_exception_type(delta.exceptions.ConcurrentAppendException), stop=stop_after_attempt(10))
// MAGIC def commonMerge(df,delta_table_name, update_set, pk_col, order_col, md5_col_list, db_table_name = None, init = 0, op_col = None):
// MAGIC   
// MAGIC   md5_df = df.withColumn("__md5", md5(concat_ws('||', *md5_col_list)))
// MAGIC   
// MAGIC #   md5_df.createOrReplaceTempView("batch_table")
// MAGIC   
// MAGIC #   print(md5_df)
// MAGIC   
// MAGIC #   filter_sql = f"select * from (select *, row_number() over(partition by {pk_col} order by {order_col} desc) as rn from batch_table) where rn = 1"
// MAGIC   
// MAGIC #   source_df = spark.sql(filter_sql).drop("rn")
// MAGIC
// MAGIC #   windowSpec = Window.partitionBy(pk_col).orderBy(F.desc(order_col))
// MAGIC #   # row_number
// MAGIC #   md5_df = md5_df.withColumn(
// MAGIC #       "rn", 
// MAGIC #       F.row_number().over(windowSpec)
// MAGIC #   ).where(F.col("rn") == "1").drop("rn")
// MAGIC
// MAGIC   
// MAGIC   # bulit join condition 
// MAGIC   join_condition = []
// MAGIC   join_list = ["__md5"]
// MAGIC   
// MAGIC   for i in pk_col.replace(' ','').split(","):
// MAGIC     join_condition.append(f"s.{i} = t.{i}")
// MAGIC     join_list.append(i)
// MAGIC   
// MAGIC   join_condition = " AND ".join(join_condition)
// MAGIC   
// MAGIC   md5_col = ",".join(md5_col_list)
// MAGIC   ods_sql = f"select {pk_col} , coalesce(__md5, md5(concat_ws('||',{md5_col}))) as __md5  from {delta_table_name}"
// MAGIC   
// MAGIC   partition_key = pk_col.replace(" ","").split(",")
// MAGIC   print("read ods table --------------->")
// MAGIC   ods_df = spark.sql(ods_sql)#.repartition(400, *partition_key)
// MAGIC
// MAGIC   
// MAGIC #   md5_df.createOrReplaceTempView("source_table")
// MAGIC #   spark.sql(ods_sql).createOrReplaceTempView("ods_table")
// MAGIC   # match md5 sql
// MAGIC #   md5_sql = f"select * from source_table s left anti join ods_table t on {join_condition} and s.__md5 = t.__md5"
// MAGIC   
// MAGIC #   source_df = spark.sql(md5_sql)
// MAGIC   print("start to remove existing record ----> " + str(md5_df.count()) )
// MAGIC   
// MAGIC   source_df = md5_df.alias("s").join(ods_df.alias("t"), join_list, how='left_anti' )
// MAGIC
// MAGIC   deltaTable = DeltaTable.forName(spark, delta_table_name)
// MAGIC
// MAGIC   # delta lake table
// MAGIC   dfUpdates = source_df#.withColumn("__md5", md5(concat_ws('||', *md5_col_list)))
// MAGIC   
// MAGIC   print("remove done ----> "+ str(dfUpdates.count()))
// MAGIC   
// MAGIC   #built insert column
// MAGIC   col_dict = {col: "s." + col for col in dfUpdates.columns}
// MAGIC   
// MAGIC   if(dfUpdates.count() > 0):
// MAGIC
// MAGIC     deltaTable.alias('t') \
// MAGIC       .merge(
// MAGIC         source = dfUpdates.alias('s'),
// MAGIC         condition = join_condition
// MAGIC       ) \
// MAGIC       .whenMatchedUpdate(set =
// MAGIC         update_set
// MAGIC       ) \
// MAGIC       .whenNotMatchedInsert(values =
// MAGIC         col_dict
// MAGIC       ) \
// MAGIC       .execute()
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## merge to rel table

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC @retry(reraise=True, retry=retry_if_exception_type(delta.exceptions.ConcurrentAppendException), stop=stop_after_attempt(10))
// MAGIC def commonRelMerge(df, pk_col, order_col, db_table_name = None, init = 0, delta_table_name = None, delta_table_path = None, op_col = None):
// MAGIC   
// MAGIC
// MAGIC   windowSpec = Window.partitionBy(pk_col).orderBy(F.desc(order_col))
// MAGIC   # row_number
// MAGIC   batch_df = df.withColumn(
// MAGIC       "rn", 
// MAGIC       F.row_number().over(windowSpec)
// MAGIC   ).where(F.col("rn") == "1").drop("rn")#.drop(op_col, order_col)
// MAGIC
// MAGIC
// MAGIC   # bulit join condition 
// MAGIC   join_condition = []
// MAGIC   join_list = ["__md5"]
// MAGIC   
// MAGIC   for i in pk_col.replace(' ','').split(","):
// MAGIC     join_condition.append(f"s.{i} = t.{i}")
// MAGIC     join_list.append(i)
// MAGIC   
// MAGIC   join_condition = " AND ".join(join_condition)
// MAGIC   
// MAGIC   
// MAGIC     
// MAGIC #   dwd_df = spark.sql(f'''select lead_id from {delta_table_name} where is_current = 1''') 
// MAGIC
// MAGIC #   src_df = src_df.join(dwd_df, join_list, "left_anti")
// MAGIC   
// MAGIC
// MAGIC   deltaTable = DeltaTable.forName(spark, delta_table_name) if delta_table_name != None else DeltaTable.forPath(spark, delta_table_path)
// MAGIC
// MAGIC
// MAGIC   deltaTable.alias('t') \
// MAGIC     .merge(
// MAGIC       source = batch_df.alias('s'),
// MAGIC       condition = join_condition
// MAGIC     ) \
// MAGIC     .whenMatchedUpdateAll() \
// MAGIC     .whenNotMatchedInsertAll() \
// MAGIC     .execute()
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

