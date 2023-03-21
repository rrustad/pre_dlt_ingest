# Databricks notebook source
from delta.tables import *
from table_config import table_configs

# COMMAND ----------

def update_bronze_table(table_config):
  # Bronze Tables are Append only
  (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", table_config['schema_path'])
    .option('header', True)
    .load(table_config['input_path'])
    .writeStream
    .trigger(once=True)
    .option("mergeSchema", "true")
    .option("checkpointLocation", table_config['checkpoint_path'])
    .option("path", table_config['output_path'])
    .toTable(table_config['table_name'])
  )
  spark.sql(f'vacuum {table_config["table_name"]}')
  if table_config["zorderby"]:
    spark.sql(f'optimize {table_config["table_name"]} ZORDER BY ({", ".join(table_config["zorderby"])})')
  


# COMMAND ----------

def update_silver_table_scd1(table_config):
  sdf = (
    spark.readStream
    .format("delta")
    .load(table_config['input_path'])
  )
    
  if not DeltaTable.isDeltaTable(spark, table_config['output_path']):
    (sdf.writeStream
    .trigger(once=True)
    .option("mergeSchema", "true")
    .option("checkpointLocation", table_config['checkpoint_path'])
    .option("path", table_config['output_path'])
    .toTable(table_config['table_name']))
  else:
    deltaTable = DeltaTable.forPath(spark, table_config['output_path'])

    # Function to upsert microBatchOutputDF into Delta table using merge
    def upsertToDelta(microBatchOutputDF, batchId):
      (deltaTable.alias("t").merge(
          microBatchOutputDF.alias("s"),
          " and ".join([f"s.{x} = t.{x}" for x in table_config['merge_key']]))
  #         f"s.{table_config['merge_key']} = t.{table_config['merge_key']}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
      )

    (sdf.writeStream
    .format("delta")
    .trigger(once=True)
    .foreachBatch(upsertToDelta)
    .option("checkpointLocation", table_config['checkpoint_path'])
    .outputMode("update")
    .start()
  )
  


# COMMAND ----------

for table_config in table_configs:
  if table_config['workflow'] == 1:
    print(table_config['table_name'])
    update_bronze_table(table_config)
# update_silver_table_scd1(table_config)

# COMMAND ----------

for table_config in table_configs:
  if table_config['workflow'] == 2:
    print(table_config['table_name'])
#     update_bronze_table(table_config)
    update_silver_table_scd1(table_config)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history hive_metastore.pre_dlt_ingest_sv.procedures

# COMMAND ----------

# dbutils.fs.rm('dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest/output/silver/encounters', True)

# COMMAND ----------

# dbutils.fs.rm(table_config['output_path'], True)
# dbutils.fs.rm(table_config['checkpoint_path'], True)
# dbutils.fs.rm(table_config['schema_path'], True)
# spark.sql('drop database hive_metastore.pre_dlt_ingest_bz cascade')
# spark.sql('drop database hive_metastore.pre_dlt_ingest_sv cascade')


# COMMAND ----------

# table_config['output_path']

# COMMAND ----------

# dbutils.fs.rm('dbfs:/home/riley.rustad@databricks.com/pre_dlt_ingest', True)

# COMMAND ----------


