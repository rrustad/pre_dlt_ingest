# Databricks notebook source
# dbutils.widgets.removeAll()
dbutils.widgets.text('demo_path','')
demo_path=dbutils.widgets.get('demo_path')
dbutils.widgets.text('db_name','pre_dlt_injest')
db_name=dbutils.widgets.get('db_name')

# COMMAND ----------

spark.sql("""
create database if not exists hive_metastore.pre_dlt_ingest_bz;
""")
spark.sql("""
create database if not exists hive_metastore.pre_dlt_ingest_sv;
""")


# COMMAND ----------

spark.sql("""
drop database hive_metastore.pre_dlt_injest_bz;
""")
spark.sql("""
drop database hive_metastore.pre_dlt_injest_sv;
""")

# COMMAND ----------

dbutils.fs.ls(demo_path+'/landed/encounters/START_MONTH=1911-09-01 00%3A00%3A00/')

# COMMAND ----------


