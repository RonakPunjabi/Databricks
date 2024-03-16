# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/FileStore/tables/formula1/pit_stops.json")

# COMMAND ----------

df.write.saveAsTable("ronak.pit_stop")

# COMMAND ----------


