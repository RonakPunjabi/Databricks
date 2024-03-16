# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/sanly/raw/jsoninputfiles/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/sanly/raw/jsoninputfiles/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/cloudthats3/formula1_processed_parquet/Ronak/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_processed_parquet/
# MAGIC

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/sanly/raw/jsoninputfiles/races.csv",header=True,inferSchema=True)

# COMMAND ----------



# COMMAND ----------


