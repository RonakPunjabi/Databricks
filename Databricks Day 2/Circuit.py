# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1.Reading csv File

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Databricks test

# MAGIC %md
# MAGIC ##Databricks Push test

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Transformation

# COMMAND ----------

from pyspark.sql.functions import *
df1=(df.withColumnRenamed("circuitId","circuit_id")
.withColumnRenamed("circuitRef","circuit_ref")
.withColumn("ingestion_Date",current_timestamp())
.drop("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: writing to table

# COMMAND ----------

df1.write.saveAsTable("ronak.circuit")

# COMMAND ----------


