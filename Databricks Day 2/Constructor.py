# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json",header=True,inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import *
df1=(df.withColumnRenamed("constructorId","constructor_Id")
.withColumn("ingestion_Date",current_timestamp())
.drop("url"))

# COMMAND ----------

df1.write.saveAsTable("ronak.const")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3B: writing to parquet FILE

# COMMAND ----------

df1.write.parquet("dbfs:/FileStore/tables/processedformual1/ronak/cirucit")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/processedformual1/ronak/cirucit")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/tables/processedformual1/ronak/cirucit`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ronak.constructor as 
# MAGIC (select constructorId as constructor_id, constructorRef as constructor_ref, name, nationality, current_timestamp() as ingestion_date from json.`dbfs:/FileStore/tables/formula1/constructors.json`)

# COMMAND ----------


