# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

users_schem_pyspark=StructType([StructField("driverId",IntegerType()),
                                StructField("driverRef",StringType()),
                                StructField("number",StringType()),
                                StructField("code",StringType()),
                                StructField("name",MapType(StringType(),StringType())),
                                StructField("dob",StringType()),
                                StructField("nationality",StringType())
])

# COMMAND ----------

df=spark.read.schema(users_schem_pyspark).json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("forename",col("name.forename")).withColumn("surname",col("name.surname")).drop("name").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df2=spark.read.json("dbfs:/FileStore/tables/formula1/drivers.json")

# COMMAND ----------

df1.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("ronak.drivers")

# COMMAND ----------

spark.sql("select * from ronak.circuit").display()

# COMMAND ----------

spark.sql("select * from json.`dbfs:/FileStore/tables/formula1/drivers.json`")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,name.forename from json.`dbfs:/FileStore/tables/formula1/drivers.json`

# COMMAND ----------


