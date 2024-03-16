# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@cloudthat.net/Day 2/formula1/includes"

# COMMAND ----------

laptimes_schema=StructType([StructField("raceId",IntegerType()),
                            StructField("driverID",IntegerType()),
                            StructField("lap",IntegerType()),
                            StructField("position",IntegerType()),
                            StructField("time",StringType()),
                            StructField("millisecond",IntegerType()),
])

# COMMAND ----------

df_laptimes=spark.read.schema(laptimes_schema).csv(f"{input_path}lap_times/",header=True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_laptimes.count()

# COMMAND ----------


