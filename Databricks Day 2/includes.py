# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

input_path="dbfs:/mnt/cloudthats3/formula1_raw/"
output_path="dbfs:/mnt/cloudthats3/formula1_processed_parquet/"

# COMMAND ----------


