# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType

# 1. Définition du schéma (Manquante actuellement)
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

# 2. Définition du chemin (Vérifie qu'elle est là aussi !)
SOURCE_PATH = "/Workspace/Users/gekkouadio@gmail.com/nyc_taxi_data/"

# 3. Ton code de lecture (Celui qui est sur ton image)
df = spark.read.schema(schema).parquet(f"file:{SOURCE_PATH}*.parquet")

# COMMAND ----------

import os
print(os.listdir("/Workspace/Users/gekkouadio@gmail.com/nyc_taxi_data/"))

# COMMAND ----------

# Affiche le contenu de ta table Bronze
df_visualisation = spark.table("workspace.bronze_db.raw_trips")
display(df_visualisation)