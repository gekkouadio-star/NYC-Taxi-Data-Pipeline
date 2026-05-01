# Databricks notebook source
# --- ÉTAPE 1 : Définir les sources ---
df_bronze = spark.table("workspace.bronze_db.raw_trips")
df_zones = spark.read.option("header", "true").csv("file:/Workspace/Users/gekkouadio@gmail.com/nyc_taxi_data/taxi_zone_lookup.csv")

# --- ÉTAPE 2 : Créer df_silver (C'est ici qu'on le définit !) ---
df_silver = df_bronze.join(df_zones, df_bronze.PULocationID == df_zones.LocationID, "left") \
    .withColumnRenamed("Borough", "quartier_depart") \
    .withColumnRenamed("Zone", "zone_precise_depart") \
    .select("tpep_pickup_datetime", "trip_distance", "total_amount", "quartier_depart", "zone_precise_depart") \
    .filter("total_amount > 0")

# --- ÉTAPE 3 : Sauvegarde (Maintenant df_silver est connu) ---
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver_db")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver_db.trips_nettoyes")

print("Table Silver créée avec succès !")

# COMMAND ----------

# Vérification : est-ce qu'on a bien récupéré les noms de quartiers ?
display(df_silver.groupBy("quartier_depart").count())

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Lecture de la table Bronze
df_bronze = spark.table("workspace.bronze_db.raw_trips")

# 2. Lecture du fichier de référence (Zones)
df_zones = spark.read.option("header", "true").csv("file:/Workspace/Users/gekkouadio@gmail.com/nyc_taxi_data/taxi_zone_lookup.csv")

# 3. Jointure et Sélection (On ajoute dropDuplicates ici)
df_silver = df_bronze.join(df_zones, df_bronze.PULocationID == df_zones.LocationID, "left") \
    .withColumnRenamed("Borough", "quartier_depart") \
    .withColumnRenamed("Zone", "zone_precise_depart") \
    .select(
        col("tpep_pickup_datetime").cast("timestamp"), 
        col("trip_distance").cast("double"), 
        col("total_amount").cast("double"), 
        "quartier_depart", 
        "zone_precise_depart"
    ) \
    .dropDuplicates() # CRUCIAL : Évite de compter deux fois le même trajet

# 4. Nettoyage poussé
# On retire les prix aberrants et les trajets de 0 km
df_silver = df_silver.filter((col("total_amount") > 0) & (col("trip_distance") > 0))

# 5. Sauvegarde avec écrasement du schéma (pour corriger ton erreur précédente)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver_db")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver_db.trips_nettoyes")

# 6. Vérification finale
print(f"Nombre de lignes en Silver : {df_silver.count()}")
display(df_silver.limit(10))