# Databricks notebook source
from pyspark.sql.functions import avg, count, round, col, desc

# 1. Lecture de la table Silver (Données propres)
df_silver = spark.table("workspace.silver_db.trips_nettoyes")

# 2. Création de l'agrégation (Le rapport Business)
# Nous voulons connaître le prix moyen et la distance moyenne par quartier
df_gold = df_silver.groupBy("quartier_depart") \
    .agg(
        round(avg("total_amount"), 2).alias("prix_moyen"),
        round(avg("trip_distance"), 2).alias("distance_moyenne"),
        count("*").alias("nombre_de_courses")
    ) \
    .filter("quartier_depart IS NOT NULL AND quartier_depart != 'Unknown'") \
    .orderBy(desc("nombre_de_courses"))

# 3. Sauvegarde de la table finale
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold_db")

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.gold_db.rapport_mensuel_quartiers")

# 4. Visualisation
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nouveau bloc : Rapport Nettoyé (Sans N/A ni Unknown)

# COMMAND ----------

# 1. INDISPENSABLE : Charger la table créée par le notebook Silver
df_silver = spark.table("workspace.silver_db.trips_nettoyes")

# 2. Importer les fonctions nécessaires
from pyspark.sql.functions import col, avg, count, round, desc

# 3. Créer le rapport sans les N/A et Unknown
df_final_report = df_silver.filter(
    (col("quartier_depart") != "N/A") & 
    (col("quartier_depart") != "Unknown") &
    (col("quartier_depart").isNotNull())
) \
.groupBy("quartier_depart") \
.agg(
    round(avg("total_amount"), 2).alias("prix_moyen"),
    round(avg("trip_distance"), 2).alias("distance_moyenne"),
    count("*").alias("nombre_de_courses")
) \
.orderBy(desc("prix_moyen"))

# 4. Afficher le résultat
display(df_final_report)