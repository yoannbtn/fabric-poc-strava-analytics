# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a3ece0f3-09f5-4890-aa41-0a3f456d5502",
# META       "default_lakehouse_name": "lh_dp600_se",
# META       "default_lakehouse_workspace_id": "30f64b62-7548-4676-bc43-b5a2cd4af01d",
# META       "known_lakehouses": [
# META         {
# META           "id": "a3ece0f3-09f5-4890-aa41-0a3f456d5502"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_strava_02_silver_gears

# CELL ********************

from pyspark.sql import functions as F

# 1. Lecture de tous les fichiers JSON Gears
# Utilisation du wildcard pour ramasser tous les snapshots d'usure
path_gears = "Files/Bronze/Strava/Gears/*.json"

try:
    # Lecture multiline pour les objets JSON Strava
    df_raw = spark.read.option("multiline", "true").json(path_gears)

    # 2. Nettoyage et transformation vers le format Silver
    df_silver = df_raw.select(
        F.col("id").alias("GearID"),
        F.col("brand_name").alias("Brand"),
        F.col("model_name").alias("Model"),
        F.col("name").alias("GearName"),
        F.col("primary").alias("IsPrimary"),
        F.col("retired").alias("IsRetired"),
        # Conversion de la distance de mètres en kilomètres
        F.round(F.col("distance") / 1000, 2).alias("Distance_km"),
        F.col("weight").alias("Weight_kg"),
        # Timestamp technique pour le suivi de l'ingestion
        F.current_timestamp().alias("IngestedAt")
    )

    # 3. Dédoublonnage intelligent
    # On ne garde une ligne que si la distance a évolué pour ce GearID précis
    df_silver_unique = df_silver.dropDuplicates(["GearID", "Distance_km"])

    # 4. Écriture de la table Silver Gears
    # On écrase (overwrite) pour reconstruire l'historique propre à chaque run
    df_silver_unique.write.format("delta").mode("overwrite").saveAsTable("stg_strava_gears")

    print(f"Succès ! Table stg_strava_gears mise à jour.")
    print(f"Nombre d'enregistrements (états d'usure) : {df_silver_unique.count()}")

except Exception as e:
    print(f"Erreur lors du traitement Silver Gears : {str(e)}")
    if "Path does not exist" in str(e):
        print("Vérifie bien le chemin : Files/Bronze/Strava/Gears/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
