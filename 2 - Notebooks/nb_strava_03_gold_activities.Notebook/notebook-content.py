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

# # nb_strava_03_gold_activities

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime

# 0. Chargement de la source Silver
df_silver = spark.table("stg_strava_activities")

# 1. Définition du pivot temporel (Mois actuel)
current_month_key = int(datetime.now().strftime("%Y%m"))

# 2. Définition des règles métier & Métriques (Inclusion de SportCategory)
# On définit d'abord la catégorie de sport avant le select final
df_with_categories = df_silver.withColumn(
    "SportCategory",
    F.when(F.col("SportType").isin("Run", "TrailRun"), "Running")
     .when(F.col("SportType").isin("Ride", "EBikeRide", "VirtualRide"), "Cycling")
     .when(F.col("SportType").isin("Swim"), "Swimming")
     .when(F.col("SportType").isin("Hike", "Walk"), "Walking")
     .when(F.col("SportType").isin("Workout", "HighIntensityIntervalTraining", "Rowing"), "Fitness")
     .otherwise("Others")
)

metrics_calculations = [
    F.round(F.col("Distance_m") / 1000, 2).alias("Distance_km"),
    F.round(F.col("MovingTime_s") / 60, 2).alias("MovingTime_min"),
    F.round(F.col("ElevationGain_m"), 0).alias("ElevationGain_m_final"),
    
    # Calcul de la VAM (Vitesse Ascensionnelle Moyenne en m/h)
    F.when(F.col("MovingTime_s") > 0, 
           F.round((F.col("ElevationGain_m") / F.col("MovingTime_s")) * 3600, 0)
    ).otherwise(0).alias("VAM_mh")
]

# 3. Logique de Snapshot dynamique
snapshot_logic = [
    F.when(
        F.date_format(F.col("StartDateTime"), "yyyyMM").cast("int") >= current_month_key, 
        "Current"
    ).otherwise("Historical").alias("SnapshotType"),
    
    F.date_format(F.col("StartDateTime"), "yyyyMM").cast("int").alias("SnapshotMonthKey")
]

# 4. Application des transformations et génération des clés
df_gold_temp = df_with_categories.select("*", *metrics_calculations, *snapshot_logic)

df_gold = df_gold_temp.withColumn(
    "SnapshotID",
    F.concat(F.col("SnapshotMonthKey").cast("string"), F.lit("-"), F.col("SnapshotType"))
).withColumn(
    "SnapshotActivityKey",
    F.concat_ws("-", F.col("ActivityID"), F.col("SnapshotMonthKey"), F.col("SnapshotType"))
)

# 5. Sélection finale (Structure de la table de faits Gold)
# Note : ActivityDate est castée en DATE pour les relations Power BI
fct_activities = df_gold.select(
    "SnapshotID",
    "SnapshotActivityKey",
    "ActivityID",
    "SnapshotType",
    "SnapshotMonthKey",
    F.col("StartDateTime").cast("date").alias("ActivityDate"),
    "ActivityName",
    "SportType",
    "SportCategory",
    "IsCommute",
    "Distance_km",
    "MovingTime_min",
    F.col("ElevationGain_m_final").alias("ElevationGain_m"),
    "VAM_mh",
    "AvgHeartRate",
    "MaxHeartRate",
    "AvgPower",
    "GearID",
    "Device"
)

# 6. Écriture dans le Lakehouse avec écrasement du schéma
fct_activities.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fct_activities")

# 7. Affichage des statistiques de contrôle
print(f"✅ Succès ! Table Gold 'fct_activities' mise à jour.")
print(f"Mois de référence : {current_month_key}")
print(f"Activités 'Current' : {fct_activities.filter(F.col('SnapshotType')=='Current').count()}")
print(f"Activités 'Historical' : {fct_activities.filter(F.col('SnapshotType')=='Historical').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.table("fct_activities")
df_check.printSchema()
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
