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

# # nb_strava_02_silver_activities

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Lecture du Bronze
path_bronze = "Files/Bronze/Strava/Activities/*.json"
df_raw = spark.read.option("multiline", "true").json(path_bronze)

# 2. Préparation Silver (Ton schéma validé)
df_updates = df_raw.select(
    F.col("id").cast("long").alias("ActivityID"),
    F.col("external_id").alias("GarminID"),
    F.to_timestamp(F.col("start_date_local")).alias("StartDateTime"),
    F.col("timezone").alias("Timezone"),
    F.col("name").alias("ActivityName"),
    F.col("sport_type").alias("SportType"),
    F.col("distance").cast("double").alias("Distance_m"),
    F.col("moving_time").cast("int").alias("MovingTime_s"),
    F.col("elapsed_time").cast("int").alias("ElapsedTime_s"),
    F.col("total_elevation_gain").cast("double").alias("ElevationGain_m"),
    F.col("average_speed").cast("double").alias("AvgSpeed_ms"),
    F.col("average_heartrate").cast("double").alias("AvgHeartRate"),
    F.col("max_heartrate").cast("double").alias("MaxHeartRate"),
    F.col("average_watts").cast("double").alias("AvgPower"),
    F.col("commute").cast("boolean").alias("IsCommute"),
    F.col("gear_id").alias("GearID"),
    F.col("device_name").alias("Device"),
    F.col("workout_type").cast("int").alias("WorkoutType"),
    F.current_timestamp().alias("LastUpdated")
).dropDuplicates(["ActivityID"])

# 3. Écriture intelligente (Merge)
target_table = "stg_strava_activities"

if spark.catalog.tableExists(target_table):
    print(f"Mise à jour de la table {target_table}...")
    dt = DeltaTable.forName(spark, target_table)
    dt.alias("t").merge(
        df_updates.alias("s"),
        "t.ActivityID = s.ActivityID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    print(f"Première création de la table {target_table}...")
    df_updates.write.format("delta").saveAsTable(target_table)

print(f"Terminé. La table contient désormais {spark.table(target_table).count()} activités.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
