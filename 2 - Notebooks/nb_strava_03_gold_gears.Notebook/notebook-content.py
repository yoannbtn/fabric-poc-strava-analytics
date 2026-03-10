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

# # nb_strava_03_gold_gears

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Chargement de la source Silver
df_silver_gears = spark.table("stg_strava_gears")

# 2. Génération de dim_gears (La Dimension)
dim_gears = df_silver_gears.select(
    "GearID", "Brand", "Model", "GearName", "IsPrimary", "IsRetired"
).dropDuplicates(["GearID"])

# 3. Génération de fct_gears avec logique SnapshotID
# Fenêtre pour identifier le snapshot le plus récent par équipement
window_gears = Window.partitionBy("GearID").orderBy(F.col("IngestedAt").desc())

# Calcul intermédiaire pour le type et les clés
df_gears_prep = df_silver_gears.withColumn("Rank", F.row_number().over(window_gears)) \
    .withColumn("SnapshotType", F.when(F.col("Rank") == 1, "Current").otherwise("Historical")) \
    .withColumn("SnapshotMonthKey", F.date_format("IngestedAt", "yyyyMM").cast("int"))

# Génération de la table de faits finale
fct_gears = df_gears_prep.select(
    # Clé composite pour la relation avec dim_snapshot (ex: "202602-Current")
    F.concat(F.col("SnapshotMonthKey").cast("string"), F.lit("-"), F.col("SnapshotType")).alias("SnapshotID"),
    "GearID",
    F.to_date("IngestedAt").cast("date").alias("IngestedDate"), # Pour lien avec dim_calendar
    "SnapshotType",
    "SnapshotMonthKey",
    "Distance_km",
    "Weight_kg"
)

# 4. Écriture des tables dans la couche Gold
dim_gears.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_gears")

fct_gears.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fct_gears")

print(f"--- Couche Gold Gears Finalisée ---")
print(f"Logique SnapshotType appliquée : 'Current' pour le dernier état, 'Historical' pour le passé.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.table("dim_gears")
df_check.printSchema()
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

df_check = spark.table("fct_gears")
df_check.printSchema()
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
