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

# # nb_strava_04_gold_dim_snapshot

# CELL ********************

from pyspark.sql import functions as F

# 1. Récupération des combinaisons uniques depuis les tables de faits Gold
# On s'assure que la table de snapshot contient toutes les versions existantes
df_snap_act = spark.table("fct_activities").select("SnapshotMonthKey", "SnapshotType")
df_snap_gear = spark.table("fct_gears").select("SnapshotMonthKey", "SnapshotType")

# 2. Union et dédoublonnage pour créer le référentiel unique
dim_snapshot = df_snap_act.union(df_snap_gear).distinct()

# 3. Création de la Clé Composite "SnapshotID" (ex: "202602-Current")
# Cette clé sera ton point d'ancrage pour les relations dans le modèle sémantique
dim_snapshot = dim_snapshot.withColumn(
    "SnapshotID", 
    F.concat(F.col("SnapshotMonthKey").cast("string"), F.lit("-"), F.col("SnapshotType"))
)

# 4. Ajout de colonnes de libellés pour faciliter le reporting Power BI
# Exemple : "Février 2026 (Current)"
dim_snapshot = dim_snapshot.withColumn(
    "SnapshotLabel",
    F.concat(
        F.col("SnapshotMonthKey").cast("string"), 
        F.lit(" ("), 
        F.col("SnapshotType"), 
        F.lit(")")
    )
)

# 5. Écriture de la table Dimension Gold
dim_snapshot.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_snapshot")

print(f"✅ Table 'dim_snapshot' créée avec succès.")
print(f"Nombre de snapshots référencés : {dim_snapshot.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.table("dim_snapshot")
df_check.printSchema()
display(df_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
