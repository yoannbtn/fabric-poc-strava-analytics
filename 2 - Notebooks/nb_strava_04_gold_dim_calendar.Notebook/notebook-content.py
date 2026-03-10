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

# # nb_strava_04_gold_dim_calendar

# CELL ********************

from pyspark.sql import functions as F

# 1. Génération de la plage de dates
start_date = "2023-01-01"
end_date = "2026-12-31"
df_dates = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as Date")

# 2. Ajout des attributs enrichis
dim_calendar = df_dates.select(
    F.col("Date"),
    F.year(F.col("Date")).alias("Year"),
    F.month(F.col("Date")).alias("MonthNumber"),
    F.date_format(F.col("Date"), "MMMM").alias("MonthName"),
    F.date_format(F.col("Date"), "yy").alias("YearShort"),
    
    # --- TRIMESTRES (Ex: Q1 24 + Tri 20241) ---
    F.concat(F.lit("Q"), F.quarter(F.col("Date"))).alias("Quarter"),
    F.concat(F.lit("Q"), F.quarter(F.col("Date")), F.lit(" "), F.date_format(F.col("Date"), "yy")).alias("QuarterYear"),
    (F.year(F.col("Date")) * 10 + F.quarter(F.col("Date"))).alias("QuarterKey"),

    # --- MOIS (Ex: Jan 24 + Tri 202401) ---
    F.date_format(F.col("Date"), "MMM yy").alias("MonthYear"), 
    F.date_format(F.col("Date"), "yyyyMM").cast("int").alias("MonthKey"),
    
    # --- SEMAINES (ISO Week Logic : Ex: W14 26 + Tri 202614) ---
    # Calcul de l'ISO Year (gère les jours de fin déc. rattachés à la semaine 1 de l'année N+1)
    F.year(F.date_sub(F.next_day(F.col("Date"), "Monday"), 4)).alias("ISO_Year"),
    F.weekofyear(F.col("Date")).alias("WeekNumber"),
    F.concat(
        F.lit("W"), 
        F.lpad(F.weekofyear(F.col("Date")).cast("string"), 2, "0"), 
        F.lit(" "), 
        F.substring(F.year(F.date_sub(F.next_day(F.col("Date"), "Monday"), 4)).cast("string"), 3, 2)
    ).alias("WeekYear"),
    (F.year(F.date_sub(F.next_day(F.col("Date"), "Monday"), 4)) * 100 + F.weekofyear(F.col("Date"))).alias("WeekKey"),

    # Jours
    F.dayofweek(F.col("Date")).alias("DayOfWeekNumber"),
    F.date_format(F.col("Date"), "EEEE").alias("DayName"),
    
    # Flags
    F.when(F.dayofweek(F.col("Date")).isin(1, 7), True).otherwise(False).alias("IsWeekend"),
    F.when(F.date_format(F.col("Date"), "yyyyMM") == F.date_format(F.current_date(), "yyyyMM"), True).otherwise(False).alias("IsCurrentMonth")
).drop("YearShort", "ISO_Year") # On nettoie les colonnes techniques de calcul

# 3. Écriture de la Dimension
dim_calendar.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_calendar")

print(f"✅ Calendrier mis à jour : {dim_calendar.count()} jours.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_check = spark.table("dim_calendar")
df_check.printSchema()
display(df_check.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
