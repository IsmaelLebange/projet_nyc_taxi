# test_phase5_analyses.py
import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
from src.config import (SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
                        SPARK_MAX_RESULT_SIZE, SPARK_SERIALIZER, OUTPUT)
from src.ingestion import charger_trajets, charger_zones
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features
from src.analyse import q1_courses_par_heure, q2_top_zones, q3_tips_par_borough, q4_pieges

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase5-Analyses") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
    .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
    .config("spark.serializer", SPARK_SERIALIZER) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("PHASE 5 : ANALYSES MÉTIER Q1 → Q4")
print("=" * 65)

df_raw = charger_trajets(spark).cache()
zones  = charger_zones(spark).cache()
df_clean = appliquer_nettoyage(df_raw, verbose=False).cache()
df_feat  = ajouter_features(df_clean).cache()

print(f"Données prêtes : {df_feat.count():,} lignes\n")

# ===== Q1 =====
print("=" * 65)
print("Q1 — COURSES PAR HEURE")
print("=" * 65)
q1 = q1_courses_par_heure(df_feat)
q1.show(24, truncate=False)

# ===== Q2 =====
print("\n" + "=" * 65)
print("Q2 — TOP 10 ZONES ACTIVES")
print("=" * 65)
q2_actives, q2_rentables = q2_top_zones(df_feat, zones)
q2_actives.show(10, truncate=False)

print("\nQ2 — TOP 10 ZONES RENTABLES")
q2_rentables.show(10, truncate=False)

# ===== Q3 =====
print("\n" + "=" * 65)
print("Q3 — TIPS PAR BOROUGH")
print("=" * 65)
q3 = q3_tips_par_borough(df_feat, zones)
q3.show(truncate=False)

# ===== Q4 =====
print("\n" + "=" * 65)
print("Q4 — VOLUMÉTRIE DES PIÈGES")
print("=" * 65)
q4 = q4_pieges(df_raw)
q4.show(truncate=False)

# ===== Sauvegarde =====
print("\n" + "=" * 65)
print("SAUVEGARDE DES RÉSULTATS")
print("=" * 65)

q1.write.mode("overwrite").parquet(f"{OUTPUT}/agregats/q1_courses_par_heure/")
q2_actives.write.mode("overwrite").parquet(f"{OUTPUT}/agregats/q2_top_zones_actives/")
q2_rentables.write.mode("overwrite").parquet(f"{OUTPUT}/agregats/q2_top_zones_rentables/")
q3.write.mode("overwrite").parquet(f"{OUTPUT}/agregats/q3_tips_par_borough/")
q4.write.mode("overwrite").parquet(f"{OUTPUT}/agregats/q4_pieges/")

print("5 dossiers écrits dans output/agregats/")

spark.stop()
print("\n✅ Phase 5 terminée")