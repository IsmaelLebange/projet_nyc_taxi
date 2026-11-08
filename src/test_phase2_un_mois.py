# test_phase2_un_mois.py
import sys
import os
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
from src.config import URLS_RAW, SPARK_DRIVER_MEMORY
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase2-TestA") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("TEST A : nettoyage + features sur JANVIER 2024 uniquement")
print("=" * 65)

# Gestion du chemin local pour éviter l'erreur HttpsFileSystem
local_path = "data/raw/yellow_tripdata_2024-01.parquet"
if not os.path.exists(local_path):
    local_path = "../data/raw/yellow_tripdata_2024-01.parquet"

df = spark.read.parquet(local_path)
print(f"\nAvant nettoyage : {df.count():,} lignes\n")

df_clean = appliquer_nettoyage(df, verbose=True)

print(f"\nAprès nettoyage : {len(df_clean.columns)} colonnes")
print("Nouvelles colonnes : duree_sec, passenger_count_known, total_coherent")

df_features = ajouter_features(df_clean)
print(f"\nAprès features : {len(df_features.columns)} colonnes")
print("Nouvelles colonnes : heure, jour_semaine, mois, annee,")
print("                     est_weekend, duree_course_min, vitesse_moyenne,")
print("                     tip_rate, est_aeroport")

print("\nAperçu :")
df_features.select(
    "tpep_pickup_datetime", "heure", "duree_course_min",
    "vitesse_moyenne", "tip_rate", "est_weekend", "est_aeroport"
).show(5, truncate=False)

print("\nStatistiques des features clés :")
df_features.select(
    "heure", "duree_course_min", "vitesse_moyenne",
    "tip_rate", "est_weekend", "est_aeroport"
).describe().show()

spark.stop()
print("\n✅ Test A terminé")