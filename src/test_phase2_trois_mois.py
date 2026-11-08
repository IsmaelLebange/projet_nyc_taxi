# test_phase2_trois_mois.py
import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import SPARK_DRIVER_MEMORY
from src.ingestion import charger_trajets
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase2-TestB") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("TEST B : nettoyage + features sur les 3 MOIS")
print("=" * 65)

df_raw = charger_trajets(spark).cache()
print(f"\nAvant nettoyage : {df_raw.count():,} lignes\n")

df_clean = appliquer_nettoyage(df_raw, verbose=True).cache()
print(f"\nAprès nettoyage : {df_clean.count():,} lignes, {len(df_clean.columns)} colonnes")

df_features = ajouter_features(df_clean).cache()
print(f"Après features : {df_features.count():,} lignes, {len(df_features.columns)} colonnes")

# Vérification finale : ce DataFrame sera celui utilisé pour le stockage
print("\nVérification colonnes de partitionnement :")
df_features.select("annee", "mois").distinct().orderBy("annee", "mois").show()

print("\nVérification features clés (moyennes globales) :")
df_features.agg(
    F.avg("heure").alias("heure_moy"),
    F.avg("duree_course_min").alias("duree_moy_min"),
    F.avg("vitesse_moyenne").alias("vitesse_moy_mph"),
    F.avg("tip_rate").alias("tip_rate_moy"),
    F.avg("est_weekend").alias("pct_weekend"),
    F.avg("est_aeroport").alias("pct_aeroport"),
).show()

spark.stop()
print("\n✅ Test B terminé")