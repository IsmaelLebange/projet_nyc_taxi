# test_phase6_ml.py
import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
from src.config import (SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
                        SPARK_MAX_RESULT_SIZE, SPARK_SERIALIZER, OUTPUT)
from src.ingestion import charger_trajets, charger_zones
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features
from src.ml import clusteriser_zones, profiler_clusters

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase6-ML") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
    .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
    .config("spark.serializer", SPARK_SERIALIZER) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("PHASE 6 : K-MEANS SUR LES ZONES")
print("=" * 65)

df = ajouter_features(
    appliquer_nettoyage(charger_trajets(spark), verbose=False)
).cache()
zones = charger_zones(spark).cache()

labels, centres = clusteriser_zones(df, zones, k=5)

print("\n" + "=" * 65)
print("PROFIL DES 5 CLUSTERS")
print("=" * 65)
profil = profiler_clusters(labels)
profil.show(truncate=False)

print("\n" + "=" * 65)
print("EXEMPLES DE ZONES PAR CLUSTER (5 premières par cluster)")
print("=" * 65)
for c in range(5):
    print(f"\n--- Cluster {c} ---")
    labels.filter(f"cluster = {c}") \
          .select("Zone", "Borough", "nb_courses", "revenu_moyen") \
          .orderBy("nb_courses", ascending=False) \
          .show(5, truncate=False)

# Sauvegarde
labels.write.mode("overwrite").parquet(f"{OUTPUT}/ml/kmeans_zones_labels/")
print("Écrit dans output/ml/kmeans_zones_labels/")

spark.stop()
print("\n✅ Phase 6 terminée")