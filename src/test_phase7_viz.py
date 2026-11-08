# test_phase7_viz.py
import sys
sys.path.insert(0, ".")

import os
import pandas as pd
from pyspark.sql import SparkSession
from src.config import (SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
                        SPARK_MAX_RESULT_SIZE, SPARK_SERIALIZER, OUTPUT)
from src.ingestion import charger_trajets, charger_zones
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features
from src.analyse import (q1_courses_par_heure, q2_top_zones,
                         q3_tips_par_borough, q4_pieges)
from src.ml import clusteriser_zones
from src.visualisation import (fig1_courses_par_heure, fig2_top_zones,
                               fig3_tips_par_borough, fig4_pieges,
                               fig5_gains, fig6_kmeans)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase7-Viz") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
    .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
    .config("spark.serializer", SPARK_SERIALIZER) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("PHASE 7 : GÉNÉRATION DES 6 GRAPHIQUES")
print("=" * 65)

# Vérifier que le dossier existe
os.makedirs(f"{OUTPUT}/graphiques", exist_ok=True)

# Ingestion + nettoyage + features
print("\nChargement des données...")
df_raw = charger_trajets(spark).cache()
zones = charger_zones(spark).cache()
df = ajouter_features(appliquer_nettoyage(df_raw, verbose=False)).cache()

# Agréger
print("Calcul des agrégats...")
q1 = q1_courses_par_heure(df).toPandas()
q2_act, q2_rent = q2_top_zones(df, zones)
q3 = q3_tips_par_borough(df, zones).toPandas()
q4 = q4_pieges(df_raw).toPandas()
labels, _ = clusteriser_zones(df, zones, k=5)
labels_pd = labels.toPandas()
gains = pd.read_csv(f"{OUTPUT}/metriques/tableau_gains.csv")

# Générer les 6 figures
print("\nGénération des graphiques :")
fig1_courses_par_heure(q1)
fig2_top_zones(q2_act.toPandas())
fig3_tips_par_borough(q3)
fig4_pieges(q4)
fig5_gains(gains)
fig6_kmeans(labels_pd)

# Vérification finale
print("\n" + "=" * 65)
print("VÉRIFICATION DES PNG GÉNÉRÉS")
print("=" * 65)
for f in sorted(os.listdir(f"{OUTPUT}/graphiques")):
    chemin = f"{OUTPUT}/graphiques/{f}"
    if f.endswith(".png"):
        taille_ko = os.path.getsize(chemin) / 1024
        print(f"  ✅ {f:<40} ({taille_ko:.0f} Ko)")

spark.stop()
print("\n✅ Phase 7 terminée — 6 PNG dans output/graphiques/")