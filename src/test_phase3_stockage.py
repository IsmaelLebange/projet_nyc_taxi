# test_phase3_stockage.py
import sys
sys.path.insert(0, ".")

import os
from glob import glob
from pyspark.sql import SparkSession
from src.config import SPARK_DRIVER_MEMORY
from src.ingestion import charger_trajets
from src.nettoyage import appliquer_nettoyage
from src.features import ajouter_features
from src.stockage import sauvegarder_trois_versions

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase3-Stockage") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 65)
print("PHASE 3 : stockage des 3 versions")
print("=" * 65)

# 1. Ingestion + nettoyage + features
df_raw = charger_trajets(spark)
df_clean = appliquer_nettoyage(df_raw, verbose=False)
df_features = ajouter_features(df_clean).cache()

n_total = df_features.count()
print(f"\nDataFrame prêt : {n_total:,} lignes, {len(df_features.columns)} colonnes\n")

# 2. Sauvegarde des 3 versions
sauvegarder_trois_versions(df_features)

# 3. Vérification : tailles sur disque
print("\n" + "=" * 65)
print("VÉRIFICATION DES SORTIES")
print("=" * 65)

def taille_dossier(path):
    """Somme des tailles de tous les fichiers d'un dossier (récursif)."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / 1e6  # Mo

taille_csv     = taille_dossier("output/csv")
taille_flat    = taille_dossier("output/parquet_flat")
taille_part    = taille_dossier("output/parquet_partitioned")

print(f"\nCSV                : {taille_csv:>8.1f} Mo")
print(f"Parquet flat       : {taille_flat:>8.1f} Mo")
print(f"Parquet partitionné: {taille_part:>8.1f} Mo")
print(f"\nRatio CSV / Parquet flat : {taille_csv / taille_flat:.2f}×")

# 4. Vérification : nombre de fichiers
print("\n" + "=" * 65)
print("NOMBRE DE FICHIERS PAR DOSSIER")
print("=" * 65)

for dossier in ["output/csv", "output/parquet_flat", "output/parquet_partitioned"]:
    n_fichiers = sum(len(files) for _, _, files in os.walk(dossier))
    print(f"{dossier:<40} : {n_fichiers} fichier(s)")

# 5. Structure du partitionné
print("\n" + "=" * 65)
print("STRUCTURE DU PARQUET PARTITIONNÉ")
print("=" * 65)

for root, dirs, files in os.walk("output/parquet_partitioned"):
    # afficher seulement les dossiers annee= et mois=
    if "annee=" in root or "mois=" in root:
        niveau = root.count(os.sep) - "output/parquet_partitioned".count(os.sep)
        indent = "  " * niveau
        print(f"{indent}{os.path.basename(root)}/")
        if files:
            for f in files[:2]:  # afficher max 2 fichiers
                print(f"{indent}  └── {f}")

spark.stop()
print("\n✅ Phase 3 terminée")