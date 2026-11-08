# src/metriques.py — G1 → G5, tout en local, tout intégré
import os
import time
import gc
from glob import glob

import pandas as pd
import psutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.config import OUTPUT, DATA_RAW_DIR


def _taille_mo(pattern: str) -> float:
    return sum(os.path.getsize(f) for f in glob(pattern)) / 1e6


def _timer(fn):
    t0 = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - t0


# ============================================================
# G1 — Taille sur disque : CSV vs Parquet flat
# ============================================================
def g1_taille_disque() -> dict:
    print("\n" + "=" * 60)
    print("G1 — TAILLE SUR DISQUE")
    print("=" * 60)

    taille_csv  = _taille_mo(f"{OUTPUT}/csv/*.csv")
    taille_flat = _taille_mo(f"{OUTPUT}/parquet_flat/*.parquet")
    gain = taille_csv / taille_flat if taille_flat > 0 else float("inf")

    print(f"  CSV          : {taille_csv:>8.1f} Mo")
    print(f"  Parquet flat : {taille_flat:>8.1f} Mo")
    print(f"  Gain         : {gain:>8.2f}×")

    return {"metrique": "G1_taille_disque",
            "naif_mo": round(taille_csv, 1),
            "optimise_mo": round(taille_flat, 1),
            "gain": round(gain, 2)}


# ============================================================
# G2 — Lecture complète : CSV vs Parquet flat
# ============================================================
def g2_lecture_complete(spark: SparkSession) -> dict:
    print("\n" + "=" * 60)
    print("G2 — LECTURE COMPLÈTE (toutes les colonnes)")
    print("=" * 60)

    def lecture_csv():
        return spark.read.csv(f"{OUTPUT}/csv/*.csv",
                              header=True, inferSchema=True).cache()

    def lecture_flat():
        return spark.read.parquet(f"{OUTPUT}/parquet_flat/*.parquet").cache()

    df_csv,  t_csv  = _timer(lecture_csv)
    df_flat, t_flat = _timer(lecture_flat)

    n_csv  = df_csv.count()
    n_flat = df_flat.count()
    assert n_csv == n_flat, f"Lignes différentes : {n_csv} vs {n_flat}"

    df_csv.unpersist()
    df_flat.unpersist()

    gain = t_csv / t_flat if t_flat > 0 else float("inf")
    print(f"  CSV          : {t_csv:>7.2f} s  ({n_csv:,} lignes)")
    print(f"  Parquet flat : {t_flat:>7.2f} s")
    print(f"  Gain         : {gain:>7.2f}×")

    return {"metrique": "G2_lecture_complete",
            "naif_s": round(t_csv, 2),
            "optimise_s": round(t_flat, 2),
            "gain": round(gain, 2)}


# ============================================================
# G3 — Lecture 2 colonnes sur 31 ⭐
# ============================================================
def g3_lecture_2cols(spark: SparkSession) -> dict:
    print("\n" + "=" * 60)
    print("G3 — LECTURE 2 COLONNES SUR 31 (métrique star)")
    print("=" * 60)

    _, t_csv = _timer(lambda: spark.read.csv(
        f"{OUTPUT}/csv/*.csv", header=True, inferSchema=True
    ).select("trip_distance", "fare_amount").count())

    _, t_pq = _timer(lambda: spark.read.parquet(
        f"{OUTPUT}/parquet_flat/*.parquet"
    ).select("trip_distance", "fare_amount").count())

    gain = t_csv / t_pq if t_pq > 0 else float("inf")
    print(f"  CSV          : {t_csv:>7.2f} s")
    print(f"  Parquet flat : {t_pq:>7.2f} s")
    print(f"  Gain         : {gain:>7.2f}×")

    return {"metrique": "G3_lecture_2cols",
            "naif_s": round(t_csv, 2),
            "optimise_s": round(t_pq, 2),
            "gain": round(gain, 2)}


# ============================================================
# G4 — Requête 1 mois : flat vs partitionné
# ============================================================
def g4_requete_1mois(spark: SparkSession) -> dict:
    print("\n" + "=" * 60)
    print("G4 — REQUÊTE 1 MOIS (filtre mois=1)")
    print("=" * 60)

    _, t_flat = _timer(lambda: spark.read.parquet(
        f"{OUTPUT}/parquet_flat/*.parquet"
    ).filter(F.col("mois") == 1).count())

    _, t_part = _timer(lambda: spark.read.parquet(
        f"{OUTPUT}/parquet_partitioned/annee=2024/mois=1/"
    ).count())

    gain = t_flat / t_part if t_part > 0 else float("inf")
    print(f"  Parquet flat       : {t_flat:>7.2f} s")
    print(f"  Parquet partitionné: {t_part:>7.2f} s")
    print(f"  Gain               : {gain:>7.2f}×")

    return {"metrique": "G4_requete_1mois",
            "naif_s": round(t_flat, 2),
            "optimise_s": round(t_part, 2),
            "gain": round(gain, 2)}


# ============================================================
# G5 — RAM : Pandas vs Spark (sur 1 mois, fichier LOCAL)
# ============================================================
def g5_ram_pandas_vs_spark(spark: SparkSession) -> dict:
    print("\n" + "=" * 60)
    print("G5 — RAM CONSOMMÉE (sur 1 mois, fichier local)")
    print("=" * 60)

    chemin = os.path.join(DATA_RAW_DIR, "yellow_tripdata_2024-01.parquet")
    print(f"  Fichier : {chemin}")

    gc.collect()
    mem_avant = psutil.Process().memory_info().rss / 1e9

    print("  Chargement Pandas...")
    df_pd = pd.read_parquet(chemin)
    n_lignes = len(df_pd)
    mem_pandas = psutil.Process().memory_info().rss / 1e9 - mem_avant
    print(f"  Pandas : {mem_pandas:.2f} Go pour {n_lignes:,} lignes")

    # Libérer AVANT de mesurer Spark (évite le pic mémoire)
    del df_pd
    gc.collect()

    # Spark lit depuis le disque (pas de chargement complet en RAM)
    print("  Spark (lecture disque, pas de chargement RAM)...")
    n_spark = spark.read.parquet(f"{OUTPUT}/parquet_flat/*.parquet").count()
    print(f"  Spark  : {n_spark:,} lignes (sur disque, sans saturer la RAM)")

    return {"metrique": "G5_ram",
            "pandas_go": round(mem_pandas, 2),
            "pandas_lignes": n_lignes,
            "spark_lignes": n_spark}


# ============================================================
# Lancement : G1 → G5, écrit le CSV après chaque
# ============================================================
def mesurer_tout(spark: SparkSession) -> pd.DataFrame:
    print("\n" + "#" * 60)
    print("#  MESURE DES 5 MÉTRIQUES (G1 → G5)")
    print("#" * 60)

    resultats = []

    def _ajouter(resultat):
        resultats.append(resultat)
        pd.DataFrame(resultats).to_csv(
            f"{OUTPUT}/metriques/tableau_gains.csv", index=False
        )
        print(f"  → CSV mis à jour ({len(resultats)} métriques)")

    _ajouter(g1_taille_disque())
    _ajouter(g2_lecture_complete(spark))
    _ajouter(g3_lecture_2cols(spark))
    _ajouter(g4_requete_1mois(spark))
    _ajouter(g5_ram_pandas_vs_spark(spark))

    df = pd.DataFrame(resultats)
    print("\n" + "=" * 60)
    print("TABLEAU FINAL (G1–G5)")
    print("=" * 60)
    print(df.to_string(index=False))
    print(f"\nÉcrit dans {OUTPUT}/metriques/tableau_gains.csv")

    return df