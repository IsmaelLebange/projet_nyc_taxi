# src/ingestion.py
import os
from pyspark.sql import SparkSession, DataFrame
from src.config import FICHIERS_RAW_LOCAUX, DATA_LOOKUP, URLS_RAW, URL_LOOKUP


def charger_trajets(spark: SparkSession) -> DataFrame:
    """Charge les 3 mois depuis les fichiers LOCAUX.

    Fallback : si un fichier local manque, on télécharge depuis l'URL.
    """
    dfs = []
    for mois, chemin_local in FICHIERS_RAW_LOCAUX.items():
        if os.path.exists(chemin_local):
            print(f"  Mois {mois} : lecture locale → {chemin_local}")
            df = spark.read.parquet(chemin_local)
        else:
            print(f"  Mois {mois} : fichier local absent, fallback URL")
            df = spark.read.parquet(URLS_RAW[mois])
        dfs.append(df)

    df_final = dfs[0]
    for d in dfs[1:]:
        df_final = df_final.unionByName(d)
    return df_final


def charger_zones(spark: SparkSession) -> DataFrame:
    """Charge le lookup des zones (265 zones NYC) — local si possible."""
    if os.path.exists(DATA_LOOKUP):
        print(f"  Zones : lecture locale → {DATA_LOOKUP}")
        return spark.read.csv(DATA_LOOKUP, header=True, inferSchema=True)
    print(f"  Zones : fichier local absent, fallback URL")
    return spark.read.csv(URL_LOOKUP, header=True, inferSchema=True)