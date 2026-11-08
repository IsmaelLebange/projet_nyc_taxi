from pyspark.sql import DataFrame
from src.config import OUTPUT 

def sauvegarder_csv(df: DataFrame)->None:
    df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(f"{OUTPUT}/csv/")
    print(f"✅ DataFrame sauvegardé en CSV dans {OUTPUT}/csv/")

def sauvegarder_parquet_flat(df: DataFrame)->None:
    df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(f"{OUTPUT}/parquet_flat/")
    print(f"✅ DataFrame sauvegardé en Parquet (flat) dans {OUTPUT}/parquet_flat/")

def sauvegarder_parquet_partitionne(df: DataFrame)->None:
    df.write \
        .mode("overwrite") \
        .partitionBy("annee", "mois") \
        .parquet(f"{OUTPUT}/parquet_partitionned/")
    print(f"✅ DataFrame sauvegardé en Parquet (partitionné) dans {OUTPUT}/parquet_partitionned/")


def sauvegarder_trois_versions(df: DataFrame)->None:
    print("\n--- Sauvegarde des 3 versions du DataFrame ---")
    sauvegarder_csv(df)
    sauvegarder_parquet_flat(df)
    sauvegarder_parquet_partitionne(df)
    print("\n✅ Sauvegarde terminée. Vérifiez le contenu du dossier data/processed/")