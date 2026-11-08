# src/nettoyage.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.config import (
    DATE_MIN, DATE_MAX,
    DUREE_MIN_SEC, DUREE_MAX_SEC,
    DISTANCE_MAX_MILES, MONTANT_MAX,
)


def appliquer_nettoyage(df: DataFrame, verbose: bool = True) -> DataFrame:
    """Applique les 8 pièges du Bloc 2.3.

    Retourne le DataFrame nettoyé. Affiche la volumétrie de chaque filtre
    si verbose=True (utile pour le debugging, à désactiver en prod).
    """
    n_avant = df.count()

    # --- Piège 1 : dates hors période ---
    n_avant_f = df.count()
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(DATE_MIN)) &
        (F.col("tpep_pickup_datetime") <  F.lit(DATE_MAX))
    )
    if verbose:
        n_apres = df.count()
        print(f"  Piège 1 (dates hors {DATE_MIN}→{DATE_MAX}) : "
              f"-{n_avant_f - n_apres:,} lignes")

    # --- Piège 2 : montants négatifs + montant max ---
    n_avant_f = df.count()
    df = df.filter(
        (F.col("fare_amount") >= 0) &
        (F.col("total_amount") >= 0) &
        (F.col("fare_amount") <= MONTANT_MAX)
    )
    if verbose:
        n_apres = df.count()
        print(f"  Piège 2 (montants négatifs ou > {MONTANT_MAX}$) : "
              f"-{n_avant_f - n_apres:,} lignes")

    # --- Piège 3 : distance = 0 avec fare > 0 ---
    n_avant_f = df.count()
    df = df.filter(~((F.col("trip_distance") == 0) & (F.col("fare_amount") > 0)))
    if verbose:
        n_apres = df.count()
        print(f"  Piège 3 (distance=0 avec fare>0) : "
              f"-{n_avant_f - n_apres:,} lignes")

    # --- Piège 4 : durée aberrante (<1 min ou >6 h) ---
    df = df.withColumn(
        "duree_sec",
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    )
    n_avant_f = df.count()
    df = df.filter(
        (F.col("duree_sec") >= DUREE_MIN_SEC) &
        (F.col("duree_sec") <= DUREE_MAX_SEC)
    )
    if verbose:
        n_apres = df.count()
        print(f"  Piège 4 (durée < {DUREE_MIN_SEC}s ou > {DUREE_MAX_SEC}s) : "
              f"-{n_avant_f - n_apres:,} lignes")

    # --- Piège 5 : flag passenger_count_known ---
    df = df.withColumn(
        "passenger_count_known",
        F.col("passenger_count").isNotNull().cast("int")
    )
    if verbose:
        n_null = df.filter(F.col("passenger_count_known") == 0).count()
        print(f"  Piège 5 (passenger_count null, gardées + flag) : "
              f"{n_null:,} lignes flaggées")

    # --- Piège 6 : tip=0 si paiement cash (payment_type=2) ---
    n_avant_f = df.filter(
        (F.col("tip_amount") > 0) & (F.col("payment_type") == 2)
    ).count()
    df = df.withColumn(
        "tip_amount",
        F.when(F.col("payment_type") == 2, F.lit(0.0)).otherwise(F.col("tip_amount"))
    )
    if verbose:
        print(f"  Piège 6 (tip>0 sur paiement cash, corrigées) : "
              f"{n_avant_f:,} lignes corrigées")

    # --- Piège 7 : flag total_coherent ---
    total_calcule = (
        F.col("fare_amount") + F.col("extra") + F.col("mta_tax")
        + F.col("tip_amount") + F.col("tolls_amount")
        + F.col("improvement_surcharge") + F.col("congestion_surcharge")
    )
    df = df.withColumn(
        "total_coherent",
        (F.abs(F.col("total_amount") - total_calcule) < 1.0).cast("int")
    )

    # --- Piège 8 : zones inconnues (gardées, filtrées plus tard à la jointure) ---
    if verbose:
        n_unknown = df.filter(
            F.col("PULocationID").isin([264, 265]) |
            F.col("DOLocationID").isin([264, 265])
        ).count()
        print(f"  Piège 8 (zones 264/265, gardées pour agrégats) : "
              f"{n_unknown:,} lignes")

    # --- Bilan ---
    n_apres = df.count()
    if verbose:
        print(f"\n  BILAN : {n_avant:,} → {n_apres:,} lignes "
              f"({100 * n_apres / n_avant:.1f}% retenues)")

    return df