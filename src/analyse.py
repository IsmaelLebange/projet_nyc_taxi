# src/analyse.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def q1_courses_par_heure(df: DataFrame) -> DataFrame:
    """Q1 — Nombre de courses par heure de la journée (0-23)."""
    return (
        df.groupBy("heure")
          .agg(F.count("*").alias("nb_courses"),
               F.round(F.avg("total_amount"), 2).alias("revenu_moyen"))
          .orderBy("heure")
    )


def q2_top_zones(df: DataFrame, zones: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Q2 — Top 10 zones actives + top 10 zones rentables.

    Jointure LEFT avec le lookup pour récupérer les noms de zones.
    """
    # Jointure : chaque course → nom de la zone de pickup
    df_j = df.join(
        zones.select("LocationID", "Borough", "Zone"),
        df["PULocationID"] == zones["LocationID"],
        how="left"
    )

    # Top 10 zones ACTIVES (par nombre de courses)
    actives = (
        df_j.groupBy("PULocationID", "Borough", "Zone")
            .agg(F.count("*").alias("nb_courses"))
            .orderBy(F.desc("nb_courses"))
            .limit(10)
    )

    # Top 10 zones RENTABLES (par revenu moyen, avec ≥1000 courses)
    rentables = (
        df_j.groupBy("PULocationID", "Borough", "Zone")
            .agg(F.count("*").alias("nb_courses"),
                 F.round(F.avg("total_amount"), 2).alias("revenu_moyen"))
            .filter(F.col("nb_courses") >= 1000)
            .orderBy(F.desc("revenu_moyen"))
            .limit(10)
    )

    return actives, rentables


def q3_tips_par_borough(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Q3 — Tip moyen par borough de départ."""
    df_j = df.join(
        zones.select("LocationID", "Borough"),
        df["PULocationID"] == zones["LocationID"],
        how="left"
    )
    return (
        df_j.filter(F.col("Borough").isNotNull())
            .groupBy("Borough")
            .agg(F.count("*").alias("nb_courses"),
                 F.round(F.avg("tip_rate"), 4).alias("tip_moyen"),
                 F.round(F.avg("total_amount"), 2).alias("montant_moyen"))
            .filter(F.col("nb_courses") > 100)
            .orderBy(F.desc("tip_moyen"))
    )


def q4_pieges(df_raw: DataFrame) -> DataFrame:
    """Q4 — Volumétrie des 8 pièges (sur les données BRUTES)."""
    total = df_raw.count()
    resultats = []

    def _row(nom, condition):
        n = df_raw.filter(condition).count()
        resultats.append({
            "piege": nom,
            "nb_lignes": n,
            "pct_total": round(100 * n / total, 2)
        })

    _row("1. Dates hors 2024-01→2024-04",
         (F.col("tpep_pickup_datetime") < "2024-01-01") |
         (F.col("tpep_pickup_datetime") >= "2024-04-01"))
    _row("2. Montants négatifs", F.col("fare_amount") < 0)
    _row("3. Distance=0 avec fare>0",
         (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0))
    _row("5. passenger_count null", F.col("passenger_count").isNull())
    _row("8. Zones inconnues (264/265)",
         F.col("PULocationID").isin([264, 265]) |
         F.col("DOLocationID").isin([264, 265]))

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    return spark.createDataFrame(resultats)