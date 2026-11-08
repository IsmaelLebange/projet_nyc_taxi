# src/features.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ajouter_features(df: DataFrame) -> DataFrame:
    """Crée les 7 features du Bloc 3.1 étape 3.

    On suppose que la colonne `duree_sec` existe déjà (créée par le nettoyage).
    """
    return (
        df
        .withColumn("heure", F.hour("tpep_pickup_datetime"))
        .withColumn("jour_semaine", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("mois", F.month("tpep_pickup_datetime"))
        .withColumn("annee", F.year("tpep_pickup_datetime"))
        .withColumn(
            "est_weekend",
            F.col("jour_semaine").isin([1, 7]).cast("int")
        )
        .withColumn(
            "duree_course_min",
            F.round(F.col("duree_sec") / 60.0, 2)
        )
        .withColumn(
            "vitesse_moyenne",
            F.when(
                F.col("duree_sec") > 0,
                F.round(F.col("trip_distance") / (F.col("duree_sec") / 3600.0), 2)
            ).otherwise(0.0)
        )
        .withColumn(
            "tip_rate",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount"), 4)
            ).otherwise(0.0)
        )
        .withColumn(
            "est_aeroport",
            F.col("PULocationID").isin([132, 138, 1]).cast("int")
        )
    )