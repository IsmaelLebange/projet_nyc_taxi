# src/ml.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans


def clusteriser_zones(df: DataFrame, zones: DataFrame, k: int = 5) -> tuple:
    """Clusterise les zones de pickup en k groupes.

    Features : nb_courses, revenu_moyen
    Retourne : (DataFrame des zones avec cluster, centroïdes)
    """
    print(f"\n  Agrégation par zone (filtre ≥1000 courses)...")
    agg = (
        df.groupBy("PULocationID")
          .agg(F.count("*").alias("nb_courses"),
               F.round(F.avg("total_amount"), 2).alias("revenu_moyen"))
          .filter(F.col("nb_courses") >= 1000)
    )
    n_zones = agg.count()
    print(f"  {n_zones} zones retenues (sur 265)")

    # Jointure avec les noms
    agg = agg.join(
        zones.select("LocationID", "Borough", "Zone"),
        agg["PULocationID"] == zones["LocationID"],
        how="left"
    ).drop("LocationID")

    # Vectoriser les features
    assembler = VectorAssembler(
        inputCols=["nb_courses", "revenu_moyen"],
        outputCol="features_raw"
    )
    agg_v = assembler.transform(agg)

    # Standardiser (obligatoire avant k-means)
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )
    agg_s = scaler.fit(agg_v).transform(agg_v)

    # K-means
    print(f"  K-means avec k={k}...")
    km = KMeans(k=k, seed=42, maxIter=20)
    model = km.fit(agg_s)

    labels = model.transform(agg_s).select(
        "PULocationID", "Borough", "Zone",
        "nb_courses", "revenu_moyen",
        F.col("prediction").alias("cluster")
    )

    centres = model.clusterCenters()
    return labels, centres


def profiler_clusters(labels: DataFrame) -> DataFrame:
    """Résume chaque cluster : nb de zones, moyennes."""
    return (
        labels.groupBy("cluster")
              .agg(F.count("*").alias("n_zones"),
                   F.round(F.avg("nb_courses"), 0).alias("moy_courses"),
                   F.round(F.avg("revenu_moyen"), 2).alias("moy_revenu"))
              .orderBy("cluster")
    )