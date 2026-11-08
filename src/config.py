# src/config.py — Toutes les constantes du projet
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ============================================================
# CHEMINS DES DONNÉES
# ============================================================
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
DATA_LOOKUP  = os.path.join(BASE_DIR, "data", "lookup", "taxi_zone_lookup.csv")

FICHIERS_RAW_LOCAUX = {
    1: os.path.join(DATA_RAW_DIR, "yellow_tripdata_2024-01.parquet"),
    2: os.path.join(DATA_RAW_DIR, "yellow_tripdata_2024-02.parquet"),
    3: os.path.join(DATA_RAW_DIR, "yellow_tripdata_2024-03.parquet"),
}

URLS_RAW = {
    1: "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    2: "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    3: "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
}
URL_LOOKUP = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# ============================================================
# SEUILS DE NETTOYAGE
# ============================================================
DATE_MIN, DATE_MAX = "2024-01-01", "2024-04-01"
DUREE_MIN_SEC, DUREE_MAX_SEC = 60, 6 * 3600
DISTANCE_MAX_MILES = 200
MONTANT_MAX = 500

# ============================================================
# SPARK
# ============================================================
SPARK_DRIVER_MEMORY      = "3g"
SPARK_SHUFFLE_PARTITIONS = "8"
SPARK_MAX_RESULT_SIZE    = "1g"
SPARK_SERIALIZER         = "org.apache.spark.serializer.KryoSerializer"

# ============================================================
# SORTIES
# ============================================================
OUTPUT = os.path.join(BASE_DIR, "output")