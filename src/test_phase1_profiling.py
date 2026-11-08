import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession, functions as F
from config import SPARK_DRIVER_MEMORY
from ingestion import charger_trajets

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase1-Test3") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = charger_trajets(spark).cache()   # cache : on va faire plusieurs actions
n_total = df.count()
print(f"Total lignes : {n_total:,}\n")

print("=" * 60)
print("NULLS PAR COLONNE (% du total)")
print("=" * 60)
nulls = df.select([
    (F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / n_total * 100).alias(c)
    for c in df.columns
]).toPandas().iloc[0].round(2)
print(nulls.to_string())

print("\n" + "=" * 60)
print("PLAGES DES COLONNES CLÉS (pour vérifier les pièges)")
print("=" * 60)
df.select(
    F.min("tpep_pickup_datetime").alias("date_min"),
    F.max("tpep_pickup_datetime").alias("date_max"),
    F.min("fare_amount").alias("fare_min"),
    F.max("fare_amount").alias("fare_max"),
    F.min("trip_distance").alias("dist_min"),
    F.max("trip_distance").alias("dist_max"),
    F.min("total_amount").alias("total_min"),
    F.max("total_amount").alias("total_max"),
).show(truncate=False)

print("=" * 60)
print("VOLUMÉTRIE DES PIÈGES (lignes affectées)")
print("=" * 60)
df.select(
    F.sum(F.when(F.col("tpep_pickup_datetime") < "2024-01-01", 1).otherwise(0)).alias("dates_avant_2024"),
    F.sum(F.when(F.col("tpep_pickup_datetime") >= "2024-04-01", 1).otherwise(0)).alias("dates_apres_avril"),
    F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("fare_negatif"),
    F.sum(F.when((F.col("trip_distance") == 0) & (F.col("fare_amount") > 0), 1).otherwise(0)).alias("dist0_fare_pos"),
    F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("passenger_null"),
    F.sum(F.when((F.col("tip_amount") > 0) & (F.col("payment_type") == 2), 1).otherwise(0)).alias("tip_cash_incoherent"),
).show(truncate=False)

spark.stop()
print("\n✅ Test 3 réussi")
