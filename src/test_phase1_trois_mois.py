import sys
sys.path.insert(0, '.')

from pyspark.sql import SparkSession
from config import SPARK_DRIVER_MEMORY
from ingestion import charger_trajets, charger_zones

spark= SparkSession.builder \
    .appName("local[*]") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("="*60)
print("TEST 2 : lecture de trois mois (janvier, février et mars 2024)")
print("="*60)

df_raw=charger_trajets(spark)
zones=charger_zones(spark)
print(f"Nombre de lignes : {df_raw.count()}")
print(f"Nombre de colonnes : {len(df_raw.columns)}")

print("\nShema zones:")
zones.printSchema()
print(f"Apercu zones ")
zones.show(5,truncate=False)

spark.stop()
print("\nTest 2 terminé avec succès !")