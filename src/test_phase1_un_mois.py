import sys
sys.path.insert(0, '.')

from pyspark.sql import SparkSession
from config import URLS_RAW,SPARK_DRIVER_MEMORY

spark= SparkSession.builder \
    .appName("local[*]") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("="*60)
print("TEST 1 : lecture d'un seul mois(janvier 2024)")
print("="*60)

df_jan=spark.read.parquet(URLS_RAW[1])
print(f"Nombre de lignes : {df_jan.count()}")
print(f"Nombre de colonnes : {len(df_jan.columns)}")
print("\nShema :")
df_jan.printSchema()

spark.stop()
print("\nTest terminé avec succès !")
