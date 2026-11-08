# test_phase4_metriques.py
import sys
sys.path.insert(0, ".")

from pyspark.sql import SparkSession
from src.config import (SPARK_DRIVER_MEMORY, SPARK_SHUFFLE_PARTITIONS,
                        SPARK_MAX_RESULT_SIZE, SPARK_SERIALIZER)
from src.metriques import mesurer_tout

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Phase4-Metriques") \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS) \
    .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
    .config("spark.serializer", SPARK_SERIALIZER) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = mesurer_tout(spark)

spark.stop()
print("\n✅ Phase 4 terminée")