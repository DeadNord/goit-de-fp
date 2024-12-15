# bronze_to_silver.py

# Етап 2 (Частина 2): Читання з bronze, чистка тексту, дедублікація, запис у silver

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    clean_text_udf = udf(clean_text, StringType())

    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        # Читаємо parquet з bronze
        df = spark.read.parquet(f"bronze/{table}")
        
        # Очищення текстових колонок
        for c in df.columns:
            if df.schema[c].dataType == StringType():
                df = df.withColumn(c, clean_text_udf(df[c]))
        
        # Дедублікація
        df = df.dropDuplicates()
        
        # Запис у silver
        df.write.mode("overwrite").parquet(f"silver/{table}")

    spark.stop()
