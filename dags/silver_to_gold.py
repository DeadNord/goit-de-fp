# Етап 3 (Частина 2): Читання з silver, джоін двох таблиць, агрегація, добавлення timestamp, запис у gold

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import FloatType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Читаємо silver таблиці
    bio_df = spark.read.parquet("silver/athlete_bio")
    event_df = spark.read.parquet("silver/athlete_event_results")

    # Приведення до числового типу
    bio_df = bio_df.withColumn("weight", col("weight").cast(FloatType())) \
                   .withColumn("height", col("height").cast(FloatType()))

    # Join за athlete_id
    joined_df = bio_df.join(event_df, "athlete_id")
    
    # Агресація за (sport, medal, sex, country_noc) + timestamp
    agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ) \
        .withColumn("timestamp", current_timestamp())
    
    # Запис у gold
    agg_df.write.mode("overwrite").parquet("gold/avg_stats")

    spark.stop()
