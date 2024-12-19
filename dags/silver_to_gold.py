import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import FloatType
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)


# Custom formatter for colored logs
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "INFO": Fore.GREEN,
        "DEBUG": Fore.BLUE,
    }

    def format(self, record):
        if record.levelname in self.COLORS:
            record.msg = f"{self.COLORS[record.levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


# Set up logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    try:
        logger.info("Starting Spark session for SilverToGold pipeline...")
        spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
        logger.info("Spark session created successfully.")

        # Читаємо silver таблиці
        logger.info("Reading silver tables...")
        bio_df = spark.read.parquet("silver/athlete_bio")
        event_df = spark.read.parquet("silver/athlete_event_results")
        logger.info("Silver tables loaded successfully.")

        # Приведення до числового типу
        logger.info("Casting weight and height columns to FloatType...")
        bio_df = bio_df.withColumn(
            "weight", col("weight").cast(FloatType())
        ).withColumn("height", col("height").cast(FloatType()))

        # Join за athlete_id
        logger.info("Performing join on athlete_id...")
        joined_df = bio_df.join(event_df, "athlete_id")
        logger.info("Join completed successfully.")

        # Агрегація за (sport, medal, sex, country_noc) + timestamp
        logger.info("Aggregating data by sport, medal, sex, and country_noc...")
        agg_df = (
            joined_df.groupBy("sport", "medal", "sex", "country_noc")
            .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
            .withColumn("timestamp", current_timestamp())
        )
        logger.info("Aggregation completed successfully.")

        # Запис у gold
        logger.info("Writing aggregated data to gold/avg_stats...")
        agg_df.write.mode("overwrite").parquet("gold/avg_stats")
        logger.info("Data written to gold/avg_stats successfully.")

        spark.stop()
        logger.info("Spark session stopped successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if "spark" in locals():
            spark.stop()
        logger.info("Spark session stopped due to an error.")
