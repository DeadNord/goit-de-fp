import logging
from colorama import Fore, Style, init
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

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

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))

if __name__ == "__main__":
    logger.info("Starting Bronze to Silver pipeline...")
    try:
        spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
        logger.info("Spark session created successfully.")

        clean_text_udf = udf(clean_text, StringType())

        tables = ["athlete_bio", "athlete_event_results"]
        for table in tables:
            logger.info(f"Processing table: {table}")

            # Read parquet from bronze
            df = spark.read.parquet(f"bronze/{table}")
            logger.info(f"Read data from bronze/{table} successfully. Row count: {df.count()}")

            # Clean text columns
            for c in df.columns:
                if df.schema[c].dataType == StringType():
                    df = df.withColumn(c, clean_text_udf(df[c]))
            logger.info(f"Cleaned text columns for table: {table}")

            # Deduplication
            df = df.dropDuplicates()
            logger.info(f"Deduplicated data for table: {table}. Row count after deduplication: {df.count()}")
            
             # Show final 20 rows before writing to Silver
            logger.info(f"Final preview (20 rows) of {table} before writing to Silver:")
            df.show(20, truncate=False)

            # Write to silver
            df.write.mode("overwrite").parquet(f"silver/{table}")
            logger.info(f"Data written to silver/{table} successfully.")

        logger.info("Bronze to Silver pipeline completed successfully.")
        spark.stop()
        logger.info("Spark session stopped.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped due to error.")
