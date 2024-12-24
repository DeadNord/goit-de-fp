import logging
from colorama import Fore, Style, init
import requests
from pyspark.sql import SparkSession

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


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    logger.info(f"Attempting to download data from: {downloading_url}")

    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(local_file_path + ".csv", "wb") as file:
            file.write(response.content)
        logger.info(f"File {local_file_path}.csv downloaded successfully.")
    else:
        logger.error(
            f"Failed to download the file {local_file_path}.csv. Status code: {response.status_code}"
        )
        raise Exception(
            f"Failed to download the file. Status code: {response.status_code}"
        )


if __name__ == "__main__":
    logger.info("Starting the Landing to Bronze process.")

    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    logger.info("Spark session created successfully.")

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        try:
            logger.info(f"Processing table: {table}")

            # Download data
            download_data(table)

            # Read CSV
            logger.info(f"Reading CSV file: {table}.csv")
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(table + ".csv")
            )

            # Show schema and a few rows to verify
            logger.info(f"Schema of {table}:")
            df.printSchema()
            logger.info(f"Preview of {table}:")
            df.show(5, truncate=False)

            # Show final 20 rows before writing
            logger.info(f"Final preview (20 rows) of {table} before writing to Bronze:")
            df.show(20, truncate=False)

            # Write to bronze
            output_path = f"bronze/{table}"
            logger.info(f"Writing data to: {output_path}")
            df.write.mode("overwrite").parquet(output_path)

            logger.info(f"Table {table} successfully processed and written to Bronze.")
        except Exception as e:
            logger.error(f"Error processing table {table}: {str(e)}")

    spark.stop()
    logger.info("Landing to Bronze process completed.")
