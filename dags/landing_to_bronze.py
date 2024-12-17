# Етап 1 (Частина 2): Завантаження даних з FTP у landing, читання в Spark та запис у bronze

import requests
from pyspark.sql import SparkSession


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".txt"
    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(local_file_path + ".txt", "wb") as file:
            file.write(response.content)
    else:
        raise Exception(
            f"Failed to download the file. Status code: {response.status_code}"
        )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    tables = ["athlete_bio", "athlete_event_results"]
    # Завантажуємо дані в landing
    for table in tables:
        download_data(table)
        # Читаємо CSV
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(table + ".txt")
        )
        # Записуємо в bronze
        df.write.mode("overwrite").parquet(f"bronze/{table}")

    spark.stop()
