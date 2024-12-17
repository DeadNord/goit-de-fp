import os
import logging
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    from_json,
    to_json,
    struct,
    regexp_replace,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from colorama import Fore, Style, init
from dotenv import load_dotenv

# Загрузить переменные окружения из .env файла
load_dotenv()

# Initialize colorama
init(autoreset=True, strip=False, convert=True)


# Custom formatter for colored logs
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "INFO": Fore.GREEN,
        "DEBUG": Fore.BLUE,
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        message = super().format(record)
        return f"{color}{message}{Style.RESET_ALL}"


# Configure logging
handler = logging.StreamHandler()
handler.setFormatter(
    ColoredFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    topic_prefix: str
    input_topic: str
    output_topic: str

    @property
    def sasl_jaas_config(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.username}" password="{self.password}";'
        )


@dataclass
class MySQLConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"


class SparkProcessor:
    def __init__(self):
        self.kafka_config = self._load_kafka_config()
        self.mysql_config = self._load_mysql_config()
        self.checkpoint_dir = "./checkpoint"

        logger.info("Loaded Kafka Configuration:")
        logger.info(vars(self.kafka_config))

        logger.info("Loaded MySQL Configuration:")
        logger.info(vars(self.mysql_config))

        self.spark = self._create_spark_session()

        # # Clean the checkpoint directory
        # self._clean_checkpoint_dir()

    # def _clean_checkpoint_dir(self):
    #     """Clean checkpoint directory."""

    #     if os.path.exists(self.checkpoint_dir):
    #         import shutil

    #         shutil.rmtree(self.checkpoint_dir)
    #         logger.info(f"Checkpoint directory '{self.checkpoint_dir}' cleared.")

    def _load_kafka_config(self) -> KafkaConfig:
        return KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "password"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            topic_prefix=os.getenv("KAFKA_TOPIC_PREFIX", "greenmoon"),
            input_topic=os.getenv("KAFKA_INPUT_TOPIC", "default_input_topic"),
            output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "default_output_topic"),
        )

    def _load_mysql_config(self) -> MySQLConfig:
        return MySQLConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "test_db"),
            user=os.getenv("MYSQL_USER", "user"),
            password=os.getenv("MYSQL_PASSWORD", "password"),
        )

    def _create_spark_session(self) -> SparkSession:
        """Initialize Spark session with integrated Spark Submit parameters."""
        mysql_jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")
        # mysql_jar_path = os.path.abspath("mysql-connector-j-8.3.0.jar")

        # Проверка наличия MySQL JAR
        if not os.path.exists(mysql_jar_path):
            raise FileNotFoundError(
                f"MySQL connector JAR not found at {mysql_jar_path}. "
                "Please download it using:\n"
                "wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar"
            )

        # Интеграция параметров Spark Submit
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--jars {mysql_jar_path} "
            "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 "
            "pyspark-shell"
        )

        logger.info("Configured Spark Submit parameters via PYSPARK_SUBMIT_ARGS.")

        # Создание SparkSession
        return (
            SparkSession.builder.appName("KafkaSparkPipeline")
            .config("spark.jars", mysql_jar_path)
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
            .getOrCreate()
        )

    def read_from_mysql(self, table_name: str) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.mysql_config.jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", self.mysql_config.user)
            .option("password", self.mysql_config.password)
            .option("partitionColumn", "athlete_id")
            .option("lowerBound", 1)
            .option("upperBound", 1000000)
            .option("numPartitions", 10)
            .load()
        )

    def write_to_kafka(self, df: DataFrame, topic: str) -> None:
        df.select(
            to_json(struct([col(c) for c in df.columns])).alias("value")
        ).write.format("kafka").option(
            "kafka.bootstrap.servers", self.kafka_config.bootstrap_servers
        ).option(
            "kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config
        ).option(
            "kafka.security.protocol", self.kafka_config.security_protocol
        ).option(
            "kafka.sasl.mechanism", self.kafka_config.sasl_mechanism
        ).option(
            "topic", topic
        ).save()

    def process_stream(self):
        input_topic = self.kafka_config.input_topic
        output_topic = self.kafka_config.output_topic

        kafka_schema = StructType(
            [
                StructField("athlete_id", IntegerType()),
                StructField("sport", StringType()),
                StructField("medal", StringType()),
            ]
        )

        kafka_stream = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers)
            .option("subscribe", input_topic)
            .load()
        )

        clean_stream = kafka_stream.withColumn(
            "value", regexp_replace(col("value").cast("string"), "\\\\", "")
        ).withColumn("value", regexp_replace(col("value"), '^"|"$', ""))

        parsed_stream = clean_stream.select(
            from_json(col("value"), kafka_schema).alias("data")
        ).select("data.*")

        bio_df = self.read_from_mysql("athlete_bio")

        aggregated_df = (
            parsed_stream.join(bio_df, "athlete_id")
            .groupBy("sport", "medal")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("timestamp"),
            )
        )

        def foreach_batch_function(batch_df, epoch_id):
            try:
                logger.info(f"Processing batch {epoch_id}")
                self.write_to_kafka(batch_df, output_topic)
                batch_df.write.jdbc(
                    url=self.mysql_config.jdbc_url,
                    table="aggregated_results",
                    mode="append",
                    properties={
                        "user": self.mysql_config.user,
                        "password": self.mysql_config.password,
                        "driver": "com.mysql.cj.jdbc.Driver",
                    },
                )
                logger.info(f"Batch {epoch_id} processed successfully")
            except Exception as e:
                logger.error(f"Error in batch {epoch_id}: {str(e)}")
                raise

        query = (
            aggregated_df.writeStream.outputMode("complete")
            .foreachBatch(foreach_batch_function)
            .start()
        )

        query.awaitTermination()


def main():
    processor = SparkProcessor()
    processor.process_stream()


if __name__ == "__main__":
    main()
