import os
import logging
from dataclasses import dataclass
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    from_json,
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
import shutil

# Загрузить переменные окружения из .env файла
load_dotenv()

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


@dataclass
class KafkaConfig:
    bootstrap_servers: List[str]
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
        self.checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "checkpoint")
        self.driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")
        self.executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
        self.spark_master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

        logger.info("Loaded Kafka Configuration:")
        logger.info(vars(self.kafka_config))

        logger.info("Loaded MySQL Configuration:")
        logger.info(vars(self.mysql_config))

        logger.info("Creating Spark Session...")
        try:
            self.spark = self._create_spark_session()
            logger.info("Spark Session created successfully!")
        except Exception as e:
            logger.error(f"Failed to create Spark Session: {e}")
            raise

        # Clean the checkpoint directory
        self._clean_checkpoint_dir()

    def _clean_checkpoint_dir(self):
        """Clean checkpoint directory."""

        if os.path.exists(self.checkpoint_dir):

            shutil.rmtree(self.checkpoint_dir)
            logger.info(f"Checkpoint directory '{self.checkpoint_dir}' cleared.")

    def _load_kafka_config(self) -> KafkaConfig:
        return KafkaConfig(
            bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")],
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
        mysql_jar_version = "8.3.0"
        mysql_jar_path = os.path.abspath(f"mysql-connector-j-{mysql_jar_version}.jar")

        # Проверка наличия MySQL JAR
        if not os.path.exists(mysql_jar_path):
            raise FileNotFoundError(
                f"MySQL connector JAR not found at {mysql_jar_path}. "
                "Please download it using:\n"
                f"wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/{mysql_jar_version}/mysql-connector-j-{mysql_jar_version}.jar"
            )

        # Интеграция параметров Spark Submit
        kafka_packages = (
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1"
        )

        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--jars {mysql_jar_path} " f"--packages {kafka_packages} " "pyspark-shell"
        )

        logger.info("Configured Spark Submit parameters via PYSPARK_SUBMIT_ARGS.")

        logger.info(f"Using MySQL connector JAR: {mysql_jar_path}")
        logger.info(f"Using Kafka packages: {kafka_packages}")
        logger.info(f"Spark Master URL: {self.spark_master_url}")
        logger.info(f"Driver Memory: {self.driver_memory}")
        logger.info(f"Executor Memory: {self.executor_memory}")

        # Create and configure Spark session
        spark = (
            # Create and configure Spark session
            SparkSession.builder.config("spark.jars", mysql_jar_path)
            .config("spark.driver.extraClassPath", mysql_jar_path)
            .config("spark.executor.extraClassPath", mysql_jar_path)
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_dir)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.driver.memory", self.driver_memory)
            .config("spark.executor.memory", self.executor_memory)
            .appName("EnhancedJDBCToKafka")
            .master(self.spark_master_url)
            .getOrCreate()
        )

        # Verify MySQL driver is loaded
        try:
            spark.sparkContext._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
            logger.info(f"MySQL driver {mysql_jar_version} successfully loaded")
        except Exception as e:
            logger.error(f"Failed to load MySQL driver: {e}")
            raise

        return spark

    def read_from_mysql(self, table_name: str, partition_column: str) -> DataFrame:
        try:
            df = (
                self.spark.read.format("jdbc")
                .option("url", self.mysql_config.jdbc_url)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table_name)
                .option("user", self.mysql_config.user)
                .option("password", self.mysql_config.password)
                .option("partitionColumn", partition_column)
                .option("lowerBound", 1)
                .option("upperBound", 1000000)
                .option("numPartitions", 10)
                .load()
            )
            df.show()
            logger.info("MySQL connection successful!")
        except Exception as e:
            logger.error(f"Error connecting to MySQL: {e}")

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

    def _process_athlete_bio(self) -> DataFrame:
        """Process athlete biography data."""
        df = self.read_from_mysql("athlete_bio", "athlete_id")
        return df.filter(
            (col("height").isNotNull())
            & (col("weight").isNotNull())
            & (col("height").cast("double").isNotNull())
            & (col("weight").cast("double").isNotNull())
        )

    def write_to_kafka(self, df: DataFrame, topic: str) -> None:
        logger.info(f"Writing data to Kafka topic: {topic}")

        try:
            (
                df.selectExpr(
                    "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
                )
                .write.format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    ",".join(self.kafka_config.bootstrap_servers),
                )
                .option("kafka.security.protocol", self.kafka_config.security_protocol)
                .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
                .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
                .option("topic", topic)
                .option("checkpointLocation", self.checkpoint_dir)
                .save()
            )
            logger.info("Data written to Kafka successfully!")
        except Exception as e:
            logger.error(f"Error writing to Kafka topic {topic}: {str(e)}")
            raise

    def _read_kafka_stream(self, topic: str) -> DataFrame:
        """Configure and read Kafka stream."""
        schema = StructType(
            [
                StructField("athlete_id", IntegerType(), True),
                StructField("sport", StringType(), True),
                StructField("medal", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

        return (
            self.spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers", ",".join(self.kafka_config.bootstrap_servers)
            )
            .option("kafka.security.protocol", self.kafka_config.security_protocol)
            .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
            .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "5")
            .option("failOnDataLoss", "false")
            .load()
            .withColumn(
                "value", regexp_replace(col("value").cast("string"), "\\\\", "")
            )
            .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.athlete_id", "data.sport", "data.medal")
        )

    def _aggregate_data(self, stream_df: DataFrame, bio_df: DataFrame) -> DataFrame:
        """Aggregate streaming data with biography data."""
        return (
            stream_df.join(bio_df, "athlete_id")
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("timestamp"),
            )
        )

    def process_stream(self):
        input_topic = self.kafka_config.input_topic
        output_topic = self.kafka_config.output_topic
        topic_prefix = self.kafka_config.topic_prefix

        logger.info(f"Kafka Configurations:")
        logger.info(f"Bootstrap Servers: {self.kafka_config.bootstrap_servers}")
        logger.info(f"Input Topic: {self.kafka_config.input_topic}")
        logger.info(f"Security Protocol: {self.kafka_config.security_protocol}")
        logger.info(f"SASL Mechanism: {self.kafka_config.sasl_mechanism}")

        try:
            logger.info("Reading data from Kafka...")
            kafka_stream = self._read_kafka_stream(input_topic)
            logger.info("Kafka stream loaded successfully. Printing schema:")
            kafka_stream.printSchema()

            logger.info("Joining parsed stream with bio_df...")
            bio_df = self._process_athlete_bio()

            # Process and aggregate data
            aggregated_df = self._aggregate_data(kafka_stream, bio_df)

            logger.info("Aggregation complete. Printing schema:")
            aggregated_df.printSchema()

            logger.info("Writing to Kafka...")

            # Start streaming with error handling
            self._start_streaming(aggregated_df, output_topic, topic_prefix)

        except Exception as e:
            logger.error(f"Error in stream processing: {str(e)}")
            raise

    def _start_streaming(
        self, df: DataFrame, output_topic: str, topic_prefix: str
    ) -> None:
        def foreach_batch_function(batch_df, epoch_id):
            try:
                logger.info(f"Starting batch processing. Epoch ID: {epoch_id}")
                logger.info(f"Batch size: {batch_df.count()}")
                self.write_to_kafka(batch_df, f"{topic_prefix}_{output_topic}")
                logger.info("Batch written to Kafka successfully.")
                batch_df.write.jdbc(
                    url=self.mysql_config.jdbc_url,
                    table=f"{topic_prefix}_{output_topic}",
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

        # Streaming to console output
        (
            df.writeStream.outputMode(
                "complete"
            )  # Use 'complete' or 'append' depending on requirements
            .format("console")
            .option("truncate", "false")  # Full output without truncation
            .option("numRows", 50)  # Number of rows to display
            .start()
        )

        # Main streaming logic with foreachBatch
        (
            df.writeStream.outputMode("complete")
            .foreachBatch(foreach_batch_function)
            .option(
                "checkpointLocation", os.path.join(self.checkpoint_dir, "streaming")
            )
            .start()
            .awaitTermination()
        )


def main():
    processor = SparkProcessor()
    processor.process_stream()


if __name__ == "__main__":
    main()
