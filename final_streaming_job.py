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
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Configuration for Kafka connection."""

    bootstrap_servers: str
    username: str
    password: str
    security_protocol: str
    sasl_mechanism: str
    input_topic: str
    output_topic: str

    def log_config(self):
        """Log Kafka configuration."""
        logger.info(f"Kafka Bootstrap Servers: {self.bootstrap_servers}")
        logger.info(f"Kafka Username: {self.username}")
        logger.info(f"Kafka Input Topic: {self.input_topic}")
        logger.info(f"Kafka Output Topic: {self.output_topic}")
        logger.info(f"Kafka Security Protocol: {self.security_protocol}")
        logger.info(f"Kafka SASL Mechanism: {self.sasl_mechanism}")

    @property
    def sasl_jaas_config(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.username}" password="{self.password}";'
        )


@dataclass
class MySQLConfig:
    """Configuration for MySQL connection."""

    url: str
    user: str
    password: str
    driver: str = "com.mysql.cj.jdbc.Driver"

    def log_config(self):
        """Log MySQL configuration."""
        logger.info(f"MySQL URL: {self.url}")
        logger.info(f"MySQL User: {self.user}")
        logger.info(f"MySQL Driver: {self.driver}")


class SparkProcessor:
    def __init__(self):
        """Initialize SparkProcessor with Kafka and MySQL configurations."""
        self.kafka_config = self._load_kafka_config()
        self.mysql_config = self._load_mysql_config()

        # Log all configs for verification
        logger.info("Loaded Kafka Configuration:")
        self.kafka_config.log_config()

        logger.info("Loaded MySQL Configuration:")
        self.mysql_config.log_config()

        self.spark = self._create_spark_session()

        # Clean the checkpoint directory
        self._clean_checkpoint_dir()

    def _clean_checkpoint_dir(self):
        """Clean checkpoint directory."""
        checkpoint_dir = "./checkpoint"
        if os.path.exists(checkpoint_dir):
            import shutil

            shutil.rmtree(checkpoint_dir)
            logger.info(f"Checkpoint directory '{checkpoint_dir}' cleared.")

    def _load_kafka_config(self) -> KafkaConfig:
        """Load Kafka configuration from environment variables."""
        config = KafkaConfig(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092"
            ),
            username=os.getenv("KAFKA_USERNAME", "admin"),
            password=os.getenv("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            input_topic=os.getenv("KAFKA_INPUT_TOPIC", "athlete_event_results"),
            output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "athlete_enriched"),
        )
        logger.info(f"Kafka Bootstrap Servers: {config.bootstrap_servers}")
        return config

    def _load_mysql_config(self) -> MySQLConfig:
        """Load MySQL configuration from environment variables."""
        host = os.getenv("MYSQL_HOST", "217.61.57.46")
        port = os.getenv("MYSQL_PORT", "3306")
        database = os.getenv("MYSQL_DATABASE", "olympic_dataset")
        return MySQLConfig(
            url=f"jdbc:mysql://{host}:{port}/{database}",
            user=os.getenv("MYSQL_USER", "neo_data_admin"),
            password=os.getenv("MYSQL_PASSWORD", "Proyahaxuqithab9oplp"),
        )

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        mysql_jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")
        return (
            SparkSession.builder.appName("RefactoredSparkApp")
            .config("spark.jars", mysql_jar_path)
            .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
            .getOrCreate()
        )

    def read_from_mysql(self, table_name: str) -> DataFrame:
        """Read data from MySQL."""
        return (
            self.spark.read.format("jdbc")
            .options(
                url=self.mysql_config.url,
                driver=self.mysql_config.driver,
                dbtable=table_name,
                user=self.mysql_config.user,
                password=self.mysql_config.password,
            )
            .load()
        )

    def write_to_kafka(self, df: DataFrame, topic: str) -> None:
        """Write DataFrame to Kafka with validation."""
        if not self.kafka_config.bootstrap_servers:
            raise ValueError(
                "Kafka Bootstrap Servers is not set. Check environment variables."
            )

        logger.info(f"Writing to Kafka topic: {topic}")
        logger.info(f"Kafka Bootstrap Servers: {self.kafka_config.bootstrap_servers}")

        df.select(
            to_json(struct([col(c) for c in df.columns])).alias("value")
        ).write.format("kafka").options(
            **{
                "kafka.bootstrap.servers": self.kafka_config.bootstrap_servers,
                "kafka.sasl.jaas.config": self.kafka_config.sasl_jaas_config,
                "kafka.security.protocol": self.kafka_config.security_protocol,
                "kafka.sasl.mechanism": self.kafka_config.sasl_mechanism,
                "topic": topic,
            }
        ).save()

    def process_stream(self) -> None:
        """Main data processing pipeline."""
        bio_df = self.read_from_mysql("athlete_bio").filter(
            (col("height").isNotNull()) & (col("weight").isNotNull())
        )
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
            .option("kafka.sasl.jaas.config", self.kafka_config.sasl_jaas_config)
            .option("kafka.security.protocol", self.kafka_config.security_protocol)
            .option("kafka.sasl.mechanism", self.kafka_config.sasl_mechanism)
            .option("subscribe", self.kafka_config.input_topic)
            .load()
        )

        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), kafka_schema).alias("data")
        ).select("data.*")

        joined_df = (
            parsed_stream.join(bio_df, "athlete_id")
            .groupBy("sport", "medal")
            .agg(
                avg(col("height")).alias("avg_height"),
                avg(col("weight")).alias("avg_weight"),
                current_timestamp().alias("timestamp"),
            )
        )

        query = (
            joined_df.writeStream.outputMode("complete")
            .foreachBatch(self._foreach_batch_function)
            .start()
        )
        query.awaitTermination()

    def _foreach_batch_function(self, batch_df: DataFrame, batch_id: int) -> None:
        """Handle batch processing: write to Kafka and MySQL."""
        logger.info(f"Processing batch {batch_id}")
        self.write_to_kafka(batch_df, self.kafka_config.output_topic)

        batch_df.write.format("jdbc").options(
            url=self.mysql_config.url,
            driver=self.mysql_config.driver,
            dbtable="avg_stats",
            user=self.mysql_config.user,
            password=self.mysql_config.password,
        ).mode("append").save()


def main():
    processor = SparkProcessor()
    processor.process_stream()


if __name__ == "__main__":
    main()
