import os
from pyspark.sql import SparkSession
from dataclasses import dataclass
from dotenv import load_dotenv
import sys
from pyspark.sql.functions import col

# Load environment variables
load_dotenv()


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


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str


def create_spark_session():
    mysql_jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")
    kafka_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {mysql_jar_path} --packages {kafka_packages} pyspark-shell"
    )

    return (
        SparkSession.builder.appName("SparkKafkaMySQLTest")
        .config("spark.jars", mysql_jar_path)
        .getOrCreate()
    )


def test_mysql_connection(spark: SparkSession, mysql_config: MySQLConfig):
    """Test connection to MySQL"""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", mysql_config.jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "athlete_bio")
            .option("user", mysql_config.user)
            .option("password", mysql_config.password)
            .load()
        )
        df.show()
        print("MySQL connection successful!")
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")


def test_kafka_connection(spark: SparkSession, kafka_config: KafkaConfig):
    """Test connection to Kafka"""
    try:
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
            .option("subscribe", kafka_config.topic)
            .load()
        )

        query = (
            kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .writeStream.outputMode("append")
            .format("console")
            .start()
        )

        print("Kafka connection successful! Listening for messages...")
        query.awaitTermination(timeout=30)
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")


def read_from_mysql(spark: SparkSession, mysql_config: MySQLConfig):
    """Read data from MySQL"""
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", mysql_config.jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", "athlete_bio")
            .option("user", mysql_config.user)
            .option("password", mysql_config.password)
            .load()
        )
        print("Data from table 'athlete_bio':")
        df.show()
    except Exception as e:
        print(f"Error reading from MySQL: {e}")


if __name__ == "__main__":
    mysql_config = MySQLConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "test_db"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "password"),
    )

    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_INPUT_TOPIC", "test_topic"),
    )

    spark = create_spark_session()

    task = sys.argv[1] if len(sys.argv) > 1 else "test_mysql"

    if task == "test_mysql":
        test_mysql_connection(spark, mysql_config)
    elif task == "read_mysql":
        read_from_mysql(spark, mysql_config)
    elif task == "test_kafka":
        test_kafka_connection(spark, kafka_config)
    else:
        print("Unknown task. Use 'test_mysql', 'read_mysql', or 'test_kafka'")
