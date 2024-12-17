import os
from pyspark.sql import SparkSession
from dataclasses import dataclass
from dotenv import load_dotenv
import sys

# Загрузить переменные окружения
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


def create_spark_session():
    mysql_jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {mysql_jar_path} pyspark-shell"

    return (
        SparkSession.builder.appName("BaseSparkSession")
        .config("spark.jars", mysql_jar_path)
        .getOrCreate()
    )


def test_mysql_connection(spark: SparkSession, mysql_config: MySQLConfig):
    """Тестирует соединение с MySQL"""
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


def read_from_mysql(spark: SparkSession, mysql_config: MySQLConfig):
    """Читает данные из MySQL"""
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
        print(f"Data from table athlete_bio:")
        df.show()
    except Exception as e:
        print(f"Error reading from MySQL: {e}")


if __name__ == "__main__":
    # Общая настройка Spark и MySQL
    mysql_config = MySQLConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "test_db"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "password"),
    )
    spark = create_spark_session()

    # Читаем аргументы командной строки
    task = sys.argv[1] if len(sys.argv) > 1 else "test_mysql"

    if task == "test_mysql":
        test_mysql_connection(spark, mysql_config)
    elif task == "read_mysql":
        read_from_mysql(spark, mysql_config)
    else:
        print("Unknown task. Use 'test_mysql' or 'read_mysql'")
