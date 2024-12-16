import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg,
    current_timestamp,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

# Доступи до MySQL (Етап 1 - читання даних із MySQL)
# Абсолютный путь к MySQL JAR
mysql_jar_path = os.path.abspath("mysql-connector-j-8.0.32.jar")
if not os.path.exists(mysql_jar_path):
    raise FileNotFoundError(
        f"MySQL connector JAR not found at {mysql_jar_path}. "
        "Please download it using:\n"
        "wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar"
    )

# Установка переменной окружения для Kafka пакетов
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
    "pyspark-shell"
)

# Создание SparkSession
spark = (
    SparkSession.builder.config("spark.jars", mysql_jar_path)
    .config("spark.driver.extraClassPath", mysql_jar_path)
    .config("spark.executor.extraClassPath", mysql_jar_path)
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.driver.memory", "4g")  # Увеличение памяти
    .config("spark.executor.memory", "4g")
    .appName("EnhancedJDBCToKafka")
    .master("local[*]")  # Локальное тестирование
    .getOrCreate()
)

# Проверка, что драйвер MySQL загружен
try:
    spark.sparkContext._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
    print("MySQL driver successfully loaded")
except Exception as e:
    print(f"Failed to load MySQL driver: {e}")
    raise

spark.sparkContext.setLogLevel("WARN")


jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
athlete_bio_table = "athlete_bio"
athlete_event_table = "athlete_event_results"

kafka_bootstrap_servers = "77.81.230.104:9092"
kafka_sasl_jaas = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
input_topic = "athlete_event_results"
output_topic = "athlete_enriched"

spark = (
    SparkSession.builder.appName("EndToEndStreaming_Debug")
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("----- Етап 1: Зчитування біоданих з MySQL -----")

print("Проверка соединения с MySQL:")
print(f"URL: {jdbc_url}")
print(f"Таблица: {athlete_bio_table}")

try:
    spark.sparkContext._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
    print("MySQL драйвер загружен успешно")
except Exception as e:
    print(f"Ошибка загрузки драйвера: {e}")

print("Проверяем доступ к таблице athlete_bio...")

try:
    df_bio = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_url,
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=athlete_bio_table,
            user=jdbc_user,
            password=jdbc_password,
            fetchsize="5000",  # Размер выборки
            queryTimeout="60",  # Таймаут в секундах
            numPartitions="10",
        )
        .load()
    )
    print("Доступ к таблице подтвержден")
except Exception as e:
    print(f"Ошибка подключения к MySQL: {e}")
    raise

df_bio.show(10, truncate=False)

print("----- Перевірка event даних з MySQL -----")
df_event = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=athlete_event_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)
df_event.show(10, truncate=False)

# Етап 1: Зчитування біоданих (повторне, оскільки надалі працюємо з bio_df)
bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=athlete_bio_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)
print("Етап 1: bio_df початковий розмір:", bio_df.count())

# Етап 2: Фільтрація некоректних даних
bio_df = bio_df.filter((col("height").isNotNull()) & (col("weight").isNotNull()))
bio_df = bio_df.filter(
    (col("height").cast("float").isNotNull())
    & (col("weight").cast("float").isNotNull())
)
print("Етап 2: bio_df після фільтрації розмір:", bio_df.count())
bio_df.show(5, truncate=False)

# Етап 3: Читання athlete_event_results з MySQL та запис у Kafka
event_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=athlete_event_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)
print("Етап 3: event_df розмір:", event_df.count())
event_df.show(5, truncate=False)

event_json_df = event_df.withColumn(
    "value", to_json(struct([col(c) for c in event_df.columns]))
).select("value")

print("Запис event_df у Kafka топік:", input_topic)
event_json_df.write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_bootstrap_servers
).option("kafka.sasl.jaas.config", kafka_sasl_jaas).option(
    "kafka.security.protocol", "SASL_PLAINTEXT"
).option(
    "kafka.sasl.mechanism", "PLAIN"
).option(
    "topic", input_topic
).save()

print("Дані event_df записані у Kafka топік", input_topic)

# Етап 3 (продовження): Зчитування з Kafka-топіку
schema = StructType(
    [
        StructField("athlete_id", IntegerType()),
        StructField("sport", StringType()),
        StructField("medal", StringType()),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", input_topic)
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas)
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("startingOffsets", "earliest")
    .load()
)

print("Стримінгове читання з Kafka топіку:", input_topic)

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

print("Етап 4: Джоін з bio_df")

joined_df = parsed_df.join(bio_df, "athlete_id")

print("Перед агрегацією, подивимось joined_df у консоль (лише для дебагу):")

debug_query = (
    joined_df.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

# Етап 5: Обчислення середніх значень
agg_df = (
    joined_df.groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        avg(col("height").cast("float")).alias("avg_height"),
        avg(col("weight").cast("float")).alias("avg_weight"),
    )
    .withColumn("timestamp", current_timestamp())
)

print("Етап 5: Агрегація готова. Дані будуть записуватись у foreachBatch.")


def foreach_batch_function(batch_df, batch_id):
    if batch_df.isEmpty():
        print(
            f"--- foreach_batch_function called for batch_id: {batch_id}, but batch is empty. Skipping. ---"
        )
        return

    print(f"--- foreach_batch_function called for batch_id: {batch_id} ---")
    print("Дані batch_df (перші 5 рядків):")
    batch_df.show(5, truncate=False)

    # Етап 6(а): Запис у вихідний Kafka-топік з такими ж параметрами SASL/PLAIN
    print("Запис batch_df у Kafka-топік:", output_topic)
    out_df = batch_df.select(
        to_json(struct([col(c) for c in batch_df.columns])).alias("value")
    )

    out_df.show(5, truncate=False)  # Для дебагу

    out_df.write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_bootstrap_servers
    ).option("kafka.sasl.jaas.config", kafka_sasl_jaas).option(
        "kafka.security.protocol", "SASL_PLAINTEXT"
    ).option(
        "kafka.sasl.mechanism", "PLAIN"
    ).option(
        "topic", output_topic
    ).save()
    print("Запис у Kafka топік завершено.")

    # Етап 6(b): Запис у базу даних MySQL
    print("Запис batch_df у MySQL таблицю avg_stats")
    batch_df.write.format("jdbc").option("url", jdbc_url).option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).option("dbtable", "avg_stats").option("user", jdbc_user).option(
        "password", jdbc_password
    ).mode(
        "append"
    ).save()
    print("Запис у MySQL завершено.")


query = (
    agg_df.writeStream.outputMode("complete")
    .foreachBatch(foreach_batch_function)
    .start()
)

print("Стримінговий запит з foreachBatch запущено. Очікуємо мікробатчі...")

query.awaitTermination()
debug_query.awaitTermination()
