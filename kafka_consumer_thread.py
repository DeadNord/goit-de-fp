from kafka import KafkaConsumer
import json

def kafka_consumer_thread(topic: str, bootstrap_servers: str):
    """
    Запускает Kafka Consumer для чтения данных из топика.
    """
    print(f"Запуск Kafka Consumer для топика: {topic}")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="consumer-group-1",
    )

    print(f"Прослушивание топика '{topic}':")
    for message in consumer:
        print(f"Получено сообщение: {message.value}")

if __name__ == "__main__":
    kafka_consumer_thread("athlete_enriched", "77.81.230.104:9092")
