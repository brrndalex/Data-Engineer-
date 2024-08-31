"""Project_producer_Kafka_Filonenko.ipynb"
"""

! pip3 install confluent_kafka

# PRODUCER
import confluent_kafka
from confluent_kafka import Producer
import json
import time

# Kafka broker and topic configuration (путь к брокеру Kafka)
kafka_broker = "kafka1:19091"
kafka_topic = "project_kafka"

# Create a Kafka producer configuration (создаем конфигурацию продьюсера)
producer_config = {
    "bootstrap.servers": kafka_broker,
    "client.id": "python-producer"
}

# Create a Kafka producer (инициализация продьюсера)
producer = Producer(producer_config)

"""Проверяем что лежит в директории:"""

! -ls

"""Не забываем отслеживать как работает producer (отправитель) в Kafdrop (веб-интерфейс для просмотра топиков Apache Kafka и групп потребителей), создав при этом топик "project_kafka"."""

import csv
def message_gen():
    with open("movies.csv", 'r') as file:
        csv_reader = csv.DictReader(file)
        data = list(csv_reader)           # Преобразовываем данные в список

    for row in data:
        yield {
            "id": row["id"],
            "name": row['name'],
            "year": row['year'],
            "rank": row['rank'],
        }

# Function to send a message to Kafka
def send_message(message):
    producer.produce("project_kafka", json.dumps(message))
    producer.flush()

generator = message_gen()

for i in range(100):
    message = next(generator)
    send_message(message)
    print(f"Sent message: {message}")
    time.sleep(1)

! hdfs dfs -ls /user/datasets/

! ls

! hdfs dfs -ls /user/outputs
