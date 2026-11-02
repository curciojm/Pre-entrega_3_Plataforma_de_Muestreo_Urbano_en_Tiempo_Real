
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")


def create_sensor_event(sensor_id):
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(15, 40), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "air_quality_index": random.randint(0, 200),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    sensors = ["sensor_01", "sensor_02", "sensor_03"]

    try:
        while True:
            for sensor_id in sensors:
                event = create_sensor_event(sensor_id)

                producer.send(KAFKA_TOPIC, value=event)

                print(f"Evento enviado: {event}")

            producer.flush()
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nProductor detenido.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
