
import json
import logging
import os
import time

from confluent_kafka import Producer
from telemetry_generator import generate_telemetry


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ev.telemetry.raw")


def delivery_report(err, msg):
    """Handle Kafka delivery acknowledgments."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Delivered vehicle=%s partition=%s offset=%s",
            msg.key().decode("utf-8"),
            msg.partition(),
            msg.offset(),
        )


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "voltstream-ev-producer",
        "acks": "all",
        "enable.idempotence": True,
    })

    vehicle_ids = [f"EV-{i}" for i in range(1001, 1006)]

    logger.info("Starting VoltStream EV telemetry producer")

    try:
        for vehicle_id in vehicle_ids:
            event = generate_telemetry(vehicle_id)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=vehicle_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Producer interrupted")

    finally:
        remaining = producer.flush(timeout=15)
        if remaining:
            logger.error("%s messages remain undelivered", remaining)
        else:
            logger.info("All queued messages processed")


if __name__ == "__main__":
    main()
