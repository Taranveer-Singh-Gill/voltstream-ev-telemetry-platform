
import json
import logging
import os

from confluent_kafka import Consumer, KafkaError
from telemetry_validator import validate_telemetry


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ev.telemetry.raw")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "voltstream-telemetry-consumers",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])

    stats = {"valid": 0, "invalid": 0}

    logger.info("Starting VoltStream Kafka consumer")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                logger.error("Kafka error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                stats["invalid"] += 1
                logger.error(
                    "Invalid JSON | partition=%s offset=%s error=%s",
                    msg.partition(),
                    msg.offset(),
                    exc,
                )
                # For this learning version, skip malformed messages.
                # A production pipeline should route them to a DLQ.
                consumer.commit(message=msg, asynchronous=False)
                continue

            is_valid, reason = validate_telemetry(event)

            if not is_valid:
                stats["invalid"] += 1
                logger.warning(
                    "Rejected telemetry | partition=%s offset=%s reason=%s",
                    msg.partition(),
                    msg.offset(),
                    reason,
                )
            else:
                stats["valid"] += 1
                logger.info(
                    "Valid telemetry | vehicle=%s partition=%s offset=%s "
                    "battery_soc=%s speed_kmh=%s",
                    event["vehicle_id"],
                    msg.partition(),
                    msg.offset(),
                    event["battery_soc"],
                    event["speed_kmh"],
                )

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    finally:
        consumer.close()

        logger.info(
            "Summary | valid=%s invalid=%s",
            stats["valid"],
            stats["invalid"],
        )


if __name__ == "__main__":
    main()
