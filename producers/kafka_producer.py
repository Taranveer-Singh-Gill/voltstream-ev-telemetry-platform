
import argparse
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

KAFKA_BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ev.telemetry.raw")


def parse_args():
    parser = argparse.ArgumentParser(
        description="VoltStream EV fleet telemetry producer"
    )

    parser.add_argument(
        "--vehicles",
        type=int,
        default=5,
        help="Number of simulated vehicles",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between fleet publishing cycles",
    )

    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Number of cycles; 0 means run continuously",
    )

    args = parser.parse_args()

    if args.vehicles < 1:
        parser.error("--vehicles must be at least 1")

    if args.interval <= 0:
        parser.error("--interval must be greater than 0")

    if args.cycles < 0:
        parser.error("--cycles cannot be negative")

    return args


def main():
    args = parse_args()

    stats = {"delivered": 0, "failed": 0}

    def delivery_report(err, msg):
        if err is not None:
            stats["failed"] += 1
            logger.error("Delivery failed: %s", err)
        else:
            stats["delivered"] += 1
            logger.info(
                "Delivered vehicle=%s partition=%s offset=%s",
                msg.key().decode("utf-8"),
                msg.partition(),
                msg.offset(),
            )

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "voltstream-ev-producer",
        "acks": "all",
        "enable.idempotence": True,
    })

    vehicle_ids = [
        f"EV-{i}" for i in range(1001, 1001 + args.vehicles)
    ]

    logger.info(
        "Starting VoltStream | vehicles=%s interval=%ss cycles=%s",
        args.vehicles,
        args.interval,
        args.cycles if args.cycles else "continuous",
    )

    cycle = 0

    try:
        while args.cycles == 0 or cycle < args.cycles:
            cycle += 1

            logger.info("Publishing cycle %s", cycle)

            for vehicle_id in vehicle_ids:
                event = generate_telemetry(vehicle_id)

                payload = json.dumps(event).encode("utf-8")

                while True:
                    try:
                        producer.produce(
                            topic=KAFKA_TOPIC,
                            key=vehicle_id.encode("utf-8"),
                            value=payload,
                            callback=delivery_report,
                        )
                        break
                    except BufferError:
                        logger.warning(
                            "Producer queue full; waiting for space"
                        )
                        producer.poll(1)

                producer.poll(0)

            if args.cycles == 0 or cycle < args.cycles:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")

    finally:
        logger.info("Flushing pending Kafka messages")

        remaining = producer.flush(timeout=30)

        logger.info(
            "Summary | delivered=%s failed=%s remaining=%s",
            stats["delivered"],
            stats["failed"],
            remaining,
        )

        if remaining or stats["failed"]:
            logger.error("Producer did not deliver all events")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
