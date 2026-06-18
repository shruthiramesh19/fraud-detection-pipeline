import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from patterns import generate_transaction

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "transactions.raw")
PRODUCER_TPS = float(os.getenv("PRODUCER_TPS", "2"))
ACCOUNTS = [f"ACC-{i:04d}" for i in range(10)]

SLEEP_INTERVAL = 1.0 / PRODUCER_TPS


def create_producer(retries: int = 10, delay: float = 5.0) -> KafkaProducer:
    """
    Retry loop for Kafka connection.
    Even with Docker health checks, Kafka may need a few extra
    seconds after reporting healthy before accepting producers.
    This retry loop handles that race condition gracefully.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",  # wait for all in-sync replicas to ack
                retries=3,  # retry failed sends up to 3 times
                linger_ms=10,  # batch messages for 10ms to improve throughput
            )
            log.info(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"Kafka not ready (attempt {attempt + 1}/{retries}), retrying in {delay}s..."
            )
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")


def main():
    log.info(f"Starting producer: {PRODUCER_TPS} TPS → topic '{RAW_TOPIC}'")
    producer = create_producer()

    sent = 0
    while True:
        try:
            # Pick a random account for this transaction
            account_id = random.choice(ACCOUNTS)
            txn = generate_transaction(account_id)

            # Add a unique transaction_id
            # Using str(uuid4) as the Kafka message key ensures
            # all messages for the same key go to the same partition
            # (useful if we ever add per-account ordering guarantees)
            txn["transaction_id"] = str(uuid.uuid4())

            producer.send(
                RAW_TOPIC,
                key=txn["transaction_id"],
                value=txn,
            )

            sent += 1
            if sent % 50 == 0:
                log.info(
                    f"Sent {sent} transactions | Last: account={account_id} "
                    f"amount=${txn['amount']} fraud={txn['is_simulated_fraud']}"
                )

            time.sleep(SLEEP_INTERVAL)

        except KeyboardInterrupt:
            log.info("Shutting down producer...")
            break
        except Exception as e:
            log.error(f"Error sending transaction: {e}")
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info("Producer stopped cleanly")


if __name__ == "__main__":
    main()
