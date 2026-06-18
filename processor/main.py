import json
import logging
import os
import time

from db import save_alert, save_transaction, wait_for_db
from detector import analyze
from escalator import escalate_to_gpt
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [PROCESSOR] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "transactions.raw")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "transactions.alerts")
ESCALATION_THRESHOLD = float(os.getenv("ESCALATION_THRESHOLD", "0.75"))


def create_consumer(retries: int = 10, delay: float = 5.0) -> KafkaConsumer:
    """
    Connect to Kafka with retry logic.

    Consumer group_id is critical: Kafka tracks which messages each
    consumer group has processed via offsets. If the processor
    restarts, it resumes from where it left off — no messages lost,
    no messages reprocessed (assuming at-least-once semantics).

    auto_offset_reset='earliest': if this consumer group has never
    consumed this topic before, start from the very first message.
    Use 'latest' in production to skip historical backlog on first deploy.
    """
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="fraud-processor-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=10,
            )
            log.info(f"✓ Connected to Kafka, consuming topic '{RAW_TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            log.warning(
                f"Kafka not ready (attempt {attempt + 1}/{retries}), retrying in {delay}s..."
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def create_alert_producer():
    """Separate producer for publishing alerts back to Kafka."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def process_transaction(txn: dict, alert_producer) -> None:
    """
    Full processing pipeline for one transaction:
    1. Save to DB
    2. Run statistical detection
    3. If high confidence → GPT escalation
    4. If anomaly → save alert + publish to alert topic
    """
    # Step 1: Persist every transaction regardless of fraud status
    save_transaction(txn)

    # Step 2: Statistical detection
    detection = analyze(txn)

    if not detection["is_anomaly"]:
        return  # Normal transaction — nothing more to do

    log.info(
        f"⚠ Anomaly detected: account={txn['account_id']} "
        f"amount=${float(txn['amount']):.2f} "
        f"confidence={detection['confidence']:.2f} "
        f"reasons={detection['reasons']}"
    )

    # Step 3: GPT escalation for high-confidence anomalies
    gpt_result = {
        "gpt_is_fraud": None,
        "gpt_risk_level": None,
        "gpt_reasoning": None,
        "gpt_escalated": False,
    }
    detection_tier = "statistical"

    if detection["confidence"] >= ESCALATION_THRESHOLD:
        log.info(f"🤖 Escalating to GPT (confidence={detection['confidence']:.2f})")
        gpt_result = escalate_to_gpt(txn, detection)
        if gpt_result.get("gpt_escalated"):
            detection_tier = "gpt"
            log.info(
                f"GPT verdict: fraud={gpt_result['gpt_is_fraud']} "
                f"risk={gpt_result['gpt_risk_level']} | {gpt_result['gpt_reasoning']}"
            )

    # Step 4: Save alert and publish to Kafka
    save_alert(txn, detection, gpt_result, detection_tier)

    alert_event = {
        "transaction_id": txn["transaction_id"],
        "account_id": txn["account_id"],
        "amount": float(txn["amount"]),
        "confidence_score": detection["confidence"],
        "detection_tier": detection_tier,
        "reasons": detection["reasons"],
        "gpt_is_fraud": gpt_result.get("gpt_is_fraud"),
        "gpt_risk_level": gpt_result.get("gpt_risk_level"),
        "gpt_reasoning": gpt_result.get("gpt_reasoning"),
    }
    alert_producer.send(ALERT_TOPIC, value=alert_event)


def main():
    log.info("Starting fraud processor...")
    wait_for_db()

    consumer = create_consumer()
    alert_producer = create_alert_producer()

    processed = 0
    alerts = 0

    log.info("✓ Processor running — waiting for transactions...")

    try:
        for message in consumer:
            txn = message.value
            try:
                detection = analyze(txn)
                process_transaction(txn, alert_producer)
                processed += 1
                if detection["is_anomaly"]:
                    alerts += 1
                if processed % 100 == 0:
                    log.info(
                        f"Stats: {processed} processed, "
                        f"{alerts} alerts ({alerts / processed * 100:.1f}% alert rate)"
                    )
            except Exception as e:
                log.error(
                    f"Error processing transaction {txn.get('transaction_id')}: {e}"
                )
                # Continue processing — don't let one bad message crash the consumer
    except KeyboardInterrupt:
        log.info("Shutting down processor...")
    finally:
        consumer.close()
        alert_producer.flush()
        alert_producer.close()
        log.info(
            f"Processor stopped. Processed {processed} transactions, {alerts} alerts."
        )


if __name__ == "__main__":
    main()
