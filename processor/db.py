import logging
import os
import time
import uuid
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# The processor is a standalone service — it doesn't share the
# FastAPI app context. It gets its own SQLAlchemy engine.
# Same pool settings as the API for consistency.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=3,
    max_overflow=5,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def wait_for_db(retries: int = 10, delay: float = 3.0) -> None:
    """Wait for PostgreSQL to be ready before starting consumption."""
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("✓ Database connection established")
            return
        except Exception as e:
            log.warning(f"DB not ready (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to database")


def save_transaction(txn: dict[str, Any]) -> bool:
    """
    Persist a transaction to PostgreSQL.
    Returns True on success, False on duplicate (idempotent).

    Why handle duplicates?
    Kafka guarantees at-least-once delivery — the same message
    can be delivered more than once if the consumer crashes after
    processing but before committing its offset. We use
    INSERT ... ON CONFLICT DO NOTHING to make writes idempotent.
    This is the standard pattern for Kafka consumers.
    """
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO transactions
                    (transaction_id, account_id, amount, merchant,
                     merchant_category, location, transaction_type,
                     is_simulated_fraud, timestamp)
                VALUES
                    (:transaction_id, :account_id, :amount, :merchant,
                     :merchant_category, :location, :transaction_type,
                     :is_simulated_fraud, :timestamp)
                ON CONFLICT (transaction_id) DO NOTHING
            """),
            {
                "transaction_id": txn["transaction_id"],
                "account_id": txn["account_id"],
                "amount": float(txn["amount"]),
                "merchant": txn["merchant"],
                "merchant_category": txn["merchant_category"],
                "location": txn["location"],
                "transaction_type": txn["transaction_type"],
                "is_simulated_fraud": txn.get("is_simulated_fraud", False),
                "timestamp": txn["timestamp"],
            },
        )
        db.commit()
        return True
    except Exception as e:
        log.error(f"Failed to save transaction {txn.get('transaction_id')}: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def save_alert(
    txn: dict[str, Any],
    detection: dict[str, Any],
    gpt_result: dict[str, Any],
    detection_tier: str,
) -> bool:
    """Persist an anomaly alert to PostgreSQL."""
    import json

    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO alerts
                    (alert_id, transaction_id, account_id, confidence_score,
                     detection_tier, anomaly_reasons, gpt_is_fraud,
                     gpt_reasoning, gpt_risk_level)
                VALUES
                    (:alert_id, :transaction_id, :account_id, :confidence_score,
                     :detection_tier, :anomaly_reasons, :gpt_is_fraud,
                     :gpt_reasoning, :gpt_risk_level)
                ON CONFLICT (alert_id) DO NOTHING
            """),
            {
                "alert_id": str(uuid.uuid4()),
                "transaction_id": txn["transaction_id"],
                "account_id": txn["account_id"],
                "confidence_score": detection["confidence"],
                "detection_tier": detection_tier,
                "anomaly_reasons": json.dumps(detection["reasons"]),
                "gpt_is_fraud": gpt_result.get("gpt_is_fraud"),
                "gpt_reasoning": gpt_result.get("gpt_reasoning"),
                "gpt_risk_level": gpt_result.get("gpt_risk_level"),
            },
        )
        db.commit()
        return True
    except Exception as e:
        log.error(f"Failed to save alert for {txn.get('transaction_id')}: {e}")
        db.rollback()
        return False
    finally:
        db.close()
