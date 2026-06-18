import uuid

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID

from app.db.session import Base


class Transaction(Base):
    """
    Every transaction consumed from Kafka is persisted here.
    is_simulated_fraud is the ground truth label injected by
    the producer — lets us measure detection precision/recall.
    """

    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(UUID(as_uuid=True), unique=True, nullable=False, index=True)
    account_id = Column(String(50), nullable=False, index=True)
    amount = Column(Numeric(12, 2), nullable=False)
    merchant = Column(String(255), nullable=False)
    merchant_category = Column(String(100), nullable=False)
    location = Column(String(100), nullable=False)
    transaction_type = Column(String(50), nullable=False)
    is_simulated_fraud = Column(Boolean, default=False, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Alert(Base):
    """
    Created when a transaction exceeds the anomaly threshold.
    detection_tier tracks whether the alert came from statistical
    rules only, or was escalated to GPT for deeper analysis.

    anomaly_reasons stored as JSONB — flexible list of triggered
    rule descriptions without needing a separate reasons table.

    gpt_* columns are NULL when detection_tier = 'statistical'
    and GPT escalation was not triggered.
    """

    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    alert_id = Column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4, index=True
    )
    transaction_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    account_id = Column(String(50), nullable=False, index=True)
    confidence_score = Column(Float, nullable=False)
    detection_tier = Column(String(20), nullable=False)  # 'statistical' or 'gpt'
    anomaly_reasons = Column(JSON, nullable=False, default=list)
    gpt_is_fraud = Column(Boolean, nullable=True)
    gpt_reasoning = Column(Text, nullable=True)
    gpt_risk_level = Column(String(20), nullable=True)  # 'high', 'medium', 'low'
    created_at = Column(DateTime(timezone=True), server_default=func.now())
