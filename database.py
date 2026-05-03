"""
Database configuration using SQLAlchemy ORM with MySQL.
Defines all models and provides session management.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    Boolean, DateTime, Text, Enum as SAEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool, StaticPool
from datetime import datetime
from typing import Generator
import enum
from loguru import logger

from backend.config import settings

# ─────────────────────────────────────────────
# Engine & Session Factory
# ─────────────────────────────────────────────
if settings.USE_SQLITE:
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
else:
    engine = create_engine(
        settings.DATABASE_URL,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────
class RiskLevel(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class TransactionStatus(str, enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    FLAGGED = "flagged"
    BLOCKED = "blocked"


# ─────────────────────────────────────────────
# ORM Models
# ─────────────────────────────────────────────
class Transaction(Base):
    """Core transaction record storing all payment and fraud analysis data."""

    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    transaction_id = Column(String(64), unique=True, index=True, nullable=False)

    # Payment fields
    amount = Column(Float, nullable=False)
    currency = Column(String(10), default="INR")
    merchant = Column(String(128), nullable=False)
    merchant_category = Column(String(64), nullable=True)
    payment_method = Column(String(32), nullable=False)

    # User / location
    user_id = Column(String(64), nullable=False, index=True)
    user_country = Column(String(64), nullable=True)
    user_city = Column(String(64), nullable=True)
    ip_address = Column(String(45), nullable=True)
    device_type = Column(String(32), nullable=True)

    # Transaction metadata
    transaction_hour = Column(Integer, nullable=True)   # 0–23
    is_weekend = Column(Boolean, default=False)
    transactions_last_hour = Column(Integer, default=0)  # velocity feature
    transactions_last_day = Column(Integer, default=0)

    # ML outputs
    risk_score = Column(Float, default=0.0)          # 0–100
    fraud_probability = Column(Float, default=0.0)   # 0.0–1.0
    is_fraud = Column(Boolean, default=False)
    risk_level = Column(SAEnum(RiskLevel), default=RiskLevel.LOW)
    model_version = Column(String(32), default="v1.0")

    # Status
    status = Column(SAEnum(TransactionStatus), default=TransactionStatus.PENDING)
    alert_sent = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    processed_at = Column(DateTime, nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "currency": self.currency,
            "merchant": self.merchant,
            "merchant_category": self.merchant_category,
            "payment_method": self.payment_method,
            "user_id": self.user_id,
            "user_country": self.user_country,
            "user_city": self.user_city,
            "ip_address": self.ip_address,
            "device_type": self.device_type,
            "transaction_hour": self.transaction_hour,
            "is_weekend": self.is_weekend,
            "transactions_last_hour": self.transactions_last_hour,
            "transactions_last_day": self.transactions_last_day,
            "risk_score": round(self.risk_score, 2),
            "fraud_probability": round(self.fraud_probability, 4),
            "is_fraud": self.is_fraud,
            "risk_level": self.risk_level.value if self.risk_level else "low",
            "model_version": self.model_version,
            "status": self.status.value if self.status else "pending",
            "alert_sent": self.alert_sent,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }


class FraudAlert(Base):
    """Audit log of every fraud alert triggered."""

    __tablename__ = "fraud_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), index=True, nullable=False)
    risk_score = Column(Float, nullable=False)
    risk_level = Column(SAEnum(RiskLevel), nullable=False)
    alert_message = Column(Text, nullable=True)
    acknowledged = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "transaction_id": self.transaction_id,
            "risk_score": self.risk_score,
            "risk_level": self.risk_level.value,
            "alert_message": self.alert_message,
            "acknowledged": self.acknowledged,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ModelMetrics(Base):
    """Tracks ML model performance metrics over time."""

    __tablename__ = "model_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_version = Column(String(32), nullable=False)
    accuracy = Column(Float, nullable=True)
    precision = Column(Float, nullable=True)
    recall = Column(Float, nullable=True)
    f1_score = Column(Float, nullable=True)
    total_predictions = Column(Integer, default=0)
    fraud_detected = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def create_tables():
    """Create all tables if they don't exist."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created / verified.")
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        raise


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a DB session and ensures cleanup."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
