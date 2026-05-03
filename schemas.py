"""
Pydantic schemas for API request/response validation.
Kept separate from ORM models to follow clean architecture.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime
from enum import Enum


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class TransactionStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    FLAGGED = "flagged"
    BLOCKED = "blocked"


# ─────────────────────────────────────────────
# Inbound: Create Transaction
# ─────────────────────────────────────────────
class TransactionCreate(BaseModel):
    """Payload sent by the producer / client to create a new transaction."""

    amount: float = Field(..., gt=0, description="Transaction amount in INR")
    currency: str = Field(default="INR", max_length=10)
    merchant: str = Field(..., max_length=128)
    merchant_category: Optional[str] = Field(None, max_length=64)
    payment_method: str = Field(
        ...,
        description="One of: credit_card, debit_card, upi, net_banking, wallet",
    )
    user_id: str = Field(..., max_length=64)
    user_country: Optional[str] = Field(None, max_length=64)
    user_city: Optional[str] = Field(None, max_length=64)
    ip_address: Optional[str] = Field(None, max_length=45)
    device_type: Optional[str] = Field(None, max_length=32)
    transaction_hour: Optional[int] = Field(None, ge=0, le=23)
    is_weekend: Optional[int] = Field(0, ge=0, le=1)
    transactions_last_hour: Optional[int] = Field(0, ge=0)
    transactions_last_day: Optional[int] = Field(0, ge=0)


# ─────────────────────────────────────────────
# Outbound: Transaction Response
# ─────────────────────────────────────────────
class TransactionResponse(BaseModel):
    """Full transaction record returned by the API."""

    id: int
    transaction_id: str
    amount: float
    currency: str
    merchant: str
    merchant_category: Optional[str]
    payment_method: str
    user_id: str
    user_country: Optional[str]
    user_city: Optional[str]
    ip_address: Optional[str]
    device_type: Optional[str]
    transaction_hour: Optional[int]
    is_weekend: bool
    transactions_last_hour: int
    transactions_last_day: int
    risk_score: float
    fraud_probability: float
    is_fraud: bool
    risk_level: str
    model_version: str
    status: str
    alert_sent: bool
    created_at: Optional[datetime]
    processed_at: Optional[datetime]

    class Config:
        from_attributes = True


# ─────────────────────────────────────────────
# Alert Schemas
# ─────────────────────────────────────────────
class AlertResponse(BaseModel):
    id: int
    transaction_id: str
    risk_score: float
    risk_level: str
    alert_message: Optional[str]
    acknowledged: bool
    created_at: Optional[datetime]

    class Config:
        from_attributes = True


# ─────────────────────────────────────────────
# Stats Schema
# ─────────────────────────────────────────────
class DashboardStats(BaseModel):
    total_transactions: int
    total_fraud: int
    total_safe: int
    fraud_rate: float
    avg_risk_score: float
    total_amount_processed: float
    high_risk_count: int
    medium_risk_count: int
    low_risk_count: int


# ─────────────────────────────────────────────
# WebSocket Message
# ─────────────────────────────────────────────
class WSMessage(BaseModel):
    """Standardized WebSocket message envelope."""

    type: str           # "transaction" | "alert" | "stats" | "ping"
    data: dict
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


# ─────────────────────────────────────────────
# Simulation Control
# ─────────────────────────────────────────────
class SimulationControl(BaseModel):
    action: str = Field(..., description="start | stop | single")
    force_fraud: bool = False
    interval: Optional[float] = Field(None, gt=0.1, le=30.0)
