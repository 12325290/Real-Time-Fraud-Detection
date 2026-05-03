"""
Local transaction producer.
Generates/sends transactions without external message brokers.
"""

import uuid
import random
import time
from datetime import datetime
from typing import Dict, Any
import numpy as np
from loguru import logger

from backend.config import settings


# ─────────────────────────────────────────────
# Synthetic Transaction Generator
# ─────────────────────────────────────────────
MERCHANTS = [
    ("Amazon India", "retail"), ("Flipkart", "retail"), ("Swiggy", "food"),
    ("Zomato", "food"), ("MakeMyTrip", "travel"), ("IRCTC", "travel"),
    ("CoinDCX", "crypto"), ("WazirX", "crypto"), ("Dream11", "gambling"),
    ("Apollo Pharmacy", "healthcare"), ("Croma", "electronics"),
    ("BigBasket", "retail"), ("Ola", "travel"), ("Uber", "travel"),
    ("Netflix", "entertainment"),
]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "wallet"]
DEVICE_TYPES = ["mobile", "desktop", "tablet", "unknown"]

def generate_transaction(force_fraud: bool = False) -> Dict[str, Any]:
    """
    Simulate a realistic payment transaction.
    If force_fraud=True, generates a high-risk transaction with anomalous features.
    """
    now = datetime.utcnow()
    merchant, category = random.choice(MERCHANTS)

    if force_fraud:
        amount = round(random.uniform(15_000, 200_000), 2)
        hour = random.choice([0, 1, 2, 3, 4, 23])
        txn_last_hour = random.randint(8, 25)
        txn_last_day = random.randint(20, 50)
        payment_method = random.choice(["credit_card", "wallet"])
        category = random.choice(["crypto", "gambling"])
        merchant = "SuspiciousMerchant" if random.random() < 0.5 else merchant
        device = "unknown"
    else:
        amount = round(float(np.random.lognormal(mean=6.5, sigma=1.2)), 2)
        amount = min(amount, 50_000)
        hour = now.hour
        txn_last_hour = random.randint(0, 3)
        txn_last_day = random.randint(0, 12)
        payment_method = random.choice(PAYMENT_METHODS)
        device = random.choice(DEVICE_TYPES[:3])

    city = random.choice(CITIES)
    user_id = f"USR_{random.randint(1000, 9999)}"

    return {
        "transaction_id": str(uuid.uuid4()),
        "amount": amount,
        "currency": "INR",
        "merchant": merchant,
        "merchant_category": category,
        "payment_method": payment_method,
        "user_id": user_id,
        "user_country": "India",
        "user_city": city,
        "ip_address": f"103.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "device_type": device,
        "transaction_hour": hour,
        "is_weekend": 1 if now.weekday() >= 5 else 0,
        "transactions_last_hour": txn_last_hour,
        "transactions_last_day": txn_last_day,
        "created_at": now.isoformat(),
    }


# ─────────────────────────────────────────────
# In-Process Producer
# ─────────────────────────────────────────────
class TransactionProducer:
    """
    Lightweight producer compatible with previous Kafka-oriented interface.
    It does not push to a broker; processing happens directly in main.py.
    """

    def __init__(self):
        self._connected = True
        logger.info("Local producer ready (Kafka removed).")

    @property
    def is_connected(self) -> bool:
        return self._connected

    def send_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Keep compatibility: acknowledge enqueue request locally."""
        txid = transaction.get("transaction_id", "unknown")
        logger.debug(f"Queued locally: {txid[:8]}…")
        return True

    def flush(self):
        return None

    def close(self):
        logger.info("Local producer closed.")


# ─────────────────────────────────────────────
# Simulation Loop (standalone runner)
# ─────────────────────────────────────────────
def run_simulation(interval: float = 2.0, fraud_rate: float = 0.15):
    """
    Continuously generate and publish transactions at `interval` seconds.
    Run as: python -m backend.producer
    """
    producer = TransactionProducer()
    logger.info(f"🚀 Simulation started — interval={interval}s, fraud_rate={fraud_rate:.0%}")

    count = 0
    try:
        while True:
            force_fraud = random.random() < fraud_rate
            txn = generate_transaction(force_fraud=force_fraud)
            producer.send_transaction(txn)
            count += 1
            logger.info(
                f"[#{count}] TXN {txn['transaction_id'][:8]}… "
                f"₹{txn['amount']:,.0f} @ {txn['merchant']} "
                f"{'🚨 FRAUD' if force_fraud else '✅'}"
            )
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("⏹️  Simulation stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_simulation(
        interval=settings.SIMULATION_INTERVAL,
        fraud_rate=settings.SIMULATION_FRAUD_RATE,
    )
