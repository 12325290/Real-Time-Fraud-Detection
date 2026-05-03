"""
FastAPI Main Application — Real-Time Fraud Detection System
Wires together: DB, local transaction pipeline, ML Model, WebSockets, REST API
"""

import uuid
import random
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect, Depends,
    HTTPException, BackgroundTasks, Query, status
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from loguru import logger
import os

from backend.config import settings
from backend.database import (
    create_tables, get_db, Transaction, FraudAlert,
    ModelMetrics, RiskLevel, TransactionStatus
)
from backend.schemas import (
    TransactionCreate, TransactionResponse, AlertResponse,
    DashboardStats, SimulationControl
)
from backend.producer import TransactionProducer, generate_transaction
from backend.consumer import TransactionConsumer
from backend.websocket_manager import manager
from backend.ml.ml_model import FraudDetector


# ─────────────────────────────────────────────
# Globals (initialized in lifespan)
# ─────────────────────────────────────────────
producer: Optional[TransactionProducer] = None
consumer: Optional[TransactionConsumer] = None
detector: Optional[FraudDetector] = None
simulation_task: Optional[asyncio.Task] = None
simulation_running: bool = False


# ─────────────────────────────────────────────
# Core Processing Pipeline
# ─────────────────────────────────────────────
async def process_transaction(txn_data: dict) -> dict:
    """
    Full pipeline for a single transaction:
      1. ML inference → risk score
      2. Persist to MySQL
      3. Trigger alert if high risk
      4. Broadcast via WebSocket
    Returns the enriched transaction dict.
    """
    from backend.database import SessionLocal

    db: Session = SessionLocal()
    try:
        # ── 1. ML Inference ──────────────────────────────
        ml_result = detector.predict(txn_data)

        risk_score = ml_result["risk_score"]
        fraud_prob = ml_result["fraud_probability"]
        is_fraud = ml_result["is_fraud"]
        risk_level_str = ml_result["risk_level"]

        risk_level = RiskLevel(risk_level_str)

        if is_fraud:
            txn_status = TransactionStatus.BLOCKED
        elif risk_level == RiskLevel.MEDIUM:
            txn_status = TransactionStatus.FLAGGED
        else:
            txn_status = TransactionStatus.APPROVED

        # ── 2. Persist Transaction ────────────────────────
        txn_record = Transaction(
            transaction_id=txn_data.get("transaction_id", str(uuid.uuid4())),
            amount=txn_data.get("amount", 0),
            currency=txn_data.get("currency", "INR"),
            merchant=txn_data.get("merchant", "Unknown"),
            merchant_category=txn_data.get("merchant_category"),
            payment_method=txn_data.get("payment_method", "unknown"),
            user_id=txn_data.get("user_id", "unknown"),
            user_country=txn_data.get("user_country"),
            user_city=txn_data.get("user_city"),
            ip_address=txn_data.get("ip_address"),
            device_type=txn_data.get("device_type"),
            transaction_hour=txn_data.get("transaction_hour"),
            is_weekend=bool(txn_data.get("is_weekend", 0)),
            transactions_last_hour=txn_data.get("transactions_last_hour", 0),
            transactions_last_day=txn_data.get("transactions_last_day", 0),
            risk_score=risk_score,
            fraud_probability=fraud_prob,
            is_fraud=is_fraud,
            risk_level=risk_level,
            model_version=ml_result.get("model_version", "v1.0"),
            status=txn_status,
            processed_at=datetime.utcnow(),
        )
        db.add(txn_record)
        db.flush()  # get auto-increment ID without committing yet

        # ── 3. Fraud Alert ────────────────────────────────
        alert_data = None
        if risk_score >= settings.HIGH_RISK_SCORE:
            alert_msg = (
                f"🚨 HIGH RISK: ₹{txn_data.get('amount', 0):,.0f} transaction "
                f"at {txn_data.get('merchant', 'Unknown')} — "
                f"Risk Score: {risk_score:.1f}/100"
            )
            alert = FraudAlert(
                transaction_id=txn_record.transaction_id,
                risk_score=risk_score,
                risk_level=risk_level,
                alert_message=alert_msg,
            )
            db.add(alert)
            db.flush()
            txn_record.alert_sent = True
            alert_data = alert.to_dict() if hasattr(alert, "to_dict") else {
                "transaction_id": txn_record.transaction_id,
                "risk_score": risk_score,
                "risk_level": risk_level_str,
                "alert_message": alert_msg,
                "acknowledged": False,
                "created_at": datetime.utcnow().isoformat(),
            }

        db.commit()
        db.refresh(txn_record)

        enriched = txn_record.to_dict()

        # ── 4. Broadcast ──────────────────────────────────
        await manager.broadcast_transaction(enriched)

        if alert_data:
            await manager.broadcast_alert(alert_data)

        # Broadcast stats every 5 transactions
        count = db.query(func.count(Transaction.id)).scalar() or 0
        if count % 5 == 0:
            stats = _compute_stats(db)
            await manager.broadcast_stats(stats)

        logger.info(
            f"✅ Processed {enriched['transaction_id'][:8]}… "
            f"₹{enriched['amount']:,.0f} | "
            f"Risk={risk_score:.1f} ({risk_level_str.upper()}) | "
            f"Fraud={is_fraud}"
        )
        return enriched

    except Exception as e:
        db.rollback()
        logger.error(f"❌ process_transaction error: {e}")
        raise
    finally:
        db.close()


def _compute_stats(db: Session) -> dict:
    """Compute dashboard aggregate stats."""
    total = db.query(func.count(Transaction.id)).scalar() or 0
    fraud = db.query(func.count(Transaction.id)).filter(Transaction.is_fraud == True).scalar() or 0
    safe = total - fraud
    avg_risk = db.query(func.avg(Transaction.risk_score)).scalar() or 0
    total_amount = db.query(func.sum(Transaction.amount)).scalar() or 0
    high = db.query(func.count(Transaction.id)).filter(Transaction.risk_level == RiskLevel.HIGH).scalar() or 0
    medium = db.query(func.count(Transaction.id)).filter(Transaction.risk_level == RiskLevel.MEDIUM).scalar() or 0
    low = db.query(func.count(Transaction.id)).filter(Transaction.risk_level == RiskLevel.LOW).scalar() or 0
    return {
        "total_transactions": total,
        "total_fraud": fraud,
        "total_safe": safe,
        "fraud_rate": round((fraud / total * 100) if total else 0, 2),
        "avg_risk_score": round(float(avg_risk), 2),
        "total_amount_processed": round(float(total_amount), 2),
        "high_risk_count": high,
        "medium_risk_count": medium,
        "low_risk_count": low,
    }


# ─────────────────────────────────────────────
# Lifespan (startup / shutdown)
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, consumer, detector

    logger.info("🚀 Starting Fraud Detection System …")

    # 1. DB
    try:
        create_tables()
    except Exception as e:
        logger.warning(f"⚠️  DB init failed (running without DB): {e}")

    # 2. ML Model
    detector = FraudDetector.get_instance()

    # 3. Local producer
    producer = TransactionProducer()

    # 4. Local consumer service
    consumer = TransactionConsumer(process_callback=process_transaction)
    loop = asyncio.get_event_loop()
    consumer.start(loop)

    # 5. Heartbeat
    asyncio.create_task(manager.start_heartbeat(interval=30))

    logger.success("✅ System ready.")
    yield

    # Shutdown
    logger.info("⏹️  Shutting down …")
    if simulation_task:
        simulation_task.cancel()
    if consumer:
        consumer.stop()
    if producer:
        producer.flush()
        producer.close()


# ─────────────────────────────────────────────
# App Instance
# ─────────────────────────────────────────────
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Real-time fraud detection powered by FastAPI, ML, and WebSockets.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


# ─────────────────────────────────────────────
# WebSocket Endpoint
# ─────────────────────────────────────────────
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_id)
    try:
        while True:
            # Keep connection alive; handle incoming pings from client
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"WS error ({client_id}): {e}")
        await manager.disconnect(client_id)


# ─────────────────────────────────────────────
# REST: Frontend Serve
# ─────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def serve_frontend():
    index = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return {"message": "Fraud Detection API is running.", "docs": "/docs"}


# ─────────────────────────────────────────────
# REST: Transactions
# ─────────────────────────────────────────────
@app.post("/api/transactions", status_code=status.HTTP_201_CREATED)
async def create_transaction(payload: TransactionCreate):
    """Submit a transaction for immediate ML scoring via local pipeline."""
    txn_data = payload.model_dump()
    txn_data["transaction_id"] = str(uuid.uuid4())
    txn_data["created_at"] = datetime.utcnow().isoformat()

    if not txn_data.get("transaction_hour"):
        txn_data["transaction_hour"] = datetime.utcnow().hour

    # Keep local producer path for backward compatibility/metrics
    if producer:
        producer.send_transaction(txn_data)

    # Also process immediately so UI gets instant feedback
    result = await process_transaction(txn_data)
    return result


@app.get("/api/transactions", response_model=List[dict])
async def list_transactions(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    risk_level: Optional[str] = Query(None),
    is_fraud: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
):
    """Paginated list of transactions with optional filters."""
    q = db.query(Transaction).order_by(desc(Transaction.created_at))

    if risk_level:
        try:
            q = q.filter(Transaction.risk_level == RiskLevel(risk_level))
        except ValueError:
            raise HTTPException(400, f"Invalid risk_level: {risk_level}")

    if is_fraud is not None:
        q = q.filter(Transaction.is_fraud == is_fraud)

    transactions = q.offset(skip).limit(limit).all()
    return [t.to_dict() for t in transactions]


@app.get("/api/transactions/{transaction_id}")
async def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """Fetch a single transaction by transaction_id."""
    txn = db.query(Transaction).filter(
        Transaction.transaction_id == transaction_id
    ).first()
    if not txn:
        raise HTTPException(404, "Transaction not found")
    return txn.to_dict()


# ─────────────────────────────────────────────
# REST: Alerts
# ─────────────────────────────────────────────
@app.get("/api/alerts", response_model=List[dict])
async def list_alerts(
    limit: int = Query(20, ge=1, le=100),
    unread_only: bool = Query(False),
    db: Session = Depends(get_db),
):
    q = db.query(FraudAlert).order_by(desc(FraudAlert.created_at))
    if unread_only:
        q = q.filter(FraudAlert.acknowledged == False)
    alerts = q.limit(limit).all()
    return [a.to_dict() for a in alerts]


@app.patch("/api/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int, db: Session = Depends(get_db)):
    alert = db.query(FraudAlert).filter(FraudAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(404, "Alert not found")
    alert.acknowledged = True
    db.commit()
    return {"success": True, "alert_id": alert_id}


# ─────────────────────────────────────────────
# REST: Dashboard Stats
# ─────────────────────────────────────────────
@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    return _compute_stats(db)


@app.get("/api/stats/timeseries")
async def get_timeseries(
    hours: int = Query(24, ge=1, le=168),
    db: Session = Depends(get_db),
):
    """Hourly transaction counts for the last N hours (for Chart.js)."""
    since = datetime.utcnow() - timedelta(hours=hours)
    transactions = (
        db.query(Transaction)
        .filter(Transaction.created_at >= since)
        .order_by(Transaction.created_at)
        .all()
    )

    # Group by hour
    buckets: dict = {}
    for txn in transactions:
        hour_key = txn.created_at.strftime("%Y-%m-%dT%H:00")
        if hour_key not in buckets:
            buckets[hour_key] = {"time": hour_key, "total": 0, "fraud": 0, "safe": 0}
        buckets[hour_key]["total"] += 1
        if txn.is_fraud:
            buckets[hour_key]["fraud"] += 1
        else:
            buckets[hour_key]["safe"] += 1

    return list(buckets.values())


# ─────────────────────────────────────────────
# REST: Simulation Control
# ─────────────────────────────────────────────
@app.post("/api/simulation")
async def control_simulation(payload: SimulationControl, background_tasks: BackgroundTasks):
    """Start, stop, or trigger a single simulated transaction."""
    global simulation_task, simulation_running

    if payload.action == "single":
        txn = generate_transaction(force_fraud=payload.force_fraud)
        if producer:
            producer.send_transaction(txn)
        result = await process_transaction(txn)
        return {"message": "Transaction generated", "transaction": result}

    elif payload.action == "start":
        if simulation_running:
            return {"message": "Simulation already running"}
        simulation_running = True
        interval = payload.interval or settings.SIMULATION_INTERVAL

        async def _sim_loop():
            global simulation_running
            while simulation_running:
                force_fraud = random.random() < settings.SIMULATION_FRAUD_RATE
                txn = generate_transaction(force_fraud=force_fraud)
                if producer:
                    producer.send_transaction(txn)
                await process_transaction(txn)
                await asyncio.sleep(interval)

        simulation_task = asyncio.create_task(_sim_loop())
        return {"message": f"Simulation started (interval={interval}s)"}

    elif payload.action == "stop":
        simulation_running = False
        if simulation_task:
            simulation_task.cancel()
            simulation_task = None
        return {"message": "Simulation stopped"}

    raise HTTPException(400, f"Unknown action: {payload.action}")


@app.get("/api/simulation/status")
async def simulation_status():
    return {
        "running": simulation_running,
        "producer_connected": producer.is_connected if producer else False,
        "consumer_running": consumer.is_running if consumer else False,
        "consumer_stats": consumer.stats if consumer else {},
        "active_ws_connections": manager.active_count,
    }


# ─────────────────────────────────────────────
# REST: System Health
# ─────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "version": settings.APP_VERSION,
        "producer": producer.is_connected if producer else False,
        "ml_model_loaded": detector is not None,
        "ws_connections": manager.active_count,
    }


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_level="info",
    )
