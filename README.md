# 🛡️ Real-Time Fraud Detection System

A production-grade fintech fraud detection platform powered by **FastAPI**, **MySQL/SQLite**, **Scikit-learn**, and **WebSockets**.

---

## 🏗️ Architecture

```
User/Browser  ──WebSocket──►  FastAPI  ◄──REST──  Simulation
                                │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
               Local Queue    ML Model    MySQL
               (in-process)  (RandomForest)
                    │
              (ML + DB + Alert + WS broadcast)
```

---

## ⚡ Quick Start (3 Steps)

### Step 1 — Install Python dependencies

```bash
cd "Fraud detection system"
pip install -r requirements.txt
```

### Step 2 — Set up MySQL

Open MySQL and run:
```sql
CREATE DATABASE fraud_detection;
```

Edit `.env` if your MySQL credentials differ from `root/root`.

### Step 3 — Run the app

```bash
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Open your browser at: **http://localhost:8000**

> The ML model trains automatically on first launch (~10 seconds).  
> Tables are created automatically.  
> Click **▶ Start Simulation** on the dashboard to stream transactions.

---

## 🚀 Runtime Mode

This project now runs fully in-process with no Kafka dependency.

### Start FastAPI
```bash
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

---

## 🧠 Train ML Model Standalone

```bash
python -m backend.ml.ml_model
```

---

## 📡 Key API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Dashboard UI |
| `GET` | `/api/health` | System health check |
| `GET` | `/api/stats` | Dashboard KPI stats |
| `GET` | `/api/transactions` | List transactions (filterable) |
| `POST` | `/api/transactions` | Submit a transaction |
| `GET` | `/api/alerts` | List fraud alerts |
| `POST` | `/api/simulation` | Control simulation (`start`/`stop`/`single`) |
| `WS` | `/ws/{client_id}` | WebSocket live stream |
| `GET` | `/docs` | Interactive Swagger UI |

---

## 🎨 Dashboard Features

- **Live transaction feed** with risk color coding (Green/Yellow/Red)
- **Real-time charts**: Volume over time + Fraud ratio doughnut
- **Risk distribution bars** (Low / Medium / High)
- **Fraud alert toasts** with popup notifications
- **Transaction detail modal** with risk gauge
- **Simulation controls**: Start auto-stream, force single, force fraud

---

## 📁 Project Structure

```
Fraud detection system/
├── backend/
│   ├── main.py              ← FastAPI app + pipeline
│   ├── producer.py          ← Local producer + simulator
│   ├── consumer.py          ← Local consumer compatibility wrapper
│   ├── database.py          ← SQLAlchemy models (MySQL)
│   ├── schemas.py           ← Pydantic request/response schemas
│   ├── websocket_manager.py ← WS connection manager
│   ├── config.py            ← Settings (env-based)
│   └── ml/
│       └── ml_model.py      ← Random Forest model + inference
├── frontend/
│   ├── index.html           ← Dashboard UI
│   ├── style.css            ← Dark fintech styles
│   └── script.js            ← WS client + charts + interactions
├── .env                     ← Environment config
├── requirements.txt
└── README.md
```

---

## ⚙️ Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | MySQL host |
| `DB_USER` | `root` | MySQL user |
| `DB_PASSWORD` | `root` | MySQL password |
| `DB_NAME` | `fraud_detection` | Database name |
| `HIGH_RISK_SCORE` | `70` | Alert threshold (0–100) |
| `SIMULATION_INTERVAL` | `2.0` | Seconds between auto transactions |
| `SIMULATION_FRAUD_RATE` | `0.15` | Fraud % in simulation |

---

## 🤖 ML Model Details

- **Algorithm**: Random Forest (200 trees, balanced class weights)
- **Training data**: 15,000 synthetic transactions (15% fraud)
- **Features**: Amount, transaction hour, velocity, payment method, merchant category, device type, + engineered features
- **Output**: Fraud probability (0–1) → Risk score (0–100)
- **Risk levels**: Low (<40), Medium (40–70), High (≥70)
- **Model persisted**: `backend/ml/fraud_model.joblib`

---

## 🐛 Troubleshooting

| Problem | Fix |
|---------|-----|
| MySQL auth error | Update `DB_PASSWORD` in `.env` |
| Model not loading | Delete `.joblib` files, restart to retrain |
| Port 8000 in use | Change `PORT=8001` in `.env` |
| CORS error | Already enabled for all origins in dev mode |
