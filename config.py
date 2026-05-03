"""
Configuration settings for the Fraud Detection System.
Uses environment variables with sensible defaults for local development.
SQLite is the default DB so the app runs with zero external dependencies.
"""

from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "Real-Time Fraud Detection System"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Database — set USE_SQLITE=false to use MySQL instead
    USE_SQLITE: bool = True
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = "root"
    DB_NAME: str = "fraud_detection"
    SQLITE_PATH: str = "fraud_detection.db"

    # ML Model
    MODEL_PATH: str = "backend/ml/fraud_model.joblib"
    SCALER_PATH: str = "backend/ml/scaler.joblib"
    FRAUD_THRESHOLD: float = 0.5   # probability threshold for fraud
    HIGH_RISK_SCORE: int = 70       # score above which triggers alert

    # Simulation
    SIMULATION_INTERVAL: float = 2.0   # seconds between auto-generated transactions
    SIMULATION_FRAUD_RATE: float = 0.15  # 15% fraud rate in simulated data

    @property
    def DATABASE_URL(self) -> str:
        if self.USE_SQLITE:
            return f"sqlite:///{self.SQLITE_PATH}"
        return (
            f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
