"""
WebSocket Connection Manager
Manages all active WebSocket connections and broadcasts messages to clients.
Supports room-based broadcasting (all clients, or targeted).
"""

import json
import asyncio
from datetime import datetime
from typing import Dict, Set, Optional, Any
from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger


class ConnectionManager:
    """
    Thread-safe WebSocket connection manager.
    - Tracks all active connections
    - Broadcasts JSON payloads to all or specific clients
    - Handles disconnects gracefully
    """

    def __init__(self):
        # Map of connection_id → WebSocket
        self._connections: Dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()
        self._message_count = 0

    # ─────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────
    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self._connections[client_id] = websocket
        logger.info(f"🔌 WS connected: {client_id}  (total={len(self._connections)})")

        # Send welcome payload
        await self.send_personal(client_id, {
            "type": "connected",
            "data": {
                "client_id": client_id,
                "message": "Connected to Fraud Detection System",
                "active_connections": len(self._connections),
            },
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def disconnect(self, client_id: str) -> None:
        """Remove a connection by ID."""
        async with self._lock:
            self._connections.pop(client_id, None)
        logger.info(f"🔌 WS disconnected: {client_id}  (remaining={len(self._connections)})")

    # ─────────────────────────────────────────────
    # Sending
    # ─────────────────────────────────────────────
    async def send_personal(self, client_id: str, message: dict) -> bool:
        """Send a JSON message to a single client. Returns False if client gone."""
        ws = self._connections.get(client_id)
        if not ws:
            return False
        try:
            await ws.send_json(message)
            return True
        except Exception as e:
            logger.warning(f"⚠️  WS send error to {client_id}: {e}")
            await self.disconnect(client_id)
            return False

    async def broadcast(self, message: dict) -> int:
        """
        Broadcast a message to ALL connected clients.
        Returns the number of clients successfully reached.
        """
        if not self._connections:
            return 0

        self._message_count += 1
        disconnected: list[str] = []
        sent = 0

        # Snapshot current connections to avoid dict-size-change during iteration
        async with self._lock:
            snapshot = dict(self._connections)

        tasks = []
        for cid, ws in snapshot.items():
            tasks.append(self._safe_send(cid, ws, message, disconnected))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        sent = sum(1 for r in results if r is True)

        # Cleanup dead connections
        for cid in disconnected:
            await self.disconnect(cid)

        return sent

    async def _safe_send(
        self,
        client_id: str,
        ws: WebSocket,
        message: dict,
        dead_list: list,
    ) -> bool:
        try:
            await ws.send_json(message)
            return True
        except Exception:
            dead_list.append(client_id)
            return False

    async def broadcast_transaction(self, transaction_data: dict) -> None:
        """Convenience: broadcast a transaction event."""
        await self.broadcast({
            "type": "transaction",
            "data": transaction_data,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def broadcast_alert(self, alert_data: dict) -> None:
        """Convenience: broadcast a fraud alert."""
        await self.broadcast({
            "type": "alert",
            "data": alert_data,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def broadcast_stats(self, stats: dict) -> None:
        """Convenience: broadcast updated dashboard stats."""
        await self.broadcast({
            "type": "stats",
            "data": stats,
            "timestamp": datetime.utcnow().isoformat(),
        })

    # ─────────────────────────────────────────────
    # Heartbeat
    # ─────────────────────────────────────────────
    async def start_heartbeat(self, interval: int = 30) -> None:
        """Send periodic pings to keep connections alive."""
        while True:
            await asyncio.sleep(interval)
            if self._connections:
                await self.broadcast({
                    "type": "ping",
                    "data": {
                        "active_connections": len(self._connections),
                        "total_messages": self._message_count,
                    },
                    "timestamp": datetime.utcnow().isoformat(),
                })

    # ─────────────────────────────────────────────
    # Properties
    # ─────────────────────────────────────────────
    @property
    def active_count(self) -> int:
        return len(self._connections)

    @property
    def total_messages(self) -> int:
        return self._message_count


# Global singleton
manager = ConnectionManager()
