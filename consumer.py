"""
Local consumer service.
Keeps compatibility for status/metrics without Kafka.
"""

import asyncio
from typing import Optional, Callable
from loguru import logger


# ─────────────────────────────────────────────
# Core Consumer Class
# ─────────────────────────────────────────────
class TransactionConsumer:
    """
    Compatibility wrapper for previous Kafka consumer contract.
    Transaction processing now happens directly through API handlers.
    """

    def __init__(self, process_callback: Optional[Callable] = None):
        self._running = False
        self._process_callback = process_callback
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stats = {
            "messages_received": 0,
            "messages_processed": 0,
            "errors": 0,
        }

    # ─────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────
    def start(self, loop: asyncio.AbstractEventLoop):
        """Mark local consumer service as active."""
        self._loop = loop
        self._running = True
        logger.info("Local consumer service started.")

    def stop(self):
        """Stop local consumer service."""
        self._running = False
        logger.info("Local consumer service stopped.")

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    @property
    def is_running(self) -> bool:
        return self._running
