import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.session import Base, engine
from app.routes import alerts, health, transactions

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [API] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)


# ── WebSocket Connection Manager ──────────────────────────────────
# Maintains the list of active WebSocket connections.
# Why a class? State needs to persist across requests — a module-level
# list would work but a class makes the intent explicit and testable.


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info(f"WebSocket connected. Active connections: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
        log.info(f"WebSocket disconnected. Active connections: {len(self.active)}")

    async def broadcast(self, message: str):
        """Send message to all connected clients. Remove dead connections."""
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)


manager = ConnectionManager()


# ── Kafka alert consumer (background task) ────────────────────────
# This runs as an asyncio task for the lifetime of the API.
# It consumes the alerts topic and broadcasts to WebSocket clients.
# Why asyncio instead of a thread?
# FastAPI is async — mixing threads with async code requires
# careful handling. Running the Kafka loop in a thread and using
# asyncio.run_coroutine_threadsafe to bridge into the async world
# is the cleanest pattern here.


async def kafka_alert_consumer():
    """
    Background task: consume alerts from Kafka and broadcast
    to all connected WebSocket clients.
    """
    import threading

    loop = asyncio.get_event_loop()

    def _consume():
        from kafka import KafkaConsumer
        from kafka.errors import NoBrokersAvailable

        retries = 15
        for attempt in range(retries):
            try:
                consumer = KafkaConsumer(
                    settings.alert_topic,
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    group_id="fraud-api-alert-group",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    auto_offset_reset="latest",  # only new alerts, not history
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                )
                log.info("✓ Alert consumer connected to Kafka")

                while True:
                    try:
                        for message in consumer:
                            alert = message.value
                            alert_json = json.dumps(alert)
                            # Bridge from thread → async event loop
                            asyncio.run_coroutine_threadsafe(
                                manager.broadcast(alert_json), loop
                            )
                    except StopIteration:
                        # consumer_timeout_ms expired — loop again
                        pass
            except NoBrokersAvailable:
                log.warning(
                    f"Kafka not ready for alert consumer "
                    f"(attempt {attempt + 1}/{retries}), retrying..."
                )
                time.sleep(5)

    thread = threading.Thread(target=_consume, daemon=True)
    thread.start()


# ── DB initialization ─────────────────────────────────────────────


def init_db(retries: int = 5, delay: float = 3.0) -> None:
    for attempt in range(retries):
        try:
            Base.metadata.create_all(bind=engine)
            log.info("✓ Database tables verified")
            return
        except Exception as e:
            if attempt < retries - 1:
                log.warning(f"DB not ready (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise RuntimeError(f"Could not initialize DB: {e}")


# ── Lifespan ──────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    init_db()
    await kafka_alert_consumer()
    log.info(f"✓ {settings.app_name} v{settings.app_version} started")
    yield
    log.info("✓ Shutting down")


# ── App ───────────────────────────────────────────────────────────

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Real-time fraud detection API with WebSocket alert streaming",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(transactions.router, prefix="/api/v1", tags=["transactions"])
app.include_router(alerts.router, prefix="/api/v1", tags=["alerts"])


# ── WebSocket endpoint ────────────────────────────────────────────


@app.websocket("/api/v1/alerts/live")
async def websocket_alerts(websocket: WebSocket):
    """
    WebSocket endpoint for real-time alert streaming.
    Clients connect once and receive every new fraud alert
    within ~1 second of detection.

    Why WebSocket instead of Server-Sent Events (SSE)?
    WebSocket is bidirectional — the client could send filters
    or acknowledgments in the future. SSE is simpler but
    unidirectional. Grafana supports both; WebSocket is more
    flexible for future features.
    """
    await manager.connect(websocket)
    try:
        # Keep connection alive — wait for client to disconnect
        while True:
            await asyncio.sleep(30)
            # Send ping to detect dead connections
            await websocket.send_text('{"type": "ping"}')
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


@app.get("/")
def root():
    return {
        "message": f"Welcome to {settings.app_name}",
        "docs": "/docs",
        "health": "/api/v1/health",
        "websocket": "/api/v1/alerts/live",
    }
