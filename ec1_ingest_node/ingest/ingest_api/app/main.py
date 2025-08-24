from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import os, uuid, json
from sqlalchemy import create_engine, text
import asyncio, json
from aiokafka import AIOKafkaProducer


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = "events.raw.v1"
producer: AIOKafkaProducer | None = None


DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS events (
      id UUID PRIMARY KEY,
      type TEXT NOT NULL,
      user_id TEXT,
      session_id TEXT,
      ts TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL
    );
    """))

class BaseEvent(BaseModel):
    user_id: str = Field(..., examples=["anon-123"])
    session_id: str = Field(..., examples=["sess-abc"])
    ts: datetime | None = None

class PageView(BaseEvent):
    type: str = "pageview"
    url: str
    referrer: str | None = None
    ua: str | None = None

class Interaction(BaseEvent):
    type: str = "interaction"
    action: str
    product_id: str
    price: float | None = None
    position: int | None = None

app = FastAPI(title="PredictApp Ingest")

@app.on_event("startup")
async def startup_kafka():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS, linger_ms=10)
    await producer.start()

@app.on_event("shutdown")
async def shutdown_kafka():
    if producer:
        await producer.stop()

@app.get("/healthz")
def health(): return {"ok": True}

@app.post("/ingest")
async def ingest(event: dict):
    et = event.get("type")
    model = PageView if et == "pageview" else Interaction if et == "interaction" else None
    if not model:
        raise HTTPException(400, "type must be 'pageview' or 'interaction'")

    parsed = model.model_validate(event)
    ts = parsed.ts or datetime.now(timezone.utc)

    row = {
        "id": str(uuid.uuid4()),
        "type": parsed.type,
        "user_id": parsed.user_id,
        "session_id": parsed.session_id,
        "ts": ts.isoformat(),
        "payload": json.dumps(event),
    }
    with engine.begin() as conn:
        conn.execute(text("""
          INSERT INTO events (id,type,user_id,session_id,ts,payload)
          VALUES (:id,:type,:user_id,:session_id,:ts,CAST(:payload AS JSONB))
        """), row)

    try:
        if producer:
            key = (parsed.session_id or parsed.user_id).encode()
            await producer.send_and_wait(KAFKA_TOPIC, json.dumps(event).encode(), key=key)
    except Exception as e:
        pass

    return {"ok": True}