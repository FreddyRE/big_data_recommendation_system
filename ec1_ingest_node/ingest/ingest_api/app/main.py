from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import os, uuid, json, logging
from sqlalchemy import create_engine, text
import asyncio, json
from aiokafka import AIOKafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    try:
        logger.info(f"Connecting to Kafka brokers: {KAFKA_BROKERS}")
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS, 
            linger_ms=10,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        logger.info("Kafka producer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        producer = None

@app.on_event("shutdown")
async def shutdown_kafka():
    if producer:
        await producer.stop()
        logger.info("Kafka producer stopped")

@app.get("/healthz")
def health(): 
    return {
        "ok": True, 
        "kafka_connected": producer is not None,
        "kafka_brokers": KAFKA_BROKERS,
        "kafka_topic": KAFKA_TOPIC
    }

@app.post("/ingest")
async def ingest(event: dict):
    logger.info(f"Received event: {event}")
    
    et = event.get("type")
    model = PageView if et == "pageview" else Interaction if et == "interaction" else None
    if not model:
        raise HTTPException(400, "type must be 'pageview' or 'interaction'")

    parsed = model.model_validate(event)
    ts = parsed.ts or datetime.now(timezone.utc)

    # Database row
    row = {
        "id": str(uuid.uuid4()),
        "type": parsed.type,
        "user_id": parsed.user_id,
        "session_id": parsed.session_id,
        "ts": ts.isoformat(),
        "payload": json.dumps(event),
    }
    
    # Save to database
    with engine.begin() as conn:
        conn.execute(text("""
          INSERT INTO events (id,type,user_id,session_id,ts,payload)
          VALUES (:id,:type,:user_id,:session_id,:ts,CAST(:payload AS JSONB))
        """), row)
    
    logger.info(f"Saved event to database with ID: {row['id']}")

    # Prepare Kafka message with timestamp
    kafka_event = event.copy()
    kafka_event["ts"] = ts.isoformat()
    kafka_event["event_id"] = row["id"]  # Add unique ID
    
    # Send to Kafka
    kafka_success = False
    try:
        if producer:
            key = (parsed.session_id or parsed.user_id).encode()
            logger.info(f"Sending to Kafka topic {KAFKA_TOPIC}: {kafka_event}")
            
            record_metadata = await producer.send_and_wait(
                KAFKA_TOPIC, 
                kafka_event,  # Will be serialized by value_serializer
                key=key
            )
            
            logger.info(f"Message sent to Kafka successfully - Topic: {record_metadata.topic}, "
                       f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            kafka_success = True
        else:
            logger.error("Kafka producer not available")
            
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        # Don't raise exception - we still saved to database
    
    return {
        "ok": True,
        "event_id": row["id"],
        "kafka_sent": kafka_success
    }

# Add debug endpoint to test Kafka connectivity
@app.post("/test-kafka")
async def test_kafka():
    if not producer:
        return {"error": "Kafka producer not available"}
    
    test_message = {
        "type": "test",
        "user_id": "test_user",
        "session_id": "test_session",
        "ts": datetime.now(timezone.utc).isoformat(),
        "message": "test message"
    }
    
    try:
        record_metadata = await producer.send_and_wait(
            KAFKA_TOPIC, 
            test_message,
            key=b"test"
        )
        return {
            "success": True,
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset
        }
    except Exception as e:
        return {"error": str(e)}