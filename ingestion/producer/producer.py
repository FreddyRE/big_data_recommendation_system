import json, os, time, random, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def build_producer(bootstrap=BOOTSTRAP, attempts=8):
    last = None
    for i in range(1, attempts + 1):
        try:
            print(f"[producer] connecting to {bootstrap} (attempt {i}/{attempts})")
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all", retries=5, request_timeout_ms=30000, 
                api_version_auto_timeout_ms=30000, linger_ms=5
            )
        except NoBrokersAvailable as e:
            last = e
            time.sleep(0.5 * 2**(i-1))
    raise last

def generate_event():
    """Generate realistic clickstream event"""
    users = [f"user_{i}" for i in range(1000, 2000)]
    products = [f"prod_{i}" for i in range(1, 500)]
    categories = ["electronics", "clothing", "books", "home", "sports"]
    event_types = ["page_view", "product_click", "add_to_cart", "purchase"]
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "category": random.choice(categories),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": f"session_{random.randint(10000, 99999)}"
    }
    
    if event["event_type"] == "purchase":
        event["price"] = round(random.uniform(10, 500), 2)
        event["quantity"] = random.randint(1, 3)
    
    return event

def main():
    print("Starting clickstream producer...")
    producer = build_producer()
    
    try:
        for i in range(1000):  # Generate 1000 events
            event = generate_event()
            
            producer.send(
                topic='clickstream',
                key=event['user_id'],
                value=event
            )
            
            if i % 100 == 0:
                print(f"Produced {i} events...")
            
            time.sleep(0.01)  # Small delay
                
        producer.flush()
        print("Finished producing events!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()