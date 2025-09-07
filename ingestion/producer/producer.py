#!/usr/bin/env python3

import json
import os
import time
import random
import uuid
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

def generate_user_features():
    """Generate user feature events for silver layer processing"""
    users = [f"user_{i}" for i in range(1000, 2000)]
    categories = ["electronics", "clothing", "books", "home", "sports"]
    
    user_id = random.choice(users)
    event = {
        "user_id": user_id,
        "preferred_categories": random.sample(categories, k=random.randint(1, 3)),
        "avg_session_duration": round(random.uniform(60, 1800), 2),  # seconds
        "total_purchases": random.randint(0, 50),
        "avg_order_value": round(random.uniform(25, 300), 2),
        "last_active": datetime.now(timezone.utc).isoformat(),
        "engagement_score": round(random.uniform(0.1, 1.0), 3),
        "age_group": random.choice(["18-25", "26-35", "36-45", "46-55", "55+"]),
        "location": random.choice(["US-East", "US-West", "EU", "Asia"]),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    return user_id, event

def generate_product_features():
    """Generate product feature events"""
    products = [f"prod_{i}" for i in range(1, 500)]
    categories = ["electronics", "clothing", "books", "home", "sports"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
    
    product_id = random.choice(products)
    event = {
        "product_id": product_id,
        "category": random.choice(categories),
        "subcategory": f"sub_{random.randint(1, 10)}",
        "brand": random.choice(brands),
        "price": round(random.uniform(10, 500), 2),
        "avg_rating": round(random.uniform(1.0, 5.0), 1),
        "review_count": random.randint(0, 1000),
        "popularity_score": round(random.uniform(0.1, 1.0), 3),
        "in_stock": random.choice([True, False]),
        "tags": random.sample(["new", "sale", "trending", "recommended", "bestseller"], 
                             k=random.randint(0, 3)),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    return product_id, event

def generate_recommendation_signals():
    """Generate recommendation signals for real-time processing"""
    users = [f"user_{i}" for i in range(1000, 2000)]
    products = [f"prod_{i}" for i in range(1, 500)]
    algorithms = ["collaborative_filtering", "content_based", "hybrid", "trending"]
    
    user_id = random.choice(users)
    event = {
        "signal_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": random.choice(products),
        "algorithm": random.choice(algorithms),
        "confidence_score": round(random.uniform(0.1, 1.0), 3),
        "context": {
            "session_id": f"session_{random.randint(10000, 99999)}",
            "page_type": random.choice(["home", "category", "product", "search"]),
            "time_of_day": datetime.now(timezone.utc).hour,
            "day_of_week": datetime.now(timezone.utc).weekday()
        },
        "features": {
            "user_category_affinity": round(random.uniform(0, 1), 3),
            "product_popularity": round(random.uniform(0, 1), 3),
            "seasonal_factor": round(random.uniform(0.8, 1.2), 3),
            "price_sensitivity": round(random.uniform(0, 1), 3)
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    return user_id, event

def produce_user_features(producer, count=100):
    """Produce user feature events"""
    print(f"Producing {count} user feature events...")
    
    for i in range(count):
        user_id, event = generate_user_features()
        
        producer.send(
            topic='user-features',
            key=user_id,
            value=event
        )
        
        if i % 25 == 0:
            print(f"  User features: {i}/{count}")
        
        time.sleep(0.02)
    
    producer.flush()
    print("User features completed!")

def produce_product_features(producer, count=50):
    """Produce product feature events"""
    print(f"Producing {count} product feature events...")
    
    for i in range(count):
        product_id, event = generate_product_features()
        
        producer.send(
            topic='product-features',
            key=product_id,
            value=event
        )
        
        if i % 10 == 0:
            print(f"  Product features: {i}/{count}")
        
        time.sleep(0.05)
    
    producer.flush()
    print("Product features completed!")

def produce_recommendation_signals(producer, count=200):
    """Produce recommendation signal events"""
    print(f"Producing {count} recommendation signal events...")
    
    for i in range(count):
        user_id, event = generate_recommendation_signals()
        
        producer.send(
            topic='recommendation-signals',
            key=user_id,
            value=event
        )
        
        if i % 50 == 0:
            print(f"  Recommendation signals: {i}/{count}")
        
        time.sleep(0.01)
    
    producer.flush()
    print("Recommendation signals completed!")

def main():
    print("Starting recommendation system data producers...")
    producer = build_producer()
    
    try:
        produce_user_features(producer, 100)
        time.sleep(2)
        
        produce_product_features(producer, 50)
        time.sleep(2)
        
        produce_recommendation_signals(producer, 200)
        
        print("All data production completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()