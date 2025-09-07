# 1-ingestion/producers/ecommerce_producers.py
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import uuid
import logging
from typing import Dict, List
import threading
import os
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class EventType(Enum):
    VIEW = "view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SEARCH = "search"
    RECOMMENDATION_SHOWN = "recommendation_shown"
    RECOMMENDATION_CLICKED = "recommendation_clicked"

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    price: float
    brand: str
    rating: float

@dataclass
class User:
    user_id: str
    email: str
    age: int
    gender: str
    location: str
    signup_date: datetime
    preferences: List[str]

class ECommerceDataGenerator:
    def __init__(self):
        self.products = self._generate_products()
        self.users = self._generate_users()
        self.sessions = {}  # Track user sessions
        
    def _generate_products(self, count=1000) -> List[Product]:
        """Generate sample products"""
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 
                     'Sports', 'Beauty', 'Automotive', 'Health', 'Toys', 'Food']
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        
        products = []
        for i in range(count):
            products.append(Product(
                product_id=f"prod_{i:06d}",
                name=fake.catch_phrase(),
                category=random.choice(categories),
                price=round(random.uniform(10, 1000), 2),
                brand=random.choice(brands),
                rating=round(random.uniform(1, 5), 1)
            ))
        return products
    
    def _generate_users(self, count=10000) -> List[User]:
        """Generate sample users"""
        users = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 
                     'Sports', 'Beauty', 'Automotive', 'Health', 'Toys', 'Food']
        
        for i in range(count):
            users.append(User(
                user_id=f"user_{i:06d}",
                email=fake.email(),
                age=random.randint(18, 80),
                gender=random.choice(['M', 'F', 'Other']),
                location=fake.city(),
                signup_date=fake.date_between(start_date='-2y', end_date='today'),
                preferences=random.sample(categories, random.randint(1, 3))
            ))
        return users

class ClickstreamProducer:
    def __init__(self, kafka_servers: str, topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = topic
        self.data_gen = ECommerceDataGenerator()
        
    def generate_clickstream_event(self) -> Dict:
        """Generate a single clickstream event"""
        user = random.choice(self.data_gen.users)
        product = random.choice(self.data_gen.products)
        event_type = random.choices(
            list(EventType), 
            weights=[40, 25, 15, 5, 10, 3, 2]  # Weighted by likelihood
        )[0]
        
        # Generate session if not exists
        session_id = self.data_gen.sessions.get(user.user_id, str(uuid.uuid4()))
        self.data_gen.sessions[user.user_id] = session_id
        
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'session_id': session_id,
            'user_id': user.user_id,
            'event_type': event_type.value,
            'product_id': product.product_id,
            'product_category': product.category,
            'product_price': product.price,
            'device_type': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['chrome', 'firefox', 'safari', 'edge']),
            'referrer': random.choice(['google', 'facebook', 'direct', 'email']),
            'page_url': f"/product/{product.product_id}",
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
            'location': user.location
        }
        
        # Add event-specific fields
        if event_type == EventType.SEARCH:
            event['search_query'] = fake.word()
            event['search_results_count'] = random.randint(0, 100)
            
        elif event_type == EventType.PURCHASE:
            event['quantity'] = random.randint(1, 5)
            event['total_amount'] = event['quantity'] * product.price
            event['payment_method'] = random.choice(['credit_card', 'debit_card', 'paypal'])
            
        elif event_type == EventType.RECOMMENDATION_SHOWN:
            event['recommendation_algorithm'] = random.choice(['collaborative', 'content_based', 'hybrid'])
            event['recommended_products'] = [random.choice(self.data_gen.products).product_id for _ in range(5)]
        
        return event
    
    def start_producing(self, events_per_second: int = 10):
        """Start producing clickstream events"""
        logger.info(f"Starting clickstream producer: {events_per_second} events/sec")
        
        while True:
            try:
                event = self.generate_clickstream_event()
                
                # Use user_id as key for partitioning
                self.producer.send(
                    self.topic,
                    key=event['user_id'],
                    value=event
                )
                
                logger.debug(f"Sent event: {event['event_type']} for user {event['user_id']}")
                time.sleep(1 / events_per_second)
                
            except Exception as e:
                logger.error(f"Error producing event: {e}")
                time.sleep(1)

class UserProfileProducer:
    def __init__(self, kafka_servers: str, topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8')
        )
        self.topic = topic
        self.data_gen = ECommerceDataGenerator()
    
    def generate_user_update(self) -> Dict:
        """Generate user profile update"""
        user = random.choice(self.data_gen.users)
        
        return {
            'user_id': user.user_id,
            'timestamp': datetime.now().isoformat(),
            'email': user.email,
            'age': user.age,
            'gender': user.gender,
            'location': user.location,
            'signup_date': user.signup_date.isoformat(),
            'preferences': user.preferences,
            'total_orders': random.randint(0, 100),
            'total_spent': round(random.uniform(0, 5000), 2),
            'avg_order_value': round(random.uniform(20, 200), 2),
            'last_login': fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
            'subscription_status': random.choice(['active', 'inactive', 'premium']),
            'marketing_consent': random.choice([True, False])
        }
    
    def start_producing(self, events_per_minute: int = 5):
        """Start producing user profile updates"""
        logger.info(f"Starting user profile producer: {events_per_minute} events/min")
        
        while True:
            try:
                user_update = self.generate_user_update()
                
                self.producer.send(
                    self.topic,
                    key=user_update['user_id'],
                    value=user_update
                )
                
                logger.debug(f"Sent user update for: {user_update['user_id']}")
                time.sleep(60 / events_per_minute)
                
            except Exception as e:
                logger.error(f"Error producing user update: {e}")
                time.sleep(1)

class ProductCatalogProducer:
    def __init__(self, kafka_servers: str, topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8')
        )
        self.topic = topic
        self.data_gen = ECommerceDataGenerator()
    
    def generate_product_update(self) -> Dict:
        """Generate product catalog update"""
        product = random.choice(self.data_gen.products)
        
        return {
            'product_id': product.product_id,
            'timestamp': datetime.now().isoformat(),
            'name': product.name,
            'category': product.category,
            'price': product.price,
            'brand': product.brand,
            'rating': product.rating,
            'stock_quantity': random.randint(0, 1000),
            'description': fake.text(max_nb_chars=200),
            'features': [fake.word() for _ in range(random.randint(3, 8))],
            'weight': round(random.uniform(0.1, 50), 2),
            'dimensions': {
                'length': round(random.uniform(1, 100), 1),
                'width': round(random.uniform(1, 100), 1),
                'height': round(random.uniform(1, 100), 1)
            },
            'availability': random.choice(['in_stock', 'out_of_stock', 'limited']),
            'discount_percentage': random.choice([0, 5, 10, 15, 20, 25]),
            'tags': random.sample(['new', 'popular', 'sale', 'premium', 'eco-friendly'], 
                                random.randint(0, 3))
        }
    
    def start_producing(self, events_per_minute: int = 2):
        """Start producing product updates"""
        logger.info(f"Starting product catalog producer: {events_per_minute} events/min")
        
        while True:
            try:
                product_update = self.generate_product_update()
                
                self.producer.send(
                    self.topic,
                    key=product_update['product_id'],
                    value=product_update
                )
                
                logger.debug(f"Sent product update for: {product_update['product_id']}")
                time.sleep(60 / events_per_minute)
                
            except Exception as e:
                logger.error(f"Error producing product update: {e}")
                time.sleep(1)

def main():
    """Main function to start all producers"""
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Initialize producers
    clickstream_producer = ClickstreamProducer(kafka_servers, 'clickstream-events')
    user_producer = UserProfileProducer(kafka_servers, 'user-events')
    product_producer = ProductCatalogProducer(kafka_servers, 'product-events')
    
    # Start producers in separate threads
    threads = [
        threading.Thread(target=clickstream_producer.start_producing, args=(10,)),  # 10 events/sec
        threading.Thread(target=user_producer.start_producing, args=(5,)),         # 5 events/min
        threading.Thread(target=product_producer.start_producing, args=(2,))       # 2 events/min
    ]
    
    for thread in threads:
        thread.daemon = True
        thread.start()
    
    logger.info("All producers started. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping producers...")

if __name__ == "__main__":
    main()