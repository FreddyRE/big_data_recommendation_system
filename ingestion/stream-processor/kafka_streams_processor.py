import os
import json
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from typing import Dict, List, Any
import threading
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

def build_consumer(bootstrap, attempts=8, base_sleep=0.5):
    last = None
    for i in range(1, attempts+1):
        try:
            print(f"[stream] connecting consumer to {bootstrap} (attempt {i}/{attempts})")
            return KafkaConsumer(
                'clickstream',
                bootstrap_servers=bootstrap,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id='feature-processor',
                auto_offset_reset='earliest',
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )
        except NoBrokersAvailable as e:
            last = e
            time.sleep(base_sleep * (2 ** (i-1)))
    raise last

def build_producer(bootstrap, attempts=8, base_sleep=0.5):
    last = None
    for i in range(1, attempts+1):
        try:
            print(f"[stream] connecting producer to {bootstrap} (attempt {i}/{attempts})")
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
                linger_ms=5
            )
        except NoBrokersAvailable as e:
            last = e
            time.sleep(base_sleep * (2 ** (i-1)))
    raise last

class RealtimeFeatureProcessor:
    def __init__(self, bootstrap_servers=None):
        bootstrap = bootstrap_servers or BOOTSTRAP
        
        self.consumer = build_consumer(bootstrap)
        self.producer = build_producer(bootstrap)
        
        # In-memory state stores
        self.user_sessions = defaultdict(lambda: {
            'events': deque(maxlen=100),
            'categories': defaultdict(int),
            'products_viewed': set(),
            'cart_items': set(),
            'session_start': None,
            'last_activity': None
        })
        
        self.product_stats = defaultdict(lambda: {
            'view_count': 0,
            'cart_adds': 0,
            'purchases': 0,
            'last_hour_views': deque(maxlen=1000)
        })
        
        self.window_size = timedelta(minutes=15)
        
    def process_event(self, event: Dict[str, Any]):
        """Process single event and extract features"""
        user_id = event['user_id']
        product_id = event['product_id']
        event_type = event['event_type']
        category = event['category']
        timestamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        
        # Update user session state
        user_state = self.user_sessions[user_id]
        user_state['events'].append(event)
        user_state['categories'][category] += 1
        user_state['last_activity'] = timestamp
        
        if user_state['session_start'] is None:
            user_state['session_start'] = timestamp
            
        # Update product statistics
        product_state = self.product_stats[product_id]
        
        # Process based on event type
        if event_type in ['page_view', 'product_click']:
            user_state['products_viewed'].add(product_id)
            product_state['view_count'] += 1
            product_state['last_hour_views'].append(timestamp)
            
        elif event_type == 'add_to_cart':
            user_state['cart_items'].add(product_id)
            product_state['cart_adds'] += 1
            
        elif event_type == 'purchase':
            product_state['purchases'] += 1
            
        # Generate real-time features
        features = self.extract_features(user_id, product_id, event, timestamp)
        
        # Send features to different topics
        self.send_features(features)
        
    def extract_features(self, user_id: str, product_id: str, event: Dict, timestamp: datetime) -> Dict:
        """Extract real-time features for recommendations"""
        user_state = self.user_sessions[user_id]
        product_state = self.product_stats[product_id]
        
        # User behavior features
        session_duration = (timestamp - user_state['session_start']).total_seconds() if user_state['session_start'] else 0
        events_in_session = len(user_state['events'])
        categories_explored = len(user_state['categories'])
        products_viewed_count = len(user_state['products_viewed'])
        
        # User preferences (top categories)
        top_categories = sorted(user_state['categories'].items(), key=lambda x: x[1], reverse=True)[:3]
        preferred_categories = [cat for cat, count in top_categories]
        
        # Product popularity features
        recent_views = len([ts for ts in product_state['last_hour_views'] 
                          if timestamp - ts <= timedelta(hours=1)])
        
        conversion_rate = (product_state['purchases'] / max(product_state['view_count'], 1)) * 100
        cart_rate = (product_state['cart_adds'] / max(product_state['view_count'], 1)) * 100
        
        # Interaction sequence features
        recent_events = list(user_state['events'])[-5:]
        event_sequence = [e['event_type'] for e in recent_events]
        
        # Time-based features
        hour_of_day = timestamp.hour
        is_weekend = timestamp.weekday() >= 5
        
        return {
            'user_id': user_id,
            'product_id': product_id,
            'timestamp': timestamp.isoformat(),
            'event_type': event['event_type'],
            'category': event['category'],
            
            # User session features
            'session_duration_seconds': session_duration,
            'events_in_session': events_in_session,
            'categories_explored': categories_explored,
            'products_viewed_count': products_viewed_count,
            'preferred_categories': preferred_categories,
            
            # Product features
            'product_recent_views': recent_views,
            'product_conversion_rate': round(conversion_rate, 2),
            'product_cart_rate': round(cart_rate, 2),
            'product_total_views': product_state['view_count'],
            
            # Sequence features
            'recent_event_sequence': event_sequence,
            'is_returning_to_product': product_id in [e.get('product_id') for e in recent_events[:-1]],
            
            # Temporal features
            'hour_of_day': hour_of_day,
            'is_weekend': is_weekend,
            
            # Real-time signals
            'processing_timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def send_features(self, features: Dict):
        """Send features to appropriate topics"""
        user_id = features['user_id']
        event_type = features['event_type']
        
        # Send to user features topic
        self.producer.send(
            'user-features',
            key=user_id,
            value={
                'user_id': user_id,
                'session_features': {
                    'session_duration_seconds': features['session_duration_seconds'],
                    'events_in_session': features['events_in_session'],
                    'categories_explored': features['categories_explored'],
                    'preferred_categories': features['preferred_categories']
                },
                'timestamp': features['timestamp']
            }
        )
        
        # Send to product features topic
        self.producer.send(
            'product-features',
            key=features['product_id'],
            value={
                'product_id': features['product_id'],
                'category': features['category'],
                'popularity_features': {
                    'recent_views': features['product_recent_views'],
                    'conversion_rate': features['product_conversion_rate'],
                    'cart_rate': features['product_cart_rate'],
                    'total_views': features['product_total_views']
                },
                'timestamp': features['timestamp']
            }
        )
        
        # Send high-value events to recommendations topic
        if event_type in ['add_to_cart', 'purchase']:
            self.producer.send(
                'recommendation-signals',
                key=user_id,
                value=features
            )
    
    def run(self):
        """Main processing loop"""
        print("Starting real-time feature processor...")
        print("Processing events from clickstream topic...")
        
        processed_count = 0
        
        try:
            for message in self.consumer:
                event = message.value
                self.process_event(event)
                
                processed_count += 1
                
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} events. "
                          f"Active users: {len(self.user_sessions)}, "
                          f"Tracked products: {len(self.product_stats)}")
                
        except KeyboardInterrupt:
            print("\nStopping processor...")
        except Exception as e:
            print(f"Error processing events: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    processor = RealtimeFeatureProcessor()
    processor.run()