import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from .base_producer import BaseProducer

class ClickstreamProducer(BaseProducer):
    
    def __init__(self, bootstrap_servers=None):
        super().__init__(bootstrap_servers)
        self.users = [f"user_{i}" for i in range(1000, 2000)]
        self.products = [f"prod_{i}" for i in range(1, 500)]
        self.categories = ["electronics", "clothing", "books", "home", "sports"]
        self.event_types = ["page_view", "product_click", "add_to_cart", "purchase"]
    
    def get_topic_name(self) -> str:
        return "clickstream"
    
    def generate_event(self) -> Tuple[str, Dict[str, Any]]:
        """Generate realistic clickstream event"""
        user_id = random.choice(self.users)
        event_type = random.choice(self.event_types)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": user_id,
            "product_id": random.choice(self.products),
            "category": random.choice(self.categories),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": f"session_{random.randint(10000, 99999)}"
        }
        
        if event_type == "purchase":
            event["price"] = round(random.uniform(10, 500), 2)
            event["quantity"] = random.randint(1, 3)
        
        return user_id, event