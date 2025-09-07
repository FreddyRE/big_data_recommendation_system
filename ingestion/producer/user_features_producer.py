import random
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from .base_producer import BaseProducer

class UserFeaturesProducer(BaseProducer):
    """Producer for user feature events"""
    
    def __init__(self, bootstrap_servers=None):
        super().__init__(bootstrap_servers)
        self.users = [f"user_{i}" for i in range(1000, 2000)]
        self.categories = ["electronics", "clothing", "books", "home", "sports"]
        self.age_groups = ["18-25", "26-35", "36-45", "46-55", "55+"]
        self.locations = ["US-East", "US-West", "EU", "Asia"]
        self.device_types = ["mobile", "desktop", "tablet"]
    
    def get_topic_name(self) -> str:
        return "user-features"
    
    def generate_event(self) -> Tuple[str, Dict[str, Any]]:
        """Generate user feature events for silver layer processing"""
        user_id = random.choice(self.users)
        
        event = {
            "user_id": user_id,
            "preferred_categories": random.sample(self.categories, k=random.randint(1, 3)),
            "avg_session_duration": round(random.uniform(60, 1800), 2),
            "total_purchases": random.randint(0, 50),
            "avg_order_value": round(random.uniform(25, 300), 2),
            "last_active": datetime.now(timezone.utc).isoformat(),
            "engagement_score": round(random.uniform(0.1, 1.0), 3),
            "age_group": random.choice(self.age_groups),
            "location": random.choice(self.locations),
            "device_type": random.choice(self.device_types),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return user_id, event