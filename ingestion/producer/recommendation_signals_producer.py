import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from .base_producer import BaseProducer

class RecommendationSignalsProducer(BaseProducer):
    """Producer for recommendation signal events"""
    
    def __init__(self, bootstrap_servers=None):
        super().__init__(bootstrap_servers)
        self.users = [f"user_{i}" for i in range(1000, 2000)]
        self.products = [f"prod_{i}" for i in range(1, 500)]
        self.algorithms = ["collaborative_filtering", "content_based", "hybrid", "trending"]
        self.page_types = ["home", "category", "product", "search"]
    
    def get_topic_name(self) -> str:
        return "recommendation-signals"
    
    def generate_event(self) -> Tuple[str, Dict[str, Any]]:
        """Generate recommendation signals for real-time processing"""
        user_id = random.choice(self.users)
        
        event = {
            "signal_id": str(uuid.uuid4()),
            "user_id": user_id,
            "product_id": random.choice(self.products),
            "algorithm": random.choice(self.algorithms),
            "confidence_score": round(random.uniform(0.1, 1.0), 3),
            "context": {
                "session_id": f"session_{random.randint(10000, 99999)}",
                "page_type": random.choice(self.page_types),
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
