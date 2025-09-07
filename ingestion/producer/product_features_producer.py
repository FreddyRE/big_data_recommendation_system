import random
from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from .base_producer import BaseProducer

class ProductFeaturesProducer(BaseProducer):
    """Producer for product feature events"""
    
    def __init__(self, bootstrap_servers=None):
        super().__init__(bootstrap_servers)
        self.products = [f"prod_{i}" for i in range(1, 500)]
        self.categories = ["electronics", "clothing", "books", "home", "sports"]
        self.brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
        self.tags = ["new", "sale", "trending", "recommended", "bestseller"]
    
    def get_topic_name(self) -> str:
        return "product-features"
    
    def generate_event(self) -> Tuple[str, Dict[str, Any]]:
        """Generate product feature events"""
        product_id = random.choice(self.products)
        
        event = {
            "product_id": product_id,
            "category": random.choice(self.categories),
            "subcategory": f"sub_{random.randint(1, 10)}",
            "brand": random.choice(self.brands),
            "price": round(random.uniform(10, 500), 2),
            "avg_rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 1000),
            "popularity_score": round(random.uniform(0.1, 1.0), 3),
            "in_stock": random.choice([True, False]),
            "tags": random.sample(self.tags, k=random.randint(0, 3)),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return product_id, event