import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

class BaseProducer(ABC):
    """Base class for all Kafka producers"""
    
    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.producer = None
        
    def build_producer(self, attempts=8):
        """Build and return a Kafka producer with retry logic"""
        last_exception = None
        
        for i in range(1, attempts + 1):
            try:
                print(f"[{self.__class__.__name__}] connecting to {self.bootstrap_servers} (attempt {i}/{attempts})")
                return KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all", 
                    retries=5, 
                    request_timeout_ms=30000, 
                    api_version_auto_timeout_ms=30000, 
                    linger_ms=5
                )
            except NoBrokersAvailable as e:
                last_exception = e
                time.sleep(0.5 * 2**(i-1))
        
        raise last_exception
    
    @abstractmethod
    def generate_event(self) -> Tuple[str, Dict[str, Any]]:
        """Generate a single event. Returns (key, event_data)"""
        pass
    
    @abstractmethod
    def get_topic_name(self) -> str:
        """Return the Kafka topic name for this producer"""
        pass
    
    def produce_events(self, count: int, delay: float = 0.01):
        """Produce a specified number of events"""
        topic = self.get_topic_name()
        print(f"Producing {count} events to topic: {topic}")
        
        if not self.producer:
            self.producer = self.build_producer()
        
        try:
            for i in range(count):
                key, event = self.generate_event()
                
                self.producer.send(
                    topic=topic,
                    key=key,
                    value=event
                )
                
                if i % 25 == 0 and i > 0:
                    print(f"  {topic}: {i}/{count}")
                
                time.sleep(delay)
            
            self.producer.flush()
            print(f"Completed producing {count} events to {topic}")
            
        except Exception as e:
            print(f"Error producing events: {e}")
            raise
        finally:
            if self.producer:
                self.producer.close()