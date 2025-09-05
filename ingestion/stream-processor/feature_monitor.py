import json
from kafka import KafkaConsumer
import threading
import time

class SimpleMonitor:
    def __init__(self):
        self.stats = {}
        
    def monitor_topic(self, topic):
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers='kafka:29092',
            auto_offset_reset='latest',
            group_id=f'monitor-{topic}'
        )
        
        print(f"Monitoring {topic}...")
        count = 0
        
        for message in consumer:
            count += 1
            if count % 10 == 0:
                print(f"{topic}: {count} messages processed")
            
            if count <= 3:  # Show first few messages
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    print(f"{topic}: {data}")
                except:
                    print(f"{topic}: {message.value}")

def main():
    monitor = SimpleMonitor()
    topics = ['user-features', 'product-features', 'recommendation-signals']
    
    print("Starting simple feature monitor...")
    
    threads = []
    for topic in topics:
        thread = threading.Thread(target=monitor.monitor_topic, args=(topic,))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    try:
        time.sleep(60)  # Run for 1 minute
    except KeyboardInterrupt:
        print("Stopping monitor...")

if __name__ == "__main__":
    main()