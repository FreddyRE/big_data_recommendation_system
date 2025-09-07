import time
from clickstream_producer import ClickstreamProducer
from user_features_producer import UserFeaturesProducer
from product_features_producer import ProductFeaturesProducer
from recommendation_signals_producer import RecommendationSignalsProducer

def main():
    print("Starting recommendation system data producers...")
    
    try:
        # Create producers
        clickstream = ClickstreamProducer()
        user_features = UserFeaturesProducer()
        product_features = ProductFeaturesProducer()
        recommendation_signals = RecommendationSignalsProducer()
        
        # Produce data
        clickstream.produce_events(1000, delay=0.01)
        time.sleep(2)
        
        user_features.produce_events(100, delay=0.02)
        time.sleep(2)
        
        product_features.produce_events(50, delay=0.05)
        time.sleep(2)
        
        recommendation_signals.produce_events(200, delay=0.01)
        
        print("All data production completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()