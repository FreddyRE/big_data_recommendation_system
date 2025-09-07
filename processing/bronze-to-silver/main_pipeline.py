# main_pipeline.py - Final Bronze-to-Silver Pipeline
"""
Production-ready bronze-to-silver processing pipeline
Consolidates all working logic into a single, robust pipeline
"""

import argparse
import sys
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from pathlib import Path

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.storage_manager import SafeMinIOManager
from utils.logger import setup_logging
from loguru import logger

class BronzeToSilverPipeline:
    """Production bronze-to-silver processing pipeline"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize pipeline with configuration"""
        
        # Default configuration
        self.config = {
            'minio_endpoint': 'localhost:9000',
            'minio_access_key': 'minioadmin',
            'minio_secret_key': 'minioadmin123',
            'default_files_per_topic': 10,
            'max_files_per_run': 50,
            'data_quality_threshold': 0.8
        }
        
        if config:
            self.config.update(config)
        
        # Initialize storage manager
        self.storage = SafeMinIOManager(
            endpoint=self.config['minio_endpoint'],
            access_key=self.config['minio_access_key'],
            secret_key=self.config['minio_secret_key']
        )
        
        # Topic configurations - WORKING LOGIC FROM YOUR TESTS
        self.topic_configs = {
            'clickstream': {
                'bronze_prefix': 'topics/clickstream-events/',
                'silver_table': 'clickstream_silver',
                'processor': self._process_clickstream_data,
                'required_fields': ['user_id', 'product_id', 'event_type', 'timestamp']
            },
            'user': {
                'bronze_prefix': 'topics/user-events/',
                'silver_table': 'user_events_silver',
                'processor': self._process_user_data,
                'required_fields': ['user_id', 'timestamp']
            },
            'product': {
                'bronze_prefix': 'topics/product-events/',
                'silver_table': 'product_events_silver',
                'processor': self._process_product_data,
                'required_fields': ['product_id', 'timestamp']
            }
        }
    
    def run_pipeline(self, topics: List[str] = None, files_per_topic: int = None) -> Dict[str, Any]:
        """Run the complete bronze-to-silver pipeline"""
        
        # Set defaults
        if topics is None:
            topics = ['clickstream', 'user', 'product']
        if files_per_topic is None:
            files_per_topic = self.config['default_files_per_topic']
        
        logger.info(f"🚀 Starting bronze-to-silver pipeline")
        logger.info(f"📋 Topics: {topics}")
        logger.info(f"📁 Files per topic: {files_per_topic}")
        
        pipeline_start = datetime.utcnow()
        results = {
            'pipeline_id': f"pipeline_{pipeline_start.strftime('%Y%m%d_%H%M%S')}",
            'start_time': pipeline_start,
            'topics_processed': {},
            'total_records_processed': 0,
            'success': False
        }
        
        # Process each topic
        for topic in topics:
            if topic not in self.topic_configs:
                logger.warning(f"⚠️  Unknown topic: {topic}")
                continue
            
            logger.info(f"\n📂 Processing topic: {topic}")
            topic_result = self._process_topic(topic, files_per_topic)
            results['topics_processed'][topic] = topic_result
            
            if topic_result['success']:
                results['total_records_processed'] += topic_result['records_processed']
        
        # Calculate overall success
        successful_topics = sum(1 for r in results['topics_processed'].values() if r['success'])
        total_topics = len(results['topics_processed'])
        results['success'] = successful_topics > 0
        
        # Finalize results
        results['end_time'] = datetime.utcnow()
        results['duration_seconds'] = (results['end_time'] - results['start_time']).total_seconds()
        results['success_rate'] = successful_topics / total_topics if total_topics > 0 else 0
        
        # Log summary
        logger.info(f"\n🏆 PIPELINE COMPLETE")
        logger.info(f"   ⏱️  Duration: {results['duration_seconds']:.1f} seconds")
        logger.info(f"   📊 Success rate: {results['success_rate']:.1%}")
        logger.info(f"   📈 Total records: {results['total_records_processed']}")
        
        # Save pipeline checkpoint
        self._save_pipeline_checkpoint(results)
        
        return results
    
    def _process_topic(self, topic: str, max_files: int) -> Dict[str, Any]:
        """Process a single topic"""
        
        config = self.topic_configs[topic]
        topic_start = datetime.utcnow()
        
        result = {
            'topic': topic,
            'start_time': topic_start,
            'success': False,
            'records_processed': 0,
            'files_processed': 0,
            'data_quality_score': 0.0,
            'error_message': None
        }
        
        try:
            # Get bronze files
            bronze_files = self.storage.list_bronze_files(config['bronze_prefix'])
            
            if not bronze_files:
                result['error_message'] = f"No bronze files found for {topic}"
                logger.warning(f"⚠️  {result['error_message']}")
                return result
            
            # Limit files
            files_to_process = bronze_files[:max_files]
            logger.info(f"📁 Found {len(bronze_files)} files, processing {len(files_to_process)}")
            
            # Read bronze data
            file_keys = [f['key'] for f in files_to_process]
            raw_records = self.storage.read_bronze_files_safely(file_keys, max_files=max_files)
            
            if not raw_records:
                result['error_message'] = f"No records found in bronze files for {topic}"
                logger.warning(f"⚠️  {result['error_message']}")
                return result
            
            logger.info(f"📖 Read {len(raw_records)} raw records")
            
            # Process data using topic-specific processor
            processed_df = config['processor'](raw_records)
            
            if processed_df.empty:
                result['error_message'] = f"No valid records after processing {topic}"
                logger.warning(f"⚠️  {result['error_message']}")
                return result
            
            # Calculate data quality
            data_quality_score = len(processed_df) / len(raw_records)
            result['data_quality_score'] = data_quality_score
            
            if data_quality_score < self.config['data_quality_threshold']:
                logger.warning(f"⚠️  Low data quality: {data_quality_score:.2%}")
            
            # Write to silver layer
            success = self.storage.write_to_silver_safely(processed_df, config['silver_table'])
            
            if success:
                result.update({
                    'success': True,
                    'records_processed': len(processed_df),
                    'files_processed': len(files_to_process),
                    'end_time': datetime.utcnow()
                })
                
                logger.success(f"✅ {topic}: {len(processed_df)} records → {config['silver_table']}")
            else:
                result['error_message'] = f"Failed to write {topic} data to silver layer"
        
        except Exception as e:
            result['error_message'] = str(e)
            logger.error(f"❌ Error processing {topic}: {e}")
        
        return result
    
    def _process_clickstream_data(self, records: List[Dict]) -> pd.DataFrame:
        """Process clickstream data - PROVEN WORKING LOGIC"""
        
        df = pd.DataFrame(records)
        if df.empty:
            return df
        
        original_count = len(df)
        logger.info(f"🔄 Processing {original_count} clickstream records")
        
        # Data cleaning
        required_fields = ['user_id', 'product_id', 'event_type', 'timestamp']
        df = df.dropna(subset=required_fields)
        
        # Type conversions
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        # Remove invalid events
        valid_events = ['view', 'click', 'add_to_cart', 'purchase', 'remove_from_cart', 'search', 'recommendation_clicked']
        df = df[df['event_type'].isin(valid_events)]
        
        # Business features - WORKING LOGIC
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.day_name()
        df['is_weekend'] = df['timestamp'].dt.dayofweek.isin([5, 6])
        
        # Event weighting for recommendations
        event_weights = {
            'view': 1, 'click': 2, 'add_to_cart': 5, 
            'purchase': 10, 'remove_from_cart': -2,
            'search': 3, 'recommendation_clicked': 4
        }
        df['event_weight'] = df['event_type'].map(event_weights).fillna(1)
        
        # Device analysis
        df['is_mobile'] = df['device_type'].str.lower().isin(['mobile', 'tablet'])
        
        # Price tiers
        if 'product_price' in df.columns and df['product_price'].notna().any():
            df['price_tier'] = pd.cut(
                df['product_price'],
                bins=[0, 50, 200, 500, 1000, float('inf')],
                labels=['budget', 'low', 'mid', 'high', 'premium']
            )
        
        # Session analysis
        if 'session_id' in df.columns:
            df = df.sort_values(['session_id', 'timestamp'])
            df['session_sequence'] = df.groupby('session_id').cumcount() + 1
        
        # Remove duplicates
        if 'event_id' in df.columns:
            df = df.drop_duplicates(subset=['event_id'])
        
        # Add processing metadata
        df['silver_processed_at'] = datetime.utcnow()
        
        cleaned_count = len(df)
        logger.info(f"🧹 Clickstream: {cleaned_count}/{original_count} records retained ({cleaned_count/original_count:.1%})")
        
        return df
    
    def _process_user_data(self, records: List[Dict]) -> pd.DataFrame:
        """Process user data - PROVEN WORKING LOGIC"""
        
        df = pd.DataFrame(records)
        if df.empty:
            return df
        
        original_count = len(df)
        logger.info(f"👥 Processing {original_count} user records")
        
        # Data cleaning
        required_fields = ['user_id', 'timestamp']
        df = df.dropna(subset=required_fields)
        
        # Type conversions
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        # Numeric fields
        numeric_fields = ['total_purchases', 'total_spent', 'age']
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # User segmentation
        if 'total_spent' in df.columns:
            df['value_segment'] = pd.cut(
                df['total_spent'].fillna(0),
                bins=[0, 100, 500, 1000, 5000, float('inf')],
                labels=['new', 'low_value', 'medium_value', 'high_value', 'premium']
            )
        
        # Account age
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df['account_age_days'] = (datetime.utcnow() - df['created_at']).dt.days
        
        # Remove duplicates (keep latest)
        df = df.sort_values('timestamp').drop_duplicates(subset=['user_id'], keep='last')
        
        # Add processing metadata
        df['silver_processed_at'] = datetime.utcnow()
        
        cleaned_count = len(df)
        logger.info(f"🧹 Users: {cleaned_count}/{original_count} records retained ({cleaned_count/original_count:.1%})")
        
        return df
    
    def _process_product_data(self, records: List[Dict]) -> pd.DataFrame:
        """Process product data - PROVEN WORKING LOGIC"""
        
        df = pd.DataFrame(records)
        if df.empty:
            return df
        
        original_count = len(df)
        logger.info(f"📦 Processing {original_count} product records")
        
        # Data cleaning
        required_fields = ['product_id', 'timestamp']
        df = df.dropna(subset=required_fields)
        
        # Type conversions
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        # Price validation
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df = df[df['price'] > 0]  # Remove invalid prices
        
        # Price tiers
        if 'price' in df.columns:
            df['price_tier'] = pd.cut(
                df['price'],
                bins=[0, 50, 200, 500, 1000, float('inf')],
                labels=['budget', 'low', 'mid', 'high', 'premium']
            )
        
        # Category standardization
        if 'category' in df.columns:
            df['category'] = df['category'].str.title().str.strip()
        
        # Stock analysis
        if 'stock_quantity' in df.columns:
            df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce')
            df['is_in_stock'] = df['stock_quantity'] > 0
        
        # Remove duplicates (keep latest)
        df = df.sort_values('timestamp').drop_duplicates(subset=['product_id'], keep='last')
        
        # Add processing metadata
        df['silver_processed_at'] = datetime.utcnow()
        
        cleaned_count = len(df)
        logger.info(f"🧹 Products: {cleaned_count}/{original_count} records retained ({cleaned_count/original_count:.1%})")
        
        return df
    
    def _save_pipeline_checkpoint(self, results: Dict[str, Any]):
        """Save pipeline execution checkpoint"""
        try:
            checkpoint_data = {
                'pipeline_execution': results,
                'config': self.config,
                'checkpoint_timestamp': datetime.utcnow().isoformat()
            }
            self.storage.save_processing_checkpoint(checkpoint_data)
        except Exception as e:
            logger.warning(f"⚠️  Failed to save pipeline checkpoint: {e}")


def main():
    """Main entry point with command line arguments"""
    parser = argparse.ArgumentParser(description='Bronze-to-Silver Processing Pipeline')
    parser.add_argument('--topics', type=str, default='clickstream,user,product',
                        help='Comma-separated list of topics to process')
    parser.add_argument('--files-per-topic', type=int, default=10,
                        help='Number of files to process per topic')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Parse topics
    topics = [topic.strip() for topic in args.topics.split(',')]
    
    # Initialize and run pipeline
    pipeline = BronzeToSilverPipeline()
    
    print("\n" + "="*60)
    print("🚀 BRONZE-TO-SILVER PROCESSING PIPELINE")
    print("="*60)
    
    results = pipeline.run_pipeline(topics=topics, files_per_topic=args.files_per_topic)
    
    # Print results
    if results['success']:
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   📊 Processed {results['total_records_processed']} records")
        print(f"   ⏱️  Duration: {results['duration_seconds']:.1f} seconds")
        print(f"   📈 Success rate: {results['success_rate']:.1%}")
    else:
        print(f"\n❌ Pipeline failed!")
        for topic, result in results['topics_processed'].items():
            if not result['success']:
                print(f"   ❌ {topic}: {result.get('error_message', 'Unknown error')}")


if __name__ == "__main__":
    main()