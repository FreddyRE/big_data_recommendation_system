# 2-processing/silver-to-gold/fixed_gold_pipeline.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import sys
import os
import logging
import io

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Fix import path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
bronze_to_silver_dir = os.path.join(parent_dir, 'bronze-to-silver')
utils_path = os.path.join(bronze_to_silver_dir, 'utils')

if os.path.exists(utils_path):
    sys.path.insert(0, utils_path)
    from storage_manager import SafeMinIOManager
    print("✅ Successfully imported SafeMinIOManager")
else:
    print(f"❌ Utils directory not found at: {utils_path}")
    sys.exit(1)

class FixedGoldProcessor:
    """
    Gold Layer processor with correct column mappings
    """
    
    def __init__(self, storage_manager: SafeMinIOManager):
        self.storage = storage_manager
        
        # Access MinIO client directly
        if hasattr(storage_manager, 'client'):
            self.client = storage_manager.client
        else:
            raise Exception("Cannot access MinIO client from SafeMinIOManager")
        
    def list_silver_files(self, table_name: str) -> list:
        """List files in silver layer"""
        try:
            if table_name == 'clickstream_silver':
                prefix = 'clickstream_silver/'
            elif table_name == 'user_events':
                prefix = 'user_events_silver/'
            elif table_name == 'product_events':
                prefix = 'product_events_silver/'
            else:
                prefix = f'{table_name}/'
            
            objects = self.client.list_objects('silver-layer', prefix=prefix, recursive=True)
            
            files = []
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    files.append(obj.object_name)
            
            print(f"📁 Found {len(files)} parquet files for {table_name}")
            return files
            
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []
    
    def read_parquet_from_minio(self, object_name: str) -> pd.DataFrame:
        """Read parquet file directly from MinIO"""
        try:
            response = self.client.get_object('silver-layer', object_name)
            parquet_data = response.read()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            response.close()
            response.release_conn()
            
            return df
            
        except Exception as e:
            print(f"❌ Error reading {object_name}: {e}")
            return pd.DataFrame()
    
    def save_parquet_to_minio(self, df: pd.DataFrame, object_name: str) -> bool:
        """Save parquet file to MinIO gold layer"""
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            self.client.put_object(
                'gold-layer',
                object_name,
                buffer,
                length=len(buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            print(f"✅ Saved to gold-layer/{object_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving {object_name}: {e}")
            return False
    
    def load_silver_data(self, table_name: str, max_files: int = 10) -> pd.DataFrame:
        """Load silver data from a specific table"""
        print(f"📥 Loading {table_name} data...")
        
        files = self.list_silver_files(table_name)
        
        if not files:
            print(f"⚠️ No files found for {table_name}")
            return pd.DataFrame()
        
        # Take most recent files
        recent_files = sorted(files)[-max_files:]
        print(f"📊 Processing {len(recent_files)} most recent files")
        
        # Load and combine data
        dfs = []
        for file_path in recent_files:
            df = self.read_parquet_from_minio(file_path)
            if not df.empty:
                dfs.append(df)
                print(f"  ✅ Loaded {len(df)} records from {os.path.basename(file_path)}")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Smart deduplication based on table type and available columns
            before = len(combined_df)
            
            if table_name == 'clickstream_silver':
                # For clickstream: dedupe by user_id, timestamp, product_id
                if all(col in combined_df.columns for col in ['user_id', 'timestamp', 'product_id']):
                    combined_df = combined_df.drop_duplicates(['user_id', 'timestamp', 'product_id'])
                elif all(col in combined_df.columns for col in ['user_id', 'timestamp']):
                    combined_df = combined_df.drop_duplicates(['user_id', 'timestamp'])
                    
            elif table_name == 'user_events':
                # For user events: dedupe by user_id, timestamp
                if all(col in combined_df.columns for col in ['user_id', 'timestamp']):
                    combined_df = combined_df.drop_duplicates(['user_id', 'timestamp'])
                elif 'user_id' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(['user_id'])
                    
            elif table_name == 'product_events':
                # For product events: dedupe by product_id, timestamp
                if all(col in combined_df.columns for col in ['product_id', 'timestamp']):
                    combined_df = combined_df.drop_duplicates(['product_id', 'timestamp'])
                elif 'product_id' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(['product_id'])
            
            after = len(combined_df)
            if before != after:
                print(f"  🧹 Removed {before - after} duplicates")
            
            print(f"📊 Total {table_name} records: {len(combined_df)}")
            return combined_df
        else:
            return pd.DataFrame()
    
    def create_user_summary(self, clickstream_df: pd.DataFrame) -> pd.DataFrame:
        """Create user summary with correct column names"""
        print("👥 Creating user summary...")
        
        if clickstream_df.empty:
            return pd.DataFrame()
        
        # Use correct column names from your data:
        # timestamp, product_price, event_weight, session_id, product_id
        
        user_summary = clickstream_df.groupby('user_id').agg({
            'timestamp': 'count',               # Total events  
            'product_id': 'nunique',            # Unique products viewed
            'session_id': 'nunique',            # Total sessions
            'event_weight': 'sum',              # Total engagement (was weighted_score)
            'product_price': ['sum', 'mean']    # Value metrics (was price)
        }).round(2)
        
        # Flatten columns
        user_summary.columns = [
            'total_events', 'products_viewed', 'total_sessions',
            'total_engagement', 'total_value_browsed', 'avg_price_interest'
        ]
        
        # Derived metrics
        user_summary['events_per_session'] = (
            user_summary['total_events'] / user_summary['total_sessions']
        ).round(2)
        
        # Activity segments
        user_summary['activity_level'] = pd.cut(
            user_summary['total_events'],
            bins=[0, 5, 15, 30, float('inf')],
            labels=['Low', 'Medium', 'High', 'Very High']
        )
        
        # Additional insights from your rich data
        if 'device_type' in clickstream_df.columns:
            device_prefs = clickstream_df.groupby('user_id')['device_type'].apply(
                lambda x: x.mode().iloc[0] if not x.empty else 'unknown'
            )
            user_summary = user_summary.join(device_prefs.rename('preferred_device'))
        
        if 'product_category' in clickstream_df.columns:
            category_prefs = clickstream_df.groupby('user_id')['product_category'].apply(
                lambda x: x.mode().iloc[0] if not x.empty else 'unknown'
            )
            user_summary = user_summary.join(category_prefs.rename('top_category'))
        
        user_summary['created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        user_summary.reset_index(inplace=True)
        
        print(f"✅ User summary: {len(user_summary)} users")
        return user_summary
    
    def create_product_summary(self, clickstream_df: pd.DataFrame) -> pd.DataFrame:
        """Create product summary with correct column names"""
        print("📦 Creating product summary...")
        
        if clickstream_df.empty:
            return pd.DataFrame()
        
        # Product metrics using correct column names
        product_summary = clickstream_df.groupby('product_id').agg({
            'user_id': 'nunique',               # Unique viewers
            'timestamp': 'count',               # Total interactions
            'session_id': 'nunique',            # Unique sessions
            'event_weight': 'sum',              # Total engagement
            'product_price': 'mean'             # Average price
        }).round(2)
        
        product_summary.columns = [
            'unique_viewers', 'total_interactions', 'unique_sessions',
            'total_engagement', 'avg_price'
        ]
        
        # Popularity score
        product_summary['popularity_score'] = (
            product_summary['unique_viewers'] * 0.5 +
            product_summary['total_interactions'] * 0.3 +
            product_summary['total_engagement'] * 0.2
        ).round(2)
        
        # Popularity tiers
        try:
            product_summary['popularity_tier'] = pd.qcut(
                product_summary['popularity_score'],
                q=4,
                labels=['Low', 'Medium', 'High', 'Top'],
                duplicates='drop'
            )
        except ValueError:
            product_summary['popularity_tier'] = 'Medium'
        
        # Add category info if available
        if 'product_category' in clickstream_df.columns:
            category_info = clickstream_df.groupby('product_id')['product_category'].first()
            product_summary = product_summary.join(category_info.rename('category'))
        
        product_summary['created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        product_summary.reset_index(inplace=True)
        
        print(f"✅ Product summary: {len(product_summary)} products")
        return product_summary
    
    def create_daily_stats(self, clickstream_df: pd.DataFrame) -> pd.DataFrame:
        """Create daily statistics with correct column names"""
        print("📅 Creating daily stats...")
        
        if clickstream_df.empty:
            return pd.DataFrame()
        
        # Convert timestamp and extract date
        clickstream_df['timestamp'] = pd.to_datetime(clickstream_df['timestamp'])
        clickstream_df['event_date'] = clickstream_df['timestamp'].dt.date
        
        # Daily aggregations
        daily_stats = clickstream_df.groupby('event_date').agg({
            'user_id': 'nunique',               # Daily active users
            'product_id': 'nunique',            # Products viewed
            'session_id': 'nunique',            # Total sessions
            'timestamp': 'count',               # Total events
            'event_weight': 'sum'               # Total engagement
        }).round(2)
        
        daily_stats.columns = [
            'daily_active_users', 'products_viewed', 'total_sessions',
            'total_events', 'total_engagement'
        ]
        
        # Derived metrics
        daily_stats['events_per_user'] = (
            daily_stats['total_events'] / daily_stats['daily_active_users']
        ).round(2)
        
        # Device and category breakdown if available
        if 'device_type' in clickstream_df.columns:
            mobile_events = clickstream_df[clickstream_df['is_mobile'] == True].groupby('event_date').size()
            daily_stats = daily_stats.join(mobile_events.rename('mobile_events'), how='left')
            daily_stats['mobile_pct'] = (daily_stats['mobile_events'] / daily_stats['total_events'] * 100).round(1)
        
        daily_stats['created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        daily_stats.reset_index(inplace=True)
        
        print(f"✅ Daily stats: {len(daily_stats)} days")
        return daily_stats
    
    def create_engagement_metrics(self, clickstream_df: pd.DataFrame) -> pd.DataFrame:
        """Create engagement metrics using your rich data"""
        print("📈 Creating engagement metrics...")
        
        if clickstream_df.empty:
            return pd.DataFrame()
        
        # Event type breakdown
        event_summary = clickstream_df.groupby('event_type').agg({
            'user_id': 'nunique',
            'timestamp': 'count',
            'event_weight': 'sum'
        }).round(2)
        
        event_summary.columns = ['unique_users', 'total_events', 'total_engagement']
        
        # Add percentages
        total_events = event_summary['total_events'].sum()
        event_summary['event_pct'] = (event_summary['total_events'] / total_events * 100).round(1)
        
        event_summary['created_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        event_summary.reset_index(inplace=True)
        
        print(f"✅ Engagement metrics: {len(event_summary)} event types")
        return event_summary
    
    def save_to_gold_layer(self, df: pd.DataFrame, table_name: str) -> bool:
        """Save DataFrame to gold layer"""
        if df.empty:
            print(f"⚠️ Skipping empty {table_name}")
            return False
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        object_name = f"{table_name}/date={current_date}/{table_name}_{current_date}.parquet"
        
        return self.save_parquet_to_minio(df, object_name)
    
    def run_pipeline(self, max_files_per_table: int = 10):
        """Run the complete gold layer pipeline"""
        print("🏆 Fixed Gold Layer Pipeline")
        print("=" * 50)
        
        results = {}
        
        # Load silver data
        print("\n📥 Loading silver data...")
        clickstream_df = self.load_silver_data('clickstream_silver', max_files_per_table)
        user_df = self.load_silver_data('user_events', max_files_per_table) 
        product_df = self.load_silver_data('product_events', max_files_per_table)
        
        print(f"\n📊 Data loaded:")
        print(f"  🔗 Clickstream: {len(clickstream_df)} records")
        print(f"  👥 Users: {len(user_df)} records")
        print(f"  📦 Products: {len(product_df)} records")
        
        if clickstream_df.empty:
            print("❌ No clickstream data found!")
            return results
        
        # Create gold tables
        print("\n🔄 Creating gold layer tables...")
        
        # Core analytics
        user_summary = self.create_user_summary(clickstream_df)
        results['user_summary'] = self.save_to_gold_layer(user_summary, 'user_summary')
        
        product_summary = self.create_product_summary(clickstream_df)
        results['product_summary'] = self.save_to_gold_layer(product_summary, 'product_summary')
        
        daily_stats = self.create_daily_stats(clickstream_df)
        results['daily_stats'] = self.save_to_gold_layer(daily_stats, 'daily_stats')
        
        # Additional analytics
        engagement_metrics = self.create_engagement_metrics(clickstream_df)
        results['engagement_metrics'] = self.save_to_gold_layer(engagement_metrics, 'engagement_metrics')
        
        # Summary
        successful = sum(results.values())
        total = len(results)
        print(f"\n🏆 Complete: {successful}/{total} tables created successfully")
        
        return results

def main():
    print("🏆 Fixed Gold Layer Pipeline")
    print("=" * 50)
    
    parser = argparse.ArgumentParser(description='Fixed Gold Layer Pipeline')
    parser.add_argument('--max-files', type=int, default=10,
                       help='Max files per table to process (default: 10)')
    
    args = parser.parse_args()
    
    # Initialize storage
    try:
        storage = SafeMinIOManager(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin123'
        )
        print("✅ Connected to MinIO")
    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        return
    
    # Run pipeline
    try:
        processor = FixedGoldProcessor(storage)
        results = processor.run_pipeline(max_files_per_table=args.max_files)
        
        print("\n📊 FINAL RESULTS")
        print("=" * 30)
        for table, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{table}: {status}")
        
        if any(results.values()):
            print(f"\n🎉 Gold layer created successfully!")
            print(f"🌐 Check MinIO UI: http://localhost:9001")
            print(f"📊 Gold layer now contains business-ready analytics!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()