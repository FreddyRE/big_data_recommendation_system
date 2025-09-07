# Check what's in your new Gold layer

import sys
import os
import pandas as pd
import io

# Add path to utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
bronze_to_silver_dir = os.path.join(parent_dir, 'bronze-to-silver')
utils_path = os.path.join(bronze_to_silver_dir, 'utils')

sys.path.insert(0, utils_path)
from storage_manager import SafeMinIOManager

def view_gold_data():
    print("🏆 GOLD LAYER ANALYTICS SUMMARY")
    print("=" * 60)
    
    # Initialize storage
    storage = SafeMinIOManager(
        endpoint='localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin123'
    )
    
    client = storage.client
    
    # Find gold layer files
    try:
        objects = client.list_objects('gold-layer', recursive=True)
        
        gold_tables = {}
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                # Extract table name
                table_name = obj.object_name.split('/')[0]
                if table_name not in gold_tables:
                    gold_tables[table_name] = []
                gold_tables[table_name].append(obj.object_name)
        
        print(f"📊 Found {len(gold_tables)} gold tables:")
        
        for table_name, files in gold_tables.items():
            print(f"\n🔍 {table_name.upper()}")
            print("-" * 40)
            
            # Read the first file to show sample
            try:
                sample_file = files[0]
                response = client.get_object('gold-layer', sample_file)
                parquet_data = response.read()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
                response.close()
                response.release_conn()
                
                print(f"📏 Records: {len(df)}")
                print(f"📋 Columns: {list(df.columns)}")
                
                # Show key insights based on table type
                if 'user_summary' in table_name:
                    print(f"\n👥 USER INSIGHTS:")
                    if 'activity_level' in df.columns:
                        activity_counts = df['activity_level'].value_counts()
                        print(f"  Activity Levels: {dict(activity_counts)}")
                    if 'total_events' in df.columns:
                        print(f"  Avg Events per User: {df['total_events'].mean():.1f}")
                        print(f"  Most Active User: {df['total_events'].max()} events")
                
                elif 'product_summary' in table_name:
                    print(f"\n📦 PRODUCT INSIGHTS:")
                    if 'popularity_tier' in df.columns:
                        pop_counts = df['popularity_tier'].value_counts()
                        print(f"  Popularity Tiers: {dict(pop_counts)}")
                    if 'unique_viewers' in df.columns:
                        print(f"  Avg Viewers per Product: {df['unique_viewers'].mean():.1f}")
                        print(f"  Top Product: {df['unique_viewers'].max()} viewers")
                
                elif 'daily_stats' in table_name:
                    print(f"\n📅 DAILY INSIGHTS:")
                    if 'daily_active_users' in df.columns:
                        print(f"  Daily Active Users: {df['daily_active_users'].iloc[0] if len(df) > 0 else 0}")
                    if 'total_events' in df.columns:
                        print(f"  Total Events Today: {df['total_events'].iloc[0] if len(df) > 0 else 0}")
                    if 'events_per_user' in df.columns:
                        print(f"  Events per User: {df['events_per_user'].iloc[0] if len(df) > 0 else 0}")
                
                elif 'engagement_metrics' in table_name:
                    print(f"\n📈 ENGAGEMENT INSIGHTS:")
                    if 'event_type' in df.columns and 'total_events' in df.columns:
                        print(f"  Event Types:")
                        for _, row in df.iterrows():
                            print(f"    {row['event_type']}: {row['total_events']} events ({row.get('event_pct', 0)}%)")
                
                # Show sample data (first 3 rows, key columns only)
                print(f"\n📝 Sample Data:")
                key_cols = [col for col in df.columns if not col.startswith('created_date')][:6]
                if key_cols:
                    print(df[key_cols].head(3).to_string(index=False))
                
            except Exception as e:
                print(f"❌ Error reading {table_name}: {e}")
        
        print(f"\n🎯 RECOMMENDATIONS:")
        print("1. ✅ Gold layer is working! Your analytics are ready")
        print("2. 📊 Next: Build dashboards (Streamlit/Grafana)")  
        print("3. 🤖 Next: Add ML recommendation models")
        print("4. ⚡ Next: Real-time processing with Airflow")
        print("5. ☁️  Next: Deploy to AWS")
        
    except Exception as e:
        print(f"❌ Error accessing gold layer: {e}")

if __name__ == "__main__":
    view_gold_data()