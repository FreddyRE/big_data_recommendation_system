# Check what columns are actually in the silver data

import sys
import os

# Add path to utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
bronze_to_silver_dir = os.path.join(parent_dir, 'bronze-to-silver')
utils_path = os.path.join(bronze_to_silver_dir, 'utils')

sys.path.insert(0, utils_path)
from storage_manager import SafeMinIOManager
import pandas as pd
import io

def check_columns():
    print("🔍 Checking Silver Data Columns")
    print("=" * 50)
    
    # Initialize storage
    storage = SafeMinIOManager(
        endpoint='localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin123'
    )
    
    client = storage.client
    
    # Check each table type
    tables = {
        'clickstream_silver': 'clickstream_silver/',
        'user_events_silver': 'user_events_silver/', 
        'product_events_silver': 'product_events_silver/'
    }
    
    for table_name, prefix in tables.items():
        print(f"\n📊 {table_name.upper()}")
        print("-" * 30)
        
        try:
            # Get first file
            objects = client.list_objects('silver-layer', prefix=prefix, recursive=True)
            first_file = None
            
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    first_file = obj.object_name
                    break
            
            if not first_file:
                print(f"❌ No parquet files found")
                continue
            
            print(f"📄 Sample file: {os.path.basename(first_file)}")
            
            # Read the file
            response = client.get_object('silver-layer', first_file)
            parquet_data = response.read()
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            response.close()
            response.release_conn()
            
            print(f"📏 Shape: {df.shape}")
            print(f"📋 Columns: {list(df.columns)}")
            print(f"🔢 Data types:")
            for col, dtype in df.dtypes.items():
                print(f"  {col}: {dtype}")
            
            # Show sample data
            if len(df) > 0:
                print(f"\n📝 Sample data (first 2 rows):")
                print(df.head(2).to_string(index=False))
            
        except Exception as e:
            print(f"❌ Error reading {table_name}: {e}")
    
    print(f"\n💡 Fix Required:")
    print("Update the gold pipeline to use the correct column names from above.")

if __name__ == "__main__":
    check_columns()