# 2-processing/silver-to-gold/check_gold_layer.py

import sys
import os
from datetime import datetime

# Fix import path - go up one level to find utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

try:
    from utils.storage_manager import SafeMinIOManager
    from utils.logger import setup_logger
except ImportError as e:
    print(f"❌ Import error: {e}")
    # Try alternative import
    utils_path = os.path.join(parent_dir, 'utils')
    if os.path.exists(utils_path):
        sys.path.insert(0, utils_path)
        from storage_manager import SafeMinIOManager
        from logger import setup_logger
    else:
        print(f"❌ Utils directory not found. Please run from correct directory.")
        sys.exit(1)

def check_gold_layer():
    """Check what's in the gold layer"""
    
    logger = setup_logger("GoldChecker")
    
    # Initialize storage
    storage = SafeMinIOManager(
        endpoint='localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin123'
    )
    
    logger.info("🔍 Checking Gold Layer Contents...")
    
    # Check gold-layer bucket structure
    try:
        gold_files = storage.list_files('gold-layer/')
        
        if not gold_files:
            print("❌ No files found in gold-layer/")
            return
        
        print(f"\n📊 GOLD LAYER SUMMARY")
        print("=" * 50)
        print(f"Total files: {len(gold_files)}")
        
        # Group by table type
        tables = {}
        for file_path in gold_files:
            if file_path.endswith('.parquet'):
                # Extract table name from path like: gold-layer/user_summary/date=2024-01-15/
                parts = file_path.split('/')
                if len(parts) >= 2:
                    table_name = parts[1]
                    if table_name not in tables:
                        tables[table_name] = []
                    tables[table_name].append(file_path)
        
        # Display each table
        for table_name, files in tables.items():
            print(f"\n📋 {table_name.upper()}")
            print("-" * 30)
            print(f"Files: {len(files)}")
            
            # Try to read one file to get sample data
            try:
                sample_file = files[0]
                df = storage.read_parquet(sample_file)
                
                print(f"Records: {len(df)}")
                print(f"Columns: {list(df.columns)}")
                
                # Show sample data
                if len(df) > 0:
                    print(f"\nSample data (first 3 rows):")
                    print(df.head(3).to_string(index=False))
                
            except Exception as e:
                print(f"Error reading {sample_file}: {e}")
        
        # Overall statistics
        print(f"\n📈 OVERALL STATISTICS")
        print("=" * 50)
        print(f"Tables created: {len(tables)}")
        print(f"Total files: {len(gold_files)}")
        
        total_records = 0
        for table_name, files in tables.items():
            try:
                for file_path in files:
                    df = storage.read_parquet(file_path)
                    total_records += len(df)
            except:
                pass
        
        print(f"Total records: {total_records}")
        print(f"Check completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error checking gold layer: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_gold_layer()