# Debug what's actually in MinIO

import sys
import os

# Add path to utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
bronze_to_silver_dir = os.path.join(parent_dir, 'bronze-to-silver')
utils_path = os.path.join(bronze_to_silver_dir, 'utils')

sys.path.insert(0, utils_path)
from storage_manager import SafeMinIOManager

def debug_minio():
    print("🔍 Debugging MinIO Contents")
    print("=" * 50)
    
    # Initialize storage
    storage = SafeMinIOManager(
        endpoint='localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin123'
    )
    
    client = storage.client
    
    # List all buckets
    print("📦 Available Buckets:")
    try:
        buckets = client.list_buckets()
        for bucket in buckets:
            print(f"  - {bucket.name} (created: {bucket.creation_date})")
    except Exception as e:
        print(f"❌ Error listing buckets: {e}")
        return
    
    # Check each bucket for contents
    bucket_names = ['bronze-layer', 'silver-layer', 'gold-layer']
    
    for bucket_name in bucket_names:
        print(f"\n📁 Contents of {bucket_name}:")
        print("-" * 30)
        
        try:
            objects = client.list_objects(bucket_name, recursive=True)
            
            file_count = 0
            folders = set()
            
            for obj in objects:
                file_count += 1
                print(f"  📄 {obj.object_name} ({obj.size} bytes)")
                
                # Track folder structure
                if '/' in obj.object_name:
                    folder = '/'.join(obj.object_name.split('/')[:-1])
                    folders.add(folder)
                
                # Limit output for readability
                if file_count >= 20:
                    remaining_objects = list(client.list_objects(bucket_name, recursive=True))
                    remaining_count = len(remaining_objects) - file_count
                    if remaining_count > 0:
                        print(f"  ... and {remaining_count} more files")
                    break
            
            print(f"\n📊 {bucket_name} Summary:")
            print(f"  Total files: {file_count}")
            if folders:
                print(f"  Folder structure:")
                for folder in sorted(folders):
                    print(f"    📂 {folder}/")
            
            if file_count == 0:
                print(f"  ⚠️ No files found in {bucket_name}")
                
        except Exception as e:
            print(f"❌ Error accessing {bucket_name}: {e}")
    
    # Specifically look for silver data patterns
    print(f"\n🔍 Looking for Silver Layer Data Patterns:")
    print("-" * 40)
    
    try:
        silver_objects = client.list_objects('silver-layer', recursive=True)
        
        clickstream_files = []
        user_files = []
        product_files = []
        
        for obj in silver_objects:
            if 'clickstream' in obj.object_name.lower():
                clickstream_files.append(obj.object_name)
            elif 'user' in obj.object_name.lower():
                user_files.append(obj.object_name)
            elif 'product' in obj.object_name.lower():
                product_files.append(obj.object_name)
        
        print(f"🔗 Clickstream files: {len(clickstream_files)}")
        for f in clickstream_files[:5]:  # Show first 5
            print(f"  - {f}")
        if len(clickstream_files) > 5:
            print(f"  ... and {len(clickstream_files) - 5} more")
            
        print(f"\n👥 User files: {len(user_files)}")
        for f in user_files[:3]:
            print(f"  - {f}")
            
        print(f"\n📦 Product files: {len(product_files)}")  
        for f in product_files[:3]:
            print(f"  - {f}")
            
    except Exception as e:
        print(f"❌ Error searching silver layer: {e}")
    
    print(f"\n💡 Recommendations:")
    print("=" * 30)
    print("1. If no silver files exist, run the bronze-to-silver pipeline first")
    print("2. Check the exact folder structure and naming conventions")
    print("3. Verify that your silver processing completed successfully")

if __name__ == "__main__":
    debug_minio()