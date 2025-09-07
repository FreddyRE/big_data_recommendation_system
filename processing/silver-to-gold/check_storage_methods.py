# Check what methods are available in SafeMinIOManager

import sys
import os

# Add path to utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
bronze_to_silver_dir = os.path.join(parent_dir, 'bronze-to-silver')
utils_path = os.path.join(bronze_to_silver_dir, 'utils')

sys.path.insert(0, utils_path)
from storage_manager import SafeMinIOManager

# Initialize storage
storage = SafeMinIOManager(
    endpoint='localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin123'
)

print("🔍 Available methods in SafeMinIOManager:")
print("=" * 50)

# Get all methods and attributes
methods = [method for method in dir(storage) if not method.startswith('_')]

for method in sorted(methods):
    attr = getattr(storage, method)
    if callable(attr):
        print(f"✅ {method}()")
    else:
        print(f"📝 {method} (attribute)")

print("\n🔍 Let's also check if we can access MinIO directly:")
try:
    # Try to access the MinIO client directly
    if hasattr(storage, 'client'):
        print("✅ Found storage.client")
        client = storage.client
        
        # List buckets
        buckets = client.list_buckets()
        print(f"📦 Buckets: {[bucket.name for bucket in buckets]}")
        
        # Try to list objects in silver-layer bucket
        try:
            objects = client.list_objects('silver-layer', recursive=True)
            print(f"📁 Objects in silver-layer:")
            count = 0
            for obj in objects:
                print(f"  - {obj.object_name}")
                count += 1
                if count >= 10:  # Limit output
                    print(f"  ... and {count} more files")
                    break
        except Exception as e:
            print(f"❌ Error listing silver-layer: {e}")
            
    else:
        print("❌ No direct client access found")
        
except Exception as e:
    print(f"❌ Error checking MinIO: {e}")

print("\n📋 To fix the gold pipeline, we need to:")
print("1. Use the correct method names")
print("2. Or access the MinIO client directly")
print("3. Or add the missing methods to SafeMinIOManager")