# check_silver.py - Quick checker for silver layer contents
from minio import Minio
from datetime import datetime

def check_silver_layer():
    """Check what's actually in the silver layer"""
    
    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False
    )
    
    silver_bucket = "silver-layer"
    
    print("=" * 60)
    print("SILVER LAYER CONTENTS")
    print("=" * 60)
    
    try:
        # List all objects in silver bucket
        objects = list(client.list_objects(silver_bucket, recursive=True))
        
        if not objects:
            print("❌ No files found in silver layer!")
            print("\nPossible issues:")
            print("1. Processing hasn't been triggered yet")
            print("2. main_pipeline.py isn't writing to silver")
            print("3. Silver bucket doesn't exist")
            return
        
        print(f"✅ Found {len(objects)} files in silver layer")
        
        # Group by table
        tables = {}
        for obj in objects:
            table_name = obj.object_name.split('/')[0]
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(obj)
        
        print(f"\n📊 SILVER TABLES:")
        for table, files in tables.items():
            total_size = sum(f.size for f in files) / (1024*1024)
            latest_file = max(files, key=lambda x: x.last_modified)
            
            print(f"\n📂 {table}:")
            print(f"   Files: {len(files)}")
            print(f"   Size: {total_size:.2f} MB")
            print(f"   Latest: {latest_file.last_modified}")
            
            # Show recent files
            recent_files = sorted(files, key=lambda x: x.last_modified, reverse=True)[:3]
            print(f"   Recent files:")
            for f in recent_files:
                size_kb = f.size / 1024
                print(f"     📄 {f.object_name} ({size_kb:.1f} KB, {f.last_modified})")
        
        print(f"\n⏰ PROCESSING ACTIVITY:")
        all_files = sorted(objects, key=lambda x: x.last_modified, reverse=True)
        
        # Show when files were last created
        now = datetime.utcnow()
        for i, f in enumerate(all_files[:5]):
            age = now - f.last_modified.replace(tzinfo=None)
            age_str = f"{age.total_seconds()/60:.1f} minutes ago"
            if age.days > 0:
                age_str = f"{age.days} days ago"
            elif age.total_seconds() > 3600:
                age_str = f"{age.total_seconds()/3600:.1f} hours ago"
            
            print(f"   {i+1}. {f.object_name.split('/')[-1]} - {age_str}")
        
        # Check if processing is recent
        latest_file = max(objects, key=lambda x: x.last_modified)
        latest_age = now - latest_file.last_modified.replace(tzinfo=None)
        
        if latest_age.total_seconds() < 600:  # Less than 10 minutes
            print(f"\n✅ Recent processing detected (last file {latest_age.total_seconds()/60:.1f} min ago)")
        else:
            print(f"\n⚠️  No recent processing (last file {latest_age.total_seconds()/3600:.1f} hours ago)")
            print("   Try running: python main_pipeline.py --files-per-topic 5")
        
    except Exception as e:
        print(f"❌ Error checking silver layer: {e}")
        if "NoSuchBucket" in str(e):
            print("The silver-layer bucket doesn't exist!")
            print("Your main_pipeline.py should create it automatically.")

if __name__ == "__main__":
    check_silver_layer()