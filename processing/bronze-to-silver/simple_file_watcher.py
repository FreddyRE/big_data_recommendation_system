# simple_file_watcher.py - Triggers main_pipeline.py without complex imports
"""
Simple file watcher that just triggers your working main_pipeline.py
No complex imports - just monitors file counts and triggers processing
"""

import time
import subprocess
import sys
import os
from datetime import datetime
from minio import Minio

class SimpleFileWatcher:
    """Simple file watcher using direct MinIO client"""
    
    def __init__(self, check_interval: int = 60, batch_size: int = 10):
        self.check_interval = check_interval
        self.batch_size = batch_size
        
        # Direct MinIO client - same config as your working setup
        self.client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            secure=False
        )
        
        self.bronze_bucket = "bronze-layer"
        
        # Track file counts instead of individual files (simpler)
        self.last_file_counts = {
            'clickstream': 0,
            'user': 0,
            'product': 0
        }
        
        # Topic prefixes
        self.topic_prefixes = {
            'clickstream': 'topics/clickstream-events/',
            'user': 'topics/user-events/',
            'product': 'topics/product-events/'
        }
        
        self.running = False
        
    def start_watching(self):
        """Start watching for new files"""
        print(f"Starting simple file watcher (check every {self.check_interval}s)")
        
        self.running = True
        
        # Initialize baseline counts
        self._initialize_baseline()
        
        try:
            while self.running:
                self._check_for_changes()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            print("\nStopping file watcher...")
            self.running = False
    
    def _initialize_baseline(self):
        """Initialize baseline file counts"""
        print("Initializing baseline file counts...")
        
        for topic, prefix in self.topic_prefixes.items():
            try:
                objects = list(self.client.list_objects(
                    self.bronze_bucket, 
                    prefix=prefix, 
                    recursive=True
                ))
                count = len(objects)
                self.last_file_counts[topic] = count
                print(f"   {topic}: {count} files")
            except Exception as e:
                print(f"   Error with {topic}: {e}")
                self.last_file_counts[topic] = 0
        
        print("Baseline complete")
    
    def _check_for_changes(self):
        """Check for file count changes"""
        current_time = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_time}] Checking for new files...")
        
        for topic, prefix in self.topic_prefixes.items():
            try:
                objects = list(self.client.list_objects(
                    self.bronze_bucket, 
                    prefix=prefix, 
                    recursive=True
                ))
                current_count = len(objects)
                last_count = self.last_file_counts[topic]
                
                new_files = current_count - last_count
                
                if new_files > 0:
                    print(f"   {topic}: {new_files} new files ({last_count} → {current_count})")
                    
                    if new_files >= self.batch_size:
                        self._trigger_processing(topic, new_files)
                        self.last_file_counts[topic] = current_count
                    else:
                        print(f"   {topic}: {new_files}/{self.batch_size} files, waiting...")
                else:
                    print(f"   {topic}: no new files ({current_count} total)")
                    
            except Exception as e:
                print(f"   Error checking {topic}: {e}")
    
    def _trigger_processing(self, topic: str, new_file_count: int):
        """Trigger your working main_pipeline.py"""
        print(f"\n🚀 TRIGGERING PROCESSING for {topic}")
        print(f"   New files: {new_file_count}")
        
        try:
            files_to_process = min(new_file_count + 5, 25)
            
            cmd = [
                sys.executable, 
                "main_pipeline.py",
                "--topics", topic,
                "--files-per-topic", str(files_to_process)
            ]
            
            print(f"   Running: {' '.join(cmd)}")
            print("-" * 50)
            
            # Run your working pipeline
            result = subprocess.run(
                cmd,
                cwd=os.getcwd(),
                timeout=300  # 5 minutes
            )
            
            if result.returncode == 0:
                print(f"✅ Processing completed successfully for {topic}")
            else:
                print(f"❌ Processing failed for {topic}")
                
            print("-" * 50)
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Processing timeout for {topic}")
        except Exception as e:
            print(f"❌ Error triggering {topic}: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Simple File Watcher')
    parser.add_argument('--interval', type=int, default=60,
                        help='Check interval in seconds')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Min new files to trigger processing')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("SIMPLE BRONZE LAYER FILE WATCHER")
    print("=" * 60)
    print(f"Check interval: {args.interval} seconds")
    print(f"Batch size: {args.batch_size} files")
    print("\nPress Ctrl+C to stop...")
    print("=" * 60)
    
    try:
        watcher = SimpleFileWatcher(
            check_interval=args.interval,
            batch_size=args.batch_size
        )
        watcher.start_watching()
    except KeyboardInterrupt:
        print("\nFile watcher stopped by user")
    except Exception as e:
        print(f"File watcher error: {e}")

if __name__ == "__main__":
    main()