# file_watcher.py - Minimal file watcher using the exact same imports as main_pipeline.py
"""
Monitors bronze layer for new files and automatically triggers processing
Uses the exact same import pattern as the working main_pipeline.py
"""

import time
import threading
from datetime import datetime
from typing import Dict, Set
import subprocess
import sys
import os
from pathlib import Path

# Use the EXACT same import pattern as main_pipeline.py
sys.path.append(str(Path(__file__).parent))

# Import the same way main_pipeline.py does
from utils.storage_manager import SafeMinIOManager
from utils.logger import setup_logging
from loguru import logger

class BronzeFileWatcher:
    """Watches bronze layer for new files and triggers processing"""
    
    def __init__(self, check_interval: int = 60, batch_size: int = 10):
        self.check_interval = check_interval
        self.batch_size = batch_size
        
        # Use the same initialization as main_pipeline.py
        self.storage = SafeMinIOManager()
        
        # Track processed files
        self.processed_files: Dict[str, Set[str]] = {
            'clickstream': set(),
            'user': set(), 
            'product': set()
        }
        
        # Topic mapping - same as main_pipeline.py
        self.topic_prefixes = {
            'clickstream': 'topics/clickstream-events/',
            'user': 'topics/user-events/',
            'product': 'topics/product-events/'
        }
        
        self.running = False
        self.last_check = {}
        
    def start_watching(self):
        """Start watching for new files"""
        logger.info(f"Starting file watcher (check every {self.check_interval}s)")
        
        self.running = True
        
        # Initialize baseline
        self._initialize_baseline()
        
        # Start monitoring
        monitor_thread = threading.Thread(target=self._monitor_loop)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping file watcher...")
            self.running = False
    
    def _initialize_baseline(self):
        """Initialize baseline of existing files"""
        logger.info("Initializing baseline...")
        
        for topic, prefix in self.topic_prefixes.items():
            try:
                files = self.storage.list_bronze_files(prefix)
                file_keys = {f['key'] for f in files}
                self.processed_files[topic] = file_keys
                logger.info(f"   {topic}: {len(file_keys)} existing files")
            except Exception as e:
                logger.error(f"Error with {topic}: {e}")
                self.processed_files[topic] = set()
        
        logger.info("Baseline complete")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self._check_for_new_files()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                time.sleep(self.check_interval)
    
    def _check_for_new_files(self):
        """Check for new files"""
        for topic, prefix in self.topic_prefixes.items():
            try:
                current_files = self.storage.list_bronze_files(prefix)
                current_keys = {f['key'] for f in current_files}
                new_files = current_keys - self.processed_files[topic]
                
                if new_files:
                    logger.info(f"Found {len(new_files)} new {topic} files")
                    
                    if len(new_files) >= self.batch_size:
                        self._trigger_processing(topic, len(new_files))
                        self.processed_files[topic].update(new_files)
                    else:
                        logger.info(f"{topic}: {len(new_files)}/{self.batch_size} files, waiting...")
                        
            except Exception as e:
                logger.error(f"Error checking {topic}: {e}")
    
    def _trigger_processing(self, topic: str, new_file_count: int):
        """Trigger processing using main_pipeline.py"""
        logger.info(f"Triggering processing for {topic} ({new_file_count} new files)")
        
        try:
            files_to_process = min(new_file_count + 5, 20)
            
            cmd = [
                sys.executable, 
                "main_pipeline.py",
                "--topics", topic,
                "--files-per-topic", str(files_to_process)
            ]
            
            logger.info(f"Running: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=os.getcwd(),
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.success(f"Processing completed for {topic}")
                # Show key results
                for line in result.stdout.split('\n'):
                    if 'records' in line and 'SUCCESS' in line:
                        logger.info(f"   {line.strip()}")
            else:
                logger.error(f"Processing failed for {topic}: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error triggering {topic}: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='File Watcher')
    parser.add_argument('--interval', type=int, default=60)
    parser.add_argument('--batch-size', type=int, default=10) 
    parser.add_argument('--log-level', type=str, default='INFO')
    
    args = parser.parse_args()
    
    # Setup logging the same way as main_pipeline.py
    setup_logging(args.log_level)
    
    print("\n" + "="*50)
    print("BRONZE LAYER FILE WATCHER")
    print("="*50)
    print(f"Check interval: {args.interval}s")
    print(f"Batch size: {args.batch_size} files")
    print("\nPress Ctrl+C to stop...")
    print("="*50)
    
    try:
        watcher = BronzeFileWatcher(
            check_interval=args.interval,
            batch_size=args.batch_size
        )
        watcher.start_watching()
    except KeyboardInterrupt:
        print("\nStopped by user")
    except Exception as e:
        logger.error(f"Watcher error: {e}")

if __name__ == "__main__":
    main()