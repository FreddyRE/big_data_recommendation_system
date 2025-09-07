# 2-processing/bronze-to-silver/pipeline.py
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

from utils.storage_manager import MinIOStorageManager
from utils.data_quality import DataQualityValidator
from utils.config import ProcessingConfig
from transformations.clickstream_transformer import ClickstreamTransformer
from transformations.user_features_transformer import UserFeaturesTransformer
from transformations.product_features_transformer import ProductFeaturesTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BronzeToSilverPipeline:
    """
    Main orchestrator for bronze to silver data transformations.
    Implements medallion architecture with data quality validation.
    """
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.storage_manager = MinIOStorageManager(config)
        self.data_quality = DataQualityValidator(config)
        
        # Initialize transformers
        self.clickstream_transformer = ClickstreamTransformer(config)
        self.user_features_transformer = UserFeaturesTransformer(config)
        self.product_features_transformer = ProductFeaturesTransformer(config)
        
        self.transformers = {
            'clickstream': self.clickstream_transformer,
            'user_features': self.user_features_transformer,
            'product_features': self.product_features_transformer
        }
        
    async def run_pipeline(self, date: Optional[str] = None) -> Dict:
        """
        Run the complete bronze to silver transformation pipeline.
        
        Args:
            date: Processing date in YYYY-MM-DD format. Defaults to today.
            
        Returns:
            Pipeline execution results
        """
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Starting bronze-to-silver pipeline for date: {date}")
        start_time = datetime.now()
        
        try:
            # Step 1: Discover bronze layer files
            bronze_files = await self._discover_bronze_files(date)
            if not bronze_files:
                logger.warning(f"No bronze files found for date: {date}")
                return {"status": "success", "message": "No files to process"}
            
            # Step 2: Process each data type in parallel
            results = await self._process_data_types(bronze_files, date)
            
            # Step 3: Generate pipeline metadata
            pipeline_metadata = self._generate_pipeline_metadata(
                start_time, datetime.now(), results, date
            )
            
            # Step 4: Save pipeline metadata
            await self._save_pipeline_metadata(pipeline_metadata, date)
            
            logger.info(f"Pipeline completed successfully for date: {date}")
            return {"status": "success", "results": results, "metadata": pipeline_metadata}
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            error_metadata = {
                "status": "failed",
                "error": str(e),
                "date": date,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            }
            await self._save_pipeline_metadata(error_metadata, date, is_error=True)
            raise
    
    async def _discover_bronze_files(self, date: str) -> Dict[str, List[str]]:
        """Discover available bronze layer files for processing."""
        bronze_files = {}
        
        for data_type in self.transformers.keys():
            try:
                files = await self.storage_manager.list_files(
                    bucket='bronze',
                    prefix=f"{data_type}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
                )
                if files:
                    bronze_files[data_type] = files
                    logger.info(f"Found {len(files)} bronze files for {data_type}")
            except Exception as e:
                logger.warning(f"Error discovering files for {data_type}: {str(e)}")
                
        return bronze_files
    
    async def _process_data_types(self, bronze_files: Dict[str, List[str]], date: str) -> Dict:
        """Process each data type in parallel."""
        results = {}
        
        # Create tasks for parallel processing
        tasks = []
        for data_type, files in bronze_files.items():
            task = asyncio.create_task(
                self._process_single_data_type(data_type, files, date)
            )
            tasks.append((data_type, task))
        
        # Wait for all tasks to complete
        for data_type, task in tasks:
            try:
                result = await task
                results[data_type] = result
                logger.info(f"Successfully processed {data_type}: {result}")
            except Exception as e:
                logger.error(f"Failed to process {data_type}: {str(e)}")
                results[data_type] = {"status": "failed", "error": str(e)}
                
        return results
    
    async def _process_single_data_type(self, data_type: str, files: List[str], date: str) -> Dict:
        """Process a single data type through the transformation pipeline."""
        transformer = self.transformers[data_type]
        processed_records = 0
        failed_records = 0
        
        try:
            for file_path in files:
                # Step 1: Read bronze data
                raw_data = await self.storage_manager.read_json_lines(
                    bucket='bronze', 
                    key=file_path
                )
                
                # Step 2: Transform data
                transformed_data = await transformer.transform(raw_data)
                
                # Step 3: Validate data quality
                validation_results = await self.data_quality.validate_batch(
                    transformed_data, data_type
                )
                
                # Step 4: Filter valid records
                valid_records = [
                    record for record, is_valid in zip(transformed_data, validation_results['valid'])
                    if is_valid
                ]
                
                # Step 5: Save to silver layer
                if valid_records:
                    silver_path = self._generate_silver_path(data_type, file_path, date)
                    await self.storage_manager.write_json_lines(
                        bucket='silver',
                        key=silver_path,
                        data=valid_records
                    )
                    
                    processed_records += len(valid_records)
                    failed_records += len(transformed_data) - len(valid_records)
                
                logger.info(f"Processed {file_path}: {len(valid_records)} valid records")
            
            return {
                "status": "success",
                "processed_records": processed_records,
                "failed_records": failed_records,
                "files_processed": len(files)
            }
            
        except Exception as e:
            logger.error(f"Error processing {data_type}: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "processed_records": processed_records,
                "failed_records": failed_records
            }
    
    def _generate_silver_path(self, data_type: str, bronze_path: str, date: str) -> str:
        """Generate silver layer file path based on bronze path."""
        # Extract filename from bronze path
        filename = bronze_path.split('/')[-1]
        
        # Create silver path with partitioning
        return (f"{data_type}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/"
                f"processed_{filename}")
    
    def _generate_pipeline_metadata(self, start_time: datetime, end_time: datetime,
                                  results: Dict, date: str) -> Dict:
        """Generate comprehensive pipeline metadata."""
        total_processed = sum(r.get('processed_records', 0) for r in results.values() if isinstance(r, dict))
        total_failed = sum(r.get('failed_records', 0) for r in results.values() if isinstance(r, dict))
        
        return {
            "pipeline_id": f"bronze_to_silver_{date}_{int(start_time.timestamp())}",
            "date": date,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "status": "success" if all(r.get('status') == 'success' for r in results.values() if isinstance(r, dict)) else "partial_success",
            "total_records_processed": total_processed,
            "total_records_failed": total_failed,
            "data_types_processed": list(results.keys()),
            "detailed_results": results,
            "config_version": self.config.version,
            "environment": os.getenv('ENVIRONMENT', 'development')
        }
    
    async def _save_pipeline_metadata(self, metadata: Dict, date: str, is_error: bool = False) -> None:
        """Save pipeline execution metadata."""
        metadata_type = "error" if is_error else "success"
        metadata_path = f"pipeline_metadata/{date}/{metadata_type}_{int(datetime.now().timestamp())}.json"
        
        await self.storage_manager.write_json(
            bucket='metadata',
            key=metadata_path,
            data=metadata
        )
        
        logger.info(f"Pipeline metadata saved to: {metadata_path}")

    async def run_incremental_processing(self, hours_back: int = 1) -> Dict:
        """
        Run incremental processing for recent data.
        Useful for near real-time processing.
        """
        logger.info(f"Starting incremental processing for last {hours_back} hours")
        
        # Calculate processing window
        end_time = datetime.now()
        start_time = end_time.replace(hour=end_time.hour - hours_back)
        
        # Process each hour in the window
        results = {}
        current_time = start_time
        
        while current_time <= end_time:
            date_str = current_time.strftime('%Y-%m-%d')
            hour_str = current_time.strftime('%H')
            
            try:
                # Discover files for this specific hour
                bronze_files = await self._discover_bronze_files_by_hour(date_str, hour_str)
                if bronze_files:
                    hour_results = await self._process_data_types(bronze_files, date_str)
                    results[f"{date_str}_{hour_str}"] = hour_results
                    
            except Exception as e:
                logger.error(f"Incremental processing failed for {current_time}: {str(e)}")
                results[f"{date_str}_{hour_str}"] = {"status": "failed", "error": str(e)}
            
            current_time = current_time.replace(hour=current_time.hour + 1)
        
        return {"incremental_results": results}
    
    async def _discover_bronze_files_by_hour(self, date: str, hour: str) -> Dict[str, List[str]]:
        """Discover bronze files for a specific hour."""
        bronze_files = {}
        
        for data_type in self.transformers.keys():
            try:
                files = await self.storage_manager.list_files(
                    bucket='bronze',
                    prefix=f"{data_type}/year={date[:4]}/month={date[5:7]}/day={date[8:10]}/hour={hour}/"
                )
                if files:
                    bronze_files[data_type] = files
            except Exception as e:
                logger.warning(f"Error discovering hourly files for {data_type}: {str(e)}")
                
        return bronze_files

# CLI Interface for running the pipeline
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze to Silver Data Pipeline')
    parser.add_argument('--date', type=str, help='Processing date (YYYY-MM-DD)')
    parser.add_argument('--incremental', action='store_true', 
                       help='Run incremental processing')
    parser.add_argument('--hours-back', type=int, default=1,
                       help='Hours to look back for incremental processing')
    
    args = parser.parse_args()
    
    # Initialize configuration
    config = ProcessingConfig()
    
    # Create and run pipeline
    pipeline = BronzeToSilverPipeline(config)
    
    async def main():
        if args.incremental:
            result = await pipeline.run_incremental_processing(args.hours_back)
        else:
            result = await pipeline.run_pipeline(args.date)
        
        print(json.dumps(result, indent=2))
    
    # Run the pipeline
    asyncio.run(main())