import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import logging
from ..utils.data_quality import DataQualityChecker

logger = logging.getLogger(__name__)

class ClickstreamTransformer:
    def __init__(self, quality_checker: DataQualityChecker):
        self.quality_checker = quality_checker
        self.required_columns = ['user_id', 'event_type', 'timestamp', 'product_id']
        
    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Transform clickstream data from bronze to silver"""
        logger.info(f"Starting clickstream transformation for {len(df)} records")
        
        if df.empty:
            return {"transformed_data": pd.DataFrame(), "quality_report": {}}
        
        # 1. Data quality check
        quality_report = self.quality_checker.check_data_quality(df, self.required_columns)
        
        if not quality_report["passes_threshold"]:
            logger.warning(f"Data quality below threshold: {quality_report['quality_score']}")
        
        # 2. Clean data
        cleaned_df = self.quality_checker.clean_data(df)
        
        if cleaned_df.empty:
            return {"transformed_data": pd.DataFrame(), "quality_report": quality_report}
        
        # 3. Feature engineering
        transformed_df = self._add_features(cleaned_df)
        
        # 4. Add processing metadata
        transformed_df['processed_at'] = datetime.now()
        transformed_df['data_quality_score'] = quality_report['quality_score']
        
        logger.info(f"Completed clickstream transformation: {len(transformed_df)} records")
        
        return {
            "transformed_data": transformed_df,
            "quality_report": quality_report
        }
    
    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features to clickstream data"""
        df = df.copy()
        
        # Time-based features
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        df['month'] = df['timestamp'].dt.month
        df['year'] = df['timestamp'].dt.year
        
        # Session analysis (sort by user and timestamp first)
        df = df.sort_values(['user_id', 'timestamp'])
        df['time_since_last_event'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds()
        
        # Event type flags
        df['is_purchase'] = df['event_type'] == 'purchase'
        df['is_cart_add'] = df['event_type'] == 'add_to_cart'
        df['is_view'] = df['event_type'].isin(['page_view', 'product_click'])
        
        # Purchase value handling
        df['purchase_value'] = df.apply(
            lambda row: row.get('price', 0) * row.get('quantity', 1) if row['is_purchase'] else 0,
            axis=1
        )
        
        # User engagement scoring (simple)
        df['engagement_weight'] = df['event_type'].map({
            'page_view': 1,
            'product_click': 2,
            'add_to_cart': 3,
            'purchase': 5
        }).fillna(1)
        
        logger.info("Added time-based, session, and engagement features")
        return df