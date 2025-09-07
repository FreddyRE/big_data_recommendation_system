import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import logging
from ..utils.data_quality import DataQualityChecker

logger = logging.getLogger(__name__)

class UserFeaturesTransformer:
    def __init__(self, quality_checker: DataQualityChecker):
        self.quality_checker = quality_checker
        self.required_columns = ['user_id', 'timestamp']
        
    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Transform user features data from bronze to silver"""
        logger.info(f"Starting user features transformation for {len(df)} records")
        
        if df.empty:
            return {"transformed_data": pd.DataFrame(), "quality_report": {}}
        
        # Data quality check
        quality_report = self.quality_checker.check_data_quality(df, self.required_columns)
        
        # Clean data
        cleaned_df = self.quality_checker.clean_data(df)
        
        if cleaned_df.empty:
            return {"transformed_data": pd.DataFrame(), "quality_report": quality_report}
        
        # Feature engineering
        transformed_df = self._add_features(cleaned_df)
        
        # Add processing metadata
        transformed_df['processed_at'] = datetime.now()
        transformed_df['data_quality_score'] = quality_report['quality_score']
        
        logger.info(f"Completed user features transformation: {len(transformed_df)} records")
        
        return {
            "transformed_data": transformed_df,
            "quality_report": quality_report
        }
    
    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features to user data"""
        df = df.copy()
        
        # Normalize engagement score
        if 'engagement_score' in df.columns:
            df['engagement_quartile'] = pd.qcut(df['engagement_score'], q=4, labels=['low', 'medium', 'high', 'very_high'])
        
        # Customer value segments
        if 'avg_order_value' in df.columns and 'total_purchases' in df.columns:
            df['customer_lifetime_value'] = df['avg_order_value'] * df['total_purchases']
            df['customer_segment'] = self._categorize_customers(df)
        
        return df
    
    def _categorize_customers(self, df: pd.DataFrame) -> pd.Series:
        """Categorize customers into segments"""
        segments = []
        for _, row in df.iterrows():
            if row['total_purchases'] == 0:
                segment = 'new'
            elif row['total_purchases'] < 5:
                segment = 'occasional'
            elif row['avg_order_value'] > 200:
                segment = 'high_value'
            else:
                segment = 'regular'
            segments.append(segment)
        return pd.Series(segments)