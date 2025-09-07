import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import logging
from ..utils.data_quality import DataQualityChecker

logger = logging.getLogger(__name__)

class ProductFeaturesTransformer:
    def __init__(self, quality_checker: DataQualityChecker):
        self.quality_checker = quality_checker
        self.required_columns = ['product_id', 'timestamp']
        
    def transform(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Transform product features data from bronze to silver"""
        logger.info(f"Starting product features transformation for {len(df)} records")
        
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
        
        logger.info(f"Completed product features transformation: {len(transformed_df)} records")
        
        return {
            "transformed_data": transformed_df,
            "quality_report": quality_report
        }
    
    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features to product data"""
        df = df.copy()
        
        # Price categories
        if 'price' in df.columns:
            df['price_category'] = pd.cut(df['price'], 
                                        bins=[0, 25, 100, 300, float('inf')], 
                                        labels=['budget', 'mid', 'premium', 'luxury'])
        
        # Rating categories
        if 'avg_rating' in df.columns:
            df['rating_category'] = df['avg_rating'].apply(
                lambda x: 'excellent' if x >= 4.5 else 
                         'good' if x >= 4.0 else 
                         'average' if x >= 3.0 else 'poor'
            )
        
        # Popularity scoring
        if 'popularity_score' in df.columns:
            df['popularity_percentile'] = df['popularity_score'].rank(pct=True)
        
        return df