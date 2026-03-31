"""ML Feature Store utilities for Snowflake.

Provides Point-in-Time (PIT) correct feature retrieval, training data generation,
and feature versioning support for ML workflows.

Usage:
    from feature_store import FeatureStore
    fs = FeatureStore(session)
    
    # Get customer features as of a specific date
    features_df = fs.get_training_data(
        entity='customer',
        observation_date='2000-01-01',
        features=['cust_rfm_composite_score', 'cust_lifetime_value']
    )
"""

import pyarrow as pa
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd


class FeatureStore:
    """Snowflake Feature Store Interface.
    
    Handles:
    - Point-in-time correct feature retrieval
    - Feature versioning and lineage tracking
    - Training data generation with historical accuracy
    - Online/offline store separation
    """
    
    def __init__(self, session):
        """Initialize Feature Store with Snowflake session.
        
        Args:
            session: Snowflake Snowpark session
        """
        self.session = session
        self.db = "RAW_SALES"
        self.schema = "FEATURE_STORE"
    
    def get_customer_features_as_of(
        self,
        customer_ids: List[int],
        observation_date: str,  # YYYY-MM-DD
        features: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get customer features as of a specific observation date (PIT correct).
        
        Args:
            customer_ids: List of CUSTOMER_IDs
            observation_date: YYYY-MM-DD string
            features: Optional list of feature names; if None, returns all
            
        Returns:
            DataFrame with shape: (len(customer_ids), num_features)
        """
        ids_list = ','.join([str(cid) for cid in customer_ids])
        
        query = f"""
        SELECT
            rfm.CUSTOMER_ID,
            rfm.OBSERVATION_DATE,
            rfm.recency_days,
            rfm.frequency_12m,
            rfm.monetary_12m,
            rfm.rfm_composite_score,
            rfm.estimated_segment,
            eng.days_since_last_purchase,
            eng.engagement_status,
            eng.lifetime_value,
            eng.avg_order_value
        FROM {self.db}.{self.schema}.customer_rfm_features_offline rfm
        JOIN {self.db}.{self.schema}.customer_engagement_features_offline eng
            ON rfm.CUSTOMER_ID = eng.CUSTOMER_ID AND rfm.OBSERVATION_DATE = eng.OBSERVATION_DATE
        WHERE rfm.CUSTOMER_ID IN ({ids_list})
          AND rfm.OBSERVATION_DATE = TO_DATE('{observation_date}', 'YYYY-MM-DD')
        """
        
        df = self.session.sql(query).to_pandas()
        return df[['CUSTOMER_ID'] + features] if features else df
    
    def get_product_features_as_of(
        self,
        product_ids: List[int],
        observation_date: str,
        features: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get product features as of a specific observation date (PIT correct)."""
        ids_list = ','.join([str(pid) for pid in product_ids])
        
        query = f"""
        SELECT
            perf.PRODUCT_ID,
            perf.OBSERVATION_DATE,
            perf.cumulative_revenue,
            perf.revenue_12m,
            perf.cumulative_units_sold,
            perf.units_sold_12m,
            perf.total_returned_items,
            perf.return_rate_pct,
            perf.revenue_rank,
            p.CATEGORY,
            p.MANUFACTURER,
            p.UNIT_PRICE
        FROM {self.db}.{self.schema}.product_performance_features_offline perf
        JOIN {self.db}.GOLD.dim_products p
            ON perf.PRODUCT_ID = p.PRODUCT_ID
        WHERE perf.PRODUCT_ID IN ({ids_list})
          AND perf.OBSERVATION_DATE = TO_DATE('{observation_date}', 'YYYY-MM-DD')
        """
        
        df = self.session.sql(query).to_pandas()
        return df[['PRODUCT_ID'] + features] if features else df
    
    def get_sales_rep_features_as_of(
        self,
        rep_ids: List[int],
        observation_date: str,
        features: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get sales rep features as of a specific observation date (PIT correct)."""
        ids_list = ','.join([str(rid) for rid in rep_ids])
        
        query = f"""
        SELECT
            REP_ID,
            OBSERVATION_DATE,
            QUOTA,
            ytd_revenue,
            cumulative_revenue,
            ytd_orders,
            quota_attainment_ratio,
            quota_attainment_pct,
            ytd_customer_count
        FROM {self.db}.{self.schema}.sales_rep_quota_features_offline
        WHERE REP_ID IN ({ids_list})
          AND OBSERVATION_DATE = TO_DATE('{observation_date}', 'YYYY-MM-DD')
        """
        
        df = self.session.sql(query).to_pandas()
        return df[['REP_ID'] + features] if features else df
    
    def get_training_dataset(
        self,
        entity_type: str,  # 'customer' | 'product' | 'sales_rep'
        date_range: tuple,  # (start_date, end_date) both YYYY-MM-DD
        sample_fraction: float = 1.0
    ) -> pd.DataFrame:
        """Generate point-in-time correct training dataset.
        
        Args:
            entity_type: customer | product | sales_rep
            date_range: (start_date, end_date) both YYYY-MM-DD
            sample_fraction: for sampling (useful for large datasets)
            
        Returns:
            Training DataFrame with temporal dimension preserved
        """
        if entity_type == 'customer':
            table = f"{self.db}.{self.schema}.training_data_customers"
        elif entity_type == 'product':
            table = f"{self.db}.{self.schema}.training_data_products"
        else:
            raise ValueError(f"Unknown entity_type: {entity_type}")
        
        query = f"""
        SELECT *
        FROM {table}
        WHERE OBSERVATION_DATE >= TO_DATE('{date_range[0]}', 'YYYY-MM-DD')
          AND OBSERVATION_DATE <= TO_DATE('{date_range[1]}', 'YYYY-MM-DD')
        """
        
        if sample_fraction < 1.0:
            query += f" AND RANDOM() < {sample_fraction}"
        
        df = self.session.sql(query).to_pandas()
        return df
    
    def list_features(
        self,
        entity_type: Optional[str] = None,
        tags: Optional[str] = None
    ) -> pd.DataFrame:
        """List available features with metadata.
        
        Args:
            entity_type: Filter by customer | product | sales_rep
            tags: Filter by tag (comma-separated)
            
        Returns:
            DataFrame with feature metadata
        """
        query = f"SELECT * FROM {self.db}.{self.schema}.feature_registry WHERE 1=1"
        
        if entity_type:
            query += f" AND ENTITY_TYPE = '{entity_type}'"
        
        if tags:
            tag_condition = ' OR '.join([f"TAGS ILIKE '%{tag}%'" for tag in tags.split(',')])
            query += f" AND ({tag_condition})"
        
        return self.session.sql(query).to_pandas()
    
    def get_feature_lineage(self, feature_id: str) -> pd.DataFrame:
        """Get feature lineage (dependencies and upstream sources)."""
        query = f"""
        SELECT *
        FROM {self.db}.{self.schema}.feature_lineage
        WHERE DOWNSTREAM_FEATURE_ID = '{feature_id}'
           OR UPSTREAM_FEATURE_ID = '{feature_id}'
        ORDER BY CREATED_AT DESC
        """
        return self.session.sql(query).to_pandas()
    
    def validate_training_data(
        self,
        df: pd.DataFrame,
        entity_type: str
    ) -> Dict[str, bool]:
        """Validate training data for completeness and structure.
        
        Args:
            df: Training DataFrame
            entity_type: customer | product | sales_rep
            
        Returns:
            Dict of validation checks
        """
        checks = {
            'has_rows': len(df) > 0,
            'has_observation_date': 'OBSERVATION_DATE' in df.columns,
            'no_nulls_in_keys': df[['CUSTOMER_ID' if entity_type == 'customer' else 
                                     'PRODUCT_ID' if entity_type == 'product' 
                                     else 'REP_ID']].notna().all().all(),
            'temporal_coverage': (df['OBSERVATION_DATE'].max() - df['OBSERVATION_DATE'].min()).days > 30 if 'OBSERVATION_DATE' in df.columns else False
        }
        return checks
    
    def estimate_feature_importance(
        self,
        training_df: pd.DataFrame,
        target_column: str
    ) -> pd.DataFrame:
        """Quick feature importance estimate using correlation with target.
        
        Args:
            training_df: Training DataFrame (must include target_column)
            target_column: Name of target variable
            
        Returns:
            DataFrame with features sorted by correlation magnitude
        """
        numerical_cols = training_df.select_dtypes(include=['number']).columns
        correlations = training_df[numerical_cols].corr()[target_column].drop(target_column)
        return pd.DataFrame({
            'feature': correlations.index,
            'correlation_with_target': correlations.values
        }).sort_values('correlation_with_target', key=abs, ascending=False)


def create_customer_training_task(session, start_date: str, end_date: str):
    """Snowpark task to generate customer training data.
    
    Creates a scheduled Task in Snowflake that computes training data
    with point-in-time correctness.
    
    Args:
        session: Snowflake Snowpark session
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
    """
    sql = f"""
    CREATE OR REPLACE TASK IF NOT EXISTS RAW_SALES.MONITORING.task_generate_ml_customer_training
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Daily at 2 AM UTC
    AS
    CREATE OR REPLACE TABLE RAW_SALES.FEATURE_STORE.customer_training_batch AS
    SELECT
        rfm.CUSTOMER_ID,
        rfm.OBSERVATION_DATE,
        rfm.recency_days,
        rfm.frequency_12m,
        rfm.monetary_12m,
        rfm.rfm_composite_score,
        rfm.estimated_segment,
        eng.engagement_status,
        eng.lifetime_value,
        eng.avg_order_value,
        CURRENT_TIMESTAMP() AS TRAINING_BATCH_CREATED_AT
    FROM RAW_SALES.FEATURE_STORE.customer_rfm_features_offline rfm
    JOIN RAW_SALES.FEATURE_STORE.customer_engagement_features_offline eng
        ON rfm.CUSTOMER_ID = eng.CUSTOMER_ID AND rfm.OBSERVATION_DATE = eng.OBSERVATION_DATE
    WHERE rfm.OBSERVATION_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')
      AND rfm.OBSERVATION_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD');
    """
    
    session.sql(sql).collect()
    print(f"✓ Task created: task_generate_ml_customer_training")


if __name__ == '__main__':
    # Example usage (requires active Snowflake session)
    print("Feature Store Utilities loaded.")
    print("\nUsage:")
    print("  fs = FeatureStore(session)")
    print("  features = fs.get_customer_features_as_of([1, 2, 3], '2000-01-01')")
    print("  training_data = fs.get_training_dataset('customer', ('1999-01-01', '2000-01-01'))")
