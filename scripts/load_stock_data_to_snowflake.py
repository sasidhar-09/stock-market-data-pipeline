"""
Complete pipeline: Fetch stock data → Load to Snowflake
Production-ready script with error handling
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from src.ingestion.fetch_stock_data import StockDataFetcher
from src.utils.snowflake_loader import load_to_snowflake
from src.utils.helpers import configure_logger
from loguru import logger
from dotenv import load_dotenv

load_dotenv()
configure_logger(level=os.getenv('LOG_LEVEL', 'INFO'))


def run_pipeline(start_date: str, end_date: str, use_mock: bool = True):
    """
    Run complete data pipeline
    
    Args:
        start_date: Start date 'YYYY-MM-DD'
        end_date: End date 'YYYY-MM-DD'
        use_mock: Use mock data or real API
    """
    
    logger.info("=" * 60)
    logger.info("STOCK MARKET DATA PIPELINE - START")
    logger.info("=" * 60)
    
    try:
        # Step 1: Fetch data
        logger.info(f"Step 1: Fetching data ({start_date} to {end_date})")
        logger.info(f"Data source: {'MOCK' if use_mock else 'YFINANCE API'}")
        
        fetcher = StockDataFetcher(use_mock=use_mock)
        df = fetcher.fetch(start_date=start_date, end_date=end_date)
        
        if df.empty:
            logger.error("No data fetched - aborting pipeline")
            return False
        
        logger.info(f"Fetched {len(df)} records for {df['symbol'].nunique()} symbols")
        
        # Step 2: Load to Snowflake
        logger.info("Step 2: Loading to Snowflake RAW layer")
        
        rows_loaded = load_to_snowflake(
            df=df,
            table_name='STOCK_PRICES_RAW',
            schema='RAW',
            mode='append'  # Change to 'replace' for full refresh
        )
        
        logger.info(f"Successfully loaded {rows_loaded} rows to Snowflake")
        
        # Step 3: Summary
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Symbols Processed: {df['symbol'].nunique()}")
        logger.info(f"Total Records: {len(df)}")
        logger.info(f"Rows Loaded: {rows_loaded}")
        logger.info(f"Target Table: RAW.STOCK_PRICES_RAW")
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 60)
        logger.exception(f"Error: {e}")
        return False


if __name__ == "__main__":
    # Configuration
    USE_MOCK = os.getenv('USE_MOCK_DATA', 'True') == 'True'
    
    # Date range for historical load (February 2026)
    START_DATE = '2026-02-01'
    END_DATE = '2026-02-28'
    
    # Run pipeline
    success = run_pipeline(
        start_date=START_DATE,
        end_date=END_DATE,
        use_mock=USE_MOCK
    )
    
    sys.exit(0 if success else 1)