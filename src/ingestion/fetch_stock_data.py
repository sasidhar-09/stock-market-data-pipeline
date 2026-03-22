"""Fetch stock data from Yahoo Finance"""

import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from loguru import logger

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.config.stocks import INDIAN_STOCKS, DATA_CONFIG
from src.utils.helpers import configure_logger, add_metadata, clean_columns

load_dotenv()
configure_logger(level=os.getenv('LOG_LEVEL', 'INFO'))


class StockDataFetcher:
    
    def __init__(self, symbols=None, use_mock=False):
        self.symbols = symbols or INDIAN_STOCKS
        self.use_mock = use_mock
        logger.info(f"Initialized with {len(self.symbols)} symbols (mock={use_mock})")
    
    def fetch(self, start_date=None, end_date=None, period='1d'):
        """Fetch stock data - from API or mock"""
        
        if self.use_mock:
            return self._fetch_mock(start_date, end_date)
        
        try:
            if start_date and end_date:
                logger.info(f"Fetching {start_date} to {end_date}")
                data = yf.download(
                    tickers=self.symbols,
                    start=start_date,
                    end=end_date,
                    auto_adjust=DATA_CONFIG['auto_adjust'],
                    prepost=DATA_CONFIG['prepost'],
                    threads=DATA_CONFIG['threads'],
                    group_by='ticker',
                    progress=False
                )
            else:
                logger.info(f"Fetching period: {period}")
                data = yf.download(
                    tickers=self.symbols,
                    period=period,
                    auto_adjust=DATA_CONFIG['auto_adjust'],
                    prepost=DATA_CONFIG['prepost'],
                    threads=DATA_CONFIG['threads'],
                    group_by='ticker',
                    progress=False
                )
            
            if data.empty:
                logger.warning("API returned no data, falling back to mock")
                return self._fetch_mock(start_date, end_date)
            
            df = self._transform(data)
            logger.info(f"Fetched {len(df)} records for {df['symbol'].nunique()} symbols")
            return df
            
        except Exception as e:
            logger.error(f"API fetch failed: {str(e)}")
            logger.warning("Falling back to mock data")
            return self._fetch_mock(start_date, end_date)
    
    def _fetch_mock(self, start_date=None, end_date=None):
        """Generate mock data for testing"""
        from src.ingestion.mock_data import generate_mock_data
        
        if not start_date or not end_date:
            end = datetime.now()
            start = end - timedelta(days=30)
            start_date = start.strftime('%Y-%m-%d')
            end_date = end.strftime('%Y-%m-%d')
        
        logger.info(f"Generating mock data: {start_date} to {end_date}")
        df = generate_mock_data(self.symbols, start_date, end_date)
        logger.info(f"Generated {len(df)} mock records")
        return df
    
    def _transform(self, data):
        """Convert yfinance format to flat table"""
        
        records = []
        for symbol in self.symbols:
            try:
                if symbol in data.columns.get_level_values(0):
                    symbol_data = data[symbol].copy()
                    if symbol_data.empty:
                        continue
                    
                    symbol_data['symbol'] = symbol
                    symbol_data['date'] = symbol_data.index
                    symbol_data.reset_index(drop=True, inplace=True)
                    records.append(symbol_data)
            except Exception as e:
                logger.warning(f"Skipped {symbol}: {str(e)}")
                continue
        
        if not records:
            return pd.DataFrame()
        
        df = pd.concat(records, ignore_index=True)
        df = clean_columns(df)
        df = add_metadata(df)
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
                'ingestion_timestamp', 'source_system', 'pipeline_run_id']
        df = df[[c for c in cols if c in df.columns]]
        df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        
        return df
    
    def get_latest(self):
        """Get most recent trading day"""
        return self.fetch(period='1d')
    
    def get_historical(self, days=30):
        """Get last N days of data"""
        end = datetime.now()
        start = end - timedelta(days=days)
        return self.fetch(
            start_date=start.strftime('%Y-%m-%d'),
            end_date=end.strftime('%Y-%m-%d')
        )


def main():
    logger.info("Starting ingestion test")
    
    # Use mock data for now (set to False when API works)
    fetcher = StockDataFetcher(use_mock=True)
    
    # Get February 2026 data
    df = fetcher.fetch(start_date='2026-02-01', end_date='2026-02-28')
    
    if not df.empty:
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Symbols: {df['symbol'].nunique()}")
        
        print("\nSample data:")
        print(df.head(15))
        
        print("\nSummary by symbol:")
        print(df.groupby('symbol').agg({
            'close': ['min', 'max', 'mean'],
            'volume': 'sum'
        }).round(2))
        
        # Save output
        os.makedirs("data/raw", exist_ok=True)
        output = f"data/raw/stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output, index=False)
        logger.info(f"Saved to {output}")
    else:
        logger.error("No data available")
    
    logger.info("Done")


if __name__ == "__main__":
    main()