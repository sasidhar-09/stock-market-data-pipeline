"""Generate mock stock data for testing"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add this line too (might be needed)
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.config.stocks import INDIAN_STOCKS
from src.utils.helpers import add_metadata, clean_columns


def generate_mock_data(symbols, start_date, end_date):
    """Generate realistic stock price data"""
    
    # DEBUG: Print inputs
    print(f"=" * 60)
    print(f"MOCK DATA GENERATION START")
    print(f"Start date: {start_date} (type: {type(start_date)})")
    print(f"End date: {end_date} (type: {type(end_date)})")
    print(f"Symbols: {len(symbols)}")
    print(f"=" * 60)
    
    records = []
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    
    # DEBUG: Check date range
    print(f"Date range has {len(date_range)} business days")
    
    # SAFETY: Stop if too many days
    if len(date_range) > 1000:
        raise ValueError(f"Date range too large: {len(date_range)} days!")
    
    print(f"Will generate {len(symbols)} * {len(date_range)} = {len(symbols) * len(date_range)} records")
    
    for i, symbol in enumerate(symbols):
        base_price = np.random.uniform(100, 5000)
        
        for date in date_range:
            daily_change = np.random.uniform(-0.05, 0.05)
            open_price = base_price * (1 + daily_change)
            
            high = open_price * np.random.uniform(1.0, 1.03)
            low = open_price * np.random.uniform(0.97, 1.0)
            close = np.random.uniform(low, high)
            volume = int(np.random.uniform(100000, 10000000))
            
            records.append({
                'symbol': symbol,
                'date': date,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume
            })
            
            base_price = close
    
    print(f"Creating DataFrame from {len(records)} records...")
    df = pd.DataFrame(records)
    
    print(f"Adding metadata...")
    df = add_metadata(df)
    
    print(f"MOCK DATA GENERATION COMPLETE: {len(df)} rows")
    print(f"=" * 60)
    return df