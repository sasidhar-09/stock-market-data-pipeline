"""Stock symbols and configuration"""

INDIAN_STOCKS = [
    'RELIANCE.NS',
    'TCS.NS',
    'HDFCBANK.NS',
    'INFY.NS',
    'ICICIBANK.NS',
    'HINDUNILVR.NS',
    'ITC.NS',
    'SBIN.NS',
    'BHARTIARTL.NS',
    'KOTAKBANK.NS'
]

DATA_CONFIG = {
    'period': '1d',
    'interval': '1d',
    'auto_adjust': True,
    'prepost': False,
    'threads': True,
    'group_by': 'ticker'
}

SCHEMA_CONFIG = {
    'bronze_schema': 'RAW',
    'silver_schema': 'STAGING',
    'gold_schema': 'MARTS',
    'bronze_table': 'STOCK_PRICES_RAW'
}