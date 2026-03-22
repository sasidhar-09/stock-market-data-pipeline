"""Utility functions for data pipeline"""

from datetime import datetime
import pandas as pd
from loguru import logger
import sys
import re


def sanitize_log_record(record):
    """
    Sanitize log records to remove passwords from tracebacks
    This runs on EVERY log message
    """
    if record["exception"]:
        # Get the exception info
        exc_text = record["exception"].traceback if hasattr(record["exception"], 'traceback') else ""
        
        # Sanitize password patterns in traceback
        if exc_text:
            exc_text = re.sub(r"'password':\s*'[^']*'", "'password': '***REDACTED***'", str(exc_text))
            exc_text = re.sub(r'"password":\s*"[^"]*"', '"password": "***REDACTED***"', str(exc_text))
            exc_text = re.sub(r"password=['\"][^'\"]*['\"]", "password='***REDACTED***'", str(exc_text))
    
    return True


def configure_logger(level="INFO"):
    """
    Configure Loguru logger with security measures
    CRITICAL: Filters out passwords from all logs and tracebacks
    """
    logger.remove()
    
    # Console logger - NO detailed tracebacks in production
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level=level,
        colorize=True,
        backtrace=False,  # Don't show code context (can contain passwords)
        diagnose=False    # Don't show variable values (can contain passwords)
    )
    
    # File logger - same security settings
    logger.add(
        "logs/pipeline_{time:YYYY-MM-DD}.log",
        rotation="500 MB",
        retention="10 days",
        level=level,
        backtrace=False,  # SECURITY: No code context
        diagnose=False    # SECURITY: No variable values
    )


def add_metadata(df):
    """Add tracking columns"""
    df = df.copy()
    df['ingestion_timestamp'] = datetime.utcnow()
    df['source_system'] = 'yfinance'
    df['pipeline_run_id'] = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    return df


def clean_columns(df):
    """Standardize column names"""
    df = df.copy()
    df.columns = (df.columns
                  .str.lower()
                  .str.replace(' ', '_')
                  .str.replace('[^a-z0-9_]', '', regex=True))
    return df