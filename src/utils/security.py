"""
Security utilities for sanitizing logs and errors
Prevents credential leakage in error messages and stack traces
"""

import re
from typing import Any


def sanitize_snowflake_error(error: Exception) -> str:
    """
    Sanitize Snowflake errors to remove credentials
    
    Snowflake connector can leak credentials in:
    - Connection strings
    - URL parameters
    - Error messages
    
    Args:
        error: Exception from Snowflake connector
    
    Returns:
        Sanitized error message safe for logging
    """
    error_str = str(error)
    
    # Remove password from connection strings
    # Pattern: password='...' or password="..."
    error_str = re.sub(
        r"password=['\"]([^'\"]+)['\"]",
        "password='***REDACTED***'",
        error_str,
        flags=re.IGNORECASE
    )
    
    # Remove password from URLs
    # Pattern: &password=xxx&
    error_str = re.sub(
        r"&password=[^&]+&",
        "&password=***REDACTED***&",
        error_str,
        flags=re.IGNORECASE
    )
    
    # Remove password from query parameters
    # Pattern: ?password=xxx or ?...&password=xxx
    error_str = re.sub(
        r"[?&]password=[^&\s]+",
        "?password=***REDACTED***",
        error_str,
        flags=re.IGNORECASE
    )
    
    # Remove entire query strings from URLs (they may contain tokens)
    # Pattern: https://...?param=value&...
    error_str = re.sub(
        r"(https?://[^\s?]+)\?[^\s]+",
        r"\1?***REDACTED***",
        error_str
    )
    
    # Remove authentication tokens if present
    error_str = re.sub(
        r"token=['\"]?[^'\"&\s]+['\"]?",
        "token='***REDACTED***'",
        error_str,
        flags=re.IGNORECASE
    )
    
    return error_str


def sanitize_dict(data: dict) -> dict:
    """
    Sanitize dictionary by redacting sensitive keys
    
    Args:
        data: Dictionary that may contain credentials
    
    Returns:
        Dictionary with sensitive values redacted
    """
    sensitive_keys = {
        'password', 'passwd', 'pwd',
        'token', 'api_key', 'apikey', 'secret',
        'authorization', 'auth',
        'private_key', 'privatekey'
    }
    
    sanitized = {}
    for key, value in data.items():
        if key.lower() in sensitive_keys:
            sanitized[key] = '***REDACTED***'
        elif isinstance(value, dict):
            sanitized[key] = sanitize_dict(value)
        else:
            sanitized[key] = value
    
    return sanitized


def sanitize_connection_params(params: dict) -> dict:
    """
    Sanitize Snowflake connection parameters for safe logging
    
    Args:
        params: Connection parameters dictionary
    
    Returns:
        Safe-to-log version with credentials removed
    """
    safe_params = params.copy()
    
    # Redact sensitive fields
    if 'password' in safe_params:
        safe_params['password'] = '***REDACTED***'
    if 'private_key' in safe_params:
        safe_params['private_key'] = '***REDACTED***'
    if 'token' in safe_params:
        safe_params['token'] = '***REDACTED***'
    
    return safe_params