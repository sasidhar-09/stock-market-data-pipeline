"""
Snowflake data loader with production-grade security
Loads data using COPY INTO for bulk operations
"""

import os
import snowflake.connector
from contextlib import contextmanager
from dotenv import load_dotenv
from loguru import logger
import pandas as pd
import tempfile
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.utils.security import sanitize_snowflake_error

load_dotenv()


class SnowflakeConfig:
    """Snowflake connection configuration from environment variables"""
    
    ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    USER = os.getenv('SNOWFLAKE_USER')
    PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'RAW')
    ROLE = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    @classmethod
    def validate(cls):
        """Validate all required configuration is present"""
        required = ['ACCOUNT', 'USER', 'PASSWORD', 'DATABASE', 'WAREHOUSE']
        missing = [key for key in required if not getattr(cls, key)]
        
        if missing:
            raise ValueError(f"Missing Snowflake configuration: {missing}")


@contextmanager
def get_snowflake_connection():
    """
    Context manager for Snowflake connections
    Automatically handles connection cleanup
    """
    SnowflakeConfig.validate()
    
    conn = None
    
    logger.info(f"Connecting to Snowflake: {SnowflakeConfig.ACCOUNT}")
    logger.info(f"User: {SnowflakeConfig.USER}, Database: {SnowflakeConfig.DATABASE}")
    
    try:
        conn = snowflake.connector.connect(
            user=SnowflakeConfig.USER,
            password=SnowflakeConfig.PASSWORD,
            account=SnowflakeConfig.ACCOUNT,
            warehouse=SnowflakeConfig.WAREHOUSE,
            database=SnowflakeConfig.DATABASE,
            schema=SnowflakeConfig.SCHEMA,
            role=SnowflakeConfig.ROLE
        )
        
        logger.info("Connection established successfully")
        yield conn
        
    except Exception as e:
        safe_error = sanitize_snowflake_error(e)
        logger.error(f"Connection failed: {safe_error}")
        raise
        
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Connection closed")
            except:
                pass


class SnowflakeLoader:
    """
    Bulk load data into Snowflake tables using COPY INTO
    """
    
    def __init__(self, table_name: str, schema: str = 'RAW'):
        self.table_name = table_name
        self.schema = schema
        self.full_table_name = f"{schema}.{table_name}"
        logger.info(f"Initialized SnowflakeLoader for {self.full_table_name}")
    
    def load_dataframe(self, df: pd.DataFrame, mode: str = 'append') -> int:
        """Load pandas DataFrame into Snowflake table"""
        if df.empty:
            logger.warning("Empty DataFrame - nothing to load")
            return 0
        
        logger.info(f"Preparing to load {len(df)} rows to {self.full_table_name}")
        
        # Validate data quality
        self._validate_dataframe(df)
        
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Verify table exists
                self._verify_table_exists(cursor)
                
                # Create temporary CSV file
                tmp_path = self._write_temp_csv(df)
                
                # Extract just the filename (not full path)
                import os
                tmp_filename = os.path.basename(tmp_path)
                
                # Upload to Snowflake internal stage
                stage_name = f"@%{self.table_name}"
                self._upload_to_stage(cursor, tmp_path, stage_name)
                
                # Truncate table if replace mode
                if mode == 'replace':
                    self._truncate_table(cursor)
                
                # Load data from stage to table
                rows_loaded = self._copy_into_table(cursor, tmp_filename)
                
                # Cleanup temporary file
                self._cleanup_temp_file(tmp_path)
                
                logger.info(f"Successfully loaded {rows_loaded} rows")
                return rows_loaded 
            
            except Exception as e:
                safe_error = sanitize_snowflake_error(e)
                logger.error(f"Load failed: {safe_error}")
                raise

            finally:
                cursor.close()
    

    def _verify_table_exists(self, cursor):
        """Verify target table exists"""
        try:
            cursor.execute(f"SELECT 1 FROM {self.full_table_name} LIMIT 1")
            logger.debug(f"Table {self.full_table_name} verified")
        except Exception as e:
            raise ValueError(
                f"Table {self.full_table_name} does not exist. "
                f"Create it first in Snowflake."
            )
    
    def _write_temp_csv(self, df: pd.DataFrame) -> str:
        """Write DataFrame to temporary CSV file"""
        tmp_file = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.csv',
            delete=False,
            newline=''
        )
        
        tmp_path = tmp_file.name
        df.to_csv(tmp_path, index=False, header=True)
        tmp_file.close()
        
        logger.debug(f"Created temp file: {tmp_path}")
        return tmp_path
    
    def _upload_to_stage(self, cursor, tmp_path: str, stage_name: str):
        """Upload CSV file to Snowflake internal stage"""
        put_query = f"""
            PUT file://{tmp_path} {stage_name}
            AUTO_COMPRESS=TRUE
            OVERWRITE=TRUE
        """
        
        logger.info(f"Uploading to stage {stage_name}")
        cursor.execute(put_query)
    
    def _truncate_table(self, cursor):
        """Truncate table (for replace mode)"""
        logger.info(f"Truncating table {self.full_table_name}")
        cursor.execute(f"TRUNCATE TABLE {self.full_table_name}")
    

    def _copy_into_table(self, cursor, temp_filename):
        """Execute COPY INTO command"""
        logger.info("Executing COPY INTO command")
        
        # Snowflake adds .gz extension during PUT with AUTO_COMPRESS=TRUE
        copy_sql = f"""
        COPY INTO {self.full_table_name}
        FROM @%{self.table_name}/{temp_filename}.gz
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('NULL', 'null', '')
        )
        PURGE = TRUE
        """
        
        try:
            result = cursor.execute(copy_sql).fetchone()
            
            # DEBUG: Log the actual result to see format
            logger.info(f"COPY INTO result: {result}")
            logger.info(f"Result type: {type(result)}, Length: {len(result) if result else 0}")
            if result:
                for i, val in enumerate(result):
                    logger.info(f"  result[{i}] = {val!r} (type: {type(val).__name__})")
            
            # Snowflake COPY INTO returns: (file, rows_loaded, rows_parsed, first_error, ...)
            if result and len(result) >= 2:
                # Try to parse the row count
                row_count_str = str(result[1])  # Convert to string first
                
                # If it says "LOADED", try other columns
                if row_count_str == "LOADED":
                    logger.warning(f"result[1] is 'LOADED', checking other columns")
                    # Maybe rows_loaded is in a different position
                    for i, val in enumerate(result):
                        try:
                            rows = int(val)
                            logger.info(f"Found numeric value {rows} at index {i}")
                            return rows
                        except (ValueError, TypeError):
                            continue
                    
                    logger.warning("Could not find row count in result, returning 0")
                    return 0
                else:
                    rows_loaded = int(row_count_str)
                    logger.info(f"Successfully loaded {rows_loaded} rows")
                    return rows_loaded
            else:
                logger.warning(f"COPY INTO returned unexpected result: {result}")
                return 0
                
        except Exception as e:
            logger.error(f"COPY INTO failed: {e}")
            raise
    

    def _cleanup_temp_file(self, tmp_path: str):
        """Delete temporary CSV file"""
        try:
            os.remove(tmp_path)
            logger.debug("Temp file cleaned up")
        except:
            logger.warning(f"Could not delete temp file: {tmp_path}")
    
    def _validate_dataframe(self, df: pd.DataFrame):
        """Validate DataFrame has required columns and data quality"""
        
        # Check required columns
        required_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        missing = set(required_cols) - set(df.columns)
        
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Check for NULL values in critical columns
        for col in ['symbol', 'date', 'close']:
            null_count = df[col].isna().sum()
            if null_count > 0:
                raise ValueError(f"Found {null_count} NULL values in column '{col}'")
        
        # Validate OHLC relationship
        invalid_ohlc = df[df['low'] > df['high']]
        if not invalid_ohlc.empty:
            raise ValueError(
                f"Invalid OHLC data: {len(invalid_ohlc)} rows have low > high"
            )
        
        logger.info("Data validation passed")
    
    def get_row_count(self) -> int:
        """Get current row count in table"""
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.full_table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
    
    def get_sample_data(self, limit: int = 5) -> pd.DataFrame:
        """Get sample rows from table"""
        with get_snowflake_connection() as conn:
            query = f"SELECT * FROM {self.full_table_name} LIMIT {limit}"
            df = pd.read_sql(query, conn)
            return df


def load_to_snowflake(
    df: pd.DataFrame,
    table_name: str,
    schema: str = 'RAW',
    mode: str = 'append'
) -> int:
    """
    Convenience function to load DataFrame to Snowflake
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        schema: Target schema (default: RAW)
        mode: 'append' or 'replace'
    
    Returns:
        Number of rows loaded
    """
    loader = SnowflakeLoader(table_name, schema)
    return loader.load_dataframe(df, mode)


if __name__ == "__main__":
    # Test the loader
    from src.utils.helpers import configure_logger
    
    configure_logger(level='INFO')
    
    logger.info("Testing Snowflake loader")
    
    # Create test data
    test_data = pd.DataFrame({
        'symbol': ['TEST.NS'] * 3,
        'date': pd.date_range('2026-02-01', periods=3),
        'open': [100.0, 101.0, 102.0],
        'high': [105.0, 106.0, 107.0],
        'low': [99.0, 100.0, 101.0],
        'close': [103.0, 104.0, 105.0],
        'volume': [1000000, 1100000, 1200000],
        'ingestion_timestamp': pd.Timestamp.now(),
        'source_system': 'test',
        'pipeline_run_id': 'test_run_001'
    })
    
    try:
        rows = load_to_snowflake(test_data, 'STOCK_PRICES_RAW', mode='append')
        logger.info(f"Test complete - loaded {rows} rows")
    except Exception as e:
        logger.error(f"Test failed: {e}")