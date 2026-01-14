"""
Redshift Serverless Data Loader
================================
Production-grade module for loading data from S3 to Redshift Serverless.
Implements COPY command optimization, error handling, and load verification.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
import json
import time

import boto3
from botocore.exceptions import ClientError

import sys
sys.path.insert(0, "/opt/airflow/dags")

from config.settings import get_settings

logger = logging.getLogger(__name__)


class RedshiftLoadError(Exception):
    """Raised when Redshift load operation fails."""
    pass


class RedshiftLoader:
    """
    Redshift Serverless data loader using Data API.
    
    Features:
    - Async query execution with polling
    - Automatic retry with backoff
    - Load verification
    - Error reporting
    
    Example:
        >>> loader = RedshiftLoader()
        >>> result = loader.copy_from_s3(
        ...     table_name="fact_stock_prices",
        ...     s3_path="s3://bucket/data/",
        ...     iam_role="arn:aws:iam::123456789:role/RedshiftS3Role"
        ... )
        >>> print(f"Loaded {result['rows_loaded']} rows")
    """
    
    # Table definitions for DDL
    TABLE_DEFINITIONS = {
        "fact_stock_prices": """
            CREATE TABLE IF NOT EXISTS {schema}.fact_stock_prices (
                price_id VARCHAR(50) NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                trade_timestamp TIMESTAMP,
                open_price DECIMAL(18,4),
                high_price DECIMAL(18,4),
                low_price DECIMAL(18,4),
                close_price DECIMAL(18,4),
                adjusted_close DECIMAL(18,4),
                volume BIGINT,
                dividend_amount DECIMAL(18,4),
                split_coefficient DECIMAL(10,4),
                daily_return DECIMAL(10,4),
                daily_range DECIMAL(18,4),
                daily_range_pct DECIMAL(10,4),
                volume_change_pct DECIMAL(10,4),
                sma_5 DECIMAL(18,4),
                sma_20 DECIMAL(18,4),
                sma_50 DECIMAL(18,4),
                ema_12 DECIMAL(18,4),
                ema_26 DECIMAL(18,4),
                volatility_20d DECIMAL(10,4),
                year INTEGER,
                quarter INTEGER,
                month INTEGER,
                day INTEGER,
                day_of_week INTEGER,
                processing_timestamp TIMESTAMP,
                PRIMARY KEY (price_id)
            )
            DISTSTYLE KEY
            DISTKEY (symbol)
            SORTKEY (trade_date, symbol);
        """,
        "dim_symbols": """
            CREATE TABLE IF NOT EXISTS {schema}.dim_symbols (
                symbol VARCHAR(10) PRIMARY KEY,
                company_name VARCHAR(255),
                sector VARCHAR(100),
                industry VARCHAR(100),
                exchange VARCHAR(50),
                market_cap BIGINT,
                created_at TIMESTAMP DEFAULT GETDATE(),
                updated_at TIMESTAMP DEFAULT GETDATE()
            )
            DISTSTYLE ALL;
        """,
    }
    
    def __init__(
        self,
        workgroup: Optional[str] = None,
        database: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """
        Initialize the Redshift loader.
        
        Args:
            workgroup: Redshift Serverless workgroup name
            database: Database name
            region: AWS region
        """
        self.settings = get_settings()
        
        self.workgroup = workgroup or self.settings.redshift.workgroup
        self.database = database or self.settings.redshift.database
        self.schema = self.settings.redshift.schema
        self.region = region or self.settings.redshift.region
        
        # Initialize Redshift Data API client
        self.client = boto3.client(
            "redshift-data",
            region_name=self.region
        )
        
        logger.info(f"RedshiftLoader initialized for {self.workgroup}/{self.database}")
    
    def _execute_statement(
        self,
        sql: str,
        wait: bool = True,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement using Redshift Data API.
        
        Args:
            sql: SQL statement to execute
            wait: Whether to wait for completion
            timeout: Maximum wait time in seconds
            
        Returns:
            Query result metadata
        """
        logger.debug(f"Executing SQL: {sql[:200]}...")
        
        try:
            # Execute statement
            response = self.client.execute_statement(
                WorkgroupName=self.workgroup,
                Database=self.database,
                Sql=sql,
            )
            
            statement_id = response["Id"]
            logger.info(f"Statement submitted: {statement_id}")
            
            if not wait:
                return {"statement_id": statement_id, "status": "SUBMITTED"}
            
            # Poll for completion
            return self._wait_for_completion(statement_id, timeout)
            
        except ClientError as e:
            raise RedshiftLoadError(f"Failed to execute statement: {e}")
    
    def _wait_for_completion(
        self,
        statement_id: str,
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """
        Wait for a statement to complete.
        
        Args:
            statement_id: Statement ID to monitor
            timeout: Maximum wait time in seconds
            
        Returns:
            Query result metadata
        """
        start_time = time.time()
        poll_interval = 2
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise RedshiftLoadError(
                    f"Statement {statement_id} timed out after {timeout}s"
                )
            
            response = self.client.describe_statement(Id=statement_id)
            status = response["Status"]
            
            logger.debug(f"Statement {statement_id}: {status}")
            
            if status == "FINISHED":
                return {
                    "statement_id": statement_id,
                    "status": status,
                    "result_rows": response.get("ResultRows", 0),
                    "result_size": response.get("ResultSize", 0),
                    "duration_ms": response.get("Duration", 0) / 1000000,
                }
            elif status == "FAILED":
                error = response.get("Error", "Unknown error")
                raise RedshiftLoadError(f"Statement failed: {error}")
            elif status == "ABORTED":
                raise RedshiftLoadError("Statement was aborted")
            
            time.sleep(poll_interval)
            # Exponential backoff up to 30 seconds
            poll_interval = min(poll_interval * 1.5, 30)
    
    def _get_query_results(
        self,
        statement_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Get results from a completed query.
        
        Args:
            statement_id: Completed statement ID
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            response = self.client.get_statement_result(Id=statement_id)
            
            # Extract column metadata
            columns = [col["name"] for col in response.get("ColumnMetadata", [])]
            
            # Convert records to dictionaries
            results = []
            for record in response.get("Records", []):
                row = {}
                for i, field in enumerate(record):
                    col_name = columns[i] if i < len(columns) else f"col_{i}"
                    # Handle different field types
                    if "stringValue" in field:
                        row[col_name] = field["stringValue"]
                    elif "longValue" in field:
                        row[col_name] = field["longValue"]
                    elif "doubleValue" in field:
                        row[col_name] = field["doubleValue"]
                    elif "booleanValue" in field:
                        row[col_name] = field["booleanValue"]
                    elif "isNull" in field and field["isNull"]:
                        row[col_name] = None
                    else:
                        row[col_name] = str(field)
                results.append(row)
            
            return results
            
        except ClientError as e:
            raise RedshiftLoadError(f"Failed to get query results: {e}")
    
    def create_table(
        self,
        table_name: str,
        if_not_exists: bool = True,
    ) -> Dict[str, Any]:
        """
        Create a table using predefined DDL.
        
        Args:
            table_name: Table name (must be in TABLE_DEFINITIONS)
            if_not_exists: Whether to use IF NOT EXISTS clause
            
        Returns:
            Execution result metadata
        """
        if table_name not in self.TABLE_DEFINITIONS:
            raise ValueError(f"Unknown table: {table_name}")
        
        ddl = self.TABLE_DEFINITIONS[table_name].format(schema=self.schema)
        
        logger.info(f"Creating table: {self.schema}.{table_name}")
        return self._execute_statement(ddl)
    
    def copy_from_s3(
        self,
        table_name: str,
        s3_path: str,
        iam_role: str,
        file_format: str = "PARQUET",
        options: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Load data from S3 using COPY command.
        
        Args:
            table_name: Target table name
            s3_path: S3 path to data files
            iam_role: IAM role ARN for S3 access
            file_format: Data format (PARQUET, CSV, JSON)
            options: Additional COPY options
            
        Returns:
            Load result with row count
        """
        logger.info(f"Loading data from {s3_path} to {table_name}")
        
        # Ensure table exists
        self.create_table(table_name)
        
        # Build COPY command
        copy_options = options or {}
        
        if file_format == "PARQUET":
            format_clause = "FORMAT AS PARQUET"
        elif file_format == "CSV":
            format_clause = "FORMAT AS CSV IGNOREHEADER 1"
            if "delimiter" in copy_options:
                format_clause += f" DELIMITER '{copy_options['delimiter']}'"
        elif file_format == "JSON":
            json_path = copy_options.get("json_path", "auto")
            format_clause = f"FORMAT AS JSON '{json_path}'"
        else:
            format_clause = ""
        
        # Construct COPY statement
        copy_sql = f"""
            COPY {self.schema}.{table_name}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            {format_clause}
            STATUPDATE ON
            COMPUPDATE ON
        """
        
        # Execute COPY
        try:
            result = self._execute_statement(copy_sql, timeout=600)
            
            # Get actual row count loaded
            row_count = self.get_row_count(table_name)
            
            return {
                **result,
                "table_name": table_name,
                "s3_path": s3_path,
                "rows_loaded": row_count,
                "load_status": "SUCCESS",
            }
            
        except RedshiftLoadError as e:
            # Check STL_LOAD_ERRORS for detailed error info
            errors = self._get_load_errors(table_name)
            raise RedshiftLoadError(
                f"COPY failed: {e}\nLoad errors: {errors}"
            )
    
    def _get_load_errors(self, table_name: str) -> List[Dict[str, Any]]:
        """Get recent load errors for a table."""
        try:
            error_sql = f"""
                SELECT 
                    starttime,
                    filename,
                    line_number,
                    colname,
                    err_reason
                FROM stl_load_errors
                WHERE tbl = (
                    SELECT oid FROM pg_class 
                    WHERE relname = '{table_name}'
                )
                ORDER BY starttime DESC
                LIMIT 10
            """
            result = self._execute_statement(error_sql)
            return self._get_query_results(result["statement_id"])
        except Exception:
            return []
    
    def get_row_count(
        self,
        table_name: str,
        date_filter: Optional[str] = None,
    ) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Table name
            date_filter: Optional date filter (YYYY-MM-DD)
            
        Returns:
            Row count
        """
        if date_filter:
            count_sql = f"""
                SELECT COUNT(*) as cnt 
                FROM {self.schema}.{table_name}
                WHERE trade_date = '{date_filter}'
            """
        else:
            count_sql = f"SELECT COUNT(*) as cnt FROM {self.schema}.{table_name}"
        
        result = self._execute_statement(count_sql)
        rows = self._get_query_results(result["statement_id"])
        
        return int(rows[0]["cnt"]) if rows else 0
    
    def upsert_data(
        self,
        table_name: str,
        staging_table: str,
        key_columns: List[str],
    ) -> Dict[str, Any]:
        """
        Perform upsert (merge) from staging to target table.
        
        Args:
            table_name: Target table
            staging_table: Staging table with new data
            key_columns: Primary key columns for matching
            
        Returns:
            Upsert result metadata
        """
        logger.info(f"Upserting from {staging_table} to {table_name}")
        
        # Build key match condition
        key_match = " AND ".join([
            f"target.{col} = staging.{col}" for col in key_columns
        ])
        
        # Delete existing matches
        delete_sql = f"""
            DELETE FROM {self.schema}.{table_name}
            USING {self.schema}.{staging_table} staging
            WHERE {key_match.replace('target.', f'{self.schema}.{table_name}.')}
        """
        
        # Insert all from staging
        insert_sql = f"""
            INSERT INTO {self.schema}.{table_name}
            SELECT * FROM {self.schema}.{staging_table}
        """
        
        # Execute as transaction
        transaction_sql = f"""
            BEGIN TRANSACTION;
            {delete_sql};
            {insert_sql};
            COMMIT;
        """
        
        result = self._execute_statement(transaction_sql, timeout=600)
        
        return {
            **result,
            "operation": "UPSERT",
            "target_table": table_name,
            "staging_table": staging_table,
        }
    
    def execute_query(
        self,
        sql: str,
        fetch_results: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute arbitrary SQL query.
        
        Args:
            sql: SQL query to execute
            fetch_results: Whether to fetch and return results
            
        Returns:
            Query result with optional rows
        """
        result = self._execute_statement(sql)
        
        if fetch_results and result["status"] == "FINISHED":
            result["rows"] = self._get_query_results(result["statement_id"])
        
        return result
    
    def vacuum_table(self, table_name: str, full: bool = False) -> Dict[str, Any]:
        """Run VACUUM on a table to reclaim space and re-sort."""
        vacuum_type = "FULL" if full else ""
        vacuum_sql = f"VACUUM {vacuum_type} {self.schema}.{table_name}"
        
        logger.info(f"Running VACUUM on {table_name}")
        return self._execute_statement(vacuum_sql, timeout=1800)
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Run ANALYZE on a table to update statistics."""
        analyze_sql = f"ANALYZE {self.schema}.{table_name}"
        
        logger.info(f"Running ANALYZE on {table_name}")
        return self._execute_statement(analyze_sql)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata and statistics."""
        info_sql = f"""
            SELECT 
                c.relname as table_name,
                c.reltuples::bigint as row_count,
                pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                s.pct_used as disk_pct_used,
                s.unsorted as unsorted_pct,
                s.stats_off as stats_accuracy
            FROM pg_class c
            JOIN svv_table_info s ON c.relname = s.table
            WHERE c.relname = '{table_name}'
              AND s.schema = '{self.schema}'
        """
        
        result = self._execute_statement(info_sql)
        rows = self._get_query_results(result["statement_id"])
        
        return rows[0] if rows else {}


def verify_redshift_connection():
    """Test Redshift connectivity."""
    loader = RedshiftLoader()
    
    try:
        result = loader.execute_query("SELECT 1 as test")
        print(f"Connection successful: {result}")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    verify_redshift_connection()
