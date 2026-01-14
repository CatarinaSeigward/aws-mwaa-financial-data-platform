"""
Data Validator Module
=====================
Enterprise data quality validation using Great Expectations.
Implements the DATA QUALITY GATE pattern - blocks bad data from entering the data warehouse.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from pathlib import Path

import pandas as pd
import boto3
from botocore.exceptions import ClientError

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import Checkpoint

import sys
sys.path.insert(0, "/opt/airflow/dags")

from config.settings import get_settings

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    """Raised when data validation fails."""
    pass


class DataValidator:
    """
    Data quality validator using Great Expectations.
    
    This is the DATA QUALITY GATE - it blocks bad data from entering downstream systems.
    
    Features:
    - Schema validation
    - Field-level value checks
    - Cross-field consistency checks
    - Detailed validation reports
    - S3 integration for audit trail
    
    Example:
        >>> validator = DataValidator()
        >>> result = validator.validate_from_s3(
        ...     s3_path="s3://bucket/data.json",
        ...     expectation_suite_name="stock_data_quality_suite"
        ... )
        >>> if not result["success"]:
        ...     raise DataValidationError("Validation failed!")
    """
    
    def __init__(
        self,
        context_root_dir: Optional[str] = None,
        expectations_path: Optional[str] = None,
    ):
        """
        Initialize the data validator.
        
        Args:
            context_root_dir: Great Expectations context root directory
            expectations_path: Path to expectation suite JSON files
        """
        self.settings = get_settings()
        self.s3_client = boto3.client("s3")
        
        # Initialize Great Expectations context
        self.context = gx.get_context()
        
        # Load expectation suites
        self.expectations_path = (
            expectations_path or self.settings.validation.expectations_suite_path
        )
        self._load_expectation_suites()
    
    def _load_expectation_suites(self) -> None:
        """Load expectation suites from JSON configuration."""
        try:
            expectations_dir = Path(self.expectations_path).parent
            
            for suite_file in expectations_dir.glob("*.json"):
                with open(suite_file, "r") as f:
                    suite_config = json.load(f)
                
                suite_name = suite_config.get("expectation_suite_name", suite_file.stem)
                
                # Create or update suite in context
                try:
                    suite = self.context.add_expectation_suite(
                        expectation_suite_name=suite_name,
                    )
                    
                    # Add expectations from config
                    for exp_config in suite_config.get("expectations", []):
                        suite.add_expectation(
                            expectation_type=exp_config["expectation_type"],
                            **exp_config.get("kwargs", {}),
                        )
                    
                    self.context.update_expectation_suite(suite)
                    logger.info(f"Loaded expectation suite: {suite_name}")
                    
                except Exception as e:
                    logger.warning(f"Suite {suite_name} may already exist: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to load expectation suites: {e}")
    
    def _read_s3_data(self, s3_path: str) -> pd.DataFrame:
        """
        Read data from S3 into a pandas DataFrame.
        
        Args:
            s3_path: S3 URI (s3://bucket/key)
            
        Returns:
            DataFrame containing the data
        """
        # Parse S3 path
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path: {s3_path}")
        
        path_parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = path_parts[0]
        key = path_parts[1] if len(path_parts) > 1 else ""
        
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            
            # Parse based on file type
            if s3_path.endswith(".json"):
                data = json.loads(content)
                # Handle our standardized format
                if isinstance(data, dict) and "data" in data:
                    return pd.DataFrame(data["data"])
                elif isinstance(data, list):
                    return pd.DataFrame(data)
                else:
                    return pd.DataFrame([data])
            elif s3_path.endswith(".csv"):
                import io
                return pd.read_csv(io.StringIO(content))
            elif s3_path.endswith(".parquet"):
                import io
                return pd.read_parquet(io.BytesIO(response["Body"].read()))
            else:
                # Try JSON first, then CSV
                try:
                    return pd.DataFrame(json.loads(content))
                except json.JSONDecodeError:
                    import io
                    return pd.read_csv(io.StringIO(content))
                    
        except ClientError as e:
            raise DataValidationError(f"Failed to read S3 data: {e}")
    
    def validate_dataframe(
        self,
        df: pd.DataFrame,
        expectation_suite_name: str,
        run_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate a pandas DataFrame against an expectation suite.
        
        Args:
            df: DataFrame to validate
            expectation_suite_name: Name of the expectation suite
            run_name: Optional run identifier
            
        Returns:
            Validation result dictionary
        """
        run_name = run_name or f"validation_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # Create a batch from the DataFrame
            batch_request = RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="stock_data",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"run_id": run_name},
            )
            
            # Get validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
            )
            
            # Run validation
            results = validator.validate()
            
            return self._format_results(results)
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            # Run manual validation as fallback
            return self._manual_validate(df, expectation_suite_name)
    
    def _manual_validate(
        self,
        df: pd.DataFrame,
        expectation_suite_name: str,
    ) -> Dict[str, Any]:
        """
        Fallback manual validation when GX context isn't available.
        Implements core validation rules programmatically.
        """
        logger.info("Running manual validation (GX context fallback)")
        
        results = {
            "success": True,
            "statistics": {
                "evaluated_expectations": 0,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
            },
            "failed_expectations": [],
            "validation_details": [],
        }
        
        validations = [
            # Required columns check
            self._check_required_columns(df),
            # Null checks for critical fields
            self._check_not_null(df, "symbol"),
            self._check_not_null(df, "timestamp"),
            self._check_not_null(df, "close_price"),
            # Value range checks
            self._check_value_range(df, "high_price", min_val=0),
            self._check_value_range(df, "low_price", min_val=0),
            self._check_value_range(df, "open_price", min_val=0),
            self._check_value_range(df, "close_price", min_val=0),
            self._check_value_range(df, "volume", min_val=0),
            # Logical consistency checks
            self._check_high_greater_than_low(df),
            # Row count check
            self._check_row_count(df, min_count=1, max_count=50000),
        ]
        
        for validation in validations:
            results["statistics"]["evaluated_expectations"] += 1
            results["validation_details"].append(validation)
            
            if validation["success"]:
                results["statistics"]["successful_expectations"] += 1
            else:
                results["statistics"]["unsuccessful_expectations"] += 1
                results["failed_expectations"].append(validation)
                results["success"] = False
        
        return results
    
    def _check_required_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that all required columns are present."""
        required = [
            "symbol", "timestamp", "open_price", "high_price",
            "low_price", "close_price", "volume"
        ]
        missing = [col for col in required if col not in df.columns]
        
        return {
            "expectation_type": "expect_table_columns_to_exist",
            "success": len(missing) == 0,
            "details": {
                "required_columns": required,
                "missing_columns": missing,
            }
        }
    
    def _check_not_null(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """Check that a column has no null values."""
        if column not in df.columns:
            return {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": column,
                "success": False,
                "details": {"error": "Column not found"},
            }
        
        null_count = df[column].isnull().sum()
        total_count = len(df)
        
        return {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": column,
            "success": null_count == 0,
            "details": {
                "null_count": int(null_count),
                "total_count": total_count,
                "null_percentage": round(null_count / total_count * 100, 2) if total_count > 0 else 0,
            }
        }
    
    def _check_value_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        mostly: float = 0.99,
    ) -> Dict[str, Any]:
        """Check that column values are within expected range."""
        if column not in df.columns:
            return {
                "expectation_type": "expect_column_values_to_be_between",
                "column": column,
                "success": False,
                "details": {"error": "Column not found"},
            }
        
        values = df[column].dropna()
        violations = 0
        
        if min_val is not None:
            violations += (values < min_val).sum()
        if max_val is not None:
            violations += (values > max_val).sum()
        
        success_rate = 1 - (violations / len(values)) if len(values) > 0 else 1
        
        return {
            "expectation_type": "expect_column_values_to_be_between",
            "column": column,
            "success": success_rate >= mostly,
            "details": {
                "min_value": min_val,
                "max_value": max_val,
                "violations": int(violations),
                "total_values": len(values),
                "success_rate": round(success_rate, 4),
                "required_success_rate": mostly,
            }
        }
    
    def _check_high_greater_than_low(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check that high price >= low price (OHLC integrity)."""
        if "high_price" not in df.columns or "low_price" not in df.columns:
            return {
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                "success": False,
                "details": {"error": "Required columns not found"},
            }
        
        valid_rows = df.dropna(subset=["high_price", "low_price"])
        violations = (valid_rows["high_price"] < valid_rows["low_price"]).sum()
        
        return {
            "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
            "column_A": "high_price",
            "column_B": "low_price",
            "success": violations == 0,
            "details": {
                "violations": int(violations),
                "total_rows": len(valid_rows),
            }
        }
    
    def _check_row_count(
        self,
        df: pd.DataFrame,
        min_count: int = 1,
        max_count: int = 50000,
    ) -> Dict[str, Any]:
        """Check that row count is within expected range."""
        row_count = len(df)
        
        return {
            "expectation_type": "expect_table_row_count_to_be_between",
            "success": min_count <= row_count <= max_count,
            "details": {
                "row_count": row_count,
                "min_expected": min_count,
                "max_expected": max_count,
            }
        }
    
    def _format_results(self, gx_results: Any) -> Dict[str, Any]:
        """Format Great Expectations results into standard structure."""
        failed = []
        
        for result in gx_results.results:
            if not result.success:
                failed.append({
                    "expectation_type": result.expectation_config.expectation_type,
                    "kwargs": result.expectation_config.kwargs,
                    "details": result.result,
                })
        
        return {
            "success": gx_results.success,
            "statistics": {
                "evaluated_expectations": gx_results.statistics.get("evaluated_expectations", 0),
                "successful_expectations": gx_results.statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": gx_results.statistics.get("unsuccessful_expectations", 0),
            },
            "failed_expectations": failed,
            "run_time": str(gx_results.meta.get("run_id", {}).get("run_time", "")),
        }
    
    def validate_from_s3(
        self,
        s3_path: str,
        expectation_suite_name: str,
        store_results: bool = True,
    ) -> Dict[str, Any]:
        """
        Validate data directly from S3.
        
        Args:
            s3_path: S3 URI to the data file
            expectation_suite_name: Name of the expectation suite
            store_results: Whether to store validation results to S3
            
        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating data from {s3_path}")
        
        # Read data from S3
        df = self._read_s3_data(s3_path)
        logger.info(f"Loaded {len(df)} rows for validation")
        
        # Run validation
        results = self.validate_dataframe(
            df=df,
            expectation_suite_name=expectation_suite_name,
            run_name=s3_path.split("/")[-1],
        )
        
        # Add metadata
        results["source_path"] = s3_path
        results["row_count"] = len(df)
        results["validation_timestamp"] = datetime.utcnow().isoformat()
        
        # Store results if configured
        if store_results and self.settings.validation.store_results:
            self._store_validation_results(s3_path, results)
        
        logger.info(
            f"Validation complete: success={results['success']}, "
            f"passed={results['statistics']['successful_expectations']}/"
            f"{results['statistics']['evaluated_expectations']}"
        )
        
        return results
    
    def _store_validation_results(
        self,
        source_path: str,
        results: Dict[str, Any],
    ) -> None:
        """Store validation results to S3 for audit trail."""
        try:
            # Generate result path
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            source_filename = source_path.split("/")[-1].replace(".", "_")
            
            result_key = (
                f"{self.settings.s3.validation_prefix}/"
                f"{source_filename}/"
                f"validation_{timestamp}.json"
            )
            
            self.s3_client.put_object(
                Bucket=self.settings.s3.validation_bucket,
                Key=result_key,
                Body=json.dumps(results, indent=2, default=str),
                ContentType="application/json",
            )
            
            logger.info(f"Validation results stored: s3://{self.settings.s3.validation_bucket}/{result_key}")
            
        except Exception as e:
            logger.error(f"Failed to store validation results: {e}")


def create_validation_report(results: Dict[str, Any]) -> str:
    """
    Create a human-readable validation report.
    
    Args:
        results: Validation results dictionary
        
    Returns:
        Formatted report string
    """
    report_lines = [
        "=" * 60,
        "DATA VALIDATION REPORT",
        "=" * 60,
        f"Source: {results.get('source_path', 'N/A')}",
        f"Timestamp: {results.get('validation_timestamp', 'N/A')}",
        f"Row Count: {results.get('row_count', 'N/A')}",
        "-" * 60,
        f"Overall Status: {'✓ PASSED' if results['success'] else '✗ FAILED'}",
        "",
        "Statistics:",
        f"  • Total Expectations: {results['statistics']['evaluated_expectations']}",
        f"  • Passed: {results['statistics']['successful_expectations']}",
        f"  • Failed: {results['statistics']['unsuccessful_expectations']}",
    ]
    
    if results["failed_expectations"]:
        report_lines.extend([
            "",
            "-" * 60,
            "FAILED EXPECTATIONS:",
        ])
        
        for i, failure in enumerate(results["failed_expectations"], 1):
            report_lines.extend([
                f"  {i}. {failure.get('expectation_type', 'Unknown')}",
                f"     Details: {json.dumps(failure.get('details', {}), indent=2)}",
            ])
    
    report_lines.append("=" * 60)
    
    return "\n".join(report_lines)


if __name__ == "__main__":
    # Test validation
    import pandas as pd
    
    # Create test data
    test_data = pd.DataFrame({
        "symbol": ["AAPL", "AAPL", "AAPL"],
        "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "open_price": [150.0, 151.0, 152.0],
        "high_price": [155.0, 156.0, 157.0],
        "low_price": [149.0, 150.0, 151.0],
        "close_price": [154.0, 155.0, 156.0],
        "adjusted_close": [154.0, 155.0, 156.0],
        "volume": [1000000, 1100000, 1200000],
        "dividend_amount": [0.0, 0.0, 0.0],
        "split_coefficient": [1.0, 1.0, 1.0],
    })
    
    validator = DataValidator()
    results = validator.validate_dataframe(test_data, "stock_data_quality_suite")
    print(create_validation_report(results))
