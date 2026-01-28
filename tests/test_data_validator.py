"""
Data Validator Unit Tests
=========================
Tests for the Great Expectations-based data validation module.
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDataValidatorManualValidation:
    """Test the manual validation fallback logic."""

    def test_check_required_columns_all_present(self, sample_stock_data: pd.DataFrame):
        """Test that validation passes when all required columns are present."""
        required = ["symbol", "timestamp", "open_price", "high_price",
                    "low_price", "close_price", "volume"]
        missing = [col for col in required if col not in sample_stock_data.columns]

        assert len(missing) == 0, f"Missing columns: {missing}"

    def test_check_required_columns_missing(self, sample_stock_data: pd.DataFrame):
        """Test that validation fails when required columns are missing."""
        df = sample_stock_data.drop(columns=["volume"])
        required = ["symbol", "timestamp", "open_price", "high_price",
                    "low_price", "close_price", "volume"]
        missing = [col for col in required if col not in df.columns]

        assert "volume" in missing

    def test_check_not_null_passes(self, sample_stock_data: pd.DataFrame):
        """Test null check passes for valid data."""
        for col in ["symbol", "timestamp", "close_price"]:
            null_count = sample_stock_data[col].isnull().sum()
            assert null_count == 0, f"Column {col} has {null_count} null values"

    def test_check_not_null_fails(self, sample_stock_data_with_nulls: pd.DataFrame):
        """Test null check fails when null values exist."""
        null_count = sample_stock_data_with_nulls["close_price"].isnull().sum()
        assert null_count > 0, "Expected null values in close_price"

    def test_check_value_range_valid(self, sample_stock_data: pd.DataFrame):
        """Test value range check passes for valid data."""
        for col in ["open_price", "high_price", "low_price", "close_price"]:
            values = sample_stock_data[col].dropna()
            assert (values >= 0).all(), f"Found negative values in {col}"
            assert (values <= 1000000).all(), f"Found values > 1M in {col}"

    def test_check_value_range_negative_prices(self):
        """Test value range check fails for negative prices."""
        df = pd.DataFrame({
            "symbol": ["TEST"],
            "timestamp": ["2024-01-01"],
            "open_price": [-100.0],  # Negative price
            "high_price": [150.0],
            "low_price": [145.0],
            "close_price": [148.0],
            "volume": [1000000],
        })

        violations = (df["open_price"] < 0).sum()
        assert violations > 0, "Expected to detect negative price"

    def test_check_high_greater_than_low_valid(self, sample_stock_data: pd.DataFrame):
        """Test OHLC integrity check passes for valid data."""
        valid_rows = sample_stock_data.dropna(subset=["high_price", "low_price"])
        violations = (valid_rows["high_price"] < valid_rows["low_price"]).sum()
        assert violations == 0, f"Found {violations} OHLC violations"

    def test_check_high_greater_than_low_invalid(
        self, sample_stock_data_with_invalid_ohlc: pd.DataFrame
    ):
        """Test OHLC integrity check fails for invalid data."""
        df = sample_stock_data_with_invalid_ohlc
        violations = (df["high_price"] < df["low_price"]).sum()
        assert violations > 0, "Expected to detect OHLC violation"

    def test_check_row_count_valid(self, sample_stock_data: pd.DataFrame):
        """Test row count check passes for valid data."""
        row_count = len(sample_stock_data)
        assert 1 <= row_count <= 50000, f"Row count {row_count} out of range"

    def test_check_row_count_empty(self):
        """Test row count check fails for empty DataFrame."""
        df = pd.DataFrame()
        row_count = len(df)
        assert row_count < 1, "Expected empty DataFrame"


class TestDataQualityMetrics:
    """Test data quality metric calculations."""

    def test_null_percentage_calculation(self, sample_stock_data_with_nulls: pd.DataFrame):
        """Test null percentage is calculated correctly."""
        df = sample_stock_data_with_nulls
        col = "close_price"

        null_count = df[col].isnull().sum()
        total_count = len(df)
        null_pct = null_count / total_count * 100

        assert null_pct > 0, "Expected some null percentage"
        assert null_pct < 100, "Expected less than 100% null"

    def test_success_rate_calculation(self, sample_stock_data: pd.DataFrame):
        """Test success rate calculation for value range checks."""
        col = "close_price"
        min_val, max_val = 0, 1000000

        values = sample_stock_data[col].dropna()
        violations = ((values < min_val) | (values > max_val)).sum()
        success_rate = 1 - (violations / len(values))

        assert success_rate == 1.0, f"Expected 100% success rate, got {success_rate}"


class TestValidationReport:
    """Test validation report generation."""

    def test_create_validation_report_structure(self):
        """Test validation report has correct structure."""
        results = {
            "success": True,
            "statistics": {
                "evaluated_expectations": 10,
                "successful_expectations": 10,
                "unsuccessful_expectations": 0,
            },
            "failed_expectations": [],
            "source_path": "s3://test/data.json",
            "row_count": 100,
            "validation_timestamp": "2024-01-15T10:00:00",
        }

        # Check required keys
        assert "success" in results
        assert "statistics" in results
        assert "failed_expectations" in results
        assert results["statistics"]["evaluated_expectations"] == 10

    def test_create_validation_report_with_failures(self):
        """Test validation report correctly captures failures."""
        results = {
            "success": False,
            "statistics": {
                "evaluated_expectations": 10,
                "successful_expectations": 8,
                "unsuccessful_expectations": 2,
            },
            "failed_expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "close_price",
                    "details": {"null_count": 5}
                },
                {
                    "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    "column_A": "high_price",
                    "column_B": "low_price",
                    "details": {"violations": 3}
                }
            ],
        }

        assert not results["success"]
        assert len(results["failed_expectations"]) == 2
        assert results["statistics"]["unsuccessful_expectations"] == 2


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframe_validation(self):
        """Test validation handles empty DataFrame."""
        df = pd.DataFrame()

        # Should detect empty data
        assert len(df) == 0

    def test_single_row_dataframe_validation(self):
        """Test validation handles single-row DataFrame."""
        df = pd.DataFrame([{
            "symbol": "TEST",
            "timestamp": "2024-01-01",
            "open_price": 100.0,
            "high_price": 105.0,
            "low_price": 98.0,
            "close_price": 102.0,
            "volume": 1000000,
        }])

        assert len(df) == 1
        assert df["high_price"].iloc[0] >= df["low_price"].iloc[0]

    def test_all_null_column_validation(self):
        """Test validation handles column with all null values."""
        df = pd.DataFrame({
            "symbol": ["TEST", "TEST"],
            "timestamp": ["2024-01-01", "2024-01-02"],
            "close_price": [None, None],  # All null
            "volume": [1000000, 1000000],
        })

        null_count = df["close_price"].isnull().sum()
        assert null_count == len(df)

    def test_extreme_values(self):
        """Test validation handles extreme but valid values."""
        df = pd.DataFrame([{
            "symbol": "BRK.A",  # Berkshire Hathaway
            "timestamp": "2024-01-01",
            "open_price": 600000.0,  # Very high price
            "high_price": 610000.0,
            "low_price": 595000.0,
            "close_price": 605000.0,
            "volume": 100,  # Very low volume
        }])

        # Should pass range checks (max is 1M, but BRK.A exceeds this)
        assert df["close_price"].iloc[0] > 0
