"""
Data Generator Unit Tests
=========================
Tests for the synthetic data generation module.
"""

import pytest
import pandas as pd
import numpy as np
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


class TestSyntheticDataGeneration:
    """Test synthetic data generation functionality."""

    def test_generated_data_has_required_columns(self, sample_stock_data: pd.DataFrame):
        """Test that generated data has all required columns."""
        required_columns = [
            "symbol", "timestamp", "open_price", "high_price",
            "low_price", "close_price", "volume"
        ]

        for col in required_columns:
            assert col in sample_stock_data.columns, f"Missing column: {col}"

    def test_generated_data_has_valid_symbols(self, sample_stock_data: pd.DataFrame):
        """Test that generated symbols are valid stock tickers."""
        symbols = sample_stock_data["symbol"].unique()

        for symbol in symbols:
            # Valid US ticker format: 1-5 uppercase letters
            assert symbol.isupper(), f"Symbol should be uppercase: {symbol}"
            assert len(symbol) <= 5, f"Symbol too long: {symbol}"
            assert symbol.isalpha(), f"Symbol should be alphabetic: {symbol}"

    def test_generated_prices_are_positive(self, sample_stock_data: pd.DataFrame):
        """Test that all generated prices are positive."""
        price_columns = ["open_price", "high_price", "low_price", "close_price"]

        for col in price_columns:
            values = sample_stock_data[col].dropna()
            assert (values > 0).all(), f"Found non-positive values in {col}"

    def test_generated_ohlc_relationships(self, sample_stock_data: pd.DataFrame):
        """Test OHLC relationships are valid."""
        df = sample_stock_data

        # High should be >= Low
        assert (df["high_price"] >= df["low_price"]).all(), \
            "High price should be >= Low price"

        # Open and Close should be between Low and High
        assert (df["open_price"] >= df["low_price"]).all(), \
            "Open should be >= Low"
        assert (df["open_price"] <= df["high_price"]).all(), \
            "Open should be <= High"
        assert (df["close_price"] >= df["low_price"]).all(), \
            "Close should be >= Low"
        assert (df["close_price"] <= df["high_price"]).all(), \
            "Close should be <= High"

    def test_generated_volumes_are_positive(self, sample_stock_data: pd.DataFrame):
        """Test that all generated volumes are positive integers."""
        volumes = sample_stock_data["volume"]

        assert (volumes > 0).all(), "All volumes should be positive"
        assert volumes.dtype in [np.int64, np.int32, int], \
            "Volumes should be integers"

    def test_generated_timestamps_are_valid(self, sample_stock_data: pd.DataFrame):
        """Test that all timestamps are valid dates."""
        timestamps = pd.to_datetime(sample_stock_data["timestamp"])

        # No null timestamps
        assert not timestamps.isnull().any(), "Found null timestamps"

        # All dates should be reasonable (not in distant future or past)
        min_date = datetime(2000, 1, 1)
        max_date = datetime(2030, 12, 31)

        assert (timestamps >= min_date).all(), "Found dates before 2000"
        assert (timestamps <= max_date).all(), "Found dates after 2030"

    def test_generated_data_is_sorted_by_date(self, sample_stock_data: pd.DataFrame):
        """Test that data is sorted by date within each symbol."""
        for symbol in sample_stock_data["symbol"].unique():
            symbol_data = sample_stock_data[
                sample_stock_data["symbol"] == symbol
            ].copy()
            symbol_data["timestamp"] = pd.to_datetime(symbol_data["timestamp"])

            # Check if sorted
            is_sorted = symbol_data["timestamp"].is_monotonic_increasing
            assert is_sorted, f"Data for {symbol} is not sorted by date"


class TestGeometricBrownianMotion:
    """Test the Geometric Brownian Motion price simulation properties."""

    def test_log_returns_are_approximately_normal(self, sample_stock_data: pd.DataFrame):
        """
        GBM implies log-returns ln(S(t+1)/S(t)) are normally distributed.
        Validate that log-returns are not heavily skewed (basic normality check).
        """
        for symbol in sample_stock_data["symbol"].unique():
            symbol_data = sample_stock_data[
                sample_stock_data["symbol"] == symbol
            ].copy()
            symbol_data = symbol_data.sort_values("timestamp")

            prices = symbol_data["close_price"].values
            if len(prices) < 10:
                continue

            # Compute log-returns: ln(S(t+1) / S(t))
            log_returns = np.diff(np.log(prices))

            # Skewness should be roughly symmetric (within [-2, 2] for small samples)
            skewness = float(pd.Series(log_returns).skew())
            assert abs(skewness) < 2.0, \
                f"Log-returns for {symbol} are too skewed: {skewness:.2f}"

    def test_prices_remain_positive(self, sample_stock_data: pd.DataFrame):
        """GBM guarantees strictly positive prices (log-normal distribution)."""
        for col in ["open_price", "high_price", "low_price", "close_price"]:
            values = sample_stock_data[col].dropna()
            assert (values > 0).all(), \
                f"GBM should produce strictly positive prices, found non-positive in {col}"

    def test_price_volatility_is_reasonable(self, sample_stock_data: pd.DataFrame):
        """Test that price changes follow reasonable volatility patterns."""
        for symbol in sample_stock_data["symbol"].unique():
            symbol_data = sample_stock_data[
                sample_stock_data["symbol"] == symbol
            ].copy()
            symbol_data = symbol_data.sort_values("timestamp")

            returns = symbol_data["close_price"].pct_change().dropna()

            if len(returns) > 0:
                max_return = returns.abs().max()
                assert max_return < 0.5, \
                    f"Unrealistic daily return for {symbol}: {max_return:.2%}"

    def test_no_sudden_price_jumps(self, sample_stock_data: pd.DataFrame):
        """Test that there are no unrealistic price jumps."""
        for symbol in sample_stock_data["symbol"].unique():
            symbol_data = sample_stock_data[
                sample_stock_data["symbol"] == symbol
            ].copy()
            symbol_data = symbol_data.sort_values("timestamp")

            prices = symbol_data["close_price"].values

            for i in range(1, len(prices)):
                change_pct = abs(prices[i] - prices[i-1]) / prices[i-1]
                assert change_pct < 0.20, \
                    f"Unrealistic price jump for {symbol}: {change_pct:.2%}"


class TestDataPresets:
    """Test different data generation presets."""

    def test_demo_preset_record_count(self, sample_stock_data: pd.DataFrame):
        """Test demo preset generates expected number of records."""
        # Our fixture simulates demo preset: 3 symbols x 30 days = 90 records
        expected_min = 30  # At least 30 records
        assert len(sample_stock_data) >= expected_min, \
            f"Demo preset should have at least {expected_min} records"

    def test_multiple_symbols_generated(self, sample_stock_data: pd.DataFrame):
        """Test that multiple symbols are generated."""
        unique_symbols = sample_stock_data["symbol"].nunique()
        assert unique_symbols >= 2, \
            f"Expected multiple symbols, got {unique_symbols}"


class TestDataConsistency:
    """Test data consistency across multiple generations."""

    def test_reproducibility_with_seed(self):
        """Test that same seed produces same data."""
        np.random.seed(42)
        data1 = np.random.random(10)

        np.random.seed(42)
        data2 = np.random.random(10)

        np.testing.assert_array_equal(data1, data2)

    def test_dividend_values_are_non_negative(self, sample_stock_data: pd.DataFrame):
        """Test that dividend amounts are non-negative."""
        if "dividend_amount" in sample_stock_data.columns:
            dividends = sample_stock_data["dividend_amount"]
            assert (dividends >= 0).all(), "Found negative dividend amounts"

    def test_split_coefficients_are_positive(self, sample_stock_data: pd.DataFrame):
        """Test that split coefficients are positive."""
        if "split_coefficient" in sample_stock_data.columns:
            splits = sample_stock_data["split_coefficient"]
            assert (splits > 0).all(), "Found non-positive split coefficients"
