"""
Transformation Logic Unit Tests
===============================
Tests for the data transformation logic (Python equivalent of Scala transforms).
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestTechnicalIndicators:
    """Test technical indicator calculations."""

    @pytest.fixture
    def price_series(self) -> pd.DataFrame:
        """Generate a simple price series for testing."""
        dates = pd.date_range(start="2024-01-01", periods=50, freq="D")
        np.random.seed(42)

        # Generate prices with a known pattern
        base_price = 100.0
        prices = [base_price]
        for _ in range(49):
            change = np.random.normal(0, 0.02) * prices[-1]
            prices.append(prices[-1] + change)

        return pd.DataFrame({
            "symbol": ["TEST"] * 50,
            "trade_date": dates,
            "close_price": prices,
            "volume": [1000000] * 50,
        })

    def test_simple_moving_average_5(self, price_series: pd.DataFrame):
        """Test 5-day SMA calculation."""
        df = price_series.copy()
        df["sma_5"] = df["close_price"].rolling(window=5).mean()

        # First 4 values should be NaN
        assert df["sma_5"].iloc[:4].isna().all()

        # 5th value should be average of first 5 prices
        expected = df["close_price"].iloc[:5].mean()
        actual = df["sma_5"].iloc[4]
        assert abs(actual - expected) < 0.01

    def test_simple_moving_average_20(self, price_series: pd.DataFrame):
        """Test 20-day SMA calculation."""
        df = price_series.copy()
        df["sma_20"] = df["close_price"].rolling(window=20).mean()

        # First 19 values should be NaN
        assert df["sma_20"].iloc[:19].isna().all()

        # 20th value should be average of first 20 prices
        expected = df["close_price"].iloc[:20].mean()
        actual = df["sma_20"].iloc[19]
        assert abs(actual - expected) < 0.01

    def test_exponential_moving_average_12(self, price_series: pd.DataFrame):
        """Test 12-day EMA calculation."""
        df = price_series.copy()
        df["ema_12"] = df["close_price"].ewm(span=12, adjust=False).mean()

        # EMA should exist for all rows (no NaN)
        assert not df["ema_12"].isna().any()

        # EMA should be close to price for recent values
        assert abs(df["ema_12"].iloc[-1] - df["close_price"].iloc[-1]) < \
            df["close_price"].iloc[-1] * 0.1  # Within 10%

    def test_daily_return_calculation(self, price_series: pd.DataFrame):
        """Test daily return percentage calculation."""
        df = price_series.copy()
        df["daily_return"] = df["close_price"].pct_change() * 100

        # First value should be NaN
        assert pd.isna(df["daily_return"].iloc[0])

        # Calculate expected return for second day
        expected = (df["close_price"].iloc[1] - df["close_price"].iloc[0]) / \
            df["close_price"].iloc[0] * 100
        actual = df["daily_return"].iloc[1]
        assert abs(actual - expected) < 0.001

    def test_volatility_20d_calculation(self, price_series: pd.DataFrame):
        """Test 20-day volatility (standard deviation of returns) calculation."""
        df = price_series.copy()
        df["daily_return"] = df["close_price"].pct_change() * 100
        df["volatility_20d"] = df["daily_return"].rolling(window=20).std()

        # First 20 values should be NaN
        assert df["volatility_20d"].iloc[:20].isna().all()

        # Volatility should be positive where defined
        valid_vol = df["volatility_20d"].dropna()
        assert (valid_vol >= 0).all()

    def test_daily_range_calculation(self, sample_stock_data: pd.DataFrame):
        """Test daily price range calculation."""
        df = sample_stock_data.copy()
        df["daily_range"] = df["high_price"] - df["low_price"]

        # Range should always be non-negative
        assert (df["daily_range"] >= 0).all()

    def test_volume_change_calculation(self, price_series: pd.DataFrame):
        """Test volume change percentage calculation."""
        df = price_series.copy()
        df["volume_change_pct"] = df["volume"].pct_change() * 100

        # First value should be NaN
        assert pd.isna(df["volume_change_pct"].iloc[0])


class TestDateDimensions:
    """Test date dimension extraction."""

    def test_year_extraction(self, sample_stock_data: pd.DataFrame):
        """Test year is correctly extracted from timestamp."""
        df = sample_stock_data.copy()
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["year"] = df["trade_date"].dt.year

        assert (df["year"] >= 2020).all()
        assert (df["year"] <= 2030).all()

    def test_quarter_extraction(self, sample_stock_data: pd.DataFrame):
        """Test quarter is correctly extracted."""
        df = sample_stock_data.copy()
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["quarter"] = df["trade_date"].dt.quarter

        assert df["quarter"].isin([1, 2, 3, 4]).all()

    def test_month_extraction(self, sample_stock_data: pd.DataFrame):
        """Test month is correctly extracted."""
        df = sample_stock_data.copy()
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["month"] = df["trade_date"].dt.month

        assert df["month"].between(1, 12).all()

    def test_day_of_week_extraction(self, sample_stock_data: pd.DataFrame):
        """Test day of week is correctly extracted."""
        df = sample_stock_data.copy()
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["day_of_week"] = df["trade_date"].dt.dayofweek + 1  # 1-7 format

        assert df["day_of_week"].between(1, 7).all()


class TestDataCleansing:
    """Test data cleansing operations."""

    def test_remove_null_symbols(self, sample_stock_data: pd.DataFrame):
        """Test removal of records with null symbols."""
        df = sample_stock_data.copy()
        df.loc[0, "symbol"] = None

        # Cleanse
        cleansed = df[df["symbol"].notna()]

        assert cleansed["symbol"].notna().all()
        assert len(cleansed) == len(df) - 1

    def test_uppercase_symbols(self, sample_stock_data: pd.DataFrame):
        """Test symbols are uppercased."""
        df = sample_stock_data.copy()
        df["symbol"] = df["symbol"].str.lower()

        # Apply transformation
        df["symbol"] = df["symbol"].str.upper().str.strip()

        assert df["symbol"].str.isupper().all()

    def test_handle_negative_prices(self):
        """Test negative prices are handled correctly."""
        df = pd.DataFrame({
            "symbol": ["TEST"],
            "close_price": [-100.0],  # Negative
        })

        # Replace negative with None
        df["close_price"] = df["close_price"].where(df["close_price"] >= 0, None)

        assert df["close_price"].isna().all()

    def test_fill_null_volumes(self):
        """Test null volumes are filled with 0."""
        df = pd.DataFrame({
            "symbol": ["TEST"],
            "volume": [None],
        })

        df["volume"] = df["volume"].fillna(0)

        assert df["volume"].iloc[0] == 0

    def test_remove_duplicates(self, sample_stock_data: pd.DataFrame):
        """Test duplicate removal by symbol and timestamp."""
        df = sample_stock_data.copy()

        # Add a duplicate
        duplicate_row = df.iloc[0:1].copy()
        df = pd.concat([df, duplicate_row], ignore_index=True)

        original_len = len(df)

        # Remove duplicates
        df = df.drop_duplicates(subset=["symbol", "timestamp"])

        assert len(df) == original_len - 1


class TestDataSkewHandling:
    """Test data skew detection and handling."""

    def test_detect_skewed_distribution(self, sample_stock_data: pd.DataFrame):
        """Test detection of skewed symbol distribution."""
        df = sample_stock_data.copy()

        # Add more records for one symbol to create skew
        extra_records = df[df["symbol"] == "AAPL"].copy()
        extra_records = pd.concat([extra_records] * 5, ignore_index=True)
        df = pd.concat([df, extra_records], ignore_index=True)

        # Analyze distribution
        symbol_counts = df.groupby("symbol").size()
        max_count = symbol_counts.max()
        min_count = symbol_counts.min()
        skew_ratio = max_count / min_count

        assert skew_ratio > 3.0, "Expected skewed distribution"

    def test_identify_hot_symbols(self, sample_stock_data: pd.DataFrame):
        """Test identification of high-volume symbols."""
        df = sample_stock_data.copy()

        # Add more records for AAPL
        extra_records = df[df["symbol"] == "AAPL"].copy()
        extra_records = pd.concat([extra_records] * 10, ignore_index=True)
        df = pd.concat([df, extra_records], ignore_index=True)

        symbol_counts = df.groupby("symbol").size()
        avg_count = symbol_counts.mean()

        hot_symbols = symbol_counts[symbol_counts > avg_count * 2].index.tolist()

        assert "AAPL" in hot_symbols, "AAPL should be identified as hot symbol"

    def test_salting_distributes_data(self):
        """Test that salting creates distributed partition keys."""
        np.random.seed(42)

        symbols = ["AAPL"] * 100  # All same symbol
        salt_range = 10

        # Apply salting
        salted_keys = [
            f"{s}_{np.random.randint(0, salt_range)}" for s in symbols
        ]

        unique_keys = len(set(salted_keys))

        # Should have multiple unique keys now
        assert unique_keys > 1, "Salting should create multiple partition keys"
        assert unique_keys <= salt_range, "Should not exceed salt range"


class TestPriceIdGeneration:
    """Test unique price ID generation."""

    def test_price_id_format(self, sample_stock_data: pd.DataFrame):
        """Test price_id is generated in correct format."""
        df = sample_stock_data.copy()
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["price_id"] = df["symbol"] + "_" + df["trade_date"].dt.strftime("%Y%m%d")

        # Check format: SYMBOL_YYYYMMDD
        sample_id = df["price_id"].iloc[0]
        parts = sample_id.split("_")

        assert len(parts) == 2
        assert parts[0].isupper()  # Symbol
        assert len(parts[1]) == 8  # YYYYMMDD

    def test_price_id_uniqueness(self, sample_stock_data: pd.DataFrame):
        """Test price_id is unique per symbol-date combination."""
        df = sample_stock_data.copy()
        df = df.drop_duplicates(subset=["symbol", "timestamp"])
        df["trade_date"] = pd.to_datetime(df["timestamp"])
        df["price_id"] = df["symbol"] + "_" + df["trade_date"].dt.strftime("%Y%m%d")

        assert df["price_id"].is_unique, "price_id should be unique"
