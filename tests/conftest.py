"""
Pytest Configuration and Fixtures
=================================
Shared fixtures for testing the Financial Data Platform.
"""

import os
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

import pytest
import pandas as pd
import numpy as np

# Set test environment
os.environ["DEPLOYMENT_MODE"] = "local"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def sample_stock_data() -> pd.DataFrame:
    """Generate sample stock data for testing."""
    np.random.seed(42)

    symbols = ["AAPL", "GOOGL", "MSFT"]
    dates = pd.date_range(start="2024-01-01", periods=30, freq="D")

    records = []
    for symbol in symbols:
        price = {"AAPL": 150.0, "GOOGL": 140.0, "MSFT": 370.0}[symbol]

        for i, date in enumerate(dates):
            # GBM-style daily step: compound from previous close
            daily_return = np.random.normal(0.001, 0.02)
            price = price * (1 + daily_return)

            high = price * (1 + abs(np.random.normal(0, 0.01)))
            low = price * (1 - abs(np.random.normal(0, 0.01)))
            open_price = low + (high - low) * np.random.random()
            close_price = low + (high - low) * np.random.random()

            records.append({
                "symbol": symbol,
                "timestamp": date.strftime("%Y-%m-%d"),
                "open_price": round(open_price, 2),
                "high_price": round(high, 2),
                "low_price": round(low, 2),
                "close_price": round(close_price, 2),
                "adjusted_close": round(close_price, 2),
                "volume": int(np.random.uniform(1000000, 5000000)),
                "dividend_amount": 0.0,
                "split_coefficient": 1.0,
            })

    return pd.DataFrame(records)


@pytest.fixture
def sample_stock_data_with_nulls(sample_stock_data: pd.DataFrame) -> pd.DataFrame:
    """Generate sample data with null values for validation testing."""
    df = sample_stock_data.copy()

    # Introduce some null values
    df.loc[0, "close_price"] = None
    df.loc[5, "volume"] = None
    df.loc[10, "high_price"] = None

    return df


@pytest.fixture
def sample_stock_data_with_invalid_ohlc() -> pd.DataFrame:
    """Generate sample data with invalid OHLC relationships."""
    return pd.DataFrame([
        {
            "symbol": "TEST",
            "timestamp": "2024-01-01",
            "open_price": 100.0,
            "high_price": 90.0,  # Invalid: high < low
            "low_price": 95.0,
            "close_price": 92.0,
            "adjusted_close": 92.0,
            "volume": 1000000,
            "dividend_amount": 0.0,
            "split_coefficient": 1.0,
        }
    ])


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary directory structure for data files."""
    raw_dir = tmp_path / "raw" / "stock_data"
    curated_dir = tmp_path / "curated"

    raw_dir.mkdir(parents=True)
    curated_dir.mkdir(parents=True)

    return tmp_path


@pytest.fixture
def sample_json_file(temp_data_dir: Path, sample_stock_data: pd.DataFrame) -> Path:
    """Create a sample JSON file for testing."""
    date_dir = temp_data_dir / "raw" / "stock_data" / "date=2024-01-15"
    date_dir.mkdir(parents=True, exist_ok=True)

    json_file = date_dir / "AAPL" / "data.json"
    json_file.parent.mkdir(parents=True, exist_ok=True)

    # Filter to AAPL only and convert to JSON format
    aapl_data = sample_stock_data[sample_stock_data["symbol"] == "AAPL"]

    with open(json_file, "w") as f:
        json.dump({
            "symbol": "AAPL",
            "data": aapl_data.to_dict(orient="records")
        }, f, indent=2)

    return json_file


@pytest.fixture
def mock_aws_credentials():
    """Mock AWS credentials for testing."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    yield

    # Cleanup
    for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"]:
        os.environ.pop(key, None)


@pytest.fixture
def validation_config() -> Dict[str, Any]:
    """Return validation configuration for testing."""
    return {
        "required_columns": [
            "symbol", "timestamp", "open_price", "high_price",
            "low_price", "close_price", "volume"
        ],
        "null_check_columns": ["symbol", "timestamp", "close_price"],
        "value_range_checks": {
            "open_price": {"min": 0, "max": 1000000},
            "high_price": {"min": 0, "max": 1000000},
            "low_price": {"min": 0, "max": 1000000},
            "close_price": {"min": 0, "max": 1000000},
            "volume": {"min": 0, "max": None},
        }
    }
